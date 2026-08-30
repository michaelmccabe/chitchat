package integration_tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"chitchat/pkg/engine"
	"chitchat/pkg/spy"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func TestChitchatRabbitMQMocking(t *testing.T) {
	ctx := context.Background()

	// 1. Start RabbitMQ Container
	rabbitContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3.11-management"),
	)
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %s", err)
	}
	defer rabbitContainer.Terminate(ctx)

	connectionString, err := rabbitContainer.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get AMQP connection string: %s", err)
	}

	// 2. Initialize Connection & Test Topology
	conn, err := amqp091.Dial(connectionString)
	if err != nil {
		t.Fatalf("failed to dial rabbitmq: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %s", err)
	}
	defer ch.Close()

	// Declare triggering queue and output validation queue
	_, err = ch.QueueDeclare("order.created.trigger", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %s", err)
	}
	_, err = ch.QueueDeclare("payment.processed.result", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %s", err)
	}

	// 3. Start chitchat Engine, Spy Server and Consumers
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: connectionString,
		},
		Rules: []config.Rule{
			{
				Name: "mock-payment-processing-flow",
				Trigger: config.Trigger{
					Queue: "order.created.trigger",
					JSONPathRules: []config.JSONPathRule{
						{Filter: "order.status", Equals: "pending"},
						{Filter: "payment.method", Equals: "credit_card"},
					},
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: "payment.processed.result",
					DelayMS:    50,
					Payload: map[string]interface{}{
						"transaction_id": "tx_{{uuid}}",
						"order_id":       "{{trigger.order.id}}",
						"amount":         "{{trigger.payment.amount}}",
						"status":         "settled",
						"processed_at":   "{{system.utc_now}}",
					},
				},
			},
		},
	}

	spyPort := 18082
	ringBuffer := spy.NewRingBuffer(100)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	if err := spyServer.Start(); err != nil {
		t.Fatalf("failed to start spy server: %s", err)
	}
	defer spyServer.Stop(context.Background())

	driver := broker.NewRabbitMQDriver()
	if err := driver.Connect(connectionString); err != nil {
		t.Fatalf("failed to connect chitchat driver: %s", err)
	}

	engineInstance := engine.NewEngine(cfg, driver, ringBuffer)
	if err := engineInstance.Start(); err != nil {
		t.Fatalf("failed to start engine: %s", err)
	}
	defer engineInstance.Stop()

	// 4. Publish Trigger Message
	payload := map[string]interface{}{
		"order": map[string]interface{}{
			"id":     "ord_12345",
			"status": "pending",
		},
		"payment": map[string]interface{}{
			"method": "credit_card",
			"amount": 150.50,
		},
	}
	body, _ := json.Marshal(payload)
	err = ch.Publish("", "order.created.trigger", false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("failed to publish trigger: %s", err)
	}

	// 5. Consume from Expected Response Queue with Timeout
	msgs, err := ch.Consume("payment.processed.result", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to register listener: %s", err)
	}

	select {
	case msg := <-msgs:
		var response map[string]interface{}
		if err := json.Unmarshal(msg.Body, &response); err != nil {
			t.Fatalf("failed to unmarshal response payload: %v", err)
		}
		if response["order_id"] != "ord_12345" {
			t.Errorf("expected order_id to be ord_12345, got %v", response["order_id"])
		}
		if response["status"] != "settled" {
			t.Errorf("expected status to be settled, got %v", response["status"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for chitchat to publish mock response")
	}

	// 6. Verify REST Spy Endpoint over HTTP
	resp, err := http.Get("http://localhost:18082/_chitchat/messages?queue=order.created.trigger")
	if err != nil {
		t.Fatalf("failed to query spy endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 from spy endpoint, got %d", resp.StatusCode)
	}

	var captured []spy.CapturedMessage
	if err := json.NewDecoder(resp.Body).Decode(&captured); err != nil {
		t.Fatalf("failed to decode spy messages: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected 1 captured message in spy server, got %d", len(captured))
	}
	if captured[0].Queue != "order.created.trigger" {
		t.Errorf("expected captured queue to be 'order.created.trigger', got %s", captured[0].Queue)
	}
}
