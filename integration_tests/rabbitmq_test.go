package integration_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"chitchat/pkg/engine"
	"chitchat/pkg/spy"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

var (
	sharedAMQPURL string
)

func getNextPort() int {
	return 0 // OS assigned port
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	rabbitContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3.11-management"),
	)
	if err != nil {
		log.Fatalf("failed to start rabbitmq testcontainer: %v", err)
	}

	url, err := rabbitContainer.AmqpURL(ctx)
	if err != nil {
		_ = rabbitContainer.Terminate(ctx)
		log.Fatalf("failed to get AMQP connection string: %v", err)
	}
	sharedAMQPURL = url

	code := m.Run()

	_ = rabbitContainer.Terminate(ctx)
	os.Exit(code)
}

// Helper to create AMQP channel for test setup & inspection
func createAMQPChannel(t *testing.T) (*amqp091.Connection, *amqp091.Channel) {
	conn, err := amqp091.Dial(sharedAMQPURL)
	if err != nil {
		t.Fatalf("failed to dial rabbitmq: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("failed to open channel: %v", err)
	}
	return conn, ch
}

// TestChitchatRabbitMQMocking tests the fundamental end-to-end trigger, JSONPath evaluation, and mock publishing.
func TestChitchatRabbitMQMocking(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	triggerQueue := "basic.order.created.trigger"
	resultQueue := "basic.payment.processed.result"

	_, _ = ch.QueueDeclare(triggerQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(resultQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(100)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	if err := spyServer.Start(); err != nil {
		t.Fatalf("failed to start spy server: %v", err)
	}
	defer spyServer.Stop(context.Background())

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
		},
		Rules: []config.Rule{
			{
				Name: "mock-payment-processing-flow",
				Trigger: config.Trigger{
					Queue: triggerQueue,
					JSONPathRules: []config.JSONPathRule{
						{Filter: "order.status", Equals: "pending"},
						{Filter: "payment.method", Equals: "credit_card"},
					},
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: resultQueue,
					DelayMS:    20,
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

	driver := broker.NewRabbitMQDriver()
	if err := driver.Connect(sharedAMQPURL); err != nil {
		t.Fatalf("failed to connect driver: %v", err)
	}
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	if err := eng.Start(); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}
	defer eng.Stop(context.Background())

	// Publish triggering message
	payload := map[string]interface{}{
		"order": map[string]interface{}{
			"id":     "ord_999",
			"status": "pending",
		},
		"payment": map[string]interface{}{
			"method": "credit_card",
			"amount": 250.00,
		},
	}
	body, _ := json.Marshal(payload)
	err := ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("failed to publish trigger: %v", err)
	}

	// Consume and assert mock response
	msgs, err := ch.Consume(resultQueue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume response: %v", err)
	}

	select {
	case msg := <-msgs:
		var response map[string]interface{}
		if err := json.Unmarshal(msg.Body, &response); err != nil {
			t.Fatalf("failed to unmarshal response payload: %v", err)
		}
		if response["order_id"] != "ord_999" {
			t.Errorf("expected order_id to be 'ord_999', got %v", response["order_id"])
		}
		if response["status"] != "settled" {
			t.Errorf("expected status to be 'settled', got %v", response["status"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for mock response")
	}

	// Verify spy REST endpoint
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/_chitchat/messages?queue=%s", spyServer.Port(), triggerQueue))
	if err != nil {
		t.Fatalf("failed to query spy endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	var captured []spy.CapturedMessage
	if err := json.NewDecoder(resp.Body).Decode(&captured); err != nil {
		t.Fatalf("failed to decode spy messages: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected 1 captured message, got %d", len(captured))
	}
}

// TestChitchatTopologyDeclaration verifies that chitchat declares exchanges, queues, and bindings properly on RabbitMQ.
func TestChitchatTopologyDeclaration(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := "events.topic.exchange"
	queueName := "events.topic.queue"
	routingKeyPattern := "order.*.completed"
	publishRoutingKey := "order.checkout.completed"
	responseQueue := "order.notification.queue"

	_, _ = ch.QueueDeclare(responseQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(50)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	_ = spyServer.Start()
	defer spyServer.Stop(context.Background())

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
			Declarations: config.Declarations{
				Exchanges: []config.ExchangeDeclaration{
					{
						Name:    exchangeName,
						Type:    "topic",
						Durable: true,
					},
				},
				Queues: []config.QueueDeclaration{
					{
						Name:    queueName,
						Durable: true,
						Bindings: []config.QueueBinding{
							{
								Exchange:   exchangeName,
								RoutingKey: routingKeyPattern,
							},
						},
					},
				},
			},
		},
		Rules: []config.Rule{
			{
				Name: "topic-event-handler",
				Trigger: config.Trigger{
					Queue: queueName,
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: responseQueue,
					Payload: map[string]interface{}{
						"event":       "notification_sent",
						"routing_key": "{{routing_key}}",
					},
				},
			},
		},
	}

	driver := broker.NewRabbitMQDriver()
	if err := driver.Connect(sharedAMQPURL); err != nil {
		t.Fatalf("failed to connect driver: %v", err)
	}
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	if err := eng.Start(); err != nil {
		t.Fatalf("failed to start engine with topology declarations: %v", err)
	}
	defer eng.Stop(context.Background())

	// Publish to topic exchange with routing key matching pattern
	err := ch.Publish(exchangeName, publishRoutingKey, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"order_id":"10101"}`),
	})
	if err != nil {
		t.Fatalf("failed to publish to topic exchange: %v", err)
	}

	// Consume and verify response triggered via declared topology
	msgs, err := ch.Consume(responseQueue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume response: %v", err)
	}

	select {
	case msg := <-msgs:
		var resp map[string]interface{}
		if err := json.Unmarshal(msg.Body, &resp); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if resp["event"] != "notification_sent" {
			t.Errorf("expected event 'notification_sent', got %v", resp["event"])
		}
		if resp["routing_key"] != publishRoutingKey {
			t.Errorf("expected routing_key '%s', got %v", publishRoutingKey, resp["routing_key"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for response from topic exchange trigger")
	}
}

// TestChitchatMultipleRulesAndFiltering verifies multi-rule routing, negative filtering, and spy buffer operations.
func TestChitchatMultipleRulesAndFiltering(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	triggerQueue := "multi.rule.trigger"
	shippedQueue := "multi.rule.shipped.result"
	cancelledQueue := "multi.rule.cancelled.result"

	_, _ = ch.QueueDeclare(triggerQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(shippedQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(cancelledQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(100)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	_ = spyServer.Start()
	defer spyServer.Stop(context.Background())

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
		},
		Rules: []config.Rule{
			{
				Name: "shipped-rule",
				Trigger: config.Trigger{
					Queue: triggerQueue,
					JSONPathRules: []config.JSONPathRule{
						{Filter: "action", Equals: "shipped"},
					},
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: shippedQueue,
					Payload: map[string]interface{}{
						"status": "shipment_confirmed",
					},
				},
			},
			{
				Name: "cancelled-rule",
				Trigger: config.Trigger{
					Queue: triggerQueue,
					JSONPathRules: []config.JSONPathRule{
						{Filter: "action", Equals: "cancelled"},
					},
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: cancelledQueue,
					Payload: map[string]interface{}{
						"status": "order_cancelled",
					},
				},
			},
		},
	}

	driver := broker.NewRabbitMQDriver()
	_ = driver.Connect(sharedAMQPURL)
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	_ = eng.Start()
	defer eng.Stop(context.Background())

	shippedMsgs, _ := ch.Consume(shippedQueue, "", true, false, false, false, nil)
	cancelledMsgs, _ := ch.Consume(cancelledQueue, "", true, false, false, false, nil)

	// Publish message 1: "shipped"
	_ = ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"action":"shipped","order_id":"1"}`),
	})

	// Publish message 2: "cancelled"
	_ = ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"action":"cancelled","order_id":"2"}`),
	})

	// Publish message 3: "unmatched" (should NOT trigger any mock response)
	_ = ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"action":"unknown_status","order_id":"3"}`),
	})

	// Assert Shipped response
	select {
	case msg := <-shippedMsgs:
		var resp map[string]interface{}
		_ = json.Unmarshal(msg.Body, &resp)
		if resp["status"] != "shipment_confirmed" {
			t.Errorf("expected shipment_confirmed, got %v", resp["status"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shipped mock response")
	}

	// Assert Cancelled response
	select {
	case msg := <-cancelledMsgs:
		var resp map[string]interface{}
		_ = json.Unmarshal(msg.Body, &resp)
		if resp["status"] != "order_cancelled" {
			t.Errorf("expected order_cancelled, got %v", resp["status"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled mock response")
	}

	// Assert spy buffer captured all 3 messages
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/_chitchat/messages", spyServer.Port()))
	if err != nil {
		t.Fatalf("failed to query spy: %v", err)
	}
	var msgsList []spy.CapturedMessage
	_ = json.NewDecoder(resp.Body).Decode(&msgsList)
	resp.Body.Close()

	if len(msgsList) != 3 {
		t.Errorf("expected 3 messages captured in spy buffer, got %d", len(msgsList))
	}

	// Assert REST /_chitchat/health endpoint
	healthResp, err := http.Get(fmt.Sprintf("http://localhost:%d/_chitchat/health", spyServer.Port()))
	if err != nil {
		t.Fatalf("failed to query health: %v", err)
	}
	var healthData map[string]interface{}
	_ = json.NewDecoder(healthResp.Body).Decode(&healthData)
	healthResp.Body.Close()
	if healthData["status"] != "ok" || healthData["messages"] != float64(3) {
		t.Errorf("unexpected health response: %v", healthData)
	}

	// Test POST /_chitchat/clear
	clearResp, err := http.Post(fmt.Sprintf("http://localhost:%d/_chitchat/clear", spyServer.Port()), "application/json", nil)
	if err != nil {
		t.Fatalf("failed to clear spy: %v", err)
	}
	clearResp.Body.Close()

	// Verify buffer is empty after clear
	emptyResp, _ := http.Get(fmt.Sprintf("http://localhost:%d/_chitchat/messages", spyServer.Port()))
	var clearedList []spy.CapturedMessage
	_ = json.NewDecoder(emptyResp.Body).Decode(&clearedList)
	emptyResp.Body.Close()
	if len(clearedList) != 0 {
		t.Errorf("expected 0 messages after clear, got %d", len(clearedList))
	}
}

// TestChitchatDynamicTemplateFunctions tests {{uuid}}, {{utc_now}}, {{random_int}}, trigger extraction, and custom headers.
func TestChitchatDynamicTemplateFunctions(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	triggerQueue := "template.func.trigger"
	resultQueue := "template.func.result"

	_, _ = ch.QueueDeclare(triggerQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(resultQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(50)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	_ = spyServer.Start()
	defer spyServer.Stop(context.Background())

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
		},
		Rules: []config.Rule{
			{
				Name: "template-functions-rule",
				Trigger: config.Trigger{
					Queue: triggerQueue,
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: resultQueue,
					Headers: map[string]interface{}{
						"X-Mock-Source": "chitchat-engine",
						"X-Test-Mode":   true,
					},
					Payload: map[string]interface{}{
						"generated_uuid": "{{uuid}}",
						"iso_timestamp":  "{{system.utc_now}}",
						"random_num":     "{{random_int 100 200}}",
						"customer_name":  "{{trigger.customer.name}}",
						"customer_plan":  "{{trigger.customer.plan}}",
						"amount":         "{{trigger.amount}}",
						"source_queue":   "{{queue}}",
					},
				},
			},
		},
	}

	driver := broker.NewRabbitMQDriver()
	_ = driver.Connect(sharedAMQPURL)
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	_ = eng.Start()
	defer eng.Stop(context.Background())

	responseChan, _ := ch.Consume(resultQueue, "", true, false, false, false, nil)

	// Publish complex nested payload
	triggerPayload := map[string]interface{}{
		"customer": map[string]interface{}{
			"name": "Alice Developer",
			"plan": "enterprise",
		},
		"amount": 49.99,
	}
	body, _ := json.Marshal(triggerPayload)
	_ = ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})

	select {
	case msg := <-responseChan:
		// Verify custom headers
		if msg.Headers["X-Mock-Source"] != "chitchat-engine" {
			t.Errorf("expected X-Mock-Source header 'chitchat-engine', got %v", msg.Headers["X-Mock-Source"])
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			t.Fatalf("failed to decode json response: %v", err)
		}

		// Verify UUID format
		parsedUUID, err := uuid.Parse(payload["generated_uuid"].(string))
		if err != nil || parsedUUID.Version() != 4 {
			t.Errorf("expected valid UUIDv4 in generated_uuid, got %v", payload["generated_uuid"])
		}

		// Verify RFC3339 timestamp
		tsStr, ok := payload["iso_timestamp"].(string)
		if !ok {
			t.Fatalf("expected iso_timestamp string, got %T", payload["iso_timestamp"])
		}
		if _, err := time.Parse(time.RFC3339, tsStr); err != nil {
			t.Errorf("expected valid RFC3339 timestamp, got %s (err: %v)", tsStr, err)
		}

		// Verify random integer in [100, 200]
		randStr, ok := payload["random_num"].(string)
		if !ok {
			t.Fatalf("expected random_num string, got %T", payload["random_num"])
		}
		randVal, err := strconv.Atoi(strings.TrimSpace(randStr))
		if err != nil || randVal < 100 || randVal > 200 {
			t.Errorf("expected random_num between 100 and 200, got %d (err: %v)", randVal, err)
		}

		// Verify extracted trigger fields
		if payload["customer_name"] != "Alice Developer" {
			t.Errorf("expected customer_name 'Alice Developer', got %v", payload["customer_name"])
		}
		if payload["customer_plan"] != "enterprise" {
			t.Errorf("expected customer_plan 'enterprise', got %v", payload["customer_plan"])
		}
		if payload["amount"] != "49.99" {
			t.Errorf("expected amount '49.99', got %v", payload["amount"])
		}
		if payload["source_queue"] != triggerQueue {
			t.Errorf("expected source_queue '%s', got %v", triggerQueue, payload["source_queue"])
		}

	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for dynamic template response")
	}
}

// TestChitchatDelayedResponse verifies that MockResponse delay_ms is respected before publishing.
func TestChitchatDelayedResponse(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	triggerQueue := "delay.test.trigger"
	resultQueue := "delay.test.result"

	_, _ = ch.QueueDeclare(triggerQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(resultQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(20)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	_ = spyServer.Start()
	defer spyServer.Stop(context.Background())

	delayDuration := 250 * time.Millisecond
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
		},
		Rules: []config.Rule{
			{
				Name: "delayed-response-rule",
				Trigger: config.Trigger{
					Queue: triggerQueue,
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: resultQueue,
					DelayMS:    250,
					Payload: map[string]interface{}{
						"status": "delayed_done",
					},
				},
			},
		},
	}

	driver := broker.NewRabbitMQDriver()
	_ = driver.Connect(sharedAMQPURL)
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	_ = eng.Start()
	defer eng.Stop(context.Background())

	responseChan, _ := ch.Consume(resultQueue, "", true, false, false, false, nil)

	startTime := time.Now()
	_ = ch.Publish("", triggerQueue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"ping": true}`),
	})

	select {
	case msg := <-responseChan:
		elapsed := time.Since(startTime)
		if elapsed < delayDuration-50*time.Millisecond {
			t.Errorf("expected response after at least %v, but arrived in %v", delayDuration, elapsed)
		}
		if !bytes.Contains(msg.Body, []byte("delayed_done")) {
			t.Errorf("unexpected body: %s", string(msg.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delayed response")
	}
}

// TestChitchatFanoutExchangeRouting verifies broadcasting over fanout exchanges with multiple consumers.
func TestChitchatFanoutExchangeRouting(t *testing.T) {
	conn, ch := createAMQPChannel(t)
	defer conn.Close()
	defer ch.Close()

	fanoutExchange := "broadcast.fanout.exchange"
	smsQueue := "broadcast.sms.queue"
	emailQueue := "broadcast.email.queue"
	smsResultQueue := "broadcast.sms.result"
	emailResultQueue := "broadcast.email.result"

	_, _ = ch.QueueDeclare(smsResultQueue, true, false, false, false, nil)
	_, _ = ch.QueueDeclare(emailResultQueue, true, false, false, false, nil)

	spyPort := getNextPort()
	ringBuffer := spy.NewRingBuffer(50)
	spyServer := spy.NewServer(spyPort, ringBuffer)
	_ = spyServer.Start()
	defer spyServer.Stop(context.Background())

	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: sharedAMQPURL,
			Declarations: config.Declarations{
				Exchanges: []config.ExchangeDeclaration{
					{
						Name:    fanoutExchange,
						Type:    "fanout",
						Durable: true,
					},
				},
				Queues: []config.QueueDeclaration{
					{
						Name:    smsQueue,
						Durable: true,
						Bindings: []config.QueueBinding{
							{
								Exchange: fanoutExchange,
							},
						},
					},
					{
						Name:    emailQueue,
						Durable: true,
						Bindings: []config.QueueBinding{
							{
								Exchange: fanoutExchange,
							},
						},
					},
				},
			},
		},
		Rules: []config.Rule{
			{
				Name: "sms-processor",
				Trigger: config.Trigger{
					Queue: smsQueue,
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: smsResultQueue,
					Payload: map[string]interface{}{
						"channel": "sms",
						"user_id": "{{trigger.user_id}}",
					},
				},
			},
			{
				Name: "email-processor",
				Trigger: config.Trigger{
					Queue: emailQueue,
				},
				MockResponse: config.MockResponse{
					Exchange:   "",
					RoutingKey: emailResultQueue,
					Payload: map[string]interface{}{
						"channel": "email",
						"user_id": "{{trigger.user_id}}",
					},
				},
			},
		},
	}

	driver := broker.NewRabbitMQDriver()
	_ = driver.Connect(sharedAMQPURL)
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	_ = eng.Start()
	defer eng.Stop(context.Background())

	smsChan, _ := ch.Consume(smsResultQueue, "", true, false, false, false, nil)
	emailChan, _ := ch.Consume(emailResultQueue, "", true, false, false, false, nil)

	// Publish single broadcast message to the fanout exchange
	err := ch.Publish(fanoutExchange, "", false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"user_id":"usr_4455"}`),
	})
	if err != nil {
		t.Fatalf("failed to publish to fanout exchange: %v", err)
	}

	// Verify both sms and email received mock responses
	var gotSMS, gotEmail bool
	timeout := time.After(5 * time.Second)

	for !gotSMS || !gotEmail {
		select {
		case msg := <-smsChan:
			var resp map[string]interface{}
			_ = json.Unmarshal(msg.Body, &resp)
			if resp["channel"] == "sms" && resp["user_id"] == "usr_4455" {
				gotSMS = true
			}
		case msg := <-emailChan:
			var resp map[string]interface{}
			_ = json.Unmarshal(msg.Body, &resp)
			if resp["channel"] == "email" && resp["user_id"] == "usr_4455" {
				gotEmail = true
			}
		case <-timeout:
			t.Fatalf("timed out waiting for fanout responses: sms=%v, email=%v", gotSMS, gotEmail)
		}
	}
}

