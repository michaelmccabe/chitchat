package engine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"github.com/tidwall/gjson"
)

type testSpyRecorder struct {
	mu       sync.Mutex
	recorded []broker.Message
}

func (s *testSpyRecorder) Record(msg broker.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, msg)
}

func TestMatchesRule(t *testing.T) {
	rule := &config.Rule{
		Name: "test-rule",
		Trigger: config.Trigger{
			Queue:      "test.queue",
			RoutingKey: "order.created",
			JSONPathRules: []config.JSONPathRule{
				{Filter: "order.status", Equals: "pending"},
				{Filter: "payment.amount", Equals: 150.50},
				{Filter: "user.active", Equals: true},
			},
		},
	}

	validBody := []byte(`{
		"order": {"id": "123", "status": "pending"},
		"payment": {"amount": 150.50},
		"user": {"active": true}
	}`)

	// Positive match
	msg := broker.Message{
		Queue:      "test.queue",
		RoutingKey: "order.created",
		Body:       validBody,
	}
	if !MatchesRule(rule, msg) {
		t.Errorf("expected MatchesRule to return true")
	}

	// Queue mismatch
	msgWrongQueue := msg
	msgWrongQueue.Queue = "other.queue"
	if MatchesRule(rule, msgWrongQueue) {
		t.Errorf("expected MatchesRule to return false on queue mismatch")
	}

	// Routing key mismatch
	msgWrongRK := msg
	msgWrongRK.RoutingKey = "other.rk"
	if MatchesRule(rule, msgWrongRK) {
		t.Errorf("expected MatchesRule to return false on routing key mismatch")
	}

	// Payload JSONPath mismatch
	invalidBody := []byte(`{
		"order": {"id": "123", "status": "shipped"},
		"payment": {"amount": 150.50},
		"user": {"active": true}
	}`)
	msgInvalidBody := msg
	msgInvalidBody.Body = invalidBody
	if MatchesRule(rule, msgInvalidBody) {
		t.Errorf("expected MatchesRule to return false on status mismatch")
	}
}

func TestRenderPayload(t *testing.T) {
	payloadDef := map[string]interface{}{
		"transaction_id": "tx_{{uuid}}",
		"order_id":       "{{trigger.order.id}}",
		"amount":         "{{trigger.payment.amount}}",
		"status":         "settled",
		"processed_at":   "{{system.utc_now}}",
		"random_code":    "{{random_int 100 999}}",
	}

	msg := broker.Message{
		Queue:      "order.created.trigger",
		RoutingKey: "order.event.created",
		Body: []byte(`{
			"order": {"id": "ord_12345"},
			"payment": {"amount": 150.50}
		}`),
	}

	renderedBytes, err := RenderPayload(payloadDef, msg)
	if err != nil {
		t.Fatalf("RenderPayload failed: %v", err)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(renderedBytes, &res); err != nil {
		t.Fatalf("failed to unmarshal rendered payload JSON: %v; raw: %s", err, string(renderedBytes))
	}

	if res["order_id"] != "ord_12345" {
		t.Errorf("expected order_id 'ord_12345', got %v", res["order_id"])
	}
	if res["status"] != "settled" {
		t.Errorf("expected status 'settled', got %v", res["status"])
	}
	if res["amount"] != "150.5" && res["amount"] != "150.50" {
		t.Errorf("expected amount '150.5', got %v", res["amount"])
	}

	txID, ok := res["transaction_id"].(string)
	if !ok || !strings.HasPrefix(txID, "tx_") || len(txID) < 10 {
		t.Errorf("invalid transaction_id generated: %v", res["transaction_id"])
	}

	processedAt, ok := res["processed_at"].(string)
	if !ok || len(processedAt) == 0 {
		t.Errorf("invalid processed_at generated: %v", res["processed_at"])
	}
}

func TestRenderPayload_EscapingAndTypedJSON(t *testing.T) {
	payloadDef := map[string]interface{}{
		"message":   "{{trigger.input}}",
		"count":     "{{trigger.count | json}}",
		"is_active": "{{json trigger.active}}",
	}

	// Input contains quotes, backslashes, and newlines that would break naive JSON concatenation
	msg := broker.Message{
		Body: []byte(`{
			"input": "Hello \"world\" \n test",
			"count": 42,
			"active": true
		}`),
	}

	renderedBytes, err := RenderPayload(payloadDef, msg)
	if err != nil {
		t.Fatalf("RenderPayload failed: %v", err)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(renderedBytes, &res); err != nil {
		t.Fatalf("rendered payload is not valid JSON: %v; raw: %s", err, string(renderedBytes))
	}

	if res["message"] != "Hello \"world\" \n test" {
		t.Errorf("expected properly escaped string, got %q", res["message"])
	}
	if count, ok := res["count"].(float64); !ok || count != 42 {
		t.Errorf("expected count to be typed number 42, got %v (%T)", res["count"], res["count"])
	}
	if active, ok := res["is_active"].(bool); !ok || !active {
		t.Errorf("expected is_active to be typed boolean true, got %v (%T)", res["is_active"], res["is_active"])
	}
}

func TestEngine_LoopGuard(t *testing.T) {
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: "amqp://localhost:5672",
		},
		Rules: []config.Rule{
			{
				Name: "echo-rule",
				Trigger: config.Trigger{
					Queue: "echo.queue",
				},
				MockResponse: config.MockResponse{
					RoutingKey: "echo.queue",
					Payload: map[string]interface{}{
						"echo": true,
					},
				},
			},
		},
	}

	mockDriver := broker.NewMockDriver()
	spy := &testSpyRecorder{}
	eng := NewEngine(cfg, mockDriver, spy)
	if err := eng.Start(); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}
	defer eng.Stop(context.Background())

	// Simulate incoming message with chitchat origin header (loop guard)
	headers := map[string]interface{}{
		config.OriginHeader: "echo-rule",
	}
	if err := mockDriver.SimulateIncomingMessage("echo.queue", "echo.queue", []byte(`{"ping":1}`), headers); err != nil {
		t.Fatalf("failed to simulate message: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Since it had the origin header, no mock response should have been published
	if len(mockDriver.GetPublished()) != 0 {
		t.Fatalf("expected 0 published messages due to loop guard, got %d", len(mockDriver.GetPublished()))
	}

	// But it SHOULD be recorded in the spy buffer
	spy.mu.Lock()
	count := len(spy.recorded)
	spy.mu.Unlock()
	if count != 1 {
		t.Errorf("expected spy to record 1 message even if ignored by rules, got %d", count)
	}
}

func TestEngine_EndToEnd(t *testing.T) {
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: "amqp://localhost:5672",
			Declarations: config.Declarations{
				Queues: []config.QueueDeclaration{
					{Name: "order.created.trigger"},
				},
			},
		},
		Rules: []config.Rule{
			{
				Name: "mock-payment-processing-flow",
				Trigger: config.Trigger{
					Queue: "order.created.trigger",
					JSONPathRules: []config.JSONPathRule{
						{Filter: "order.status", Equals: "pending"},
					},
				},
				MockResponse: config.MockResponse{
					Exchange:   "order.exchange",
					RoutingKey: "payment.event.processed",
					DelayMS:    20,
					Payload: map[string]interface{}{
						"transaction_id": "tx_{{uuid}}",
						"order_id":       "{{trigger.order.id}}",
						"status":         "settled",
					},
				},
			},
		},
	}

	mockDriver := broker.NewMockDriver()
	spy := &testSpyRecorder{}

	eng := NewEngine(cfg, mockDriver, spy)
	if err := eng.Start(); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}

	triggerBody := []byte(`{"order": {"id": "ord_999", "status": "pending"}}`)
	if err := mockDriver.SimulateIncomingMessage("order.created.trigger", "order.event.created", triggerBody, nil); err != nil {
		t.Fatalf("failed to simulate message: %v", err)
	}

	// Wait for async delayed mock dispatch
	time.Sleep(100 * time.Millisecond)

	published := mockDriver.GetPublished()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}

	pub := published[0]
	if pub.Exchange != "order.exchange" || pub.RoutingKey != "payment.event.processed" {
		t.Errorf("unexpected publication target: %+v", pub)
	}

	orderID := gjson.GetBytes(pub.Body, "order_id").String()
	if orderID != "ord_999" {
		t.Errorf("expected order_id 'ord_999', got %s", orderID)
	}

	status := gjson.GetBytes(pub.Body, "status").String()
	if status != "settled" {
		t.Errorf("expected status 'settled', got %s", status)
	}

	// Check spy recorded both received trigger and published mock response
	spy.mu.Lock()
	recCount := len(spy.recorded)
	recordedCopy := make([]broker.Message, len(spy.recorded))
	copy(recordedCopy, spy.recorded)
	spy.mu.Unlock()
	if recCount != 2 {
		t.Fatalf("expected 2 spy recorded messages (received + published), got %d", recCount)
	}
	if recordedCopy[0].Direction != broker.DirectionReceived {
		t.Errorf("expected first recorded message to have direction %q, got %q", broker.DirectionReceived, recordedCopy[0].Direction)
	}
	if recordedCopy[1].Direction != broker.DirectionPublished {
		t.Errorf("expected second recorded message to have direction %q, got %q", broker.DirectionPublished, recordedCopy[1].Direction)
	}

	if err := eng.Stop(context.Background()); err != nil {
		t.Fatalf("failed to stop engine: %v", err)
	}
}

type closeCountingDriver struct {
	*broker.MockDriver
	closeCalls atomic.Int32
}

func (d *closeCountingDriver) Close() error {
	d.closeCalls.Add(1)
	return d.MockDriver.Close()
}

// TestEngine_ShutdownRace is a regression test for bug 1.2: stopping the engine while
// producers are actively pushing messages and delayed responses are in flight must never
// panic (send on closed channel), deadlock, race, or close the broker driver more than once.
func TestEngine_ShutdownRace(t *testing.T) {
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			URL: "amqp://localhost:5672/",
		},
		Rules: []config.Rule{
			{
				Name: "immediate-rule",
				Trigger: config.Trigger{
					Queue: "race.queue",
				},
				MockResponse: config.MockResponse{
					Exchange:   "race.exchange",
					RoutingKey: "race.out",
					Payload: map[string]interface{}{
						"id": "{{trigger.id}}",
					},
				},
			},
			{
				Name: "delayed-rule",
				Trigger: config.Trigger{
					Queue: "race.queue",
				},
				MockResponse: config.MockResponse{
					Exchange:   "race.exchange",
					RoutingKey: "race.delayed",
					DelayMS:    15,
					Payload: map[string]interface{}{
						"id": "{{trigger.id}}",
					},
				},
			},
		},
	}

	const iterations = 25
	const producers = 4
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for i := 0; i < iterations; i++ {
		drv := &closeCountingDriver{MockDriver: broker.NewMockDriver()}
		spy := &testSpyRecorder{}
		eng := NewEngine(cfg, drv, spy)
		eng.logger = discardLogger
		if err := eng.Start(); err != nil {
			t.Fatalf("iteration %d: failed to start engine: %v", i, err)
		}

		var prodWG sync.WaitGroup
		prodWG.Add(producers)
		for p := 0; p < producers; p++ {
			go func() {
				defer prodWG.Done()
				body := []byte(`{"id":"123"}`)
				for {
					if err := drv.SimulateIncomingMessage("race.queue", "race.in", body, nil); err != nil {
						return
					}
				}
			}()
		}

		// Let messages flow briefly before triggering concurrent Stop calls.
		time.Sleep(2 * time.Millisecond)

		var stopWG sync.WaitGroup
		stopErrs := make([]error, 2)
		for s := 0; s < 2; s++ {
			stopWG.Add(1)
			go func(idx int) {
				defer stopWG.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				stopErrs[idx] = eng.Stop(ctx)
			}(s)
		}

		stopWG.Wait()
		prodWG.Wait()

		for idx, err := range stopErrs {
			if err != nil {
				t.Fatalf("iteration %d: Stop[%d] returned error: %v", i, idx, err)
			}
		}
		if got := drv.closeCalls.Load(); got != 1 {
			t.Fatalf("iteration %d: expected driver.Close() to be called exactly once, got %d", i, got)
		}
	}
}

func TestValidateTemplates(t *testing.T) {
	validCfg := &config.Config{
		Rules: []config.Rule{
			{
				Name: "structured",
				MockResponse: config.MockResponse{
					Payload: map[string]interface{}{
						"id":     "{{trigger.order.id}}",
						"amount": "{{trigger.amount | json}}",
						"nested": []interface{}{"{{uuid}}", "{{system.utc_now}}"},
					},
				},
			},
			{
				Name: "raw-string",
				MockResponse: config.MockResponse{
					Payload: `{"id": {{json trigger.id}}}`,
				},
			},
		},
	}
	if err := ValidateTemplates(validCfg); err != nil {
		t.Fatalf("expected valid templates to pass, got: %v", err)
	}

	invalidCfg := &config.Config{
		Rules: []config.Rule{
			{
				Name: "bad-leaf",
				MockResponse: config.MockResponse{
					Payload: map[string]interface{}{
						"broken": "{{trigger.id",
					},
				},
			},
			{
				Name: "bad-string",
				MockResponse: config.MockResponse{
					Payload: `{{unknownFunc trigger.id}}`,
				},
			},
		},
	}
	err := ValidateTemplates(invalidCfg)
	if err == nil {
		t.Fatal("expected error for invalid templates, got nil")
	}
	if !strings.Contains(err.Error(), "bad-leaf") || !strings.Contains(err.Error(), "bad-string") {
		t.Errorf("expected error to report both invalid rules, got: %v", err)
	}
}
