package config

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleYAML = `
broker:
  url: "amqp://guest:guest@localhost:5672/"
  declarations:
    exchanges:
      - name: "order.exchange"
        type: "topic"
        durable: true
    queues:
      - name: "order.created.trigger"
        durable: true
        bindings:
          - exchange: "order.exchange"
            routing_key: "order.event.created"

rules:
  - name: "mock-payment-processing-flow"
    trigger:
      queue: "order.created.trigger"
      jsonpath_rules:
        - filter: "order.status"
          equals: "pending"
        - filter: "payment.method"
          equals: "credit_card"
    mock_response:
      exchange: "order.exchange"
      routing_key: "payment.event.processed"
      delay_ms: 100
      payload:
        transaction_id: "tx_{{uuid}}"
        order_id: "{{trigger.order.id}}"
        amount: "{{trigger.payment.amount}}"
        status: "settled"
        processed_at: "{{system.utc_now}}"
`

func TestParseConfig_Valid(t *testing.T) {
	cfg, err := ParseConfig([]byte(sampleYAML))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Broker.URL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("unexpected broker url: %s", cfg.Broker.URL)
	}

	if len(cfg.Broker.Declarations.Exchanges) != 1 {
		t.Fatalf("expected 1 exchange, got %d", len(cfg.Broker.Declarations.Exchanges))
	}
	ex := cfg.Broker.Declarations.Exchanges[0]
	if ex.Name != "order.exchange" || ex.Type != "topic" || !ex.Durable {
		t.Errorf("unexpected exchange decl: %+v", ex)
	}

	if len(cfg.Broker.Declarations.Queues) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(cfg.Broker.Declarations.Queues))
	}
	q := cfg.Broker.Declarations.Queues[0]
	if q.Name != "order.created.trigger" || !q.Durable || len(q.Bindings) != 1 {
		t.Errorf("unexpected queue decl: %+v", q)
	}

	if len(cfg.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(cfg.Rules))
	}
	r := cfg.Rules[0]
	if r.Name != "mock-payment-processing-flow" {
		t.Errorf("unexpected rule name: %s", r.Name)
	}
	if r.Trigger.Queue != "order.created.trigger" {
		t.Errorf("unexpected trigger queue: %s", r.Trigger.Queue)
	}
	if len(r.Trigger.JSONPathRules) != 2 {
		t.Errorf("expected 2 jsonpath rules, got %d", len(r.Trigger.JSONPathRules))
	}
	if r.MockResponse.Exchange != "order.exchange" || r.MockResponse.RoutingKey != "payment.event.processed" {
		t.Errorf("unexpected mock response: %+v", r.MockResponse)
	}
	if r.MockResponse.DelayMS != 100 {
		t.Errorf("expected delay_ms 100, got %d", r.MockResponse.DelayMS)
	}

	// Verify defaults for Server and Logging
	if cfg.Server.Port != 8082 {
		t.Errorf("expected default port 8082, got %d", cfg.Server.Port)
	}
	if cfg.Server.RingBufferSize != 1000 {
		t.Errorf("expected default ring buffer size 1000, got %d", cfg.Server.RingBufferSize)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("expected default log level info, got %s", cfg.Logging.Level)
	}
}

func TestParseConfig_InvalidYAML(t *testing.T) {
	_, err := ParseConfig([]byte("invalid: yaml: ["))
	if err == nil {
		t.Fatalf("expected error for invalid YAML, got nil")
	}
}

func TestApplyOverrides(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyOverrides("amqp://custom:5672/", 9090, "debug")

	if cfg.Broker.URL != "amqp://custom:5672/" {
		t.Errorf("expected overridden broker url, got %s", cfg.Broker.URL)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("expected overridden port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected overridden log level debug, got %s", cfg.Logging.Level)
	}

	// Verify partial overrides don't clear non-zero values
	cfg.ApplyOverrides("", 0, "")
	if cfg.Broker.URL != "amqp://custom:5672/" || cfg.Server.Port != 9090 || cfg.Logging.Level != "debug" {
		t.Errorf("empty overrides altered settings: %+v", cfg)
	}
}

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test-rules.yaml")
	if err := os.WriteFile(filePath, []byte(sampleYAML), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadConfig(filePath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Rules[0].Name != "mock-payment-processing-flow" {
		t.Errorf("unexpected rule name: %s", cfg.Rules[0].Name)
	}

	_, err = LoadConfig(filepath.Join(tmpDir, "non-existent.yaml"))
	if err == nil {
		t.Errorf("expected error for non-existent file, got nil")
	}
}
