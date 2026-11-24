package config

import (
	"os"
	"path/filepath"
	"strings"
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
	cfg.ApplyOverrides(Overrides{
		BrokerURL:      "amqp://custom:5672/",
		Port:           9090,
		LogLevel:       "debug",
		LogFormat:      "json",
		ConnectTimeout: "15s",
	})

	if cfg.Broker.URL != "amqp://custom:5672/" {
		t.Errorf("expected overridden broker url, got %s", cfg.Broker.URL)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("expected overridden port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected overridden log level debug, got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected overridden log format json, got %s", cfg.Logging.Format)
	}
	if cfg.Broker.ConnectTimeout != "15s" {
		t.Errorf("expected overridden connect_timeout 15s, got %s", cfg.Broker.ConnectTimeout)
	}

	// Verify empty overrides don't clear existing values
	cfg.ApplyOverrides(Overrides{})
	if cfg.Broker.URL != "amqp://custom:5672/" || cfg.Server.Port != 9090 || cfg.Logging.Level != "debug" || cfg.Logging.Format != "json" || cfg.Broker.ConnectTimeout != "15s" {
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

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(c *Config)
		wantError bool
	}{
		{
			name: "valid config",
			modify: func(c *Config) {
				c.Broker.URL = "amqp://localhost:5672/"
				c.Rules = []Rule{
					{
						Name: "r1",
						Trigger: Trigger{
							Queue: "q1",
						},
						MockResponse: MockResponse{
							Exchange:   "ex1",
							RoutingKey: "rk1",
						},
					},
				}
			},
			wantError: false,
		},
		{
			name: "invalid broker url scheme",
			modify: func(c *Config) {
				c.Broker.URL = "http://localhost:5672/"
			},
			wantError: true,
		},
		{
			name: "duplicate rule names",
			modify: func(c *Config) {
				c.Rules = []Rule{
					{Name: "dup", Trigger: Trigger{Queue: "q1"}, MockResponse: MockResponse{RoutingKey: "k1"}},
					{Name: "dup", Trigger: Trigger{Queue: "q2"}, MockResponse: MockResponse{RoutingKey: "k2"}},
				}
			},
			wantError: true,
		},
		{
			name: "negative delay",
			modify: func(c *Config) {
				c.Rules = []Rule{
					{Name: "r1", Trigger: Trigger{Queue: "q1"}, MockResponse: MockResponse{RoutingKey: "k1", DelayMS: -5}},
				}
			},
			wantError: true,
		},
		{
			name: "invalid server port",
			modify: func(c *Config) {
				c.Server.Port = 70000
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestConfig_WarningsAndLoopGuard(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Broker.Declarations.Queues = []QueueDeclaration{
		{
			Name: "order.created",
			Bindings: []QueueBinding{
				{Exchange: "order.exchange", RoutingKey: "order.#"},
			},
		},
	}
	cfg.Rules = []Rule{
		{
			Name: "order-flow",
			Trigger: Trigger{
				Queue: "order.created",
			},
			MockResponse: MockResponse{
				Exchange:   "order.exchange",
				RoutingKey: "order.item.created",
			},
		},
	}

	warns := cfg.Warnings()
	foundLoop := false
	for _, w := range warns {
		if strings.Contains(w, "loop guard") {
			foundLoop = true
			break
		}
	}
	if !foundLoop {
		t.Errorf("expected warning about potential loop, got warnings: %v", warns)
	}
}

func TestConfig_UnknownFields(t *testing.T) {
	yamlWithTypos := `
broker:
  url: "amqp://localhost:5672/"
rules:
  - name: "test"
    trigger:
      queue: "q1"
    mock_response:
      routing_key: "rk1"
      delay_msec: 100 # Typo! Should be delay_ms
`
	_, err := ParseConfig([]byte(yamlWithTypos))
	if err == nil {
		t.Fatalf("expected error on unknown YAML field 'delay_msec', got nil")
	}
}

func TestConfig_NeutralAliases(t *testing.T) {
	neutralYAML := `
broker:
  url: "amqp://localhost:5672/"
  connect_timeout: "10s"
spy:
  sources:
    - "audit.events"
  topics:
    - "metrics.events"
rules:
  - name: "neutral-rule"
    priority: 10
    stop_on_match: true
    times: 3
    trigger:
      source: "orders.in"
      key: "order.*"
      header_rules:
        - name: "x-tenant"
          equals: "acme"
      jsonpath_rules:
        - filter: "order.total"
          gt: 10.5
    mock_response:
      destination: "orders.exchange"
      key: "order.processed"
      delay_ms_range: [10, 50]
      duplicate: 2
      payload:
        ok: true
`
	cfg, err := ParseConfig([]byte(neutralYAML))
	if err != nil {
		t.Fatalf("ParseConfig failed for neutral aliases: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate failed for neutral aliases: %v", err)
	}

	if len(cfg.Spy.Queues) != 2 || cfg.Spy.Queues[0] != "audit.events" || cfg.Spy.Queues[1] != "metrics.events" {
		t.Errorf("expected spy queues to merge sources & topics, got %v", cfg.Spy.Queues)
	}
	r := cfg.Rules[0]
	if r.Trigger.Queue != "orders.in" || r.Trigger.RoutingKey != "order.*" {
		t.Errorf("unexpected normalized trigger: %+v", r.Trigger)
	}
	if r.MockResponse.Exchange != "orders.exchange" || r.MockResponse.RoutingKey != "order.processed" {
		t.Errorf("unexpected normalized mock_response: %+v", r.MockResponse)
	}
}

func TestConfig_ConflictingAliases(t *testing.T) {
	conflictYAML := `
broker:
  url: "amqp://localhost:5672/"
rules:
  - name: "conflict"
    trigger:
      queue: "q1"
      source: "q2"
    mock_response:
      exchange: "ex1"
      destination: "ex2"
      routing_key: "rk1"
`
	_, err := ParseConfig([]byte(conflictYAML))
	if err == nil {
		t.Fatal("expected error for conflicting aliases, got nil")
	}
	if !strings.Contains(err.Error(), "conflicting aliases") {
		t.Errorf("expected conflicting aliases error, got: %v", err)
	}
}

func TestConfig_ValidateNewFields(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(c *Config)
		wantError bool
	}{
		{
			name: "invalid connect_timeout",
			modify: func(c *Config) {
				c.Broker.ConnectTimeout = "not-a-duration"
			},
			wantError: true,
		},
		{
			name: "invalid delay_ms_range order",
			modify: func(c *Config) {
				c.Rules = []Rule{{
					Name:         "r1",
					Trigger:      Trigger{Queue: "q1"},
					MockResponse: MockResponse{RoutingKey: "rk1", DelayMSRange: []int{100, 50}},
				}}
			},
			wantError: true,
		},
		{
			name: "invalid regex in jsonpath_rules.matches",
			modify: func(c *Config) {
				c.Rules = []Rule{{
					Name: "r1",
					Trigger: Trigger{
						Queue:         "q1",
						JSONPathRules: []JSONPathRule{{Filter: "a", Matches: "[unclosed"}},
					},
					MockResponse: MockResponse{RoutingKey: "rk1"},
				}}
			},
			wantError: true,
		},
		{
			name: "drop rule does not require exchange or routing_key",
			modify: func(c *Config) {
				c.Rules = []Rule{{
					Name:         "drop-rule",
					Trigger:      Trigger{Queue: "q1"},
					MockResponse: MockResponse{Drop: true},
				}}
			},
			wantError: false,
		},
		{
			name: "both mock_response and mock_responses set",
			modify: func(c *Config) {
				c.Rules = []Rule{{
					Name:          "both",
					Trigger:       Trigger{Queue: "q1"},
					MockResponse:  MockResponse{RoutingKey: "rk1"},
					MockResponses: []MockResponse{{RoutingKey: "rk2"}},
				}}
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}
