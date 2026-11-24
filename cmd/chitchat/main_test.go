package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
)

func resetFlags(t *testing.T) {
	t.Helper()
	origConfigPath := configPath
	origPortFlag := portFlag
	origAMQPURL := amqpURL
	origLogLevel := logLevel
	origLogFormat := logFormat

	configPath = ""
	portFlag = 0
	amqpURL = ""
	logLevel = ""
	logFormat = ""

	t.Cleanup(func() {
		configPath = origConfigPath
		portFlag = origPortFlag
		amqpURL = origAMQPURL
		logLevel = origLogLevel
		logFormat = origLogFormat
	})
}

// TestConnectBroker_UsesUnsanitizedURL is a regression test for bug 1.1:
// connectBroker must pass the real credentials to driver.Connect rather than
// the password-masked URL returned by broker.SanitizeURL.
func TestConnectBroker_UsesUnsanitizedURL(t *testing.T) {
	const rawURL = "amqp://appuser:supersecret@broker.internal:5672/vhost"
	cfg := config.DefaultConfig()
	cfg.Broker.URL = rawURL

	drv := broker.NewMockDriver()
	if err := connectBroker(drv, cfg); err != nil {
		t.Fatalf("connectBroker failed: %v", err)
	}

	if !drv.Connected {
		t.Fatal("expected driver to be connected")
	}
	if drv.URL != rawURL {
		t.Fatalf("connectBroker dialed %q; want real unsanitized URL %q", drv.URL, rawURL)
	}
	if drv.URL == broker.SanitizeURL(rawURL) {
		t.Fatalf("connectBroker dialed sanitized URL %q instead of real credentials", drv.URL)
	}
}

func TestRunValidate_ValidConfig(t *testing.T) {
	resetFlags(t)

	cfgYAML := `
broker:
  url: "amqp://guest:secret@localhost:5672/"
  declarations:
    exchanges:
      - name: "orders"
        type: "topic"
        durable: true
    queues:
      - name: "orders.created"
        durable: true
        bindings:
          - exchange: "orders"
            routing_key: "order.created"
rules:
  - name: "ack-order"
    trigger:
      queue: "orders.created"
      jsonpath_rules:
        - filter: "order.status"
          equals: "pending"
    mock_response:
      exchange: "orders"
      routing_key: "order.acked"
      payload:
        id: "{{trigger.order.id}}"
        amount: "{{trigger.amount | json}}"
`
	path := filepath.Join(t.TempDir(), "valid.yaml")
	if err := os.WriteFile(path, []byte(cfgYAML), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	configPath = path

	var out, errOut bytes.Buffer
	if err := runValidate(&out, &errOut); err != nil {
		t.Fatalf("runValidate returned unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "is valid (1 rules, 0 spy queues, 0 warnings)") {
		t.Errorf("unexpected stdout: %q", out.String())
	}
	if errOut.Len() != 0 {
		t.Errorf("expected no warnings on stderr, got: %q", errOut.String())
	}
}

func TestRunValidate_InvalidTemplateAndConfig(t *testing.T) {
	resetFlags(t)

	cfgYAML := `
broker:
  url: "amqp://localhost:5672/"
rules:
  - name: "broken-template"
    trigger:
      queue: "orders.created"
    mock_response:
      exchange: "orders"
      routing_key: "order.acked"
      payload:
        id: "{{trigger.order.id"
`
	path := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(path, []byte(cfgYAML), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	configPath = path

	var out, errOut bytes.Buffer
	err := runValidate(&out, &errOut)
	if err == nil {
		t.Fatal("expected runValidate to fail on invalid template syntax, got nil")
	}
	if !strings.Contains(err.Error(), "broken-template") {
		t.Errorf("expected error to mention rule name, got: %v", err)
	}
	// Rule has no jsonpath_rules or routing_key so a warning should also be emitted to stderr.
	if !strings.Contains(errOut.String(), "warning:") {
		t.Errorf("expected warning on stderr, got: %q", errOut.String())
	}
}
