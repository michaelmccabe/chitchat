package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
)

func resetFlags(t *testing.T) {
	t.Helper()
	origConfigPath := configPath
	origPortFlag := portFlag
	origBrokerURL := brokerURL
	origAMQPURL := amqpURL
	origLogLevel := logLevel
	origLogFormat := logFormat
	origConnectTimeout := connectTimeout

	configPath = ""
	portFlag = 0
	brokerURL = ""
	amqpURL = ""
	logLevel = ""
	logFormat = ""
	connectTimeout = ""

	t.Cleanup(func() {
		configPath = origConfigPath
		portFlag = origPortFlag
		brokerURL = origBrokerURL
		amqpURL = origAMQPURL
		logLevel = origLogLevel
		logFormat = origLogFormat
		connectTimeout = origConnectTimeout
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

type flakyDriver struct {
	*broker.MockDriver
	failAttempts int32
	attempts     atomic.Int32
}

func (f *flakyDriver) Connect(url string) error {
	n := f.attempts.Add(1)
	if n <= f.failAttempts {
		return errors.New("connection refused")
	}
	return f.MockDriver.Connect(url)
}

func TestConnectBrokerWithRetry(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.DefaultConfig()
	cfg.Broker.URL = "amqp://user:pass@localhost:5672/"

	// Succeeds on 3rd attempt within timeout
	drv := &flakyDriver{MockDriver: broker.NewMockDriver(), failAttempts: 2}
	if err := connectBrokerWithRetry(context.Background(), drv, cfg, 2*time.Second, discard); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if got := drv.attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
	if drv.URL != cfg.Broker.URL {
		t.Errorf("expected unsanitized URL %q, got %q", cfg.Broker.URL, drv.URL)
	}

	// Fails immediately when timeout is 0
	drvImmediate := &flakyDriver{MockDriver: broker.NewMockDriver(), failAttempts: 5}
	if err := connectBrokerWithRetry(context.Background(), drvImmediate, cfg, 0, discard); err == nil {
		t.Fatal("expected error when timeout is 0")
	}
	if got := drvImmediate.attempts.Load(); got != 1 {
		t.Errorf("expected exactly 1 attempt with timeout=0, got %d", got)
	}
}

func TestResolveBrokerURLFlag(t *testing.T) {
	resetFlags(t)
	t.Setenv("CHITCHAT_BROKER_URL", "amqp://env:secret@envhost:5672/")

	if got := resolveBrokerURLFlag(); got != "amqp://env:secret@envhost:5672/" {
		t.Errorf("expected env fallback, got %q", got)
	}

	amqpURL = "amqp://legacy@host:5672/"
	if got := resolveBrokerURLFlag(); got != "amqp://legacy@host:5672/" {
		t.Errorf("expected deprecated --amqp-url to override env, got %q", got)
	}

	brokerURL = "amqp://neutral@host:5672/"
	if got := resolveBrokerURLFlag(); got != "amqp://neutral@host:5672/" {
		t.Errorf("expected --broker-url to take precedence, got %q", got)
	}
}
