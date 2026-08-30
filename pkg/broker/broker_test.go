package broker

import (
	"testing"

	"chitchat/pkg/config"
)

func TestSanitizeURL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "amqp://guest:secret@localhost:5672/",
			expected: "amqp://guest:xxxxx@localhost:5672/",
		},
		{
			input:    "amqp://localhost:5672/",
			expected: "amqp://localhost:5672/",
		},
		{
			input:    "://bad-url",
			expected: "[invalid url]",
		},
	}

	for _, tc := range tests {
		got := sanitizeURL(tc.input)
		if got != tc.expected {
			t.Errorf("sanitizeURL(%q) = %q; expected %q", tc.input, got, tc.expected)
		}
	}
}

func TestRabbitMQDriver_UnconnectedErrors(t *testing.T) {
	drv := NewRabbitMQDriver()

	if err := drv.DeclareTopology(&config.Declarations{}); err == nil {
		t.Errorf("expected error declaring topology when unconnected, got nil")
	}

	msgChan := make(chan Message, 1)
	if err := drv.StartConsuming("test-queue", msgChan); err == nil {
		t.Errorf("expected error consuming when unconnected, got nil")
	}

	if err := drv.Publish("ex", "rk", []byte("payload"), nil); err == nil {
		t.Errorf("expected error publishing when unconnected, got nil")
	}

	if err := drv.Close(); err != nil {
		t.Errorf("unexpected error on Close: %v", err)
	}
}

func TestMockDriver(t *testing.T) {
	drv := NewMockDriver()
	if err := drv.Connect("amqp://localhost:5672"); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if !drv.Connected {
		t.Errorf("expected Connected to be true")
	}

	top := &config.Declarations{
		Queues: []config.QueueDeclaration{{Name: "q1"}},
	}
	if err := drv.DeclareTopology(top); err != nil {
		t.Fatalf("DeclareTopology failed: %v", err)
	}
	if drv.Topology != top {
		t.Errorf("expected topology to match")
	}

	msgChan := make(chan Message, 1)
	if err := drv.StartConsuming("q1", msgChan); err != nil {
		t.Fatalf("StartConsuming failed: %v", err)
	}

	payload := []byte(`{"hello":"world"}`)
	if err := drv.SimulateIncomingMessage("q1", "rk.test", payload, map[string]interface{}{"h": 1}); err != nil {
		t.Fatalf("SimulateIncomingMessage failed: %v", err)
	}

	msg := <-msgChan
	if msg.Queue != "q1" || msg.RoutingKey != "rk.test" || string(msg.Body) != string(payload) {
		t.Errorf("unexpected message: %+v", msg)
	}

	if err := drv.Publish("ex.out", "rk.out", []byte(`{"res":true}`), nil); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	pub := drv.GetPublished()
	if len(pub) != 1 || pub[0].Exchange != "ex.out" || pub[0].RoutingKey != "rk.out" {
		t.Errorf("unexpected published messages: %+v", pub)
	}

	if err := drv.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !drv.Closed {
		t.Errorf("expected Closed to be true")
	}
}
