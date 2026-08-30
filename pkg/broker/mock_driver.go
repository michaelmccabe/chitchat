package broker

import (
	"fmt"
	"sync"

	"chitchat/pkg/config"
)

// PublishedMessage records an invocation of Publish.
type PublishedMessage struct {
	Exchange   string
	RoutingKey string
	Body       []byte
	Headers    map[string]interface{}
}

// MockDriver is an in-memory test driver implementing the Driver interface.
type MockDriver struct {
	mu           sync.Mutex
	Connected    bool
	URL          string
	Topology     *config.Declarations
	Consumers    map[string]chan<- Message
	Published    []PublishedMessage
	Closed       bool
	PublishHook  func(exchange, routingKey string, body []byte, headers map[string]interface{}) error
}

// NewMockDriver creates a new MockDriver instance.
func NewMockDriver() *MockDriver {
	return &MockDriver{
		Consumers: make(map[string]chan<- Message),
		Published: make([]PublishedMessage, 0),
	}
}

func (m *MockDriver) Connect(rawURL string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Connected = true
	m.URL = rawURL
	return nil
}

func (m *MockDriver) DeclareTopology(declarations *config.Declarations) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Topology = declarations
	return nil
}

func (m *MockDriver) StartConsuming(queue string, messageChan chan<- Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Consumers[queue] = messageChan
	return nil
}

func (m *MockDriver) Publish(exchange, routingKey string, body []byte, headers map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.PublishHook != nil {
		if err := m.PublishHook(exchange, routingKey, body, headers); err != nil {
			return err
		}
	}

	m.Published = append(m.Published, PublishedMessage{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Body:       body,
		Headers:    headers,
	})
	return nil
}

func (m *MockDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

// SimulateIncomingMessage pushes a message to a registered consumer queue.
func (m *MockDriver) SimulateIncomingMessage(queue, routingKey string, body []byte, headers map[string]interface{}) error {
	m.mu.Lock()
	ch, ok := m.Consumers[queue]
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("no consumer registered for queue %q", queue)
	}

	ch <- Message{
		Queue:      queue,
		RoutingKey: routingKey,
		Headers:    headers,
		Body:       body,
	}
	return nil
}

// GetPublished returns a copy of published messages.
func (m *MockDriver) GetPublished() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]PublishedMessage, len(m.Published))
	copy(copied, m.Published)
	return copied
}
