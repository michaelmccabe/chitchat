package broker

import "chitchat/pkg/config"

// Message represents an intercepted or published message across the broker abstraction.
type Message struct {
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Headers    map[string]interface{} `json:"headers"`
	Body       []byte                 `json:"body"`
}

// Driver defines the interface that all messaging broker adapters must implement.
type Driver interface {
	// Connect establishes a connection to the message broker.
	Connect(url string) error

	// DeclareTopology idempotently creates exchanges, queues, and bindings.
	DeclareTopology(declarations *config.Declarations) error

	// StartConsuming attaches a consumer loop to the given queue and emits received messages to messageChan.
	StartConsuming(queue string, messageChan chan<- Message) error

	// Publish sends a message to the specified exchange and routing key.
	Publish(exchange, routingKey string, body []byte, headers map[string]interface{}) error

	// Close cleanly closes all open channels and broker connections.
	Close() error
}
