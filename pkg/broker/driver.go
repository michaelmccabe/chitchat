package broker

import "chitchat/pkg/config"

// Message directions recorded by the spy.
const (
	// DirectionReceived marks a message consumed from a broker queue.
	DirectionReceived = "received"
	// DirectionPublished marks a mock response published by chitchat.
	DirectionPublished = "published"
)

// Message represents an intercepted or published message across the broker abstraction.
type Message struct {
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Headers    map[string]interface{} `json:"headers"`
	Body       []byte                 `json:"body"`

	// Direction is DirectionReceived or DirectionPublished. Drivers may leave it empty
	// (it is treated as "received" by the engine).
	Direction string `json:"direction,omitempty"`
	// Exchange is the destination exchange of a published message.
	Exchange string `json:"exchange,omitempty"`
	// Rule is the name of the rule that produced a published message.
	Rule string `json:"rule,omitempty"`
	// MessageID is the broker supplied message identifier, if any.
	MessageID string `json:"message_id,omitempty"`
}

// Driver defines the interface that all messaging broker adapters must implement.
//
// Implementations own any goroutines started by StartConsuming: Close must stop them
// and only return once they have exited, so callers can safely tear down the channels
// they handed to StartConsuming afterwards.
type Driver interface {
	// Connect establishes a connection to the message broker.
	Connect(url string) error

	// DeclareTopology idempotently creates exchanges, queues, and bindings.
	DeclareTopology(declarations *config.Declarations) error

	// StartConsuming attaches a consumer loop to the given queue and emits received messages to messageChan.
	StartConsuming(queue string, messageChan chan<- Message) error

	// Publish sends a message to the specified exchange and routing key.
	Publish(exchange, routingKey string, body []byte, headers map[string]interface{}) error

	// Close cleanly closes all open channels and broker connections and waits for
	// consumer goroutines to exit. It is safe to call more than once.
	Close() error
}
