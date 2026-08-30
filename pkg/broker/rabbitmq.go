package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"chitchat/pkg/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQDriver implements the Driver interface for AMQP 0-9-1 RabbitMQ servers.
type RabbitMQDriver struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	mu         sync.RWMutex
	closed     bool
	logger     *slog.Logger
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewRabbitMQDriver creates a new RabbitMQ broker driver.
func NewRabbitMQDriver() *RabbitMQDriver {
	ctx, cancel := context.WithCancel(context.Background())
	return &RabbitMQDriver{
		logger:     slog.Default(),
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Connect dials the RabbitMQ instance at the given URL and establishes a working channel.
func (r *RabbitMQDriver) Connect(amqpURL string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}

	sanitizedURL := SanitizeURL(amqpURL)
	r.logger.Info("connecting to rabbitmq", "url", sanitizedURL)

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to dial rabbitmq at %s: %w", sanitizedURL, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}

	r.conn = conn
	r.channel = ch
	r.closed = false

	r.logger.Info("connected to rabbitmq successfully", "url", sanitizedURL)
	return nil
}

// DeclareTopology declares exchanges, queues, and queue bindings on the RabbitMQ broker.
func (r *RabbitMQDriver) DeclareTopology(declarations *config.Declarations) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil || r.closed {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	if declarations == nil {
		return nil
	}

	// 1. Declare Exchanges
	for _, ex := range declarations.Exchanges {
		exType := ex.Type
		if exType == "" {
			exType = "direct"
		}
		var args amqp.Table
		if len(ex.Args) > 0 {
			args = amqp.Table(ex.Args)
		}

		err := r.channel.ExchangeDeclare(
			ex.Name,
			exType,
			ex.Durable,
			ex.AutoDelete,
			ex.Internal,
			ex.NoWait,
			args,
		)
		if err != nil {
			return fmt.Errorf("failed to declare exchange %q: %w", ex.Name, err)
		}
		r.logger.Info("declared exchange", "name", ex.Name, "type", exType, "durable", ex.Durable)
	}

	// 2. Declare Queues and Bindings
	for _, q := range declarations.Queues {
		var qArgs amqp.Table
		if len(q.Args) > 0 {
			qArgs = amqp.Table(q.Args)
		}

		_, err := r.channel.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			q.NoWait,
			qArgs,
		)
		if err != nil {
			return fmt.Errorf("failed to declare queue %q: %w", q.Name, err)
		}
		r.logger.Info("declared queue", "name", q.Name, "durable", q.Durable)

		// 3. Declare Bindings
		for _, b := range q.Bindings {
			var bArgs amqp.Table
			if len(b.Args) > 0 {
				bArgs = amqp.Table(b.Args)
			}
			err := r.channel.QueueBind(
				q.Name,
				b.RoutingKey,
				b.Exchange,
				b.NoWait,
				bArgs,
			)
			if err != nil {
				return fmt.Errorf("failed to bind queue %q to exchange %q with routing key %q: %w",
					q.Name, b.Exchange, b.RoutingKey, err)
			}
			r.logger.Info("bound queue", "queue", q.Name, "exchange", b.Exchange, "routing_key", b.RoutingKey)
		}
	}

	return nil
}

// StartConsuming attaches a consumer loop to the given queue and pipes deliveries to messageChan.
// StartConsuming attaches a consumer loop to the given queue.
// Note: The consumer goroutine reads r.ctx and writes to messageChan without holding the lock.
// This is safe because r.ctx is immutable after construction.
func (r *RabbitMQDriver) StartConsuming(queue string, messageChan chan<- Message) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.channel == nil || r.closed {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	deliveries, err := r.channel.Consume(
		queue,
		"",    // consumer tag (auto-generated)
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer for queue %q: %w", queue, err)
	}

	r.logger.Info("started consuming", "queue", queue)

	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					r.logger.Debug("consumer delivery channel closed", "queue", queue)
					return
				}

				headers := make(map[string]interface{}, len(d.Headers))
				for k, v := range d.Headers {
					headers[k] = v
				}

				msg := Message{
					Queue:      queue,
					RoutingKey: d.RoutingKey,
					Headers:    headers,
					Body:       d.Body,
				}

				r.logger.Debug("received message",
					"queue", queue,
					"routing_key", d.RoutingKey,
					"bytes", len(d.Body),
				)

				select {
				case messageChan <- msg:
				case <-r.ctx.Done():
					return
				}
			}
		}
	}()

	return nil
}

// Publish sends a message to the specified exchange and routing key.
func (r *RabbitMQDriver) Publish(exchange, routingKey string, body []byte, headers map[string]interface{}) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.channel == nil || r.closed {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	var amqpHeaders amqp.Table
	if len(headers) > 0 {
		amqpHeaders = amqp.Table(headers)
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	err := r.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     amqpHeaders,
			Timestamp:   time.Now().UTC(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to exchange %q routing_key %q: %w", exchange, routingKey, err)
	}

	r.logger.Info("published mock message",
		"exchange", exchange,
		"routing_key", routingKey,
		"bytes", len(body),
	)
	r.logger.Debug("published message payload",
		"exchange", exchange,
		"routing_key", routingKey,
		"payload", string(body),
	)

	return nil
}

// Close cleanly shuts down consumer loops, channels, and connection.
func (r *RabbitMQDriver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	var firstErr error
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			firstErr = err
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	r.logger.Info("closed rabbitmq connection")
	return firstErr
}

// SanitizeURL masks credentials in broker connection strings for safe logging.
func SanitizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[invalid url]"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "xxxxx")
	}
	return u.String()
}
