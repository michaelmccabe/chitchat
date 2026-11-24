package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"chitchat/pkg/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	initialReconnectBackoff = 500 * time.Millisecond
	maxReconnectBackoff     = 10 * time.Second
)

// RabbitMQDriver implements the Driver interface for AMQP 0-9-1 RabbitMQ servers.
// It maintains separate channels for consuming and publishing (with publisher confirms
// and mandatory return logging) and automatically reconnects on connection/channel loss.
type RabbitMQDriver struct {
	mu            sync.RWMutex
	publishMu     sync.Mutex // serializes publishes and confirms on publishCh
	amqpURL       string
	conn          *amqp.Connection
	consumeCh     *amqp.Channel
	publishCh     *amqp.Channel
	connected     atomic.Bool
	closed        bool
	declarations  *config.Declarations
	subscriptions map[string]chan<- Message
	logger        *slog.Logger
	ctx           context.Context
	cancelFunc    context.CancelFunc
	// wg tracks running consumer loops, return listeners, and reconnect monitors; Close waits on it.
	wg sync.WaitGroup
}

// NewRabbitMQDriver creates a new RabbitMQ broker driver.
func NewRabbitMQDriver() *RabbitMQDriver {
	ctx, cancel := context.WithCancel(context.Background())
	return &RabbitMQDriver{
		subscriptions: make(map[string]chan<- Message),
		logger:        slog.Default(),
		ctx:           ctx,
		cancelFunc:    cancel,
	}
}

// IsConnected reports whether the driver currently has an active broker connection.
func (r *RabbitMQDriver) IsConnected() bool {
	return r.connected.Load()
}

// Connect dials the RabbitMQ instance at the given URL and opens separate consume and publish channels.
func (r *RabbitMQDriver) Connect(amqpURL string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("rabbitmq driver is closed")
	}
	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}

	return r.openConnectionLocked(amqpURL)
}

// openConnectionLocked dials RabbitMQ, sets up consume and publish channels (with confirms
// and return notifications), and starts background return-logging and reconnect monitoring.
// Callers must hold r.mu.
func (r *RabbitMQDriver) openConnectionLocked(amqpURL string) error {
	sanitizedURL := SanitizeURL(amqpURL)
	r.logger.Info("connecting to rabbitmq", "url", sanitizedURL)

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to dial rabbitmq at %s: %w", sanitizedURL, err)
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open rabbitmq consume channel: %w", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		_ = consumeCh.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to open rabbitmq publish channel: %w", err)
	}

	if err := publishCh.Confirm(false); err != nil {
		_ = publishCh.Close()
		_ = consumeCh.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to enable publisher confirms on rabbitmq channel: %w", err)
	}

	returns := publishCh.NotifyReturn(make(chan amqp.Return, 32))
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	consumeClose := consumeCh.NotifyClose(make(chan *amqp.Error, 1))
	publishClose := publishCh.NotifyClose(make(chan *amqp.Error, 1))

	r.amqpURL = amqpURL
	r.conn = conn
	r.consumeCh = consumeCh
	r.publishCh = publishCh
	r.connected.Store(true)

	r.wg.Add(1)
	go r.watchReturns(returns)

	r.wg.Add(1)
	go r.monitorConnection(connClose, consumeClose, publishClose)

	r.logger.Info("connected to rabbitmq successfully", "url", sanitizedURL)
	return nil
}

// watchReturns logs any unroutable mock responses returned by the broker when mandatory=true.
func (r *RabbitMQDriver) watchReturns(returns <-chan amqp.Return) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ret, ok := <-returns:
			if !ok {
				return
			}
			r.logger.Warn("published mock message was unroutable (returned by broker)",
				"exchange", ret.Exchange,
				"routing_key", ret.RoutingKey,
				"reply_code", ret.ReplyCode,
				"reply_text", ret.ReplyText,
			)
		}
	}
}

// monitorConnection watches for unexpected connection or channel closures and reconnects with backoff.
func (r *RabbitMQDriver) monitorConnection(connClose, consumeClose, publishClose <-chan *amqp.Error) {
	defer r.wg.Done()

	var cause *amqp.Error
	select {
	case <-r.ctx.Done():
		return
	case cause = <-connClose:
	case cause = <-consumeClose:
	case cause = <-publishClose:
	}

	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed || r.ctx.Err() != nil {
		return
	}

	r.connected.Store(false)
	r.logger.Warn("rabbitmq connection or channel lost, starting reconnect loop", "error", cause)

	backoff := initialReconnectBackoff
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(backoff):
		}

		if err := r.reconnectOnce(); err != nil {
			r.logger.Warn("rabbitmq reconnect attempt failed", "error", err, "retry_in", backoff)
			backoff *= 2
			if backoff > maxReconnectBackoff {
				backoff = maxReconnectBackoff
			}
			continue
		}

		r.logger.Info("reconnected to rabbitmq and restored topology and consumers")
		return
	}
}

func (r *RabbitMQDriver) reconnectOnce() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed || r.ctx.Err() != nil {
		return nil
	}

	// Best-effort cleanup of stale handles before re-dialing.
	if r.publishCh != nil {
		_ = r.publishCh.Close()
		r.publishCh = nil
	}
	if r.consumeCh != nil {
		_ = r.consumeCh.Close()
		r.consumeCh = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}

	if err := r.openConnectionLocked(r.amqpURL); err != nil {
		return err
	}

	if r.declarations != nil {
		if err := r.declareTopologyLocked(r.declarations); err != nil {
			r.connected.Store(false)
			return fmt.Errorf("failed to re-declare topology on reconnect: %w", err)
		}
	}

	for queue, ch := range r.subscriptions {
		if err := r.startConsumerLocked(queue, ch); err != nil {
			r.connected.Store(false)
			return fmt.Errorf("failed to re-subscribe to queue %q on reconnect: %w", queue, err)
		}
	}

	return nil
}

// DeclareTopology declares exchanges, queues, and queue bindings on the RabbitMQ broker.
func (r *RabbitMQDriver) DeclareTopology(declarations *config.Declarations) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.consumeCh == nil || r.closed {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	r.declarations = declarations
	return r.declareTopologyLocked(declarations)
}

func (r *RabbitMQDriver) declareTopologyLocked(declarations *config.Declarations) error {
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

		err := r.consumeCh.ExchangeDeclare(
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

		_, err := r.consumeCh.QueueDeclare(
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
			err := r.consumeCh.QueueBind(
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
// The consumer goroutine is tracked by the driver and stopped (and waited for) in Close.
// Subscriptions are remembered so they are automatically restored on reconnect.
func (r *RabbitMQDriver) StartConsuming(queue string, messageChan chan<- Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.consumeCh == nil || r.closed {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	if err := r.startConsumerLocked(queue, messageChan); err != nil {
		return err
	}
	r.subscriptions[queue] = messageChan
	return nil
}

func (r *RabbitMQDriver) startConsumerLocked(queue string, messageChan chan<- Message) error {
	deliveries, err := r.consumeCh.Consume(
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

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
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
					Direction:  DirectionReceived,
					MessageID:  d.MessageId,
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

// Publish sends a message on the dedicated publish channel with mandatory=true and awaits
// the broker publisher confirmation.
func (r *RabbitMQDriver) Publish(exchange, routingKey string, body []byte, headers map[string]interface{}) error {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	r.mu.RLock()
	pubCh := r.publishCh
	closed := r.closed
	r.mu.RUnlock()

	if pubCh == nil || closed || !r.connected.Load() {
		return fmt.Errorf("rabbitmq driver is not connected")
	}

	var amqpHeaders amqp.Table
	if len(headers) > 0 {
		amqpHeaders = amqp.Table(headers)
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	confirm, err := pubCh.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,  // mandatory: trigger NotifyReturn if unroutable
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

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("failed waiting for publish confirmation on exchange %q routing_key %q: %w", exchange, routingKey, err)
	}
	if !acked {
		return fmt.Errorf("broker nacked published message to exchange %q routing_key %q", exchange, routingKey)
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

// Close cleanly shuts down consumer loops, return listeners, reconnect monitor, channels, and connection.
// It blocks until all background goroutines have exited, so after it returns nothing
// will write to a message channel previously passed to StartConsuming.
func (r *RabbitMQDriver) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.connected.Store(false)

	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	consumeCh := r.consumeCh
	publishCh := r.publishCh
	conn := r.conn
	r.consumeCh = nil
	r.publishCh = nil
	r.conn = nil
	r.mu.Unlock()

	var firstErr error
	if publishCh != nil {
		if err := publishCh.Close(); err != nil {
			firstErr = err
		}
	}
	if consumeCh != nil {
		if err := consumeCh.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Consumers, return watcher, and reconnect monitor exit on ctx cancellation / handle closure.
	r.wg.Wait()

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
