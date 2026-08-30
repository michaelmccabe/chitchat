package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
)

// SpyRecorder abstracts recording incoming messages for spy inspection.
type SpyRecorder interface {
	Record(msg broker.Message)
}

// Engine coordinates message consumption, rule evaluation, and mock response dispatching.
type Engine struct {
	cfg         *config.Config
	driver      broker.Driver
	spyRecorder SpyRecorder
	logger      *slog.Logger
	msgChan     chan broker.Message
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	running     bool
	mu          sync.Mutex
}

// NewEngine constructs a new mock engine instance.
func NewEngine(cfg *config.Config, driver broker.Driver, spyRecorder SpyRecorder) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		cfg:         cfg,
		driver:      driver,
		spyRecorder: spyRecorder,
		logger:      slog.Default(),
		msgChan:     make(chan broker.Message, 100),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start declares broker topology, attaches consumers to trigger queues, and starts message processing.
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	// 1. Declare Topology if configured
	if err := e.driver.DeclareTopology(&e.cfg.Broker.Declarations); err != nil {
		return fmt.Errorf("failed to declare broker topology: %w", err)
	}

	// 2. Determine unique queues to subscribe to from rules
	subscribedQueues := make(map[string]struct{})
	for _, rule := range e.cfg.Rules {
		q := rule.Trigger.Queue
		if q != "" {
			subscribedQueues[q] = struct{}{}
		}
	}

	for q := range subscribedQueues {
		if err := e.driver.StartConsuming(q, e.msgChan); err != nil {
			return fmt.Errorf("failed to consume from queue %q: %w", q, err)
		}
	}

	// 3. Start Dispatcher Worker
	e.running = true
	e.wg.Add(1)
	go e.processLoop()

	e.logger.Info("engine started successfully", "subscribed_queues", len(subscribedQueues), "rules", len(e.cfg.Rules))
	return nil
}

func (e *Engine) processLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case msg, ok := <-e.msgChan:
			if !ok {
				return
			}
			e.handleMessage(msg)
		}
	}
}

func (e *Engine) handleMessage(msg broker.Message) {
	// Record in Spy Buffer
	if e.spyRecorder != nil {
		e.spyRecorder.Record(msg)
	}

	for _, rule := range e.cfg.Rules {
		if MatchesRule(&rule, msg) {
			e.logger.Info("matched rule",
				"rule", rule.Name,
				"queue", msg.Queue,
				"routing_key", msg.RoutingKey,
				"delay_ms", rule.MockResponse.DelayMS,
			)

			renderedPayload, err := RenderPayload(rule.MockResponse.Payload, msg)
			if err != nil {
				e.logger.Error("failed to render payload template", "rule", rule.Name, "error", err)
				continue
			}

			// Asynchronously dispatch mock response with optional delay
			e.wg.Add(1)
			go func(r config.Rule, payload []byte) {
				defer e.wg.Done()
				if r.MockResponse.DelayMS > 0 {
					select {
					case <-time.After(time.Duration(r.MockResponse.DelayMS) * time.Millisecond):
					case <-e.ctx.Done():
						return
					}
				}

				if err := e.driver.Publish(r.MockResponse.Exchange, r.MockResponse.RoutingKey, payload, r.MockResponse.Headers); err != nil {
					e.logger.Error("failed to publish mock response", "rule", r.Name, "error", err)
				}
			}(rule, renderedPayload)
		}
	}
}

// Stop gracefully shuts down the engine and its background workers.
func (e *Engine) Stop() error {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = false
	e.mu.Unlock()

	e.cancel()
	close(e.msgChan)
	e.wg.Wait()

	return e.driver.Close()
}
