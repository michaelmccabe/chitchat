package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
)

// SpyRecorder abstracts recording incoming and published messages for spy inspection.
type SpyRecorder interface {
	Record(msg broker.Message)
}

// RuleStat is a read-only snapshot of a loaded rule and how often it has matched.
type RuleStat struct {
	Name               string `json:"name"`
	Queue              string `json:"queue"`
	RoutingKey         string `json:"routing_key,omitempty"`
	ResponseExchange   string `json:"response_exchange"`
	ResponseRoutingKey string `json:"response_routing_key"`
	DelayMS            int    `json:"delay_ms"`
	Matches            int64  `json:"matches"`
}

// ruleState pairs a rule with its runtime match counter.
type ruleState struct {
	mu      sync.Mutex
	rule    config.Rule
	matches int64
}

func (rs *ruleState) incMatches() {
	rs.mu.Lock()
	rs.matches++
	rs.mu.Unlock()
}

func (rs *ruleState) getMatches() int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.matches
}

func newRuleStates(rules []config.Rule) []*ruleState {
	states := make([]*ruleState, len(rules))
	for i, r := range rules {
		states[i] = &ruleState{rule: r}
	}
	return states
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

	mu         sync.Mutex // guards cfg, running, stopped, subscribed
	running    bool
	stopped    bool
	subscribed map[string]struct{}

	rulesMu sync.RWMutex // guards rules
	rules   []*ruleState
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
		subscribed:  make(map[string]struct{}),
		rules:       newRuleStates(cfg.Rules),
	}
}

// desiredQueues returns the sorted set of queues to consume: rule trigger queues plus spy-only queues.
func desiredQueues(cfg *config.Config) []string {
	set := make(map[string]struct{})
	for _, rule := range cfg.Rules {
		if rule.Trigger.Queue != "" {
			set[rule.Trigger.Queue] = struct{}{}
		}
	}
	for _, q := range cfg.Spy.Queues {
		if q != "" {
			set[q] = struct{}{}
		}
	}
	queues := make([]string, 0, len(set))
	for q := range set {
		queues = append(queues, q)
	}
	sort.Strings(queues)
	return queues
}

// Start declares broker topology, attaches consumers to trigger queues, and starts message processing.
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.stopped {
		return errors.New("engine has been stopped")
	}
	if e.running {
		return nil
	}

	// 1. Declare Topology if configured
	if err := e.driver.DeclareTopology(&e.cfg.Broker.Declarations); err != nil {
		return fmt.Errorf("failed to declare broker topology: %w", err)
	}

	// 2. Subscribe to trigger queues and spy-only queues
	queues := desiredQueues(e.cfg)
	for _, q := range queues {
		if err := e.driver.StartConsuming(q, e.msgChan); err != nil {
			return fmt.Errorf("failed to consume from queue %q: %w", q, err)
		}
		e.subscribed[q] = struct{}{}
	}

	// 3. Start Dispatcher Worker
	e.running = true
	e.wg.Add(1)
	go e.processLoop()

	e.logger.Info("engine started successfully",
		"subscribed_queues", len(queues),
		"spy_queues", len(e.cfg.Spy.Queues),
		"rules", len(e.cfg.Rules),
	)
	return nil
}

// Reload swaps the active rule set for the one in cfg without dropping the broker connection.
// New topology is declared and newly referenced queues are subscribed; queues that are no
// longer referenced stay subscribed (their traffic is still recorded by the spy).
// Broker connection settings are not changed by a reload.
func (e *Engine) Reload(cfg *config.Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.stopped || !e.running {
		return errors.New("engine is not running")
	}

	if err := e.driver.DeclareTopology(&cfg.Broker.Declarations); err != nil {
		return fmt.Errorf("failed to declare broker topology: %w", err)
	}
	for _, q := range desiredQueues(cfg) {
		if _, ok := e.subscribed[q]; ok {
			continue
		}
		if err := e.driver.StartConsuming(q, e.msgChan); err != nil {
			return fmt.Errorf("failed to consume from queue %q: %w", q, err)
		}
		e.subscribed[q] = struct{}{}
	}

	e.rulesMu.Lock()
	e.rules = newRuleStates(cfg.Rules)
	e.rulesMu.Unlock()
	e.cfg = cfg

	e.logger.Info("engine rules reloaded", "rules", len(cfg.Rules), "subscribed_queues", len(e.subscribed))
	return nil
}

// RuleStats returns a snapshot of loaded rules and their match counters.
func (e *Engine) RuleStats() []RuleStat {
	rules := e.snapshotRules()
	stats := make([]RuleStat, len(rules))
	for i, rs := range rules {
		stats[i] = RuleStat{
			Name:               rs.rule.Name,
			Queue:              rs.rule.Trigger.Queue,
			RoutingKey:         rs.rule.Trigger.RoutingKey,
			ResponseExchange:   rs.rule.MockResponse.Exchange,
			ResponseRoutingKey: rs.rule.MockResponse.RoutingKey,
			DelayMS:            rs.rule.MockResponse.DelayMS,
			Matches:            rs.getMatches(),
		}
	}
	return stats
}

func (e *Engine) snapshotRules() []*ruleState {
	e.rulesMu.RLock()
	defer e.rulesMu.RUnlock()
	return e.rules
}

func (e *Engine) processLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case msg := <-e.msgChan:
			e.handleMessage(msg)
		}
	}
}

func (e *Engine) handleMessage(msg broker.Message) {
	if msg.Direction == "" {
		msg.Direction = broker.DirectionReceived
	}

	// Record in Spy Buffer
	if e.spyRecorder != nil {
		e.spyRecorder.Record(msg)
	}

	// Loop guard: never react to messages chitchat itself published.
	if origin, ok := msg.Headers[config.OriginHeader]; ok && origin != nil {
		e.logger.Debug("skipping rule evaluation for chitchat-originated message",
			"queue", msg.Queue,
			"routing_key", msg.RoutingKey,
			"origin", fmt.Sprint(origin),
		)
		return
	}

	matched := false
	for _, rs := range e.snapshotRules() {
		if !MatchesRule(&rs.rule, msg) {
			continue
		}
		matched = true
		rs.incMatches()
		rule := rs.rule

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
			e.dispatch(r, payload)
		}(rule, renderedPayload)
	}

	if !matched {
		e.logger.Debug("no rule matched message", "queue", msg.Queue, "routing_key", msg.RoutingKey)
	}
}

// dispatch publishes a rendered mock response (after the optional delay) and records it in the spy.
func (e *Engine) dispatch(r config.Rule, payload []byte) {
	if r.MockResponse.DelayMS > 0 {
		select {
		case <-time.After(time.Duration(r.MockResponse.DelayMS) * time.Millisecond):
		case <-e.ctx.Done():
			return
		}
	}

	// Copy configured headers (never mutate the shared config map) and stamp the loop-guard header.
	headers := make(map[string]interface{}, len(r.MockResponse.Headers)+1)
	for k, v := range r.MockResponse.Headers {
		headers[k] = v
	}
	headers[config.OriginHeader] = r.Name

	if err := e.driver.Publish(r.MockResponse.Exchange, r.MockResponse.RoutingKey, payload, headers); err != nil {
		e.logger.Error("failed to publish mock response", "rule", r.Name, "error", err)
		return
	}

	if e.spyRecorder != nil {
		e.spyRecorder.Record(broker.Message{
			Direction:  broker.DirectionPublished,
			Exchange:   r.MockResponse.Exchange,
			RoutingKey: r.MockResponse.RoutingKey,
			Rule:       r.Name,
			Headers:    headers,
			Body:       payload,
		})
	}
}

// Stop gracefully shuts down the engine and its background workers.
// Workers are drained first (bounded by ctx) and the broker driver is closed exactly once afterwards.
// The message channel is intentionally never closed: drivers stop their consumer goroutines in Close,
// so there is no writer left that could panic on a closed channel.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return nil
	}
	e.stopped = true
	e.running = false
	e.mu.Unlock()

	e.cancel()

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = fmt.Errorf("timed out waiting for engine workers to stop: %w", ctx.Err())
	}

	closeErr := e.driver.Close()
	return errors.Join(waitErr, closeErr)
}
