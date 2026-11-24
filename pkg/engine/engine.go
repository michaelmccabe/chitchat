package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
)

// SpyRecorder abstracts recording incoming and published messages for spy inspection.
type SpyRecorder interface {
	Record(msg broker.Message)
}

// ConnectionReporter is an optional interface that broker drivers can implement so
// health/metrics endpoints can report live broker connectivity.
type ConnectionReporter interface {
	IsConnected() bool
}

// Metrics holds runtime observability counters for the engine.
type Metrics struct {
	MessagesReceived  int64 `json:"messages_received"`
	MessagesPublished int64 `json:"messages_published"`
	RulesMatched      int64 `json:"rules_matched"`
	Errors            int64 `json:"errors"`
}

// HealthInfo summarizes engine and broker health for /_chitchat/health and /_chitchat/metrics.
type HealthInfo struct {
	BrokerConnected  bool     `json:"broker_connected"`
	SubscribedQueues []string `json:"subscribed_queues"`
	Rules            int      `json:"rules"`
	Metrics          Metrics  `json:"metrics"`
}

// RuleStat is a read-only snapshot of a loaded rule and how often it has matched.
type RuleStat struct {
	Name               string `json:"name"`
	Queue              string `json:"queue"`
	RoutingKey         string `json:"routing_key,omitempty"`
	Priority           int    `json:"priority,omitempty"`
	StopOnMatch        bool   `json:"stop_on_match,omitempty"`
	Times              int    `json:"times,omitempty"`
	ResponseExchange   string `json:"response_exchange"`
	ResponseRoutingKey string `json:"response_routing_key"`
	DelayMS            int    `json:"delay_ms"`
	Matches            int64  `json:"matches"`
}

type compiledResponse struct {
	spec           config.MockResponse
	payload        *CompiledPayload
	routingKeyTmpl *compiledLeaf
	headerTmpls    map[string]*compiledLeaf
}

// ruleState pairs a rule with its pre-compiled responses and runtime match counter.
type ruleState struct {
	mu        sync.Mutex
	rule      config.Rule
	responses []compiledResponse
	matches   int64
}

// tryClaimMatch increments the match counter and returns true unless the rule has
// exhausted its configured Times limit.
func (rs *ruleState) tryClaimMatch() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.rule.Times > 0 && rs.matches >= int64(rs.rule.Times) {
		return false
	}
	rs.matches++
	return true
}

func (rs *ruleState) getMatches() int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.matches
}

func compileRuleStates(rules []config.Rule, counter *atomic.Int64) ([]*ruleState, error) {
	// Sort by priority descending while preserving YAML order on ties.
	ordered := make([]config.Rule, len(rules))
	copy(ordered, rules)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Priority > ordered[j].Priority
	})

	states := make([]*ruleState, 0, len(ordered))
	for _, r := range ordered {
		resps := r.Responses()
		compiled := make([]compiledResponse, 0, len(resps))
		for _, spec := range resps {
			cp, err := CompilePayload(spec.Payload, counter)
			if err != nil {
				return nil, fmt.Errorf("rule %q: %w", r.Name, err)
			}
			cr := compiledResponse{
				spec:    spec,
				payload: cp,
			}
			if strings.Contains(spec.RoutingKey, "{{") {
				leaf, err := compileLeaf(spec.RoutingKey, true, counter)
				if err != nil {
					return nil, fmt.Errorf("rule %q routing_key: %w", r.Name, err)
				}
				cr.routingKeyTmpl = leaf
			}
			for hk, hv := range spec.Headers {
				if s, ok := hv.(string); ok && strings.Contains(s, "{{") {
					leaf, err := compileLeaf(s, true, counter)
					if err != nil {
						return nil, fmt.Errorf("rule %q header %q: %w", r.Name, hk, err)
					}
					if cr.headerTmpls == nil {
						cr.headerTmpls = make(map[string]*compiledLeaf)
					}
					cr.headerTmpls[hk] = leaf
				}
			}
			compiled = append(compiled, cr)
		}
		states = append(states, &ruleState{
			rule:      r,
			responses: compiled,
		})
	}
	return states, nil
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

	tmplCounter       atomic.Int64
	messagesReceived  atomic.Int64
	messagesPublished atomic.Int64
	rulesMatched      atomic.Int64
	errorsTotal       atomic.Int64
}

// NewEngine constructs a new mock engine instance.
func NewEngine(cfg *config.Config, driver broker.Driver, spyRecorder SpyRecorder) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:         cfg,
		driver:      driver,
		spyRecorder: spyRecorder,
		logger:      slog.Default(),
		msgChan:     make(chan broker.Message, 100),
		ctx:         ctx,
		cancel:      cancel,
		subscribed:  make(map[string]struct{}),
	}
	// Best-effort compile here so RuleStats works even before Start; Start returns any compile error.
	if states, err := compileRuleStates(cfg.Rules, &e.tmplCounter); err == nil {
		e.rules = states
	}
	return e
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

// Start compiles rule templates, declares broker topology, attaches consumers to trigger queues,
// and starts message processing.
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.stopped {
		return errors.New("engine has been stopped")
	}
	if e.running {
		return nil
	}

	if err := e.cfg.Normalize(); err != nil {
		return fmt.Errorf("failed to normalize config: %w", err)
	}

	// 1. Pre-compile all rule payload/routing_key/header templates
	states, err := compileRuleStates(e.cfg.Rules, &e.tmplCounter)
	if err != nil {
		return fmt.Errorf("failed to compile rule templates: %w", err)
	}
	e.rulesMu.Lock()
	e.rules = states
	e.rulesMu.Unlock()

	// 2. Declare Topology if configured
	if err := e.driver.DeclareTopology(&e.cfg.Broker.Declarations); err != nil {
		return fmt.Errorf("failed to declare broker topology: %w", err)
	}

	// 3. Subscribe to trigger queues and spy-only queues
	queues := desiredQueues(e.cfg)
	for _, q := range queues {
		if err := e.driver.StartConsuming(q, e.msgChan); err != nil {
			return fmt.Errorf("failed to consume from queue %q: %w", q, err)
		}
		e.subscribed[q] = struct{}{}
	}

	// 4. Start Dispatcher Worker
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

	if err := cfg.Normalize(); err != nil {
		return fmt.Errorf("failed to normalize config: %w", err)
	}

	states, err := compileRuleStates(cfg.Rules, &e.tmplCounter)
	if err != nil {
		return fmt.Errorf("failed to compile rule templates: %w", err)
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
	e.rules = states
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
		firstResp := rs.rule.Responses()[0]
		stats[i] = RuleStat{
			Name:               rs.rule.Name,
			Queue:              rs.rule.Trigger.Queue,
			RoutingKey:         rs.rule.Trigger.RoutingKey,
			Priority:           rs.rule.Priority,
			StopOnMatch:        rs.rule.StopOnMatch,
			Times:              rs.rule.Times,
			ResponseExchange:   firstResp.Exchange,
			ResponseRoutingKey: firstResp.RoutingKey,
			DelayMS:            firstResp.DelayMS,
			Matches:            rs.getMatches(),
		}
	}
	return stats
}

// MetricsSnapshot returns current engine counters.
func (e *Engine) MetricsSnapshot() Metrics {
	return Metrics{
		MessagesReceived:  e.messagesReceived.Load(),
		MessagesPublished: e.messagesPublished.Load(),
		RulesMatched:      e.rulesMatched.Load(),
		Errors:            e.errorsTotal.Load(),
	}
}

// HealthSnapshot reports broker connectivity, subscribed queues, rule count, and counters.
func (e *Engine) HealthSnapshot() HealthInfo {
	e.mu.Lock()
	queues := make([]string, 0, len(e.subscribed))
	for q := range e.subscribed {
		queues = append(queues, q)
	}
	running := e.running && !e.stopped
	e.mu.Unlock()
	sort.Strings(queues)

	connected := running
	if cr, ok := e.driver.(ConnectionReporter); ok {
		connected = cr.IsConnected()
	}

	return HealthInfo{
		BrokerConnected:  connected,
		SubscribedQueues: queues,
		Rules:            len(e.snapshotRules()),
		Metrics:          e.MetricsSnapshot(),
	}
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
	e.messagesReceived.Add(1)

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
		if !rs.tryClaimMatch() {
			e.logger.Debug("rule match skipped because times limit reached",
				"rule", rs.rule.Name,
				"times", rs.rule.Times,
			)
			continue
		}

		matched = true
		e.rulesMatched.Add(1)
		rule := rs.rule

		e.logger.Info("matched rule",
			"rule", rule.Name,
			"queue", msg.Queue,
			"routing_key", msg.RoutingKey,
			"responses", len(rs.responses),
		)

		data := buildTemplateContext(msg)
		for _, cr := range rs.responses {
			if cr.spec.Drop {
				e.logger.Info("dropping mock response (fault injection)", "rule", rule.Name)
				continue
			}

			renderedPayload, err := cr.payload.Render(msg)
			if err != nil {
				e.errorsTotal.Add(1)
				e.logger.Error("failed to render payload template", "rule", rule.Name, "error", err)
				continue
			}

			routingKey := cr.spec.RoutingKey
			if cr.routingKeyTmpl != nil {
				rk, err := cr.routingKeyTmpl.execute(data, msg.Headers, &e.tmplCounter)
				if err != nil {
					e.errorsTotal.Add(1)
					e.logger.Error("failed to render routing_key template", "rule", rule.Name, "error", err)
					continue
				}
				routingKey = rk
			}

			headers := make(map[string]interface{}, len(cr.spec.Headers)+1)
			headerErr := false
			for hk, hv := range cr.spec.Headers {
				if leaf, ok := cr.headerTmpls[hk]; ok {
					renderedVal, err := leaf.execute(data, msg.Headers, &e.tmplCounter)
					if err != nil {
						e.errorsTotal.Add(1)
						e.logger.Error("failed to render header template", "rule", rule.Name, "header", hk, "error", err)
						headerErr = true
						break
					}
					headers[hk] = renderedVal
				} else {
					headers[hk] = hv
				}
			}
			if headerErr {
				continue
			}
			headers[config.OriginHeader] = rule.Name

			delay := computeDelay(cr.spec)
			copies := cr.spec.Duplicate
			if copies <= 0 {
				copies = 1
			}

			// Asynchronously dispatch mock response with optional delay
			e.wg.Add(1)
			go func(ruleName, exchange, rk string, payload []byte, hdrs map[string]interface{}, d time.Duration, count int) {
				defer e.wg.Done()
				e.dispatch(ruleName, exchange, rk, payload, hdrs, d, count)
			}(rule.Name, cr.spec.Exchange, routingKey, renderedPayload, headers, delay, copies)
		}

		if rule.StopOnMatch {
			break
		}
	}

	if !matched {
		e.logger.Warn("no rule matched message", "queue", msg.Queue, "routing_key", msg.RoutingKey)
	}
}

func computeDelay(spec config.MockResponse) time.Duration {
	if len(spec.DelayMSRange) == 2 {
		minMS, maxMS := spec.DelayMSRange[0], spec.DelayMSRange[1]
		if maxMS > minMS {
			return time.Duration(minMS+rand.N(maxMS-minMS+1)) * time.Millisecond
		}
		return time.Duration(minMS) * time.Millisecond
	}
	if spec.DelayMS > 0 {
		return time.Duration(spec.DelayMS) * time.Millisecond
	}
	return 0
}

// dispatch publishes a rendered mock response (after the optional delay) and records it in the spy.
func (e *Engine) dispatch(ruleName, exchange, routingKey string, payload []byte, headers map[string]interface{}, delay time.Duration, copies int) {
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-e.ctx.Done():
			return
		}
	}

	for i := 0; i < copies; i++ {
		if err := e.driver.Publish(exchange, routingKey, payload, headers); err != nil {
			e.errorsTotal.Add(1)
			e.logger.Error("failed to publish mock response", "rule", ruleName, "error", err)
			return
		}
		e.messagesPublished.Add(1)

		if e.spyRecorder != nil {
			e.spyRecorder.Record(broker.Message{
				Direction:  broker.DirectionPublished,
				Exchange:   exchange,
				RoutingKey: routingKey,
				Rule:       ruleName,
				Headers:    headers,
				Body:       payload,
			})
		}
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
