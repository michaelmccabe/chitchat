package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ValidationError aggregates every problem found in a configuration so users can fix
// them all in one pass instead of one error at a time.
type ValidationError struct {
	Problems []string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "invalid configuration (%d problem", len(e.Problems))
	if len(e.Problems) != 1 {
		sb.WriteString("s")
	}
	sb.WriteString("):")
	for _, p := range e.Problems {
		sb.WriteString("\n  - ")
		sb.WriteString(p)
	}
	return sb.String()
}

var (
	validExchangeTypes = map[string]bool{"": true, "direct": true, "topic": true, "fanout": true, "headers": true}
	validLogLevels     = map[string]bool{"debug": true, "info": true, "warn": true, "warning": true, "error": true}
	validLogFormats    = map[string]bool{"text": true, "json": true}
)

func ruleLabel(i int, r Rule) string {
	if r.Name == "" {
		return fmt.Sprintf("rules[%d]", i)
	}
	return fmt.Sprintf("rules[%d] (%q)", i, r.Name)
}

// resolveAlias resolves a canonical field against neutral/broker-specific aliases.
// If multiple non-empty fields are set with different values, it returns an error.
func resolveAlias(ctx string, entries ...struct {
	name string
	val  string
}) (string, error) {
	var chosenName, chosenVal string
	for _, e := range entries {
		if e.val == "" {
			continue
		}
		if chosenVal == "" {
			chosenName = e.name
			chosenVal = e.val
		} else if e.val != chosenVal {
			return "", fmt.Errorf("%s: conflicting aliases %s=%q and %s=%q", ctx, chosenName, chosenVal, e.name, e.val)
		}
	}
	return chosenVal, nil
}

// Normalize reconciles broker-neutral aliases (source/topic -> queue, destination/topic -> exchange,
// key -> routing_key, spy.sources/spy.topics -> spy.queues) into canonical fields.
// Conflicting aliases return a *ValidationError.
func (c *Config) Normalize() error {
	var problems []string

	// Merge spy.sources and spy.topics into spy.queues (preserving order and deduplicating).
	if len(c.Spy.Sources) > 0 || len(c.Spy.Topics) > 0 {
		combined := make([]string, 0, len(c.Spy.Queues)+len(c.Spy.Sources)+len(c.Spy.Topics))
		combined = append(combined, c.Spy.Queues...)
		combined = append(combined, c.Spy.Sources...)
		combined = append(combined, c.Spy.Topics...)
		c.Spy.Queues = dedupe(combined)
		c.Spy.Sources = nil
		c.Spy.Topics = nil
	}

	for i := range c.Rules {
		r := &c.Rules[i]
		label := ruleLabel(i, *r)

		q, err := resolveAlias(label+".trigger",
			struct{ name, val string }{"queue", r.Trigger.Queue},
			struct{ name, val string }{"source", r.Trigger.Source},
			struct{ name, val string }{"topic", r.Trigger.Topic},
		)
		if err != nil {
			problems = append(problems, err.Error())
		} else {
			r.Trigger.Queue = q
		}

		rk, err := resolveAlias(label+".trigger",
			struct{ name, val string }{"routing_key", r.Trigger.RoutingKey},
			struct{ name, val string }{"key", r.Trigger.Key},
		)
		if err != nil {
			problems = append(problems, err.Error())
		} else {
			r.Trigger.RoutingKey = rk
		}

		normalizeResp := func(respLabel string, m *MockResponse) {
			ex, err := resolveAlias(respLabel,
				struct{ name, val string }{"exchange", m.Exchange},
				struct{ name, val string }{"destination", m.Destination},
				struct{ name, val string }{"topic", m.Topic},
			)
			if err != nil {
				problems = append(problems, err.Error())
			} else {
				m.Exchange = ex
			}

			mrk, err := resolveAlias(respLabel,
				struct{ name, val string }{"routing_key", m.RoutingKey},
				struct{ name, val string }{"key", m.Key},
			)
			if err != nil {
				problems = append(problems, err.Error())
			} else {
				m.RoutingKey = mrk
			}
		}

		normalizeResp(label+".mock_response", &r.MockResponse)
		for j := range r.MockResponses {
			normalizeResp(fmt.Sprintf("%s.mock_responses[%d]", label, j), &r.MockResponses[j])
		}
	}

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// Validate checks the configuration for errors that would prevent chitchat from running
// correctly. It returns a *ValidationError listing all problems, or nil when valid.
// Template syntax in payloads is validated separately by the engine package.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...interface{}) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if err := c.Normalize(); err != nil {
		if ve, ok := err.(*ValidationError); ok {
			problems = append(problems, ve.Problems...)
		} else {
			problems = append(problems, err.Error())
		}
	}

	// Broker
	if strings.TrimSpace(c.Broker.URL) == "" {
		add("broker.url must not be empty")
	} else if u, err := url.Parse(c.Broker.URL); err != nil {
		add("broker.url is not a valid URL")
	} else if u.Scheme != "amqp" && u.Scheme != "amqps" {
		add("broker.url scheme must be amqp or amqps, got %q", u.Scheme)
	}
	if strings.TrimSpace(c.Broker.ConnectTimeout) != "" {
		d, err := time.ParseDuration(c.Broker.ConnectTimeout)
		if err != nil {
			add("broker.connect_timeout %q is not a valid duration (e.g. 30s)", c.Broker.ConnectTimeout)
		} else if d < 0 {
			add("broker.connect_timeout must not be negative, got %q", c.Broker.ConnectTimeout)
		}
	}

	// Declarations
	exchanges := make(map[string]bool)
	for i, ex := range c.Broker.Declarations.Exchanges {
		switch {
		case ex.Name == "":
			add("broker.declarations.exchanges[%d]: name must not be empty", i)
		case exchanges[ex.Name]:
			add("broker.declarations.exchanges[%d]: duplicate exchange %q", i, ex.Name)
		default:
			exchanges[ex.Name] = true
		}
		if !validExchangeTypes[ex.Type] {
			add("broker.declarations.exchanges[%d] (%q): unknown type %q (expected direct, topic, fanout or headers)", i, ex.Name, ex.Type)
		}
	}
	queues := make(map[string]bool)
	for i, q := range c.Broker.Declarations.Queues {
		switch {
		case q.Name == "":
			add("broker.declarations.queues[%d]: name must not be empty", i)
		case queues[q.Name]:
			add("broker.declarations.queues[%d]: duplicate queue %q", i, q.Name)
		default:
			queues[q.Name] = true
		}
		for j, b := range q.Bindings {
			if b.Exchange == "" {
				add("broker.declarations.queues[%d] (%q).bindings[%d]: exchange must not be empty", i, q.Name, j)
			}
		}
	}

	// Server & logging
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		add("server.port must be between 1 and 65535, got %d", c.Server.Port)
	}
	if c.Server.RingBufferSize < 1 {
		add("server.ring_buffer_size must be at least 1, got %d", c.Server.RingBufferSize)
	}
	if !validLogLevels[strings.ToLower(strings.TrimSpace(c.Logging.Level))] {
		add("logging.level %q is not one of debug, info, warn, error", c.Logging.Level)
	}
	if !validLogFormats[strings.ToLower(strings.TrimSpace(c.Logging.Format))] {
		add("logging.format %q is not one of text, json", c.Logging.Format)
	}

	// Spy
	for i, q := range c.Spy.Queues {
		if strings.TrimSpace(q) == "" {
			add("spy.queues[%d]: queue name must not be empty", i)
		}
	}

	validateMockResp := func(respLabel string, m MockResponse) {
		if !m.Drop && m.Exchange == "" && m.RoutingKey == "" {
			add("%s: needs an exchange/destination and/or routing_key/key (an empty exchange publishes via the default exchange to the queue named by routing_key)", respLabel)
		}
		if m.DelayMS < 0 {
			add("%s.delay_ms must not be negative, got %d", respLabel, m.DelayMS)
		}
		if len(m.DelayMSRange) > 0 {
			if len(m.DelayMSRange) != 2 {
				add("%s.delay_ms_range must have exactly 2 elements [min, max], got %d", respLabel, len(m.DelayMSRange))
			} else if m.DelayMSRange[0] < 0 || m.DelayMSRange[1] < m.DelayMSRange[0] {
				add("%s.delay_ms_range must satisfy 0 <= min <= max, got [%d, %d]", respLabel, m.DelayMSRange[0], m.DelayMSRange[1])
			}
		}
		if m.Duplicate < 0 {
			add("%s.duplicate must not be negative, got %d", respLabel, m.Duplicate)
		}
	}

	// Rules
	seen := make(map[string]int)
	for i, r := range c.Rules {
		label := ruleLabel(i, r)
		if strings.TrimSpace(r.Name) == "" {
			add("%s: name must not be empty", label)
		} else if first, dup := seen[r.Name]; dup {
			add("%s: duplicate rule name (first used by rules[%d])", label, first)
		} else {
			seen[r.Name] = i
		}
		if r.Times < 0 {
			add("%s: times must not be negative, got %d", label, r.Times)
		}
		if r.Trigger.Queue == "" {
			add("%s: trigger.queue (or trigger.source/topic) must not be empty", label)
		}
		for j, jr := range r.Trigger.JSONPathRules {
			if strings.TrimSpace(jr.Filter) == "" {
				add("%s: trigger.jsonpath_rules[%d].filter must not be empty", label, j)
			}
			if jr.Matches != "" {
				if _, err := regexp.Compile(jr.Matches); err != nil {
					add("%s: trigger.jsonpath_rules[%d].matches %q is not a valid regex: %v", label, j, jr.Matches, err)
				}
			}
		}
		for j, hr := range r.Trigger.HeaderRules {
			if strings.TrimSpace(hr.Name) == "" {
				add("%s: trigger.header_rules[%d].name must not be empty", label, j)
			}
			if hr.Matches != "" {
				if _, err := regexp.Compile(hr.Matches); err != nil {
					add("%s: trigger.header_rules[%d].matches %q is not a valid regex: %v", label, j, hr.Matches, err)
				}
			}
		}

		if len(r.MockResponses) > 0 {
			if !r.MockResponse.IsZero() {
				add("%s: specify either mock_response or mock_responses, not both", label)
			}
			for j, m := range r.MockResponses {
				validateMockResp(fmt.Sprintf("%s.mock_responses[%d]", label, j), m)
			}
		} else {
			validateMockResp(label+".mock_response", r.MockResponse)
		}
	}

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// Warnings returns non-fatal observations about the configuration, such as rules that
// match every message or responses that could feed back into a trigger queue.
func (c *Config) Warnings() []string {
	var warns []string

	if len(c.Rules) == 0 && len(c.Spy.Queues) == 0 {
		warns = append(warns, "no rules or spy.queues configured: chitchat will not consume from any queue")
	}

	declaredQueues := make(map[string]bool)
	for _, q := range c.Broker.Declarations.Queues {
		declaredQueues[q.Name] = true
	}
	declaredExchanges := make(map[string]string)
	for _, ex := range c.Broker.Declarations.Exchanges {
		declaredExchanges[ex.Name] = ex.Type
	}

	triggerQueues := make(map[string]bool)
	for i, r := range c.Rules {
		label := ruleLabel(i, r)
		triggerQueues[r.Trigger.Queue] = true

		if len(r.Trigger.JSONPathRules) == 0 && len(r.Trigger.HeaderRules) == 0 && r.Trigger.RoutingKey == "" {
			warns = append(warns, fmt.Sprintf("%s: no jsonpath_rules, header_rules or routing_key, so it matches every message on queue %q", label, r.Trigger.Queue))
		}
		if len(declaredQueues) > 0 && r.Trigger.Queue != "" && !declaredQueues[r.Trigger.Queue] {
			warns = append(warns, fmt.Sprintf("%s: trigger queue %q is not in broker.declarations.queues (assumed to already exist)", label, r.Trigger.Queue))
		}
		for _, resp := range r.Responses() {
			if resp.Drop {
				continue
			}
			ex := resp.Exchange
			if _, ok := declaredExchanges[ex]; len(declaredExchanges) > 0 && ex != "" && !ok && !strings.HasPrefix(ex, "amq.") {
				warns = append(warns, fmt.Sprintf("%s: response exchange %q is not in broker.declarations.exchanges (assumed to already exist)", label, ex))
			}
		}
	}

	// Detect responses that are routed back into a queue some rule listens on.
	for i, r := range c.Rules {
		for _, resp := range r.Responses() {
			if resp.Drop {
				continue
			}
			for _, q := range c.Broker.Declarations.Queues {
				if !triggerQueues[q.Name] {
					continue
				}
				if c.responseReachesQueue(resp, q) {
					warns = append(warns, fmt.Sprintf("%s publishes to %s which is routed to trigger queue %q; chitchat's loop guard (header %s) stops it re-triggering on its own responses",
						ruleLabel(i, r), describeTarget(resp), q.Name, OriginHeader))
				}
			}
			if resp.Exchange == "" && triggerQueues[resp.RoutingKey] && resp.RoutingKey != "" {
				warns = append(warns, fmt.Sprintf("%s publishes to trigger queue %q via the default exchange; chitchat's loop guard (header %s) stops it re-triggering on its own responses",
					ruleLabel(i, r), resp.RoutingKey, OriginHeader))
			}
		}
	}

	return dedupe(warns)
}

// OriginHeader is stamped on every message chitchat publishes so it never reacts to its own output.
const OriginHeader = "x-chitchat-origin"

func describeTarget(m MockResponse) string {
	return fmt.Sprintf("exchange %q with routing key %q", m.Exchange, m.RoutingKey)
}

// responseReachesQueue reports whether a published mock response would be routed into the
// declared queue via one of its bindings (default-exchange routing is handled by the caller).
func (c *Config) responseReachesQueue(resp MockResponse, q QueueDeclaration) bool {
	if resp.Exchange == "" {
		return false
	}
	exType := "topic" // undeclared exchanges: assume topic semantics, which is a superset of direct matching
	for _, ex := range c.Broker.Declarations.Exchanges {
		if ex.Name == resp.Exchange {
			exType = ex.Type
			if exType == "" {
				exType = "direct"
			}
		}
	}
	for _, b := range q.Bindings {
		if b.Exchange != resp.Exchange {
			continue
		}
		switch exType {
		case "fanout":
			return true
		case "direct":
			if b.RoutingKey == resp.RoutingKey {
				return true
			}
		case "topic":
			if TopicMatches(b.RoutingKey, resp.RoutingKey) {
				return true
			}
		}
	}
	return false
}

// TopicMatches implements AMQP topic exchange matching: '*' matches exactly one word and
// '#' matches zero or more words; words are separated by dots.
func TopicMatches(pattern, key string) bool {
	return matchWords(strings.Split(pattern, "."), strings.Split(key, "."))
}

func matchWords(pattern, key []string) bool {
	if len(pattern) == 0 {
		return len(key) == 0
	}
	switch pattern[0] {
	case "#":
		for i := 0; i <= len(key); i++ {
			if matchWords(pattern[1:], key[i:]) {
				return true
			}
		}
		return false
	case "*":
		return len(key) > 0 && matchWords(pattern[1:], key[1:])
	default:
		return len(key) > 0 && pattern[0] == key[0] && matchWords(pattern[1:], key[1:])
	}
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
