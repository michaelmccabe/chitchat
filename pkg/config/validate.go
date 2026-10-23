package config

import (
	"fmt"
	"net/url"
	"strings"
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

// Validate checks the configuration for errors that would prevent chitchat from running
// correctly. It returns a *ValidationError listing all problems, or nil when valid.
// Template syntax in payloads is validated separately by the engine package.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...interface{}) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	// Broker
	if strings.TrimSpace(c.Broker.URL) == "" {
		add("broker.url must not be empty")
	} else if u, err := url.Parse(c.Broker.URL); err != nil {
		add("broker.url is not a valid URL")
	} else if u.Scheme != "amqp" && u.Scheme != "amqps" {
		add("broker.url scheme must be amqp or amqps, got %q", u.Scheme)
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
		if r.Trigger.Queue == "" {
			add("%s: trigger.queue must not be empty", label)
		}
		for j, jr := range r.Trigger.JSONPathRules {
			if strings.TrimSpace(jr.Filter) == "" {
				add("%s: trigger.jsonpath_rules[%d].filter must not be empty", label, j)
			}
		}
		if r.MockResponse.Exchange == "" && r.MockResponse.RoutingKey == "" {
			add("%s: mock_response needs an exchange and/or routing_key (an empty exchange publishes via the default exchange to the queue named by routing_key)", label)
		}
		if r.MockResponse.DelayMS < 0 {
			add("%s: mock_response.delay_ms must not be negative, got %d", label, r.MockResponse.DelayMS)
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

		if len(r.Trigger.JSONPathRules) == 0 && r.Trigger.RoutingKey == "" {
			warns = append(warns, fmt.Sprintf("%s: no jsonpath_rules or routing_key, so it matches every message on queue %q", label, r.Trigger.Queue))
		}
		if len(declaredQueues) > 0 && r.Trigger.Queue != "" && !declaredQueues[r.Trigger.Queue] {
			warns = append(warns, fmt.Sprintf("%s: trigger queue %q is not in broker.declarations.queues (assumed to already exist)", label, r.Trigger.Queue))
		}
		ex := r.MockResponse.Exchange
		if _, ok := declaredExchanges[ex]; len(declaredExchanges) > 0 && ex != "" && !ok && !strings.HasPrefix(ex, "amq.") {
			warns = append(warns, fmt.Sprintf("%s: response exchange %q is not in broker.declarations.exchanges (assumed to already exist)", label, ex))
		}
	}

	// Detect responses that are routed back into a queue some rule listens on.
	for i, r := range c.Rules {
		for _, q := range c.Broker.Declarations.Queues {
			if !triggerQueues[q.Name] {
				continue
			}
			if c.responseReachesQueue(r.MockResponse, q) {
				warns = append(warns, fmt.Sprintf("%s publishes to %s which is routed to trigger queue %q; chitchat's loop guard (header %s) stops it re-triggering on its own responses",
					ruleLabel(i, r), describeTarget(r.MockResponse), q.Name, OriginHeader))
			}
		}
		if r.MockResponse.Exchange == "" && triggerQueues[r.MockResponse.RoutingKey] && r.MockResponse.RoutingKey != "" {
			warns = append(warns, fmt.Sprintf("%s publishes to trigger queue %q via the default exchange; chitchat's loop guard (header %s) stops it re-triggering on its own responses",
				ruleLabel(i, r), r.MockResponse.RoutingKey, OriginHeader))
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
			if topicMatches(b.RoutingKey, resp.RoutingKey) {
				return true
			}
		}
	}
	return false
}

// topicMatches implements AMQP topic exchange matching: '*' matches exactly one word and
// '#' matches zero or more words; words are separated by dots.
func topicMatches(pattern, key string) bool {
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
