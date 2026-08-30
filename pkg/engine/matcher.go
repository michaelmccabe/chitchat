package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"regexp"
	"strings"
	"text/template"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

var (
	triggerRegex    = regexp.MustCompile(`\{\{\s*trigger\.`)
	systemRegex     = regexp.MustCompile(`\{\{\s*system\.`)
	queueRegex      = regexp.MustCompile(`\{\{\s*queue\b`)
	routingKeyRegex = regexp.MustCompile(`\{\{\s*routing_key\b`)
	headersRegex    = regexp.MustCompile(`\{\{\s*headers\.`)
)

// MatchesRule checks whether an incoming broker message satisfies a rule's trigger conditions.
func MatchesRule(rule *config.Rule, msg broker.Message) bool {
	if rule.Trigger.Queue != "" && rule.Trigger.Queue != msg.Queue {
		return false
	}
	if rule.Trigger.RoutingKey != "" && rule.Trigger.RoutingKey != msg.RoutingKey {
		return false
	}
	for _, jRule := range rule.Trigger.JSONPathRules {
		if !evalJSONPathRule(msg.Body, jRule) {
			return false
		}
	}
	return true
}

// evalJSONPathRule evaluates a single JSONPath rule against message bytes.
func evalJSONPathRule(body []byte, rule config.JSONPathRule) bool {
	res := gjson.GetBytes(body, rule.Filter)
	if !res.Exists() {
		return false
	}
	return compareValues(res.Value(), rule.Equals)
}

// compareValues compares gjson extracted value against the expected YAML value.
func compareValues(actual, expected interface{}) bool {
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	if reflect.DeepEqual(actual, expected) {
		return true
	}

	// Compare formatted string representations (handles number conversions e.g. int vs float64)
	actualStr := fmt.Sprintf("%v", actual)
	expectedStr := fmt.Sprintf("%v", expected)
	if actualStr == expectedStr {
		return true
	}

	// Handle boolean string comparisons ("true" == true)
	if strings.EqualFold(actualStr, expectedStr) {
		return true
	}

	return false
}

// TemplateFuncs provides built-in helper functions for template rendering.
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"uuid": func() string {
			return uuid.New().String()
		},
		"system_utc_now": func() string {
			return time.Now().UTC().Format(time.RFC3339)
		},
		"utc_now": func() string {
			return time.Now().UTC().Format(time.RFC3339)
		},
		"random_int": func(min, max int) int {
			if min >= max {
				return min
			}
			return rand.N(max-min+1) + min
		},
		"now": func() string {
			return time.Now().UTC().Format(time.RFC3339)
		},
	}
}

// preprocessTemplate adapts friendly template syntax (e.g. {{trigger.id}}) to Go dot-notation ({{.trigger.id}}).
func preprocessTemplate(s string) string {
	s = triggerRegex.ReplaceAllString(s, "{{.trigger.")
	s = systemRegex.ReplaceAllString(s, "{{.system.")
	s = queueRegex.ReplaceAllString(s, "{{.queue")
	s = routingKeyRegex.ReplaceAllString(s, "{{.routing_key")
	s = headersRegex.ReplaceAllString(s, "{{.headers.")
	return s
}

// RenderPayload dynamically renders a mock response payload using Go's text/template engine.
func RenderPayload(payloadDef interface{}, msg broker.Message) ([]byte, error) {
	if payloadDef == nil {
		return []byte("{}"), nil
	}

	var templateStr string
	switch p := payloadDef.(type) {
	case string:
		templateStr = p
	default:
		marshaled, err := json.Marshal(payloadDef)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload definition to template string: %w", err)
		}
		templateStr = string(marshaled)
	}

	templateStr = preprocessTemplate(templateStr)

	var triggerMap interface{}
	if len(msg.Body) > 0 {
		_ = json.Unmarshal(msg.Body, &triggerMap)
	}
	if triggerMap == nil {
		triggerMap = make(map[string]interface{})
	}

	templateContext := map[string]interface{}{
		"trigger":     triggerMap,
		"queue":       msg.Queue,
		"routing_key": msg.RoutingKey,
		"headers":     msg.Headers,
		"system": map[string]interface{}{
			"utc_now": time.Now().UTC().Format(time.RFC3339),
		},
	}

	tmpl, err := template.New("payload").Funcs(TemplateFuncs()).Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateContext); err != nil {
		return nil, fmt.Errorf("failed to execute payload template: %w", err)
	}

	return buf.Bytes(), nil
}
