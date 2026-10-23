package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"text/template"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"github.com/google/uuid"
)

var (
	// contextVarRegex matches trigger./system./headers. references that are not already
	// dot-prefixed, wherever they appear inside a template action.
	contextVarRegex = regexp.MustCompile(`(^|[\s(|,])((?:trigger|system|headers)\.)`)
	// bareVarRegex matches the bare {{queue}} / {{routing_key}} shortcuts.
	bareVarRegex = regexp.MustCompile(`(^|[\s(|,])(queue|routing_key)\b`)
	actionRegex  = regexp.MustCompile(`(?s)\{\{(.*?)\}\}`)
	// typedJSONRegex matches a leaf that consists solely of a json helper call, e.g.
	// "{{ json trigger.amount }}" or "{{ trigger.amount | json }}".
	typedJSONRegex = regexp.MustCompile(`^\s*\{\{-?\s*(?:json\s+.+|.+\|\s*json)\s*-?\}\}\s*$`)
)

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
		// json encodes any value as JSON. Used on its own as a payload leaf
		// ("{{ trigger.amount | json }}") it yields a typed JSON value (number, bool, object...);
		// inside a string payload it is safe to splice into JSON text.
		"json": func(v interface{}) (string, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(b), nil
		},
	}
}

// preprocessTemplate adapts friendly template syntax (e.g. {{trigger.id}}) to Go dot-notation
// ({{.trigger.id}}). Unlike a plain prefix replacement it rewrites references anywhere inside an
// action (e.g. {{ json trigger.id }}, {{ if trigger.flag }}) while leaving string literals untouched.
func preprocessTemplate(s string) string {
	return actionRegex.ReplaceAllStringFunc(s, func(action string) string {
		inner := action[2 : len(action)-2]
		return "{{" + rewriteAction(inner) + "}}"
	})
}

// rewriteAction rewrites the code segments of an action body, skipping quoted string literals.
func rewriteAction(content string) string {
	var out strings.Builder
	i := 0
	for i < len(content) {
		if c := content[i]; c == '"' || c == '`' {
			j := i + 1
			for j < len(content) && content[j] != c {
				if c == '"' && content[j] == '\\' {
					j++
				}
				j++
			}
			if j < len(content) {
				j++ // closing quote
			}
			if j > len(content) {
				j = len(content)
			}
			out.WriteString(content[i:j])
			i = j
			continue
		}
		j := i
		for j < len(content) && content[j] != '"' && content[j] != '`' {
			j++
		}
		out.WriteString(rewriteCode(content[i:j]))
		i = j
	}
	return out.String()
}

func rewriteCode(code string) string {
	code = contextVarRegex.ReplaceAllString(code, "${1}.${2}")
	code = bareVarRegex.ReplaceAllString(code, "${1}.${2}")
	return code
}

// leafFunc transforms a single string found in a payload definition. isKey is true for map keys,
// which must always stay strings.
type leafFunc func(s string, isKey bool) (interface{}, error)

// transformPayload returns a copy of v with fn applied to every string (map keys included).
func transformPayload(v interface{}, fn leafFunc) (interface{}, error) {
	switch t := v.(type) {
	case string:
		return fn(t, false)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			nk, nv, err := transformEntry(k, val, fn)
			if err != nil {
				return nil, err
			}
			out[nk] = nv
		}
		return out, nil
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			nk, nv, err := transformEntry(fmt.Sprint(k), val, fn)
			if err != nil {
				return nil, err
			}
			out[nk] = nv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			nv, err := transformPayload(val, fn)
			if err != nil {
				return nil, err
			}
			out[i] = nv
		}
		return out, nil
	default:
		return v, nil
	}
}

func transformEntry(key string, val interface{}, fn leafFunc) (string, interface{}, error) {
	nk, err := fn(key, true)
	if err != nil {
		return "", nil, err
	}
	nv, err := transformPayload(val, fn)
	if err != nil {
		return "", nil, err
	}
	ks, _ := nk.(string)
	return ks, nv, nil
}

// buildTemplateContext assembles the data exposed to payload templates.
func buildTemplateContext(msg broker.Message) map[string]interface{} {
	var triggerMap interface{}
	if len(msg.Body) > 0 {
		_ = json.Unmarshal(msg.Body, &triggerMap)
	}
	if triggerMap == nil {
		triggerMap = make(map[string]interface{})
	}

	return map[string]interface{}{
		"trigger":     triggerMap,
		"queue":       msg.Queue,
		"routing_key": msg.RoutingKey,
		"headers":     msg.Headers,
		"system": map[string]interface{}{
			"utc_now": time.Now().UTC().Format(time.RFC3339),
		},
	}
}

func parseTemplate(text string) (*template.Template, error) {
	tmpl, err := template.New("payload").Funcs(TemplateFuncs()).Parse(preprocessTemplate(text))
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload template %q: %w", text, err)
	}
	return tmpl, nil
}

func executeTemplate(text string, data map[string]interface{}) (string, error) {
	tmpl, err := parseTemplate(text)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute payload template %q: %w", text, err)
	}
	return buf.String(), nil
}

// RenderPayload renders a mock response payload using Go's text/template engine.
//
// Structured payloads (maps/lists from YAML) are rendered leaf by leaf: every string value (and
// map key) is its own template and the result is re-marshalled, so values extracted from the
// triggering message can never break or inject into the resulting JSON. Leaves render to strings;
// a leaf made solely of the json helper ("{{ trigger.amount | json }}") renders as a typed JSON value.
//
// A payload that is itself a string is treated as one raw template and written verbatim, which
// allows non-JSON bodies; use the json helper there to splice values safely.
func RenderPayload(payloadDef interface{}, msg broker.Message) ([]byte, error) {
	if payloadDef == nil {
		return []byte("{}"), nil
	}

	data := buildTemplateContext(msg)

	if raw, ok := payloadDef.(string); ok {
		out, err := executeTemplate(raw, data)
		if err != nil {
			return nil, err
		}
		return []byte(out), nil
	}

	rendered, err := transformPayload(payloadDef, func(s string, isKey bool) (interface{}, error) {
		if !strings.Contains(s, "{{") {
			return s, nil
		}
		out, err := executeTemplate(s, data)
		if err != nil {
			return nil, err
		}
		if !isKey && strings.Count(s, "{{") == 1 && typedJSONRegex.MatchString(s) {
			if !json.Valid([]byte(out)) {
				return nil, fmt.Errorf("json helper in %q produced invalid JSON: %s", s, out)
			}
			return json.RawMessage(out), nil
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}

	marshaled, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal rendered payload: %w", err)
	}
	return marshaled, nil
}

// ValidateTemplates checks that every payload template in the configuration parses.
// It does not execute templates, so references to fields that only exist in real messages are fine.
func ValidateTemplates(cfg *config.Config) error {
	var errs []error
	for i, rule := range cfg.Rules {
		label := rule.Name
		if label == "" {
			label = fmt.Sprintf("rules[%d]", i)
		}
		check := func(s string, _ bool) (interface{}, error) {
			if strings.Contains(s, "{{") {
				if _, err := parseTemplate(s); err != nil {
					errs = append(errs, fmt.Errorf("rule %q: %w", label, err))
				}
			}
			return s, nil
		}
		if rule.MockResponse.Payload == nil {
			continue
		}
		if raw, ok := rule.MockResponse.Payload.(string); ok {
			_, _ = check(raw, false)
			continue
		}
		_, _ = transformPayload(rule.MockResponse.Payload, check)
	}
	return errors.Join(errs...)
}
