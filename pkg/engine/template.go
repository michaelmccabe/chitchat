package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
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

	globalTemplateCounter atomic.Int64
)

// TemplateFuncs provides built-in helper functions for template rendering.
func TemplateFuncs() template.FuncMap {
	return templateFuncsWithHeaders(nil, &globalTemplateCounter)
}

func templateFuncsWithHeaders(headers map[string]interface{}, counter *atomic.Int64) template.FuncMap {
	if counter == nil {
		counter = &globalTemplateCounter
	}
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
		// header looks up a header value from the triggering message (converting []byte to string).
		"header": func(name string) string {
			if headers == nil {
				return ""
			}
			v, ok := headers[name]
			if !ok || v == nil {
				return ""
			}
			if b, ok := v.([]byte); ok {
				return string(b)
			}
			return fmt.Sprint(v)
		},
		// env reads an environment variable by name.
		"env": os.Getenv,
		// counter returns a monotonically increasing integer starting at 1.
		"counter": func() int64 {
			return counter.Add(1)
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

// preprocessTemplate adapts friendly template syntax (e.g. {{trigger.id}}, {{- trigger.id -}})
// to Go dot-notation ({{.trigger.id}}). Unlike a plain prefix replacement it rewrites references
// anywhere inside an action (e.g. {{ json trigger.id }}, {{ if trigger.flag }}) while leaving
// string literals untouched.
func preprocessTemplate(s string) string {
	return actionRegex.ReplaceAllStringFunc(s, func(action string) string {
		inner := action[2 : len(action)-2]
		prefix := ""
		suffix := ""
		if strings.HasPrefix(inner, "-") {
			prefix = "-"
			inner = inner[1:]
		}
		if strings.HasSuffix(inner, "-") {
			suffix = "-"
			inner = inner[:len(inner)-1]
		}
		return "{{" + prefix + rewriteAction(inner) + suffix + "}}"
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

// compiledLeaf holds a pre-parsed template for a single payload/header/routing-key string.
type compiledLeaf struct {
	raw        string
	tmpl       *template.Template
	typedJSON  bool
	usesHeader bool
}

func compileLeaf(text string, isKey bool, counter *atomic.Int64) (*compiledLeaf, error) {
	tmpl, err := parseTemplateWithCounter(text, counter)
	if err != nil {
		return nil, err
	}
	return &compiledLeaf{
		raw:        text,
		tmpl:       tmpl,
		typedJSON:  !isKey && strings.Count(text, "{{") == 1 && typedJSONRegex.MatchString(text),
		usesHeader: strings.Contains(text, "header"),
	}, nil
}

func (c *compiledLeaf) execute(data map[string]interface{}, headers map[string]interface{}, counter *atomic.Int64) (string, error) {
	t := c.tmpl
	if c.usesHeader {
		cloned, err := c.tmpl.Clone()
		if err != nil {
			return "", fmt.Errorf("failed to clone payload template %q: %w", c.raw, err)
		}
		t = cloned.Funcs(templateFuncsWithHeaders(headers, counter))
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute payload template %q: %w", c.raw, err)
	}
	return buf.String(), nil
}

// CompiledPayload represents a pre-parsed mock response payload ready for repeated execution
// without re-parsing templates on every matched message.
type CompiledPayload struct {
	isNil    bool
	rawLeaf  *compiledLeaf
	tree     interface{} // mirrors payload structure with *compiledLeaf replacing templated strings
	counter  *atomic.Int64
	rawValue interface{}
}

// CompilePayload pre-parses all template strings inside payloadDef.
func CompilePayload(payloadDef interface{}, counter *atomic.Int64) (*CompiledPayload, error) {
	if payloadDef == nil {
		return &CompiledPayload{isNil: true, counter: counter}, nil
	}
	if raw, ok := payloadDef.(string); ok {
		if !strings.Contains(raw, "{{") {
			return &CompiledPayload{rawValue: raw, counter: counter}, nil
		}
		leaf, err := compileLeaf(raw, false, counter)
		if err != nil {
			return nil, err
		}
		return &CompiledPayload{rawLeaf: leaf, counter: counter}, nil
	}

	tree, err := compileNode(payloadDef, counter)
	if err != nil {
		return nil, err
	}
	return &CompiledPayload{tree: tree, counter: counter}, nil
}

type compiledMapEntry struct {
	keyLiteral string
	keyLeaf    *compiledLeaf
	val        interface{}
}

func compileNode(v interface{}, counter *atomic.Int64) (interface{}, error) {
	switch t := v.(type) {
	case string:
		if !strings.Contains(t, "{{") {
			return t, nil
		}
		return compileLeaf(t, false, counter)
	case map[string]interface{}:
		entries := make([]compiledMapEntry, 0, len(t))
		for k, val := range t {
			entry, err := compileMapEntry(k, val, counter)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		return entries, nil
	case map[interface{}]interface{}:
		entries := make([]compiledMapEntry, 0, len(t))
		for k, val := range t {
			entry, err := compileMapEntry(fmt.Sprint(k), val, counter)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		return entries, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			nv, err := compileNode(val, counter)
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

func compileMapEntry(key string, val interface{}, counter *atomic.Int64) (compiledMapEntry, error) {
	var entry compiledMapEntry
	if strings.Contains(key, "{{") {
		kl, err := compileLeaf(key, true, counter)
		if err != nil {
			return entry, err
		}
		entry.keyLeaf = kl
	} else {
		entry.keyLiteral = key
	}
	nv, err := compileNode(val, counter)
	if err != nil {
		return entry, err
	}
	entry.val = nv
	return entry, nil
}

// Render executes the pre-compiled payload against msg and returns the JSON/raw bytes.
func (cp *CompiledPayload) Render(msg broker.Message) ([]byte, error) {
	if cp == nil || cp.isNil {
		return []byte("{}"), nil
	}
	if cp.rawValue != nil {
		return []byte(cp.rawValue.(string)), nil
	}
	data := buildTemplateContext(msg)
	if cp.rawLeaf != nil {
		out, err := cp.rawLeaf.execute(data, msg.Headers, cp.counter)
		if err != nil {
			return nil, err
		}
		return []byte(out), nil
	}

	rendered, err := cp.evalNode(cp.tree, data, msg.Headers)
	if err != nil {
		return nil, err
	}
	marshaled, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal rendered payload: %w", err)
	}
	return marshaled, nil
}

func (cp *CompiledPayload) evalNode(v interface{}, data map[string]interface{}, headers map[string]interface{}) (interface{}, error) {
	switch t := v.(type) {
	case *compiledLeaf:
		out, err := t.execute(data, headers, cp.counter)
		if err != nil {
			return nil, err
		}
		if t.typedJSON {
			if !json.Valid([]byte(out)) {
				return nil, fmt.Errorf("json helper in %q produced invalid JSON: %s", t.raw, out)
			}
			return json.RawMessage(out), nil
		}
		return out, nil
	case []compiledMapEntry:
		out := make(map[string]interface{}, len(t))
		for _, entry := range t {
			k := entry.keyLiteral
			if entry.keyLeaf != nil {
				var err error
				k, err = entry.keyLeaf.execute(data, headers, cp.counter)
				if err != nil {
					return nil, err
				}
			}
			nv, err := cp.evalNode(entry.val, data, headers)
			if err != nil {
				return nil, err
			}
			out[k] = nv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			nv, err := cp.evalNode(val, data, headers)
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

// buildTemplateContext assembles the data exposed to payload templates.
func buildTemplateContext(msg broker.Message) map[string]interface{} {
	var triggerMap interface{}
	if len(msg.Body) > 0 {
		_ = json.Unmarshal(msg.Body, &triggerMap)
	}
	if triggerMap == nil {
		triggerMap = make(map[string]interface{})
	}

	normHeaders := make(map[string]interface{}, len(msg.Headers))
	for k, v := range msg.Headers {
		if b, ok := v.([]byte); ok {
			normHeaders[k] = string(b)
		} else {
			normHeaders[k] = v
		}
	}

	return map[string]interface{}{
		"trigger":     triggerMap,
		"queue":       msg.Queue,
		"source":      msg.Queue,
		"routing_key": msg.RoutingKey,
		"key":         msg.RoutingKey,
		"headers":     normHeaders,
		"system": map[string]interface{}{
			"utc_now": time.Now().UTC().Format(time.RFC3339),
		},
	}
}

func parseTemplateWithCounter(text string, counter *atomic.Int64) (*template.Template, error) {
	tmpl, err := template.New("payload").Funcs(templateFuncsWithHeaders(nil, counter)).Parse(preprocessTemplate(text))
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload template %q: %w", text, err)
	}
	return tmpl, nil
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
	compiled, err := CompilePayload(payloadDef, &globalTemplateCounter)
	if err != nil {
		return nil, err
	}
	return compiled.Render(msg)
}

// ValidateTemplates checks that every payload, routing_key, and header template in the configuration parses.
// It does not execute templates, so references to fields that only exist in real messages are fine.
func ValidateTemplates(cfg *config.Config) error {
	var errs []error
	for i, rule := range cfg.Rules {
		label := rule.Name
		if label == "" {
			label = fmt.Sprintf("rules[%d]", i)
		}
		for _, resp := range rule.Responses() {
			if _, err := CompilePayload(resp.Payload, &globalTemplateCounter); err != nil {
				errs = append(errs, fmt.Errorf("rule %q: %w", label, err))
			}
			if strings.Contains(resp.RoutingKey, "{{") {
				if _, err := compileLeaf(resp.RoutingKey, true, &globalTemplateCounter); err != nil {
					errs = append(errs, fmt.Errorf("rule %q routing_key: %w", label, err))
				}
			}
			for hk, hv := range resp.Headers {
				if s, ok := hv.(string); ok && strings.Contains(s, "{{") {
					if _, err := compileLeaf(s, true, &globalTemplateCounter); err != nil {
						errs = append(errs, fmt.Errorf("rule %q header %q: %w", label, hk, err))
					}
				}
			}
		}
	}
	return errors.Join(errs...)
}
