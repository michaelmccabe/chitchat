package engine

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"github.com/tidwall/gjson"
)

// MatchesRule checks whether an incoming broker message satisfies a rule's trigger conditions.
func MatchesRule(rule *config.Rule, msg broker.Message) bool {
	if rule.Trigger.Queue != "" && rule.Trigger.Queue != msg.Queue {
		return false
	}
	if rule.Trigger.RoutingKey != "" && !matchesRoutingKey(rule.Trigger.RoutingKey, msg.RoutingKey) {
		return false
	}
	for _, hRule := range rule.Trigger.HeaderRules {
		if !evalHeaderRule(msg.Headers, hRule) {
			return false
		}
	}
	for _, jRule := range rule.Trigger.JSONPathRules {
		if !evalJSONPathRule(msg.Body, jRule) {
			return false
		}
	}
	return true
}

// matchesRoutingKey checks an exact match unless pattern contains AMQP wildcards ('*' or '#').
func matchesRoutingKey(pattern, key string) bool {
	if strings.ContainsAny(pattern, "*#") {
		return config.TopicMatches(pattern, key)
	}
	return pattern == key
}

// evalHeaderRule evaluates a single header condition against message headers.
func evalHeaderRule(headers map[string]interface{}, rule config.HeaderRule) bool {
	raw, exists := headers[rule.Name]
	if b, ok := raw.([]byte); ok {
		raw = string(b)
	}

	if rule.Exists != nil {
		if *rule.Exists != exists {
			return false
		}
		if !exists {
			return true
		}
	}
	if !exists {
		// If only not_equals is specified and header is missing, it is not equal.
		if rule.NotEquals != nil && rule.Equals == nil && rule.Contains == "" && rule.Matches == "" && len(rule.In) == 0 {
			return true
		}
		return false
	}

	if rule.Equals != nil && !compareValues(raw, rule.Equals) {
		return false
	}
	if rule.NotEquals != nil && compareValues(raw, rule.NotEquals) {
		return false
	}
	if rule.Contains != "" && !strings.Contains(fmt.Sprint(raw), rule.Contains) {
		return false
	}
	if rule.Matches != "" {
		ok, err := regexp.MatchString(rule.Matches, fmt.Sprint(raw))
		if err != nil || !ok {
			return false
		}
	}
	if len(rule.In) > 0 && !matchesAny(raw, rule.In) {
		return false
	}
	return true
}

// evalJSONPathRule evaluates a single JSONPath rule against message bytes.
func evalJSONPathRule(body []byte, rule config.JSONPathRule) bool {
	res := gjson.GetBytes(body, rule.Filter)
	exists := res.Exists()

	if rule.Exists != nil {
		if *rule.Exists != exists {
			return false
		}
		if !exists {
			return true
		}
	}
	if !exists {
		if rule.NotEquals != nil &&
			rule.Equals == nil &&
			rule.Contains == "" &&
			rule.Matches == "" &&
			rule.GT == nil &&
			rule.GTE == nil &&
			rule.LT == nil &&
			rule.LTE == nil &&
			len(rule.In) == 0 {
			return true
		}
		return false
	}

	actual := res.Value()

	if rule.Equals != nil && !compareValues(actual, rule.Equals) {
		return false
	}
	if rule.NotEquals != nil && compareValues(actual, rule.NotEquals) {
		return false
	}
	if rule.Contains != "" {
		if res.IsArray() {
			found := false
			for _, elem := range res.Array() {
				if compareValues(elem.Value(), rule.Contains) || elem.String() == rule.Contains {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		} else if !strings.Contains(res.String(), rule.Contains) {
			return false
		}
	}
	if rule.Matches != "" {
		ok, err := regexp.MatchString(rule.Matches, res.String())
		if err != nil || !ok {
			return false
		}
	}
	if rule.GT != nil || rule.GTE != nil || rule.LT != nil || rule.LTE != nil {
		num, ok := toFloat64(actual)
		if !ok {
			return false
		}
		if rule.GT != nil && !(num > *rule.GT) {
			return false
		}
		if rule.GTE != nil && !(num >= *rule.GTE) {
			return false
		}
		if rule.LT != nil && !(num < *rule.LT) {
			return false
		}
		if rule.LTE != nil && !(num <= *rule.LTE) {
			return false
		}
	}
	if len(rule.In) > 0 && !matchesAny(actual, rule.In) {
		return false
	}
	return true
}

func matchesAny(actual interface{}, candidates []interface{}) bool {
	for _, c := range candidates {
		if compareValues(actual, c) {
			return true
		}
	}
	return false
}

// compareValues performs a type-aware, case-sensitive comparison between a gjson/header value
// and an expected YAML value. Numeric types (int, uint, float) are compared numerically so YAML
// integers match JSON float64 values, while strings, booleans, and numbers do not coerce across types.
func compareValues(actual, expected interface{}) bool {
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	if aNum, aOK := toFloat64(actual); aOK {
		if eNum, eOK := toFloat64(expected); eOK {
			return aNum == eNum
		}
		return false
	}

	if aBool, aOK := actual.(bool); aOK {
		if eBool, eOK := expected.(bool); eOK {
			return aBool == eBool
		}
		return false
	}

	if aStr, aOK := actual.(string); aOK {
		if eStr, eOK := expected.(string); eOK {
			return aStr == eStr
		}
		return false
	}

	return reflect.DeepEqual(actual, expected)
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
