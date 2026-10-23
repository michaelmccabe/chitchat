package engine

import (
	"fmt"
	"reflect"
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
