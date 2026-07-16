// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querytraffic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRulesAcceptsJSONRuleArray(t *testing.T) {
	rules, err := ParseRules(`[{"name":"local","routes":[{"name":"any","weight":100,"destinationLabels":{"any":true}}]}]`)
	require.NoError(t, err)
	require.Equal(t, []RuleConfig{
		{
			Name: "local",
			Routes: []RouteConfig{
				{
					Name:              "any",
					Weight:            100,
					DestinationLabels: MatcherConfig{Any: true},
				},
			},
		},
	}, rules)
}

func TestParseRulesAcceptsPolicyObject(t *testing.T) {
	rules, err := ParseRules(`{"rules":[{"name":"fallback","match":{"sourceLabels":{"any":true}}}]}`)
	require.NoError(t, err)
	require.Equal(t, []RuleConfig{
		{
			Name: "fallback",
			Match: RuleMatchConfig{
				SourceLabels: MatcherConfig{Any: true},
			},
		},
	}, rules)
}

func TestParseRulesEmptyValueDisablesPolicy(t *testing.T) {
	rules, err := ParseRules("")
	require.NoError(t, err)
	require.Nil(t, rules)
}
