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

func TestMatcherSupportsAllOperators(t *testing.T) {
	m, err := CompileMatcher(MatcherConfig{
		Exists:    []string{"AZ", "RESOURCE_GROUP"},
		NotExists: []string{"DRAINING"},
		Eq:        map[string]string{"AZ": "${source.AZ}"},
		Ne:        map[string]string{"RESOURCE_GROUP": "rg-old"},
		In:        map[string][]string{"RESOURCE_GROUP": {"rg1", "rg2"}},
		NotIn:     map[string][]string{"CANARY": {"disabled"}},
		Match:     map[string]string{"RESOURCE_GROUP": "^rg[0-9]+$"},
		NotMatch:  map[string]string{"VERSION": "^bad-"},
	})
	require.NoError(t, err)

	require.True(t, m.Match(Labels{"AZ": "az1"}, Labels{
		"AZ":             "az1",
		"RESOURCE_GROUP": "rg1",
		"CANARY":         "enabled",
		"VERSION":        "good-1",
	}))
	require.False(t, m.Match(Labels{"AZ": "az1"}, Labels{
		"AZ":             "az2",
		"RESOURCE_GROUP": "rg1",
	}))
}

func TestCompileMatcherRejectsInvalidRegex(t *testing.T) {
	_, err := CompileMatcher(MatcherConfig{
		Match: map[string]string{"RESOURCE_GROUP": "["},
	})
	require.Error(t, err)
}

func TestCompileMatcherRejectsPartialSourceReference(t *testing.T) {
	_, err := CompileMatcher(MatcherConfig{
		Eq: map[string]string{"AZ": "prefix-${source.AZ}"},
	})
	require.Error(t, err)
}

func TestMatcherAnyMatchesEverything(t *testing.T) {
	m, err := CompileMatcher(MatcherConfig{Any: true})
	require.NoError(t, err)

	require.True(t, m.Match(Labels{}, Labels{}))
	require.True(t, m.Match(Labels{"AZ": "az1"}, Labels{"AZ": "az2", "RESOURCE_GROUP": "rg1"}))
	require.True(t, m.Match(Labels{"AZ": "az1"}, Labels{}))
}

func TestCompileMatcherRejectsAnyWithOtherFields(t *testing.T) {
	_, err := CompileMatcher(MatcherConfig{
		Any:    true,
		Exists: []string{"AZ"},
	})
	require.Error(t, err)

	_, err = CompileMatcher(MatcherConfig{
		Any: true,
		Eq:  map[string]string{"AZ": "az1"},
	})
	require.Error(t, err)
}

func TestMatcherNilAndEmptyMatchEverything(t *testing.T) {
	var nilMatcher *Matcher
	require.True(t, nilMatcher.Match(Labels{}, Labels{}))

	empty, err := CompileMatcher(MatcherConfig{})
	require.NoError(t, err)
	require.True(t, empty.Match(Labels{"AZ": "az1"}, Labels{"RESOURCE_GROUP": "rg1"}))
}

func TestMatcherNeAndNotInAllowMissingKey(t *testing.T) {
	m, err := CompileMatcher(MatcherConfig{
		Ne:    map[string]string{"RESOURCE_GROUP": "rg-old"},
		NotIn: map[string][]string{"CANARY": {"disabled"}},
	})
	require.NoError(t, err)

	// an absent key is not equal to any value and is not in any list
	require.True(t, m.Match(Labels{}, Labels{"AZ": "az1"}))

	// a key equal to the ne value fails, a key in the not_in list fails
	require.False(t, m.Match(Labels{}, Labels{"RESOURCE_GROUP": "rg-old"}))
	require.False(t, m.Match(Labels{}, Labels{"CANARY": "disabled"}))
}

func TestPolicyRoutesByRuleOrderAndFallback(t *testing.T) {
	policy, err := Compile(PolicyConfig{
		Rules: []RuleConfig{
			{
				Name: "local-empty",
				Match: RuleMatchConfig{
					SourceLabels: MatcherConfig{Exists: []string{"AZ"}},
				},
				Routes: []RouteConfig{
					{
						Name:   "local-az",
						Weight: 100,
						DestinationLabels: MatcherConfig{
							Eq: map[string]string{"AZ": "${source.AZ}"},
						},
					},
				},
			},
			{
				Name: "fallback",
				Match: RuleMatchConfig{
					SourceLabels: MatcherConfig{Any: true},
				},
				Routes: []RouteConfig{
					{
						Name:              "any",
						Weight:            100,
						DestinationLabels: MatcherConfig{Any: true},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	routed := policy.Route(Labels{"AZ": "az1"}, []Candidate{
		{NodeID: 10, Labels: Labels{"AZ": "az2"}},
		{NodeID: 11, Labels: Labels{"AZ": "az2"}},
	})

	require.Equal(t, []WeightedCandidate{
		{NodeID: 10, Weight: 100},
		{NodeID: 11, Weight: 100},
	}, routed)

	result := policy.RouteWithResult(Labels{"AZ": "az1"}, []Candidate{
		{NodeID: 10, Labels: Labels{"AZ": "az2"}},
		{NodeID: 11, Labels: Labels{"AZ": "az2"}},
	})
	require.Equal(t, "fallback", result.RuleName)
	require.Empty(t, result.FallbackReason)
	require.Equal(t, routed, result.Candidates)
}

func TestPolicyUsesFirstMatchingRouteAndPerNodeWeight(t *testing.T) {
	policy, err := Compile(PolicyConfig{
		Rules: []RuleConfig{
			{
				Name: "az-affinity-with-rg",
				Match: RuleMatchConfig{
					SourceLabels: MatcherConfig{Exists: []string{"AZ"}},
				},
				Routes: []RouteConfig{
					{
						Name:   "rg1-old",
						Weight: 90,
						DestinationLabels: MatcherConfig{
							Eq: map[string]string{"AZ": "${source.AZ}", "RESOURCE_GROUP": "rg1-0"},
						},
					},
					{
						Name:   "rg1-new",
						Weight: 10,
						DestinationLabels: MatcherConfig{
							Eq: map[string]string{"AZ": "${source.AZ}", "RESOURCE_GROUP": "rg1-1"},
						},
					},
					{
						Name:   "other-local-rgs",
						Weight: 100,
						DestinationLabels: MatcherConfig{
							Eq:    map[string]string{"AZ": "${source.AZ}"},
							NotIn: map[string][]string{"RESOURCE_GROUP": {"rg1-0", "rg1-1"}},
						},
					},
					{
						Name:   "local-shadowed",
						Weight: 1,
						DestinationLabels: MatcherConfig{
							Eq: map[string]string{"AZ": "${source.AZ}"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	routed := policy.Route(Labels{"AZ": "az1"}, []Candidate{
		{NodeID: 1, Labels: Labels{"AZ": "az1", "RESOURCE_GROUP": "rg1-0"}},
		{NodeID: 2, Labels: Labels{"AZ": "az1", "RESOURCE_GROUP": "rg1-1"}},
		{NodeID: 3, Labels: Labels{"AZ": "az1", "RESOURCE_GROUP": "rg2-0"}},
		{NodeID: 4, Labels: Labels{"AZ": "az1", "RESOURCE_GROUP": "rg3-0"}},
		{NodeID: 5, Labels: Labels{"AZ": "az2", "RESOURCE_GROUP": "rg9-0"}},
	})

	require.Equal(t, []WeightedCandidate{
		{NodeID: 1, Weight: 90},
		{NodeID: 2, Weight: 10},
		{NodeID: 3, Weight: 100},
		{NodeID: 4, Weight: 100},
	}, routed)
}

func TestPolicyIgnoresZeroWeightRoutes(t *testing.T) {
	policy, err := Compile(PolicyConfig{
		Rules: []RuleConfig{
			{
				Name: "zero-weight",
				Match: RuleMatchConfig{
					SourceLabels: MatcherConfig{Any: true},
				},
				Routes: []RouteConfig{
					{
						Name:              "disabled",
						Weight:            0,
						DestinationLabels: MatcherConfig{Any: true},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	require.Empty(t, policy.Route(Labels{}, []Candidate{
		{NodeID: 1, Labels: Labels{"AZ": "az1"}},
	}))

	result := policy.RouteWithResult(Labels{}, []Candidate{
		{NodeID: 1, Labels: Labels{"AZ": "az1"}},
	})
	require.Equal(t, "no_candidate", result.FallbackReason)
	require.Empty(t, result.Candidates)
}
