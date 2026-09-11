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
	"regexp"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// maxRuleNameLength bounds rule names so the rule_name metric label stays
// bounded under repeated hot reloads.
const maxRuleNameLength = 64

// ruleNameCharset restricts rule names to metric-label-friendly characters.
var ruleNameCharset = regexp.MustCompile(`^[0-9A-Za-z_.-]+$`)

type Policy struct {
	rules []rule
}

type rule struct {
	name              string
	sourceLabelsMatch *Matcher
	routes            []route
}

type route struct {
	name                   string
	weight                 int
	destinationLabelsMatch *Matcher
}

func Compile(cfg PolicyConfig) (*Policy, error) {
	p := &Policy{
		rules: make([]rule, 0, len(cfg.Rules)),
	}
	seenRuleNames := make(map[string]struct{}, len(cfg.Rules))
	for _, ruleCfg := range cfg.Rules {
		if err := validateRuleName(ruleCfg.Name, seenRuleNames); err != nil {
			return nil, err
		}
		seenRuleNames[ruleCfg.Name] = struct{}{}
		if len(ruleCfg.Routes) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("rule %q has no routes", ruleCfg.Name)
		}
		sourceLabelsMatch, err := CompileMatcher(ruleCfg.Match.SourceLabels)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "compile rule %q source labels", ruleCfg.Name)
		}
		if !sourceLabelsMatch.any && !hasMatcherConditions(ruleCfg.Match.SourceLabels) {
			return nil, merr.WrapErrParameterInvalidMsg("rule %q source labels matcher has no conditions, use any: true to match all sources", ruleCfg.Name)
		}
		r := rule{
			name:              ruleCfg.Name,
			sourceLabelsMatch: sourceLabelsMatch,
			routes:            make([]route, 0, len(ruleCfg.Routes)),
		}
		for _, routeCfg := range ruleCfg.Routes {
			if routeCfg.Weight < 0 {
				return nil, merr.WrapErrParameterInvalidMsg("route %q has negative weight %d", routeCfg.Name, routeCfg.Weight)
			}
			destinationLabelsMatch, err := CompileMatcher(routeCfg.DestinationLabels)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "compile route %q destination labels", routeCfg.Name)
			}
			if !destinationLabelsMatch.any && !hasMatcherConditions(routeCfg.DestinationLabels) {
				return nil, merr.WrapErrParameterInvalidMsg("route %q destination labels matcher has no conditions, use any: true to match all candidates", routeCfg.Name)
			}
			r.routes = append(r.routes, route{
				name:                   routeCfg.Name,
				weight:                 routeCfg.Weight,
				destinationLabelsMatch: destinationLabelsMatch,
			})
		}
		p.rules = append(p.rules, r)
	}
	return p, nil
}

// validateRuleName enforces the constraints that keep the rule_name metric
// label bounded and unambiguous: names must be non-empty, unique, length
// bounded and restricted to metric-label-friendly characters.
func validateRuleName(name string, seen map[string]struct{}) error {
	if name == "" {
		return merr.WrapErrParameterInvalidMsg("rule name must not be empty")
	}
	if len(name) > maxRuleNameLength {
		return merr.WrapErrParameterInvalidMsg("rule name %q exceeds max length %d", name, maxRuleNameLength)
	}
	if !ruleNameCharset.MatchString(name) {
		return merr.WrapErrParameterInvalidMsg("rule name %q contains unsupported characters, allowed: [0-9A-Za-z_.-]", name)
	}
	if _, ok := seen[name]; ok {
		return merr.WrapErrParameterInvalidMsg("duplicate rule name %q", name)
	}
	return nil
}

func (p *Policy) Route(source Labels, candidates []Candidate) []WeightedCandidate {
	return p.RouteWithResult(source, candidates).Candidates
}

func (p *Policy) RouteWithResult(source Labels, candidates []Candidate) RouteResult {
	if p == nil {
		return RouteResult{
			FallbackReason: "no_policy",
		}
	}
	matchedRule := false
	for _, rule := range p.rules {
		if !rule.sourceLabelsMatch.Match(source, source) {
			continue
		}
		matchedRule = true
		routed := rule.route(source, candidates)
		if len(routed) > 0 {
			return RouteResult{
				Candidates: routed,
				RuleName:   rule.name,
			}
		}
	}
	fallbackReason := "no_matching_rule"
	if matchedRule {
		fallbackReason = "no_candidate"
	}
	return RouteResult{
		FallbackReason: fallbackReason,
	}
}

func (r rule) route(source Labels, candidates []Candidate) []WeightedCandidate {
	routed := make([]WeightedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		for _, route := range r.routes {
			if !route.destinationLabelsMatch.Match(source, candidate.Labels) {
				continue
			}
			if route.weight > 0 {
				routed = append(routed, WeightedCandidate{
					NodeID: candidate.NodeID,
					Weight: route.weight,
				})
			}
			break
		}
	}
	return routed
}
