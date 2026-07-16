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

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

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
	for _, ruleCfg := range cfg.Rules {
		sourceLabelsMatch, err := CompileMatcher(ruleCfg.Match.SourceLabels)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "compile rule %q source labels", ruleCfg.Name)
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

func (p *Policy) Route(source Labels, candidates []Candidate) []WeightedCandidate {
	if p == nil {
		return nil
	}
	for _, rule := range p.rules {
		if !rule.sourceLabelsMatch.Match(source, source) {
			continue
		}
		routed := rule.route(source, candidates)
		if len(routed) > 0 {
			return routed
		}
	}
	return nil
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
