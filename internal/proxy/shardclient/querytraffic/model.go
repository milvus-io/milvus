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

type Labels map[string]string

type Candidate struct {
	NodeID int64
	Labels Labels
}

type WeightedCandidate struct {
	NodeID int64
	Weight int
}

type PolicyConfig struct {
	Rules []RuleConfig `json:"rules" yaml:"rules"`
}

type RuleConfig struct {
	Name   string          `json:"name" yaml:"name"`
	Match  RuleMatchConfig `json:"match" yaml:"match"`
	Routes []RouteConfig   `json:"routes" yaml:"routes"`
}

type RuleMatchConfig struct {
	SourceLabels MatcherConfig `json:"sourceLabels" yaml:"sourceLabels"`
}

type RouteConfig struct {
	Name              string        `json:"name" yaml:"name"`
	Weight            int           `json:"weight" yaml:"weight"`
	DestinationLabels MatcherConfig `json:"destinationLabels" yaml:"destinationLabels"`
}

type MatcherConfig struct {
	Any       bool                `json:"any" yaml:"any"`
	Exists    []string            `json:"exists" yaml:"exists"`
	NotExists []string            `json:"not_exists" yaml:"not_exists"`
	Eq        map[string]string   `json:"eq" yaml:"eq"`
	Ne        map[string]string   `json:"ne" yaml:"ne"`
	In        map[string][]string `json:"in" yaml:"in"`
	NotIn     map[string][]string `json:"not_in" yaml:"not_in"`
	Match     map[string]string   `json:"match" yaml:"match"`
	NotMatch  map[string]string   `json:"not_match" yaml:"not_match"`
}
