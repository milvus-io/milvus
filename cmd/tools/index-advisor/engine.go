// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// rule is one step of the recommendation chain. Each rule handles one concern
// (the primary key, the AUTOINDEX baseline, BITMAP, ...): it reads the field
// context, may add candidates or notes to the advice, and returns stop to end
// the chain for the column.
type rule interface {
	apply(fc *fieldContext, adv *advice) verdict
}

// verdict tells the engine whether to pass the column to the next rule.
type verdict int

const (
	proceed verdict = iota
	stop
)

// ruleEngine runs its rules on each column in order, highest priority first,
// until one returns stop, then decides the column's action.
type ruleEngine struct {
	rules []rule
}

// defaultRuleEngine returns the rules this tool applies, in priority order:
// the rules that exclude a column, then the AUTOINDEX baseline, then the
// evidence-gated alternatives, then the check against existing indexes and
// the JSON shredding note.
func defaultRuleEngine() *ruleEngine {
	return &ruleEngine{rules: []rule{
		primaryKeyRule{},
		unfilteredColumnRule{},
		jsonCastTypeRule{},
		indexableOperatorRule{},
		baselineAutoIndexRule{},
		bitmapRule{},
		ngramRule{},
		jsonArrayRule{},
		existingIndexRule{},
		jsonShreddingRule{},
	}}
}

// evaluate runs the chain on fc. A column with an existing index is always
// reviewed, whatever the chain found, because its presence says nothing about
// whether requests use it.
func (e *ruleEngine) evaluate(fc *fieldContext) advice {
	var adv advice
	for _, r := range e.rules {
		if r.apply(fc, &adv) == stop {
			break
		}
	}
	switch {
	case len(fc.existing) > 0:
		adv.action = actionReviewExisting
	case len(adv.candidates) == 0:
		adv.action = actionNoAction
	default:
		adv.action = actionValidate
	}
	return adv
}
