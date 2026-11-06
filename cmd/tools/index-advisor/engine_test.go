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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingRule notes its name and returns a fixed verdict.
type recordingRule struct {
	name    string
	verdict verdict
	propose bool
}

func (r recordingRule) apply(_ *fieldContext, adv *advice) verdict {
	adv.note(r.name)
	if r.propose {
		adv.add(candidate{IndexType: r.name})
	}
	return r.verdict
}

func TestEngineRunsRulesInOrderUntilStop(t *testing.T) {
	e := &ruleEngine{rules: []rule{
		recordingRule{name: "first", verdict: proceed},
		recordingRule{name: "second", verdict: stop},
		recordingRule{name: "third", verdict: proceed},
	}}
	adv := e.evaluate(testField(t, "price", "price > 1"))
	assert.Equal(t, []string{"first", "second"}, adv.notes)
}

func TestEngineAction(t *testing.T) {
	proposing := &ruleEngine{rules: []rule{recordingRule{name: "X", propose: true}}}
	silent := &ruleEngine{rules: []rule{recordingRule{name: "nothing"}}}

	fc := testField(t, "price", "price > 1")
	assert.Equal(t, actionValidate, proposing.evaluate(fc).action)
	assert.Equal(t, actionNoAction, silent.evaluate(fc).action)

	fc.existing = []indexSnapshot{{IndexName: "p", IndexType: indexSort}}
	assert.Equal(t, actionReviewExisting, proposing.evaluate(fc).action)
	assert.Equal(t, actionReviewExisting, silent.evaluate(fc).action, "an existing index is reviewed even with no candidate")
}

// The default chain ends at the first rule that excludes a column, so no later
// rule proposes a candidate for it.
func TestDefaultEngine(t *testing.T) {
	e := defaultRuleEngine()

	adv := e.evaluate(testField(t, "id", "id in [1]"))
	assert.Equal(t, actionNoAction, adv.action)
	assert.Equal(t, []string{"Primary key: Milvus serves ==, IN and range filters on it through its built-in primary-key lookup, and this tool proposes no index for it."}, adv.notes)

	adv = e.evaluate(testField(t, `dyn["a"]`, `dyn["a"] == 1`))
	assert.Equal(t, actionValidate, adv.action, "a nested dynamic-field path gets candidates")

	fc := testField(t, "city", `city == "a"`)
	fc.stats.distinctCount = int64p(3)
	adv = e.evaluate(fc)
	require.Len(t, adv.candidates, 2)
	assert.Equal(t, []string{indexAuto, indexBitmap}, []string{adv.candidates[0].IndexType, adv.candidates[1].IndexType}, "baseline first, then alternatives in rule order")

	fc.existing = []indexSnapshot{{IndexName: "c", IndexType: indexBitmap}}
	adv = e.evaluate(fc)
	assert.Equal(t, actionReviewExisting, adv.action)
	assert.Len(t, adv.candidates, 1, "the existing BITMAP is not proposed again")
}
