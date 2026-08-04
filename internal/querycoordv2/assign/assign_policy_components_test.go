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

package assign

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TestHasEnoughBenefitForNodes_NegativeTargetPriority reproduces a case where the target
// node's priority (currentScore - assignedScore) is negative, which used to make the
// ScoreUnbalanceTolerationFactor check a no-op regardless of how small the actual diff was.
func TestHasEnoughBenefitForNodes_NegativeTargetPriority(t *testing.T) {
	paramtable.Init()
	evaluator := &commonScoreBasedBenefitEvaluator{}

	// Two nodes with ~6.3M assigned quota each, but current load noise pushes their
	// priorities apart by ~4%, well under the default 5% toleration.
	source := NewNodeItem(6417000, 1)
	source.AssignedScore = 6350000 // priority = +67000

	target := NewNodeItem(6164000, 2)
	target.AssignedScore = 6350000 // priority = -186000 (negative)

	// diff = |67000 - (-186000)| = 253000, target assigned score * 5% = 317500 -> should be blocked
	assert.False(t, evaluator.HasEnoughBenefitForNodes(&source, &target, 59000))

	// A genuine imbalance (diff > 5% of assigned score) should still trigger a move
	bigSource := NewNodeItem(7000000, 3)
	bigSource.AssignedScore = 6350000 // priority = +650000 (~10%)

	assert.True(t, evaluator.HasEnoughBenefitForNodes(&bigSource, &target, 59000))
}
