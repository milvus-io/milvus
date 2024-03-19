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

package ratelimitutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestRateLimiterNode_AddAndGetChild(t *testing.T) {
	rln := NewRateLimiterNode(internalpb.RateScope_Cluster)
	child := NewRateLimiterNode(internalpb.RateScope_Cluster)

	// Positive test case
	rln.AddChild(1, child)
	if rln.GetChild(1) != child {
		t.Error("AddChild did not add the child correctly")
	}

	// Negative test case
	invalidChild := &RateLimiterNode{}
	rln.AddChild(2, child)
	if rln.GetChild(2) == invalidChild {
		t.Error("AddChild added an invalid child")
	}
}

func TestTraverseRateLimiterTree(t *testing.T) {
	limiters := typeutil.NewConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter]()
	limiters.Insert(internalpb.RateType_DDLCollection, ratelimitutil.NewLimiter(ratelimitutil.Inf, 0))
	quotaStates := typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]()
	quotaStates.Insert(milvuspb.QuotaState_DenyToWrite, commonpb.ErrorCode_ForceDeny)

	root := NewRateLimiterNode(internalpb.RateScope_Cluster)
	root.SetLimiters(limiters)
	root.SetQuotaStates(quotaStates)

	// Add a child to the root node
	child := NewRateLimiterNode(internalpb.RateScope_Cluster)
	child.SetLimiters(limiters)
	child.SetQuotaStates(quotaStates)
	root.AddChild(123, child)

	// Add a child to the root node
	child2 := NewRateLimiterNode(internalpb.RateScope_Cluster)
	child2.SetLimiters(limiters)
	child2.SetQuotaStates(quotaStates)
	child.AddChild(123, child2)

	// Positive test case for fn1
	var fn1Count int
	fn1 := func(rateType internalpb.RateType, limiter *ratelimitutil.Limiter) bool {
		fn1Count++
		return true
	}

	// Negative test case for fn2
	var fn2Count int
	fn2 := func(state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
		fn2Count++
		return true
	}

	// Call TraverseRateLimiterTree with fn1 and fn2
	TraverseRateLimiterTree(root, fn1, fn2)

	assert.Equal(t, 3, fn1Count)
	assert.Equal(t, 3, fn2Count)
}
