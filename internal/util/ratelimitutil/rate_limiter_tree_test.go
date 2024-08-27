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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	fn2 := func(node *RateLimiterNode, state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
		fn2Count++
		return true
	}

	// Call TraverseRateLimiterTree with fn1 and fn2
	TraverseRateLimiterTree(root, fn1, fn2)

	assert.Equal(t, 3, fn1Count)
	assert.Equal(t, 3, fn2Count)
}

func TestRateLimiterNodeCancel(t *testing.T) {
	t.Run("cancel not exist type", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		limitNode.Cancel(internalpb.RateType_DMLInsert, 10)
	})
}

func TestRateLimiterNodeCheck(t *testing.T) {
	t.Run("quota exceed", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		limitNode.limiters.Insert(internalpb.RateType_DMLInsert, ratelimitutil.NewLimiter(0, 0))
		limitNode.quotaStates.Insert(milvuspb.QuotaState_DenyToWrite, commonpb.ErrorCode_ForceDeny)
		err := limitNode.Check(internalpb.RateType_DMLInsert, 10)
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
	})

	t.Run("rate limit", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		limitNode.limiters.Insert(internalpb.RateType_DMLInsert, ratelimitutil.NewLimiter(0.01, 0.01))
		{
			err := limitNode.Check(internalpb.RateType_DMLInsert, 1)
			assert.NoError(t, err)
		}
		{
			err := limitNode.Check(internalpb.RateType_DMLInsert, 1)
			assert.True(t, errors.Is(err, merr.ErrServiceRateLimit))
		}
	})
}

func TestRateLimiterNodeGetQuotaExceededError(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		limitNode.quotaStates.Insert(milvuspb.QuotaState_DenyToWrite, commonpb.ErrorCode_ForceDeny)
		err := limitNode.GetQuotaExceededError(internalpb.RateType_DMLInsert)
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
		// reference: ratelimitutil.GetQuotaErrorString(errCode)
		assert.True(t, strings.Contains(err.Error(), "disabled"))
	})

	t.Run("read", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		limitNode.quotaStates.Insert(milvuspb.QuotaState_DenyToRead, commonpb.ErrorCode_ForceDeny)
		err := limitNode.GetQuotaExceededError(internalpb.RateType_DQLSearch)
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
		// reference: ratelimitutil.GetQuotaErrorString(errCode)
		assert.True(t, strings.Contains(err.Error(), "disabled"))
	})

	t.Run("unknown", func(t *testing.T) {
		limitNode := NewRateLimiterNode(internalpb.RateScope_Cluster)
		err := limitNode.GetQuotaExceededError(internalpb.RateType_DDLCompaction)
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
		assert.True(t, strings.Contains(err.Error(), "rate type"))
	})
}

func TestRateLimiterTreeClearInvalidLimiterNode(t *testing.T) {
	root := NewRateLimiterNode(internalpb.RateScope_Cluster)
	tree := NewRateLimiterTree(root)

	generateNodeFFunc := func(level internalpb.RateScope) func() *RateLimiterNode {
		return func() *RateLimiterNode {
			return NewRateLimiterNode(level)
		}
	}

	tree.GetOrCreatePartitionLimiters(1, 10, 100,
		generateNodeFFunc(internalpb.RateScope_Database),
		generateNodeFFunc(internalpb.RateScope_Collection),
		generateNodeFFunc(internalpb.RateScope_Partition),
	)
	tree.GetOrCreatePartitionLimiters(1, 10, 200,
		generateNodeFFunc(internalpb.RateScope_Database),
		generateNodeFFunc(internalpb.RateScope_Collection),
		generateNodeFFunc(internalpb.RateScope_Partition),
	)
	tree.GetOrCreatePartitionLimiters(1, 20, 300,
		generateNodeFFunc(internalpb.RateScope_Database),
		generateNodeFFunc(internalpb.RateScope_Collection),
		generateNodeFFunc(internalpb.RateScope_Partition),
	)
	tree.GetOrCreatePartitionLimiters(2, 30, 400,
		generateNodeFFunc(internalpb.RateScope_Database),
		generateNodeFFunc(internalpb.RateScope_Collection),
		generateNodeFFunc(internalpb.RateScope_Partition),
	)

	assert.Equal(t, 2, root.GetChildren().Len())
	assert.Equal(t, 2, root.GetChild(1).GetChildren().Len())
	assert.Equal(t, 2, root.GetChild(1).GetChild(10).GetChildren().Len())

	tree.ClearInvalidLimiterNode(&proxypb.LimiterNode{
		Children: map[int64]*proxypb.LimiterNode{
			1: {
				Children: map[int64]*proxypb.LimiterNode{
					10: {
						Children: map[int64]*proxypb.LimiterNode{
							100: {},
						},
					},
				},
			},
		},
	})

	assert.Equal(t, 1, root.GetChildren().Len())
	assert.Equal(t, 1, root.GetChild(1).GetChildren().Len())
	assert.Equal(t, 1, root.GetChild(1).GetChild(10).GetChildren().Len())
}
