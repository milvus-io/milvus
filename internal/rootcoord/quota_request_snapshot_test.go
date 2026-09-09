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

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

func TestQuotaRequestUsesOneProxyCount(t *testing.T) {
	manager := proxyutil.NewMockProxyClientManagerInterface(t)
	manager.EXPECT().GetProxyCount().Return(2).Once()
	manager.EXPECT().GetProxyCount().Return(4).Once()
	q := NewQuotaCenter(manager, nil, nil, nil)
	t.Cleanup(q.cancel)
	makeNode := func(scope internalpb.RateScope) func() *rlinternal.RateLimiterNode {
		return func() *rlinternal.RateLimiterNode { return rlinternal.NewRateLimiterNode(scope) }
	}
	leaf := q.rateLimiter.GetOrCreatePartitionLimiters(10, 20, 30,
		makeNode(internalpb.RateScope_Database), makeNode(internalpb.RateScope_Collection), makeNode(internalpb.RateScope_Partition))
	nodes := []*rlinternal.RateLimiterNode{q.rateLimiter.GetRootLimiters(), q.rateLimiter.GetDatabaseLimiters(10), q.rateLimiter.GetCollectionLimiters(10, 20), leaf}
	for _, node := range nodes {
		rate := ratelimitutil.NewLimiter(100, 0)
		rate.SetHasUpdated(true)
		node.GetLimiters().Insert(internalpb.RateType_DMLInsert, rate)
		node.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite,
			&rlinternal.QuotaStateInfo{ErrorCode: commonpb.ErrorCode_ForceDeny, Reason: "snapshot test"})
	}
	check := func(request *proxypb.SetRatesRequest, expected float64) {
		t.Helper()
		wireNodes := []*proxypb.LimiterNode{
			request.RootLimiter, request.RootLimiter.Children[10],
			request.RootLimiter.Children[10].Children[20], request.RootLimiter.Children[10].Children[20].Children[30],
		}
		for _, node := range wireNodes {
			require.Equal(t, []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: expected}}, node.Limiter.Rates)
			require.Equal(t, []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite}, node.Limiter.States)
			require.Equal(t, []commonpb.ErrorCode{commonpb.ErrorCode_ForceDeny}, node.Limiter.Codes)
			require.Equal(t, []string{"snapshot test"}, node.Limiter.Reasons)
		}
		require.Nil(t, wireNodes[3].Children)
	}
	first := q.toRatesRequest()
	check(first, 50)
	saved := proto.Clone(first)
	for _, node := range nodes {
		limiter, _ := node.GetLimiters().Get(internalpb.RateType_DMLInsert)
		limiter.SetLimit(200)
	}
	second := q.toRatesRequest()
	check(second, 50)
	require.True(t, proto.Equal(saved, first), "later calculations must not alias published rates")
	encoded, err := proto.Marshal(first)
	require.NoError(t, err)
	var decoded proxypb.SetRatesRequest
	require.NoError(t, proto.Unmarshal(encoded, &decoded))
	require.True(t, proto.Equal(first, &decoded))
}

func TestQuotaRequestWithoutProxies(t *testing.T) {
	manager := proxyutil.NewMockProxyClientManagerInterface(t)
	manager.EXPECT().GetProxyCount().Return(0).Once()
	q := NewQuotaCenter(manager, nil, nil, nil)
	t.Cleanup(q.cancel)
	require.Nil(t, q.toRatesRequest().RootLimiter.Limiter)
}
