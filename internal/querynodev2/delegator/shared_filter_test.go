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

package delegator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// termPredicate builds a distinguishable predicate: two calls with the same
// field produce byte-identical plans, two calls with different fields do not.
func termPredicate(fieldID int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: fieldID, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}},
			},
		},
	}
}

// subReq builds a sub-request whose plan carries `predicate`. A nil predicate
// means "no filter", which cannot be shared.
func subReq(t *testing.T, vectorFieldID int64, predicate *planpb.Expr, opts ...func(*internalpb.SubSearchRequest, *planpb.QueryInfo)) *internalpb.SubSearchRequest {
	t.Helper()
	queryInfo := &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}"}
	sub := &internalpb.SubSearchRequest{
		Nq:      1,
		Topk:    10,
		FieldId: vectorFieldID,
	}
	for _, opt := range opts {
		opt(sub, queryInfo)
	}
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId:    vectorFieldID,
				Predicates: predicate,
				QueryInfo:  queryInfo,
			},
		},
	}
	blob, err := proto.Marshal(plan)
	require.NoError(t, err)
	sub.SerializedExprPlan = blob
	return sub
}

func withIterativeFilter(_ *internalpb.SubSearchRequest, qi *planpb.QueryInfo) {
	qi.Hints = iterativeFilterHint
}

func withIgnoreGrowing(sub *internalpb.SubSearchRequest, _ *planpb.QueryInfo) {
	sub.IgnoreGrowing = true
}

func withPartitions(ids ...int64) func(*internalpb.SubSearchRequest, *planpb.QueryInfo) {
	return func(sub *internalpb.SubSearchRequest, _ *planpb.QueryInfo) {
		sub.PartitionIDs = ids
	}
}

func enableSharedFilter(t *testing.T, enabled bool) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.HybridSearchSharedFilterEnabled.Key
	val := "false"
	if enabled {
		val = "true"
	}
	require.NoError(t, paramtable.Get().Save(key, val))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func TestGroupSubReqsBySharedFilter(t *testing.T) {
	pred := termPredicate(100)
	other := termPredicate(101)

	cases := []struct {
		name    string
		enabled bool
		subReqs []*internalpb.SubSearchRequest
		want    [][]int
	}{
		{
			name:    "identical predicate groups",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred), subReq(t, 2, pred),
			},
			want: [][]int{{0, 1}},
		},
		{
			name:    "different predicate does not group",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred), subReq(t, 2, other),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "no predicate cannot be shared",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, nil), subReq(t, 2, nil),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "iterative filter cannot be shared",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred, withIterativeFilter), subReq(t, 2, pred, withIterativeFilter),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "IgnoreGrowing splits an otherwise identical pair",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred), subReq(t, 2, pred, withIgnoreGrowing),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "PartitionIDs do NOT split: they are not part of the key",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred, withPartitions(1, 2)), subReq(t, 2, pred, withPartitions(2, 1, 3)),
			},
			want: [][]int{{0, 1}},
		},
		{
			name:    "disabled gives every sub-request its own group",
			enabled: false,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred), subReq(t, 2, pred),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "single sub-request",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{subReq(t, 1, pred)},
			want:    [][]int{{0}},
		},
		{
			name:    "groups keep the caller's sub-request order",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, other), subReq(t, 2, pred), subReq(t, 3, other), subReq(t, 4, pred),
			},
			want: [][]int{{0, 2}, {1, 3}},
		},
		{
			name:    "three branches on one predicate form one group",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, pred), subReq(t, 2, pred), subReq(t, 3, pred),
			},
			want: [][]int{{0, 1, 2}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enableSharedFilter(t, tc.enabled)
			got := groupSubReqsBySharedFilter(context.Background(), 1, tc.subReqs)
			assert.Equal(t, tc.want, got)

			// Whatever the grouping, every sub-request must appear exactly once:
			// the response demux maps a group's branch positions back to these.
			seen := make(map[int]int)
			for _, g := range got {
				for _, idx := range g {
					seen[idx]++
				}
			}
			assert.Len(t, seen, len(tc.subReqs))
			for idx, n := range seen {
				assert.Equalf(t, 1, n, "sub-request %d appeared %d times", idx, n)
			}
		})
	}
}

func TestBuildSharedFilterSearchRequest(t *testing.T) {
	pred := termPredicate(100)
	subReqs := []*internalpb.SubSearchRequest{
		subReq(t, 1, pred), subReq(t, 2, pred), subReq(t, 3, pred),
	}
	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:   42,
			OutputFieldsId: []int64{100},
		},
		DmlChannels:     []string{"ch-0"},
		TotalChannelNum: 1,
	}

	t.Run("branch 0 lands in req, the rest in extra", func(t *testing.T) {
		got := buildSharedFilterSearchRequest(req, subReqs, []int{1, 0, 2}, 555)
		// group[0] is the head, so req carries sub-request 1.
		assert.Equal(t, subReqs[1].GetFieldId(), got.GetReq().GetFieldId())
		require.Len(t, got.GetExtraFilterSharingReqs(), 2)
		assert.Equal(t, subReqs[0].GetFieldId(), got.GetExtraFilterSharingReqs()[0].GetFieldId())
		assert.Equal(t, subReqs[2].GetFieldId(), got.GetExtraFilterSharingReqs()[1].GetFieldId())
		// envelope fields survive
		assert.EqualValues(t, 42, got.GetReq().GetCollectionID())
		assert.Equal(t, []int64{100}, got.GetReq().GetOutputFieldsId())
	})

	t.Run("a single-member group carries no extras", func(t *testing.T) {
		got := buildSharedFilterSearchRequest(req, subReqs, []int{0}, 555)
		assert.Empty(t, got.GetExtraFilterSharingReqs())
	})

	t.Run("mvcc falls back to tSafe only when unset", func(t *testing.T) {
		got := buildSharedFilterSearchRequest(req, subReqs, []int{0}, 555)
		assert.EqualValues(t, 555, got.GetReq().GetMvccTimestamp())

		withMvcc := &querypb.SearchRequest{Req: &internalpb.SearchRequest{MvccTimestamp: 7}}
		got = buildSharedFilterSearchRequest(withMvcc, subReqs, []int{0}, 555)
		assert.EqualValues(t, 7, got.GetReq().GetMvccTimestamp())
	})

	t.Run("extras are cloned, so per-branch rewrites do not leak back", func(t *testing.T) {
		got := buildSharedFilterSearchRequest(req, subReqs, []int{0, 1}, 555)
		// sd.search rewrites a branch's placeholder group and plan in place.
		got.ExtraFilterSharingReqs[0].PlaceholderGroup = []byte("rewritten")
		got.ExtraFilterSharingReqs[0].SerializedExprPlan = []byte("rewritten")
		assert.Nil(t, subReqs[1].GetPlaceholderGroup(), "caller's sub-request was mutated")
		assert.NotEqual(t, []byte("rewritten"), subReqs[1].GetSerializedExprPlan())
	})
}

func TestDemuxSharedFilterResults(t *testing.T) {
	workerResult := func(branches int, serviceTime int64) *internalpb.SearchResults {
		res := &internalpb.SearchResults{
			CostAggregation:   &internalpb.CostAggregation{ServiceTime: serviceTime, TotalRelatedDataSize: 1000},
			ChannelsMvcc:      map[string]uint64{"ch-0": 9},
			IsTopkReduce:      true,
			ScannedTotalBytes: 64,
		}
		for i := 0; i < branches; i++ {
			res.SubResults = append(res.SubResults, &internalpb.SubSearchResults{
				ReqIndex:   int64(i),
				MetricType: "L2",
				TopK:       int64(10 + i),
			})
		}
		return res
	}

	t.Run("splits by req_index across workers", func(t *testing.T) {
		got, err := demuxSharedFilterResults([]*internalpb.SearchResults{
			workerResult(2, 5), workerResult(2, 7),
		}, 2)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for branch, results := range got {
			require.Len(t, results, 2, "branch %d", branch)
			for _, r := range results {
				assert.EqualValues(t, 10+branch, r.GetTopK())
			}
		}
	})

	t.Run("cost lands on branch 0 only, and is never nil", func(t *testing.T) {
		// mergeRequestCost dereferences every entry without a nil check, and
		// TotalRelatedDataSize is summed across sub-results, so branch 0 takes
		// the real cost and the rest take a zeroed -- not nil -- one.
		got, err := demuxSharedFilterResults([]*internalpb.SearchResults{workerResult(3, 5)}, 3)
		require.NoError(t, err)
		for branch, results := range got {
			for _, r := range results {
				require.NotNil(t, r.GetCostAggregation(), "branch %d has a nil cost", branch)
				if branch == 0 {
					assert.EqualValues(t, 5, r.GetCostAggregation().GetServiceTime())
					assert.EqualValues(t, 1000, r.GetCostAggregation().GetTotalRelatedDataSize())
					assert.True(t, r.GetIsTopkReduce())
					assert.EqualValues(t, 64, r.GetScannedTotalBytes())
				} else {
					assert.Zero(t, r.GetCostAggregation().GetServiceTime())
					assert.Zero(t, r.GetCostAggregation().GetTotalRelatedDataSize())
					assert.False(t, r.GetIsTopkReduce())
					assert.Zero(t, r.GetScannedTotalBytes())
				}
			}
		}
	})

	t.Run("rejects a worker response with the wrong branch count", func(t *testing.T) {
		_, err := demuxSharedFilterResults([]*internalpb.SearchResults{workerResult(2, 5)}, 3)
		assert.Error(t, err)
	})

	t.Run("rejects an out-of-range branch index", func(t *testing.T) {
		bad := workerResult(2, 5)
		bad.SubResults[1].ReqIndex = 9
		_, err := demuxSharedFilterResults([]*internalpb.SearchResults{bad}, 2)
		assert.Error(t, err)
	})

	t.Run("tolerates a nil worker response", func(t *testing.T) {
		got, err := demuxSharedFilterResults([]*internalpb.SearchResults{nil, workerResult(2, 5)}, 2)
		require.NoError(t, err)
		assert.Len(t, got[0], 1)
		assert.Len(t, got[1], 1)
	})
}
