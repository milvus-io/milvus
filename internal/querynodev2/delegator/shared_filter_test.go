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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// subReq builds a sub-request carrying the proxy's filter-sharing hint.
// Equal non-zero groups mean identical predicates; 0 means the proxy found
// nothing to share -- no predicate, or an iterative filter. Which of those two
// it was is not recoverable here, and is covered by the proxy's own tests.
//
// The serialized plan still travels with the sub-request: grouping no longer
// reads it, but buildSharedFilterSearchRequest carries it to the worker.
func subReq(t *testing.T, vectorFieldID int64, group int32, opts ...func(*internalpb.SubSearchRequest)) *internalpb.SubSearchRequest {
	t.Helper()
	sub := &internalpb.SubSearchRequest{
		Nq:                 1,
		Topk:               10,
		FieldId:            vectorFieldID,
		FilterSharingGroup: group,
	}
	for _, opt := range opts {
		opt(sub)
	}
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId:   vectorFieldID,
				QueryInfo: &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}"},
			},
		},
	}
	blob, err := proto.Marshal(plan)
	require.NoError(t, err)
	sub.SerializedExprPlan = blob
	return sub
}

func withIgnoreGrowing(sub *internalpb.SubSearchRequest) {
	sub.IgnoreGrowing = true
}

func withPartitions(ids ...int64) func(*internalpb.SubSearchRequest) {
	return func(sub *internalpb.SubSearchRequest) {
		sub.PartitionIDs = ids
	}
}

func withNQ(nq int64) func(*internalpb.SubSearchRequest) {
	return func(sub *internalpb.SubSearchRequest) {
		sub.Nq = nq
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
	cases := []struct {
		name    string
		enabled bool
		subReqs []*internalpb.SubSearchRequest
		want    [][]int
	}{
		{
			name:    "same hint group shares",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 1),
			},
			want: [][]int{{0, 1}},
		},
		{
			name:    "different hint groups do not share",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 2),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "group 0 never shares",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 0), subReq(t, 2, 0),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "an unshareable sub-request does not join a shared group",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 0), subReq(t, 2, 1), subReq(t, 3, 1),
			},
			want: [][]int{{0}, {1, 2}},
		},
		{
			name:    "IgnoreGrowing splits an otherwise identical pair",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 1, withIgnoreGrowing),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "PartitionIDs do NOT split: they are not part of the key",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1, withPartitions(1, 2)), subReq(t, 2, 1, withPartitions(2, 1, 3)),
			},
			want: [][]int{{0, 1}},
		},
		{
			name:    "disabled gives every sub-request its own group",
			enabled: false,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 1),
			},
			want: [][]int{{0}, {1}},
		},
		{
			name:    "single sub-request",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{subReq(t, 1, 1)},
			want:    [][]int{{0}},
		},
		{
			name:    "groups keep the caller's sub-request order",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 2), subReq(t, 2, 1), subReq(t, 3, 2), subReq(t, 4, 1),
			},
			want: [][]int{{0, 2}, {1, 3}},
		},
		{
			name:    "three branches on one hint group form one group",
			enabled: true,
			subReqs: []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 1), subReq(t, 3, 1),
			},
			want: [][]int{{0, 1, 2}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enableSharedFilter(t, tc.enabled)
			got := groupSubReqsBySharedFilter(context.Background(), 1, nil, tc.subReqs)
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

func TestGroupSubReqsBySharedFilterNQBudget(t *testing.T) {
	const collectionID = int64(1)

	group := func(t *testing.T, maxNQ string, subReqs []*internalpb.SubSearchRequest) [][]int {
		t.Helper()
		enableSharedFilter(t, true)
		setParam(t, paramtable.Get().QueryNodeCfg.MaxGroupNQ.Key, maxNQ)
		return groupSubReqsBySharedFilter(context.Background(), collectionID, nil, subReqs)
	}

	t.Run("caps a 1024 branch group at the existing work unit", func(t *testing.T) {
		subReqs := make([]*internalpb.SubSearchRequest, 1024)
		for i := range subReqs {
			subReqs[i] = &internalpb.SubSearchRequest{Nq: 1, FilterSharingGroup: 1}
		}

		got := group(t, "64", subReqs)
		require.Len(t, got, 16)
		for groupIdx, indexes := range got {
			require.Len(t, indexes, 64)
			for branchIdx, index := range indexes {
				assert.Equal(t, groupIdx*64+branchIdx, index)
			}
		}
	})

	t.Run("exact fits share and an odd tail is budget limited", func(t *testing.T) {
		subReqs := []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(32)),
			subReq(t, 2, 1, withNQ(32)),
			subReq(t, 3, 1, withNQ(32)),
			subReq(t, 4, 1, withNQ(32)),
			subReq(t, 5, 1, withNQ(32)),
		}
		enableSharedFilter(t, true)
		setParam(t, paramtable.Get().QueryNodeCfg.MaxGroupNQ.Key, "64")
		budgetMetric := metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
			paramtable.GetStringNodeID(), strconv.FormatInt(collectionID, 10), sharedFilterFallbackNQBudget)
		noPeerMetric := metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
			paramtable.GetStringNodeID(), strconv.FormatInt(collectionID, 10), sharedFilterFallbackNoPeer)
		beforeBudget := testutil.ToFloat64(budgetMetric)
		beforeNoPeer := testutil.ToFloat64(noPeerMetric)

		assert.Equal(t, [][]int{{0, 1}, {2, 3}, {4}},
			groupSubReqsBySharedFilter(context.Background(), collectionID, nil, subReqs))
		assert.Equal(t, beforeBudget+1, testutil.ToFloat64(budgetMetric))
		assert.Equal(t, beforeNoPeer, testutil.ToFloat64(noPeerMetric),
			"the tail has matching peers and must not be reported as no_matching_peer")
	})

	t.Run("more than half the budget keeps every branch singleton", func(t *testing.T) {
		subReqs := []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(33)),
			subReq(t, 2, 1, withNQ(33)),
			subReq(t, 3, 1, withNQ(33)),
		}
		assert.Equal(t, [][]int{{0}, {1}, {2}}, group(t, "64", subReqs))
	})

	t.Run("split chunks stay ordered by their first member", func(t *testing.T) {
		subReqs := []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(2)),
			subReq(t, 2, 2),
			subReq(t, 3, 1, withNQ(2)),
			subReq(t, 4, 2),
			subReq(t, 5, 1),
		}
		assert.Equal(t, [][]int{{0}, {1, 3}, {2, 4}}, group(t, "3", subReqs))
	})

	t.Run("an oversized branch does not break later sharing", func(t *testing.T) {
		subReqs := []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(65)),
			subReq(t, 2, 1),
			subReq(t, 3, 1),
		}
		assert.Equal(t, [][]int{{0}, {1, 2}}, group(t, "64", subReqs))
	})

	t.Run("non-positive budget or NQ uses the singleton path", func(t *testing.T) {
		assert.Equal(t, [][]int{{0}, {1}}, group(t, "0", []*internalpb.SubSearchRequest{
			subReq(t, 1, 1), subReq(t, 2, 1),
		}))
		assert.Equal(t, [][]int{{0}, {1, 2}}, group(t, "64", []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(-1)), subReq(t, 2, 1), subReq(t, 3, 1),
		}))
	})

	t.Run("near MaxInt64 splits without overflowing", func(t *testing.T) {
		const maxInt64 = int64(9223372036854775807)
		assert.Equal(t, [][]int{{0}, {1}}, group(t, "9223372036854775807", []*internalpb.SubSearchRequest{
			subReq(t, 1, 1, withNQ(maxInt64)), subReq(t, 2, 1),
		}))
	})

	t.Run("a real singleton keeps the no-peer reason", func(t *testing.T) {
		enableSharedFilter(t, true)
		setParam(t, paramtable.Get().QueryNodeCfg.MaxGroupNQ.Key, "64")
		budgetMetric := metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
			paramtable.GetStringNodeID(), strconv.FormatInt(collectionID, 10), sharedFilterFallbackNQBudget)
		noPeerMetric := metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
			paramtable.GetStringNodeID(), strconv.FormatInt(collectionID, 10), sharedFilterFallbackNoPeer)
		beforeBudget := testutil.ToFloat64(budgetMetric)
		beforeNoPeer := testutil.ToFloat64(noPeerMetric)

		assert.Equal(t, [][]int{{0}, {1, 2}},
			groupSubReqsBySharedFilter(context.Background(), collectionID, nil, []*internalpb.SubSearchRequest{
				subReq(t, 1, 1), subReq(t, 2, 2), subReq(t, 3, 2),
			}))
		assert.Equal(t, beforeBudget, testutil.ToFloat64(budgetMetric))
		assert.Equal(t, beforeNoPeer+1, testutil.ToFloat64(noPeerMetric))
	})
}

func TestBuildSharedFilterSearchRequest(t *testing.T) {
	subReqs := []*internalpb.SubSearchRequest{
		subReq(t, 1, 1), subReq(t, 2, 1), subReq(t, 3, 1),
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

	t.Run("extras are copied, so per-branch rewrites do not leak back", func(t *testing.T) {
		got := buildSharedFilterSearchRequest(req, subReqs, []int{0, 1}, 555)
		// sd.search rewrites a branch's placeholder group and plan by
		// assigning new slices.
		got.ExtraFilterSharingReqs[0].PlaceholderGroup = []byte("rewritten")
		got.ExtraFilterSharingReqs[0].SerializedExprPlan = []byte("rewritten")
		assert.Nil(t, subReqs[1].GetPlaceholderGroup(), "caller's sub-request was mutated")
		assert.NotEqual(t, []byte("rewritten"), subReqs[1].GetSerializedExprPlan())
	})

	t.Run("the copy is shallow: it carries every field and shares the bytes", func(t *testing.T) {
		src := &internalpb.SubSearchRequest{
			Dsl: "a > 1", PlaceholderGroup: []byte("ph"), DslType: 1, SerializedExprPlan: []byte("plan"),
			Nq: 2, PartitionIDs: []int64{3}, Topk: 4, Offset: 5, MetricType: "IP", GroupByFieldId: 6,
			GroupSize: 7, FieldId: 8, IgnoreGrowing: true, AnalyzerName: "an", FilterSharingGroup: 9,
		}
		got := shallowCopySubSearchRequest(src)
		assert.True(t, proto.Equal(src, got), "a field was dropped by the copy")
		assert.Same(t, &src.PlaceholderGroup[0], &got.PlaceholderGroup[0], "the query vectors must not be duplicated")
		assert.Same(t, &src.SerializedExprPlan[0], &got.SerializedExprPlan[0])
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

// What the proxy hint removes, on the shape this design targets: a
// multi-kilobyte predicate, keyed once per sub-request and repeated on every
// shard the request touches.
//
// "hint" is what sharedFilterKeyOf does now. "unmarshal_and_digest" is what it
// did before: unmarshal the whole plan, re-marshal its predicate, hash it.
func BenchmarkSharedFilterKeyOf(b *testing.B) {
	// A term list roughly the size of the phrase predicates this targets.
	values := make([]*planpb.GenericValue, 0, 256)
	for i := 0; i < 256; i++ {
		values = append(values, &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{
				StringVal: fmt.Sprintf("term-%03d-%s", i, strings.Repeat("x", 12)),
			},
		})
	}
	predicate := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar},
				Values:     values,
			},
		},
	}
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId:    1,
				Predicates: predicate,
				QueryInfo:  &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}"},
			},
		},
	}
	blob, err := proto.Marshal(plan)
	require.NoError(b, err)
	b.Logf("serialized plan: %d bytes", len(blob))

	sub := &internalpb.SubSearchRequest{SerializedExprPlan: blob, FilterSharingGroup: 1}

	b.Run("hint", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, ok := sharedFilterKeyOf(sub); !ok {
				b.Fatal("expected a shareable sub-request")
			}
		}
	})

	b.Run("unmarshal_and_digest", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			decoded := &planpb.PlanNode{}
			if err := proto.Unmarshal(sub.GetSerializedExprPlan(), decoded); err != nil {
				b.Fatal(err)
			}
			raw, err := proto.Marshal(decoded.GetVectorAnns().GetPredicates())
			if err != nil {
				b.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			_ = hex.EncodeToString(digest[:])
		}
	})
}

// planWithPredicate serializes a VectorANNS plan that carries a predicate, so
// OptimizeSearchParams reports withFilter=true and actually invokes the hook.
func planWithPredicate(t *testing.T, topk int64) []byte {
	t.Helper()
	blob, err := proto.Marshal(&planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId:    1,
				VectorType: planpb.VectorType_FloatVector,
				Predicates: &planpb.Expr{},
				// GroupByFieldId is -1 when there is no group-by, as the proxy
				// emits it; the hook's optimize / recall paths key off `< 0`.
				QueryInfo: &planpb.QueryInfo{Topk: topk, MetricType: "L2", SearchParams: "{}", GroupByFieldId: -1},
			},
		},
	})
	require.NoError(t, err)
	return blob
}

func setParam(t *testing.T, key, val string) {
	t.Helper()
	require.NoError(t, paramtable.Get().Save(key, val))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

// The AutoIndex query hook must run for every branch, not just branch 0:
// OptimizeSearchParams only rewrites req.Req, and an extra branch left
// untouched would reach segcore with the client's raw search params.
func TestOptimizeSearchParamsRunsHookForEveryBranch(t *testing.T) {
	paramtable.Init()
	setParam(t, paramtable.Get().AutoIndexConfig.Enable.Key, "true")

	hook := mock_optimizers.NewMockQueryHook(t)
	var mu sync.Mutex
	var topks []int64
	hook.EXPECT().Run(mock.Anything).RunAndReturn(func(params map[string]any) error {
		mu.Lock()
		defer mu.Unlock()
		topks = append(topks, params[common.TopKKey].(int64))
		return nil
	})

	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID: 1, Topk: 200, SerializedExprPlan: planWithPredicate(t, 200),
		},
		ExtraFilterSharingReqs: []*internalpb.SubSearchRequest{
			{Topk: 100, SerializedExprPlan: planWithPredicate(t, 100)},
			{Topk: 50, SerializedExprPlan: planWithPredicate(t, 50)},
		},
	}

	sd := &shardDelegator{}
	_, err := sd.optimizeSearchParams(context.Background(), req, hook, 5)
	require.NoError(t, err)

	assert.ElementsMatch(t, []int64{200, 100, 50}, topks)
}

// The hook reads IsTopkReduce / IsRecallEvaluation as its input and overwrites
// the same fields with its output. Every branch must be asked with the
// request's input, not with whatever the previous branch's hook left behind.
func TestOptimizeSearchParamsFeedsEveryBranchTheRequestInput(t *testing.T) {
	paramtable.Init()
	setParam(t, paramtable.Get().AutoIndexConfig.Enable.Key, "true")
	setParam(t, paramtable.Get().AutoIndexConfig.EnableOptimize.Key, "true")

	type hookInput struct {
		withOptimize bool
		recallEval   bool
	}
	var mu sync.Mutex
	var inputs []hookInput
	hook := mock_optimizers.NewMockQueryHook(t)
	hook.EXPECT().Run(mock.Anything).RunAndReturn(func(params map[string]any) error {
		mu.Lock()
		defer mu.Unlock()
		inputs = append(inputs, hookInput{
			withOptimize: params[common.WithOptimizeKey].(bool),
			recallEval:   params[common.RecallEvalKey].(bool),
		})
		// Leave topk alone (no reduction, so branch 0's output IsTopkReduce
		// is false) and turn recall evaluation on (so branch 0's output
		// IsRecallEvaluation is true). Both outputs differ from the input;
		// a branch fed the output instead would show up below.
		params[common.RecallEvalKey] = true
		return nil
	})

	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID: 1, Topk: 100, IsTopkReduce: true, IsRecallEvaluation: false,
			SerializedExprPlan: planWithPredicate(t, 100),
		},
		ExtraFilterSharingReqs: []*internalpb.SubSearchRequest{
			{Topk: 100, SerializedExprPlan: planWithPredicate(t, 100)},
			{Topk: 100, SerializedExprPlan: planWithPredicate(t, 100)},
		},
	}

	sd := &shardDelegator{}
	optimized, err := sd.optimizeSearchParams(context.Background(), req, hook, 1)
	require.NoError(t, err)

	want := hookInput{withOptimize: true, recallEval: false}
	assert.Equal(t, []hookInput{want, want, want}, inputs, "each branch sees the request's own flags")
	// Outputs are OR-ed across branches: no branch reduced topk, every branch
	// asked for recall evaluation.
	assert.False(t, optimized.GetReq().GetIsTopkReduce())
	assert.True(t, optimized.GetReq().GetIsRecallEvaluation())
}

func TestOptimizeSearchParamsStartsBranchZeroWithExtras(t *testing.T) {
	if hardware.GetCPUNum() < 2 {
		t.Skip("the branch limiter needs two slots to demonstrate overlap")
	}
	paramtable.Init()
	setParam(t, paramtable.Get().AutoIndexConfig.Enable.Key, "true")

	started := make(chan int64, 2)
	release := make(chan struct{})
	hook := mock_optimizers.NewMockQueryHook(t)
	hook.EXPECT().Run(mock.Anything).RunAndReturn(func(params map[string]any) error {
		started <- params[common.TopKKey].(int64)
		<-release
		return nil
	})

	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:       1,
			Topk:               100,
			SerializedExprPlan: planWithPredicate(t, 100),
		},
		ExtraFilterSharingReqs: []*internalpb.SubSearchRequest{
			{Topk: 200, SerializedExprPlan: planWithPredicate(t, 200)},
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := (&shardDelegator{}).optimizeSearchParams(context.Background(), req, hook, 1)
		done <- err
	}()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	seen := make(map[int64]struct{}, 2)
	for len(seen) < 2 {
		select {
		case topk := <-started:
			seen[topk] = struct{}{}
		case <-timer.C:
			close(release)
			require.NoError(t, <-done)
			t.Fatal("branch 0 completed as a barrier before the extra branch started")
		}
	}
	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, map[int64]struct{}{100: {}, 200: {}}, seen)
}

func TestPrepareSharedFilterBranchFunctions(t *testing.T) {
	newReq := func() *querypb.SearchRequest {
		return &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 1, FieldId: 10},
			ExtraFilterSharingReqs: []*internalpb.SubSearchRequest{
				{FieldId: 20},
			},
		}
	}

	t.Run("starts branch zero with the extra and returns its avgdl", func(t *testing.T) {
		if hardware.GetCPUNum() < 2 {
			t.Skip("the branch limiter needs two slots to demonstrate overlap")
		}
		started := make(chan int64, 2)
		release := make(chan struct{})
		patch := mockey.Mock((*shardDelegator).prepareSearchFunction).
			To(func(_ *shardDelegator, _ context.Context, req *internalpb.SearchRequest) (float64, bool, error) {
				started <- req.GetFieldId()
				<-release
				if req.GetFieldId() == 10 {
					return 12.5, false, nil
				}
				return 0, false, nil
			}).Build()
		defer patch.UnPatch()

		type result struct {
			avgdl float64
			skip  bool
			err   error
		}
		done := make(chan result, 1)
		go func() {
			avgdl, skip, err := (&shardDelegator{}).prepareSharedFilterBranchFunctions(
				context.Background(), newReq())
			done <- result{avgdl: avgdl, skip: skip, err: err}
		}()

		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		seen := make(map[int64]struct{}, 2)
		for len(seen) < 2 {
			select {
			case fieldID := <-started:
				seen[fieldID] = struct{}{}
			case <-timer.C:
				close(release)
				<-done
				t.Fatal("branch 0 completed as a barrier before the extra branch started")
			}
		}
		close(release)
		got := <-done
		require.NoError(t, got.err)
		assert.False(t, got.skip)
		assert.Equal(t, 12.5, got.avgdl)
		assert.Equal(t, map[int64]struct{}{10: {}, 20: {}}, seen)
	})

	t.Run("a real error cancels an in-flight sibling", func(t *testing.T) {
		if hardware.GetCPUNum() < 2 {
			t.Skip("the branch limiter needs two slots to demonstrate cancellation")
		}
		wantErr := errors.New("extra preparation failed")
		branch0Started := make(chan struct{})
		branch0Canceled := make(chan error, 1)
		patch := mockey.Mock((*shardDelegator).prepareSearchFunction).
			To(func(_ *shardDelegator, ctx context.Context, req *internalpb.SearchRequest) (float64, bool, error) {
				switch req.GetFieldId() {
				case 10:
					close(branch0Started)
					<-ctx.Done()
					branch0Canceled <- ctx.Err()
					return 0, false, ctx.Err()
				case 20:
					<-branch0Started
					return 0, false, wantErr
				default:
					return 0, false, nil
				}
			}).Build()
		defer patch.UnPatch()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, skip, err := (&shardDelegator{}).prepareSharedFilterBranchFunctions(ctx, newReq())
		assert.ErrorIs(t, err, wantErr)
		assert.False(t, skip)
		assert.ErrorIs(t, <-branch0Canceled, context.Canceled)
	})

	t.Run("a real error is not hidden by a skipped branch", func(t *testing.T) {
		wantErr := errors.New("extra preparation failed")
		patch := mockey.Mock((*shardDelegator).prepareSearchFunction).
			To(func(_ *shardDelegator, _ context.Context, req *internalpb.SearchRequest) (float64, bool, error) {
				if req.GetFieldId() == 10 {
					return 12.5, true, nil
				}
				return 0, false, wantErr
			}).Build()
		defer patch.UnPatch()

		_, skip, err := (&shardDelegator{}).prepareSharedFilterBranchFunctions(
			context.Background(), newReq())
		assert.ErrorIs(t, err, wantErr)
		assert.False(t, skip)
	})
}

// Vector-clustering-key pruning keeps only the segments near each query
// vector, which is a per-branch decision a group cannot make. Collections with
// such a key stay ungrouped, so each branch is still pruned by its own vector;
// scalar-key pruning reads the shared predicate and does not stop grouping.
//
// The decision must not depend on EnableSegmentPrune: that value is
// refreshable and sd.search re-reads it per group, so a group formed while it
// was off would be pruned by branch 0's vector if it were turned on before
// execution got there.
func TestVectorClusteringKeyKeepsSubRequestsUngrouped(t *testing.T) {
	paramtable.Init()
	enableSharedFilter(t, true)
	// A vector clustering key is itself gated; without this the schema below
	// has no clustering key at all as far as pruning is concerned.
	setParam(t, paramtable.Get().CommonCfg.EnableVectorClusteringKey.Key, "true")

	vectorKey := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, IsClusteringKey: true},
	}}
	scalarKey := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64, IsClusteringKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	noKey := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	subReqs := []*internalpb.SubSearchRequest{subReq(t, 1, 1), subReq(t, 2, 1)}

	const collectionID = int64(1)
	vectorPruneFallbacks := func() float64 {
		return testutil.ToFloat64(metrics.QueryNodeSharedFilterFallbackTotal.WithLabelValues(
			paramtable.GetStringNodeID(),
			strconv.FormatInt(collectionID, 10),
			sharedFilterFallbackVectorPrune,
		))
	}

	for _, segmentPrune := range []string{"true", "false"} {
		t.Run("segment prune "+segmentPrune, func(t *testing.T) {
			setParam(t, paramtable.Get().QueryNodeCfg.EnableSegmentPrune.Key, segmentPrune)

			before := vectorPruneFallbacks()
			assert.Equal(t, [][]int{{0}, {1}},
				groupSubReqsBySharedFilter(context.Background(), collectionID, vectorKey, subReqs),
				"a vector clustering key keeps every branch pruned by its own vector")
			assert.Equal(t, before+2, vectorPruneFallbacks(),
				"both sub-requests fall back for the vector_prune reason")

			assert.Equal(t, [][]int{{0, 1}},
				groupSubReqsBySharedFilter(context.Background(), collectionID, scalarKey, subReqs),
				"scalar key: the predicate is shared, so is the pruning")
			assert.Equal(t, [][]int{{0, 1}},
				groupSubReqsBySharedFilter(context.Background(), collectionID, noKey, subReqs))
		})
	}
}

// Every branch's optimized plan must land at that branch's own index. The
// branches run concurrently now, so a shared destination or an index captured
// by reference would hand a branch another branch's search params -- tuned for
// a different topk, with nothing to report an error.
func TestOptimizeSearchParamsStoresEachBranchOutputAtItsOwnIndex(t *testing.T) {
	paramtable.Init()
	setParam(t, paramtable.Get().AutoIndexConfig.Enable.Key, "true")
	setParam(t, paramtable.Get().AutoIndexConfig.EnableOptimize.Key, "true")

	// Rewrite each branch's search params into something naming the topk the
	// hook was asked about, then set the two output flags on one branch each.
	hook := mock_optimizers.NewMockQueryHook(t)
	hook.EXPECT().Run(mock.Anything).RunAndReturn(func(params map[string]any) error {
		topk := params[common.TopKKey].(int64)
		params[common.SearchParamKey] = fmt.Sprintf(`{"from":%d}`, topk)
		if topk == 30 {
			params[common.RecallEvalKey] = true
		}
		if topk == 40 {
			// A lower final topk is what makes IsTopkReduce true.
			params[common.TopKKey] = int64(1)
		}
		return nil
	})

	extraTopks := []int64{10, 20, 30, 40, 50}
	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID: 1, Topk: 100, IsTopkReduce: true, IsRecallEvaluation: false,
			SerializedExprPlan: planWithPredicate(t, 100),
		},
	}
	for _, topk := range extraTopks {
		req.ExtraFilterSharingReqs = append(req.ExtraFilterSharingReqs,
			&internalpb.SubSearchRequest{Topk: topk, SerializedExprPlan: planWithPredicate(t, topk)})
	}

	sd := &shardDelegator{}
	optimized, err := sd.optimizeSearchParams(context.Background(), req, hook, 1)
	require.NoError(t, err)

	searchParamsOf := func(plan []byte) string {
		decoded := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(plan, decoded))
		return decoded.GetVectorAnns().GetQueryInfo().GetSearchParams()
	}
	assert.Equal(t, `{"from":100}`, searchParamsOf(optimized.GetReq().GetSerializedExprPlan()))
	require.Len(t, optimized.GetExtraFilterSharingReqs(), len(extraTopks))
	for i, topk := range extraTopks {
		assert.Equalf(t, fmt.Sprintf(`{"from":%d}`, topk),
			searchParamsOf(optimized.GetExtraFilterSharingReqs()[i].GetSerializedExprPlan()),
			"branch %d got another branch's optimized plan", i)
	}

	// Both flags are OR-ed over the branches: one branch reduced its topk, a
	// different one asked for recall evaluation, and branch 0 did neither.
	assert.True(t, optimized.GetReq().GetIsTopkReduce())
	assert.True(t, optimized.GetReq().GetIsRecallEvaluation())
}
