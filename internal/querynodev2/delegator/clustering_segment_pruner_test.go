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
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise hint production and actual worker dispatch together: a present empty
// list prunes a segment only when every query has a valid candidate decision.
func TestClusteringSegmentPruneDispatch(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name                                          string
		labels                                        []int64
		scope, want                                   []int64
		fallback, mismatch, missing, disabled         bool
		twoStage, partialFailure, growing, sameWorker bool
	}{
		{name: "same_worker", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10}, sameWorker: true},
		{name: "empty_segment", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10}},
		{name: "all_queries_empty", labels: []int64{0, 1}, scope: []int64{10, 20}, want: []int64{10}},
		{name: "union_of_queries", labels: []int64{0, 2}, scope: []int64{10, 20}, want: []int64{10, 20}},
		{name: "query_fallback", labels: []int64{0, -1}, scope: []int64{10, 20}, want: []int64{10, 20}},
		{name: "invalid_label", labels: []int64{0, 99}, scope: []int64{10, 20}, want: []int64{10, 20}},
		{name: "probe_error", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10, 20}, fallback: true},
		{name: "row_mismatch", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10, 20}, mismatch: true},
		{name: "missing_binding", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10, 20}, missing: true},
		{name: "uncovered_segment", labels: []int64{0}, scope: []int64{10, 20, 30}, want: []int64{10, 30}},
		{name: "disabled", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10, 20}, disabled: true},
		{name: "all_segments_pruned", labels: []int64{0}, scope: []int64{20}},
		{name: "growing_passthrough", labels: []int64{0}, scope: []int64{20}, want: []int64{40}, growing: true},
		{name: "two_stage_fallback", labels: []int64{0}, scope: []int64{10, 20}, want: []int64{10, 20, 10, 20}, twoStage: true},
		{name: "partial_result_denominator", labels: []int64{0}, scope: []int64{10, 20, 30}, want: []int64{10, 30}, partialFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := paramtable.Get()
			params := map[string]string{
				cfg.QueryNodeCfg.EnableSegmentPrune.Key:              strconv.FormatBool(!tc.disabled),
				cfg.QueryNodeCfg.DefaultSegmentFilterRatio.Key:       "0.3",
				cfg.QueryNodeCfg.EnableSegmentFilter.Key:             "false",
				cfg.AutoIndexConfig.TwoStageSearchEnabled.Key:        strconv.FormatBool(tc.twoStage),
				cfg.AutoIndexConfig.TwoStageSearchMinNumSegments.Key: "1",
				cfg.AutoIndexConfig.TwoStageSearchMinTopk.Key:        "1",
				cfg.QueryNodeCfg.PartialResultRequiredDataRatio.Key:  "0.5",
			}
			for key, value := range params {
				old := cfg.GetWithDefault(key, "")
				require.NoError(t, cfg.Save(key, value))
				t.Cleanup(func() { require.NoError(t, cfg.Save(key, old)) })
			}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
			}}
			binding := testStatsBinding(100, false)
			searcher := testClusterSearcher{labels: tc.labels}
			if tc.fallback {
				searcher.err = errors.New("probe failed")
			}
			binding.index = searcher
			bindings := map[int64]*partitionStatsBinding{2: binding}
			if tc.missing {
				bindings = nil
			}
			rows := map[int64]int64{10: 8, 20: 4, 30: 7}
			if tc.mismatch {
				rows[20]++
			}
			originalRows := map[int64]int64{}
			for id, n := range rows {
				originalRows[id] = n
			}
			sealed := make([]SnapshotItem, 0, len(tc.scope))
			for i, id := range tc.scope {
				sealed = append(sealed, SnapshotItem{NodeID: int64(i + 1), Segments: []SegmentEntry{{NodeID: int64(i + 1), SegmentID: id, PartitionID: 2}}})
			}
			if tc.sameWorker {
				sealed = []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{NodeID: 1, SegmentID: 10, PartitionID: 2}, {NodeID: 1, SegmentID: 20, PartitionID: 2}}}}
			}
			originalScope := make([]SnapshotItem, len(sealed))
			for i, item := range sealed {
				originalScope[i] = SnapshotItem{NodeID: item.NodeID, Segments: append([]SegmentEntry(nil), item.Segments...)}
			}
			values := make([][]byte, len(tc.labels))
			for i := range values {
				values[i] = make([]byte, 8)
			}
			ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values}}})
			require.NoError(t, err)
			predicate := &planpb.Expr{Expr: &planpb.Expr_AlwaysTrueExpr{AlwaysTrueExpr: &planpb.AlwaysTrueExpr{}}}
			expr, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, Predicates: predicate, QueryInfo: &planpb.QueryInfo{Topk: 1, SearchParams: "{}"}}}})
			require.NoError(t, err)
			req := &querypb.SearchRequest{Req: &internalpb.SearchRequest{CollectionID: 1000, FieldId: 100, MetricType: "L2", Topk: 1, Nq: int64(len(values)), PlaceholderGroup: ph, SerializedExprPlan: expr, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER}}
			requests := make(chan *querypb.SearchRequest, 8)
			worker := cluster.NewMockWorker(t)
			worker.EXPECT().SearchSegments(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *querypb.SearchRequest) (*internalpb.SearchResults, error) {
				requests <- proto.Clone(r).(*querypb.SearchRequest)
				if tc.partialFailure && r.GetSegmentIDs()[0] == 30 {
					return nil, errors.New("uncovered worker unavailable")
				}
				return &internalpb.SearchResults{Status: merr.Success()}, nil
			}).Maybe()
			manager := cluster.NewMockManager(t)
			manager.EXPECT().GetWorker(mock.Anything, mock.Anything).Return(worker, nil).Maybe()
			sd := &shardDelegator{collectionID: 1000, vchannelName: "clustering-prune", collection: segments.NewCollectionWithoutSegcoreForTest(1000, schema), workerManager: manager}
			require.NoError(t, function.GetManager().Alloc(sd.collectionID, delegatorFunctionRunnerKey(sd.vchannelName), schema))
			t.Cleanup(func() { function.GetManager().Release(sd.collectionID, delegatorFunctionRunnerKey(sd.vchannelName)) })
			var growing []SegmentEntry
			if tc.growing {
				growing = []SegmentEntry{{SegmentID: 40, PartitionID: 2}}
			}
			results, err := sd.search(context.Background(), req, sealed, growing, rows, bindings)
			require.NoError(t, err)
			close(requests)
			var seen []int64
			for r := range requests {
				seen = append(seen, r.GetSegmentIDs()...)
				require.Equal(t, int64(len(values)), r.GetReq().GetNq())
				require.Equal(t, ph, r.GetReq().GetPlaceholderGroup())
				var gotPlan planpb.PlanNode
				require.NoError(t, proto.Unmarshal(r.GetReq().GetSerializedExprPlan(), &gotPlan))
				require.True(t, proto.Equal(predicate, gotPlan.GetVectorAnns().GetPredicates()))
				for id := range r.GetClusteringSearchHints() {
					require.Contains(t, r.GetSegmentIDs(), id)
				}
				if tc.twoStage || r.GetScope() == querypb.DataScope_Streaming {
					require.Empty(t, r.GetClusteringSearchHints())
				}
			}
			require.ElementsMatch(t, tc.want, seen)
			if len(tc.want) == 0 {
				require.Empty(t, results)
			}
			if tc.partialFailure {
				require.Len(t, results, 1)
			}
			require.Equal(t, originalScope, sealed, "search must not change the pinned segment view")
			require.Equal(t, originalRows, rows, "search must not change pinned row counts")
		})
	}
}

// Protobuf presence must distinguish an explicit no-candidate decision from a
// missing hint after transport. Incomplete metadata must never drop a segment.
func TestClusteringSegmentPruneHintPresence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hints  *querypb.SegmentClusterSearchHints
		nq     int64
		pruned bool
	}{
		{name: "explicit_empty", nq: 1, pruned: true, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 4, Queries: []*querypb.QueryClusterSearchHints{{Hints: &querypb.ClusterSearchHintList{}}}}},
		{name: "absent", nq: 1},
		{name: "absent_query", nq: 1, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 4, Queries: []*querypb.QueryClusterSearchHints{nil}}},
		{name: "absent_list", nq: 1, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 4, Queries: []*querypb.QueryClusterSearchHints{{}}}},
		{name: "missing_query", nq: 2, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 4, Queries: []*querypb.QueryClusterSearchHints{{Hints: &querypb.ClusterSearchHintList{}}}}},
		{name: "row_mismatch", nq: 1, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 5, Queries: []*querypb.QueryClusterSearchHints{{Hints: &querypb.ClusterSearchHintList{}}}}},
		{name: "missing_version", nq: 1, hints: &querypb.SegmentClusterSearchHints{NumRows: 4, Queries: []*querypb.QueryClusterSearchHints{{Hints: &querypb.ClusterSearchHintList{}}}}},
		{name: "empty_batch", nq: 0, hints: &querypb.SegmentClusterSearchHints{SnapshotVersion: 100, NumRows: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hints := map[int64]*querypb.SegmentClusterSearchHints{}
			if tc.hints != nil {
				data, err := proto.Marshal(tc.hints)
				require.NoError(t, err)
				received := &querypb.SegmentClusterSearchHints{}
				require.NoError(t, proto.Unmarshal(data, received))
				hints[20] = received
			}
			sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 20, PartitionID: 2}}}}
			rows := map[int64]int64{20: 4}
			kept, keptRows := pruneClusteringSegments(sealed, rows, hints, tc.nq)
			if tc.pruned {
				require.Empty(t, kept)
				require.Empty(t, keptRows)
			} else {
				require.Equal(t, sealed, kept)
				require.Equal(t, rows, keptRows)
			}
			require.Len(t, sealed[0].Segments, 1)
			require.Equal(t, int64(4), rows[20])
		})
	}
}
