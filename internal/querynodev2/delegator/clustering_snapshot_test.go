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
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type testClusterSearcher struct {
	labels    []int64
	distances []float32
	err       error
}

func (s testClusterSearcher) Search(context.Context, []byte, int64, int64) ([]int64, []float32, error) {
	distances := s.distances
	if distances == nil {
		distances = make([]float32, len(s.labels))
	}
	return s.labels, distances, s.err
}

func testStatsBinding(version int64, reverse bool) *partitionStatsBinding {
	c0, c1 := uint32(0), uint32(1)
	if reverse {
		c0, c1 = c1, c0
	}
	stats := &storage.PartitionStatsSnapshot{Version: version, SegmentStats: map[int64]storage.SegmentStats{
		10: {NumRows: 8, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: c0, RowOffset: 0, RowCount: 3}, {ClusterOffset: c1, RowOffset: 3, RowCount: 5}}},
		20: {NumRows: 4, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 2, RowCount: 4}}},
	}, ClusteringStats: &clusteringpb.PartitionClusteringStats{
		FormatVersion: 1, FieldId: 100, DataType: schemapb.DataType_FloatVector, Dim: 2, NumClusters: 3,
		ClustersFile: "clusters", ClusteringIndex: &clusteringpb.ClusteringIndexInfo{IndexEngineVersion: 10, IndexSize: 100, IndexFilePaths: []string{"index"}, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "FLAT"}, {Key: "metric_type", Value: "L2"}}},
		Groups: []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10, 20}}},
	}}
	return &partitionStatsBinding{stats: stats, index: testClusterSearcher{labels: []int64{0, -1}}, locations: clusteringLocations(stats)}
}

func TestClusteringBindingPinnedWithSegments(t *testing.T) {
	d := NewDistribution("ch", NewChannelQueryView(nil, map[int64]int64{10: 8, 20: 4}, []int64{2}, 1))
	d.AddDistributions(SegmentEntry{NodeID: 1, SegmentID: 10, PartitionID: 2}, SegmentEntry{NodeID: 1, SegmentID: 20, PartitionID: 2})
	old := testStatsBinding(100, false)
	next := testStatsBinding(101, true)
	require.NoError(t, old.stats.ValidateClusteringStats())
	require.NoError(t, next.stats.ValidateClusteringStats())
	action := &querypb.SyncAction{TargetVersion: 1, SealedInTarget: []int64{10, 20}, SealedSegmentRowCount: map[int64]int64{10: 8, 20: 4}}
	d.SyncTargetVersion(action, []int64{2}, map[int64]*partitionStatsBinding{2: old})
	_, _, pin := d.PinOnlineSegments()
	defer d.Unpin(pin)
	action.TargetVersion = 2
	d.SyncTargetVersion(action, []int64{2}, map[int64]*partitionStatsBinding{2: next})
	_, _, newPin := d.PinOnlineSegments()
	defer d.Unpin(newPin)
	require.Same(t, old, d.pinnedStatsBindings(pin)[2])
	require.Same(t, next, d.pinnedStatsBindings(newPin)[2])
	// A distribution update carries the active target binding without replacing it.
	d.AddDistributions(SegmentEntry{NodeID: 2, SegmentID: 30, PartitionID: 2})
	require.Same(t, old, d.pinnedStatsBindings(pin)[2])
}

func TestClusteringCandidateRanges(t *testing.T) {
	binding := testStatsBinding(100, false)
	ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 8)}}}})
	require.NoError(t, err)
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}})
	require.NoError(t, err)
	req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: 1, PlaceholderGroup: ph, SerializedExprPlan: plan}
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, PartitionID: 2}, {SegmentID: 20, PartitionID: 2}, {SegmentID: 30, PartitionID: 2}}}}
	rows := map[int64]int64{10: 8, 20: 4, 30: 7}
	call := func() map[int64]*querypb.SegmentClusterSearchHints {
		return clusteringSearchHints(context.Background(), map[int64]*partitionStatsBinding{2: binding}, req, sealed, rows, 0.5)
	}
	result := call()
	require.Len(t, result, 2)
	require.Len(t, result[10].Queries[0].Hints.Ranges, 1)
	require.Equal(t, int64(3), result[10].Queries[0].Hints.Ranges[0].RowCount)
	require.Empty(t, result[20].Queries[0].Hints.Ranges)
	require.NotContains(t, result, int64(30))
	// A group is a directory: hitting c0 cannot expand to c1/c2 in the same group.
	binding.index = testClusterSearcher{labels: []int64{0, 1}}
	require.Len(t, call()[10].Queries[0].Hints.Ranges, 2)
	binding.index = testClusterSearcher{labels: []int64{-1}}
	require.Empty(t, call())
	binding.index = testClusterSearcher{labels: []int64{99}}
	require.Empty(t, call())
	binding.index = testClusterSearcher{err: errors.New("corrupt index")}
	require.Empty(t, call())
	binding.index = testClusterSearcher{labels: []int64{0, -1}}
	rows[10] = 9
	require.Empty(t, call())
	rows[10] = 8
	req.MetricType = "COSINE"
	require.Empty(t, call())
	req.MetricType = "L2"
	req.FieldId = 101
	require.Empty(t, call())
	req.FieldId = 100
	binding.index = nil
	require.Empty(t, call())
}

func TestClusteringHalfCandidateRanges(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) {
			binding := testStatsBinding(100, false)
			binding.stats.ClusteringStats.DataType = dtype
			pt := commonpb.PlaceholderType_Float16Vector
			other := commonpb.PlaceholderType_BFloat16Vector
			if dtype == schemapb.DataType_BFloat16Vector {
				pt, other = other, pt
			}
			plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}})
			require.NoError(t, err)
			req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: 1, SerializedExprPlan: plan}
			call := func(typ commonpb.PlaceholderType, values ...[]byte) map[int64]*querypb.SegmentClusterSearchHints {
				ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: typ, Values: values}}})
				require.NoError(t, err)
				req.PlaceholderGroup = ph
				return clusteringSearchHints(context.Background(), map[int64]*partitionStatsBinding{2: binding}, req,
					[]SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, PartitionID: 2}, {SegmentID: 20, PartitionID: 2}}}}, map[int64]int64{10: 8, 20: 4}, 0.5)
			}
			require.Equal(t, int64(3), call(pt, make([]byte, 4))[10].Queries[0].Hints.Ranges[0].RowCount)
			require.Empty(t, call(other, make([]byte, 4)), "same byte width does not make FP16 and BF16 interchangeable")
			require.Empty(t, call(commonpb.PlaceholderType_FloatVector, make([]byte, 8)))
			for _, n := range []int{0, 3, 5, 8} {
				require.Empty(t, call(pt, make([]byte, n)))
			}
			require.Empty(t, call(pt, make([]byte, 4), make([]byte, 4)), "NQ mismatch must not narrow scope")
		})
	}
}

func TestClusteringSearchHintsPerQuery(t *testing.T) {
	binding := testStatsBinding(100, false)
	binding.index = testClusterSearcher{labels: []int64{0, 2, 99}, distances: []float32{1.25, 2.5, 0}}
	ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8)}}}})
	require.NoError(t, err)
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}})
	require.NoError(t, err)
	req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: 3, PlaceholderGroup: ph, SerializedExprPlan: plan}
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, PartitionID: 2}, {SegmentID: 20, PartitionID: 2}, {SegmentID: 30, PartitionID: 2}}}}
	result := clusteringSearchHints(context.Background(), map[int64]*partitionStatsBinding{2: binding}, req, sealed, map[int64]int64{10: 8, 20: 4, 30: 7}, 0.3)
	require.Len(t, result, 2)
	require.Len(t, result[10].Queries, 3)
	require.Equal(t, float32(1.25), result[10].Queries[0].Hints.Ranges[0].CentroidDistance)
	require.Empty(t, result[10].Queries[1].Hints.Ranges)
	require.NotNil(t, result[20].Queries[0].Hints)
	require.Empty(t, result[20].Queries[0].Hints.Ranges)
	require.Equal(t, float32(2.5), result[20].Queries[1].Hints.Ranges[0].CentroidDistance)
	require.Nil(t, result[10].Queries[2].Hints)
	require.Nil(t, result[20].Queries[2].Hints)
	// Presence survives transport: ordinary and explicit empty are not aliases.
	raw, err := proto.Marshal(result[10])
	require.NoError(t, err)
	decoded := &querypb.SegmentClusterSearchHints{}
	require.NoError(t, proto.Unmarshal(raw, decoded))
	require.True(t, proto.Equal(result[10], decoded))
}

func TestClusteringSearchHintProbeFallback(t *testing.T) {
	binding := testStatsBinding(100, false)
	ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 8), make([]byte, 8)}}}})
	require.NoError(t, err)
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{FieldId: 100, QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}}
	req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: 2, PlaceholderGroup: ph}
	call := func() map[int64]*querypb.SegmentClusterSearchHints {
		req.SerializedExprPlan, _ = proto.Marshal(plan)
		return clusteringSearchHints(context.Background(), map[int64]*partitionStatsBinding{2: binding}, req, []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, PartitionID: 2}, {SegmentID: 20, PartitionID: 2}}}}, map[int64]int64{10: 8, 20: 4}, 0.3)
	}
	for _, bad := range []testClusterSearcher{
		{labels: []int64{0, -1}, distances: []float32{1, 0}},
		{labels: []int64{0, 3}, distances: []float32{1, 0}},
		{labels: []int64{0, 2}, distances: []float32{1, float32(math.NaN())}},
		{labels: []int64{0, 2}, distances: []float32{1, float32(math.Inf(1))}},
	} {
		binding.index = bad
		h := call()
		require.NotNil(t, h[10].Queries[0].Hints)
		require.Nil(t, h[10].Queries[1].Hints)
	}
	binding.index = testClusterSearcher{labels: []int64{0, 2}, distances: []float32{1}}
	require.Empty(t, call())
	binding.index = testClusterSearcher{labels: []int64{0, 2}}
	plan.GetVectorAnns().FieldId = 101
	require.Empty(t, call())
	plan.GetVectorAnns().FieldId = 100
	plan.GetVectorAnns().QueryInfo.Hints = "iterative_filter"
	require.Empty(t, call())
	plan.GetVectorAnns().QueryInfo.Hints = ""
	plan.GetVectorAnns().QueryInfo.GroupByFieldId = 102
	require.Empty(t, call())
	plan.GetVectorAnns().QueryInfo.GroupByFieldId = 0
	plan.GetVectorAnns().QueryInfo.SearchParams = `{"radius":100}`
	require.Empty(t, call())
}
