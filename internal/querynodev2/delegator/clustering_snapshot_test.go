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
	labels []int64
	err    error
}

func (s testClusterSearcher) Search(context.Context, []byte, int64, int64) ([]int64, error) {
	return s.labels, s.err
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
	return &partitionStatsBinding{stats: stats, index: testClusterSearcher{labels: []int64{0}}, locations: clusteringLocations(stats)}
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
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{QueryInfo: &planpb.QueryInfo{SearchParams: "{}"}}}})
	require.NoError(t, err)
	req := &internalpb.SearchRequest{FieldId: 100, MetricType: "L2", Nq: 1, PlaceholderGroup: ph, SerializedExprPlan: plan}
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, PartitionID: 2}, {SegmentID: 20, PartitionID: 2}, {SegmentID: 30, PartitionID: 2}}}}
	rows := map[int64]int64{10: 8, 20: 4, 30: 7}
	call := func() map[int64]*querypb.ClusteringRowRanges {
		return clusteringCandidateRanges(context.Background(), map[int64]*partitionStatsBinding{2: binding}, req, sealed, rows, 0.5)
	}
	result := call()
	require.Len(t, result, 2)
	require.Len(t, result[10].Ranges, 1)
	require.Equal(t, int64(3), result[10].Ranges[0].RowCount)
	require.Empty(t, result[20].Ranges)
	require.NotContains(t, result, int64(30))
	// A group is a directory: hitting c0 cannot expand to c1/c2 in the same group.
	binding.index = testClusterSearcher{labels: []int64{0, 1, 0}}
	require.Len(t, call()[10].Ranges, 2)
	binding.index = testClusterSearcher{labels: []int64{-1}}
	require.Empty(t, call())
	binding.index = testClusterSearcher{labels: []int64{99}}
	require.Empty(t, call())
	binding.index = testClusterSearcher{err: errors.New("corrupt index")}
	require.Empty(t, call())
	binding.index = testClusterSearcher{labels: []int64{0}}
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
