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

package datacoord

import (
	"context"
	"path"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClusteringPublicationRecovery(t *testing.T) {
	ctx := context.Background()
	m, view, _ := coverageMeta(t)
	catalog := mocks.NewDataCoordCatalog(t)
	m.catalog = catalog
	m.partitionStatsMeta.catalog = catalog
	m.partitionStatsMeta.ctx = ctx
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Once()
	var err error
	m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	interrupted := errors.New("interrupt after snapshot commit")
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(interrupted).Once()
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	m.segments.SetSegment(60, NewSegmentInfo(&datapb.SegmentInfo{ID: 60, CollectionID: 1, PartitionID: 2, InsertChannel: "ch", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2, IsInvisible: true, PartitionStatsVersion: 8, NumOfRows: 3}))
	stats := coverageSnapshot()
	stats.Version = 8
	delete(stats.SegmentStats, 10)
	delete(stats.SegmentStats, 20)
	stats.SegmentStats[30] = storage.SegmentStats{NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 0, RowCount: 2}}}
	stats.SegmentStats[40] = storage.SegmentStats{NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 1, RowCount: 2}}}
	stats.SegmentStats[60] = storage.SegmentStats{NumRows: 3, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 2, RowCount: 3}}}
	stats.ClusteringStats.NumClusters = 3
	stats.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{30, 40}}, {SegmentIds: []int64{60}}}
	key := path.Join(m.chunkManager.RootPath(), "part_stats/1/2/ch/8")
	stats.ClusteringStats.ClustersFile = key + ".clusters"
	stats.ClusteringStats.ClusteringIndex.IndexFilePaths = []string{key + ".clustering_index/data"}
	for _, object := range []string{stats.ClusteringStats.ClustersFile, stats.ClusteringStats.ClusteringIndex.IndexFilePaths[0]} {
		require.NoError(t, m.chunkManager.Write(ctx, object, []byte("artifact")))
	}
	blob, err := storage.SerializePartitionStatsSnapshot(stats)
	require.NoError(t, err)
	require.NoError(t, m.chunkManager.Write(ctx, key, blob))
	task := &clusteringCompactionTask{meta: m}
	task.SetTask(&datapb.CompactionTask{PlanID: 8, Type: datapb.CompactionType_ClusteringCompaction, CollectionID: 1, PartitionID: 2, Channel: "ch", BasePartitionStatsVersion: 7, ClusteringKeyField: view.clusteringKeyField, InputSegments: []int64{10, 20, 50}, ResultSegments: []int64{60}, State: datapb.CompactionTaskState_indexing})
	require.ErrorIs(t, task.completeSnapshotTask(), interrupted)
	require.Equal(t, int64(8), m.partitionStatsMeta.GetCurrentPartitionStatsVersion(1, 2, "ch"))
	require.True(t, m.GetSegment(ctx, 60).GetIsInvisible(), "cleanup has not run")
	projected, versions := m.clusteringRecoveryView(1, "ch")
	require.Equal(t, int64(8), versions[2])
	visible := []int64{}
	for _, s := range projected {
		if s.GetState() == commonpb.SegmentState_Flushed && !s.GetIsInvisible() {
			visible = append(visible, s.ID)
		}
	}
	require.ElementsMatch(t, []int64{30, 40, 60}, visible, "the committed receipt controls recovery")
	require.True(t, m.clusteringSegmentReferenced(m.GetSegment(ctx, 10)))
	require.True(t, m.clusteringSegmentReferenced(m.GetSegment(ctx, 50)), "fresh input can still be pinned by the old view")
	require.NoError(t, task.completeSnapshotTask())
	require.Equal(t, datapb.CompactionTaskState_completed, task.GetTaskProto().GetState())
	require.False(t, m.GetSegment(ctx, 60).GetIsInvisible())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 10).GetState())
	require.ErrorIs(t, m.partitionStatsMeta.SavePartitionStatsAndVersion(&datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch", Version: 9}, 9, 7), merr.ErrCompactionPlanConflict)
}

func TestClusteringRecordedWorkerIsNeverResubmitted(t *testing.T) {
	m := NewMockCompactionMeta(t)
	m.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &clusteringCompactionTask{meta: m}
	task.SetTask(&datapb.CompactionTask{PlanID: 100, NodeID: 5, State: datapb.CompactionTaskState_pipelining})
	cluster := session.NewMockCluster(t) // No CreateCompaction or Drop call is allowed.
	require.NoError(t, task.dispatchSnapshotTask(9, cluster))
	require.Equal(t, int64(5), task.GetTaskProto().GetNodeID())
	require.Equal(t, datapb.CompactionTaskState_executing, task.GetTaskProto().GetState())
}
