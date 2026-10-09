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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func indexServingQueryIDs(s *Server) []int64 {
	return newServerHandler(s).GetQueryVChanPositions(&channelMeta{
		Name: "lineage_channel", CollectionID: 1,
		StartPosition: &msgpb.MsgPosition{ChannelName: "lineage_channel", Timestamp: 1},
	}).GetFlushedSegmentIds()
}

func assertIndexServingState(t *testing.T, s *Server, index *model.Index, want commonpb.IndexState) {
	t.Helper()
	for _, realTime := range []bool{false, true} {
		stats := s.selectSegmentIndexesStats(context.Background(), WithCollection(1), SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return segment.GetLevel() != datapb.SegmentLevel_L0 && (isFlush(segment) || segment.GetState() == commonpb.SegmentState_Dropped)
		}))
		info := &indexpb.IndexInfo{IndexID: index.IndexID}
		s.completeIndexInfo(info, index, stats, realTime, 100)
		require.Equal(t, want, info.GetState(), "realTime=%t", realTime)
	}
	response, err := s.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{
		CollectionID: 1, IndexName: index.IndexName, Timestamp: 100,
	})
	require.NoError(t, err)
	require.Len(t, response.GetIndexInfos(), 1)
	require.Equal(t, want, response.GetIndexInfos()[0].GetState(), "DescribeIndex uses the collector's query frontier")
	state, err := s.GetIndexState(context.Background(), &indexpb.GetIndexStateRequest{CollectionID: 1, IndexName: index.IndexName})
	require.NoError(t, err)
	require.Equal(t, want, state.GetState())
	statistics, err := s.GetIndexStatistics(context.Background(), &indexpb.GetIndexStatisticsRequest{CollectionID: 1, IndexName: index.IndexName})
	require.NoError(t, err)
	require.Len(t, statistics.GetIndexInfos(), 1)
	require.Equal(t, want, statistics.GetIndexInfos()[0].GetState())
	_, err = s.GetIndexBuildProgress(context.Background(), &indexpb.GetIndexBuildProgressRequest{CollectionID: 1, IndexName: index.IndexName})
	require.NoError(t, err)
}

func TestIndexServingReadinessRecoverability(t *testing.T) {
	for _, test := range []struct {
		name     string
		change   func(*SegmentInfo)
		selected bool
		covered  bool
	}{
		{name: "raw binlogs", selected: true, covered: true},
		{name: "metadata without data footprint", change: func(s *SegmentInfo) { s.Binlogs = nil }},
		{name: "start position", selected: true, covered: true, change: func(s *SegmentInfo) { s.Binlogs = nil; s.StartPosition = &msgpb.MsgPosition{Timestamp: 50} }},
		{name: "dml position", selected: true, covered: true, change: func(s *SegmentInfo) { s.Binlogs = nil; s.DmlPosition = &msgpb.MsgPosition{Timestamp: 60} }},
		{name: "committed V3 manifest", selected: true, covered: true, change: func(s *SegmentInfo) {
			s.Binlogs = nil
			s.StorageVersion = storage.StorageV3
			s.ManifestPath = packed.MarshalManifestPath("root/insert_log/1/1/1", 1)
		}},
		{name: "earliest V3 manifest", change: func(s *SegmentInfo) {
			s.Binlogs = nil
			s.StorageVersion = storage.StorageV3
			s.ManifestPath = packed.MarshalManifestPath("root/insert_log/1/1/1", packed.ManifestEarliest)
		}},
		{name: "earliest V3 manifest with old binlog footprint", selected: true, change: func(s *SegmentInfo) {
			s.StorageVersion = storage.StorageV3
			s.ManifestPath = packed.MarshalManifestPath("root/insert_log/1/1/1", packed.ManifestEarliest)
		}},
		{name: "malformed V3 manifest", change: func(s *SegmentInfo) { s.StorageVersion = storage.StorageV3; s.ManifestPath = "malformed" }},
		{name: "missing V3 manifest with old binlog footprint", selected: true, change: func(s *SegmentInfo) { s.StorageVersion = storage.StorageV3; s.ManifestPath = "" }},
		{name: "fake segment", change: func(s *SegmentInfo) { s.IsFake = true }},
		{name: "importing segment", change: func(s *SegmentInfo) { s.IsImporting = true }},
		{name: "invisible parent", change: func(s *SegmentInfo) { s.IsInvisible = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
				newIndexLineageSegment(2, 90, commonpb.SegmentState_Flushed, 1),
			}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
			index.IndexName = "serving_index"
			parent := s.meta.segments.GetSegment(1).Clone()
			if test.change != nil {
				test.change(parent)
			}
			s.meta.segments.SetSegment(1, parent)
			if test.selected {
				require.ElementsMatch(t, []int64{1}, indexServingQueryIDs(s))
			} else {
				require.ElementsMatch(t, []int64{2}, indexServingQueryIDs(s))
			}
			want := commonpb.IndexState_InProgress
			if test.covered {
				want = commonpb.IndexState_Finished
			}
			assertIndexServingState(t, s, index, want)
			// Finished current output tasks retain their own readiness semantics.
			outputIndexes, ok := s.meta.indexMeta.segmentIndexes.Get(2)
			require.True(t, ok)
			output, ok := outputIndexes.Get(10)
			require.True(t, ok)
			output.IndexState = commonpb.IndexState_Finished
			assertIndexServingState(t, s, index, commonpb.IndexState_Finished)
		})
	}
}

func addIndexServingTask(s *Server, segmentID, indexID int64, state commonpb.IndexState) {
	indexes, ok := s.meta.indexMeta.segmentIndexes.Get(segmentID)
	if !ok {
		indexes = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		s.meta.indexMeta.segmentIndexes.Insert(segmentID, indexes)
	}
	indexes.Insert(indexID, &model.SegmentIndex{
		SegmentID: segmentID, CollectionID: 1, IndexID: indexID,
		IndexState: state, CurrentIndexVersion: 7, CurrentScalarIndexVersion: 9,
	})
}

func TestIndexServingReadinessRequestedIndexFrontier(t *testing.T) {
	for _, test := range []struct {
		name         string
		childVector  commonpb.IndexState
		parentScalar commonpb.IndexState
		forceAll     bool
		wantIDs      []int64
		want         commonpb.IndexState
	}{
		{name: "vector child ready cannot borrow scalar parents", childVector: commonpb.IndexState_Finished, parentScalar: commonpb.IndexState_Finished, wantIDs: []int64{3}, want: commonpb.IndexState_InProgress},
		{name: "selected parents cover requested scalar", childVector: commonpb.IndexState_InProgress, parentScalar: commonpb.IndexState_Finished, wantIDs: []int64{1, 2}, want: commonpb.IndexState_Finished},
		{name: "selected parents need same scalar index", childVector: commonpb.IndexState_InProgress, parentScalar: commonpb.IndexState_InProgress, wantIDs: []int64{1, 2}, want: commonpb.IndexState_InProgress},
		{name: "force all indexes selects scalar ready parents", childVector: commonpb.IndexState_Finished, parentScalar: commonpb.IndexState_Finished, forceAll: true, wantIDs: []int64{1, 2}, want: commonpb.IndexState_Finished},
		{name: "force all indexes rejects partial scalar parents", childVector: commonpb.IndexState_Finished, parentScalar: commonpb.IndexState_InProgress, forceAll: true, wantIDs: []int64{3}, want: commonpb.IndexState_InProgress},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := newIndexReadinessTestServer([]*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
				newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
			}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: test.childVector})
			key := paramtable.Get().DataCoordCfg.DVForceAllIndexReady.Key
			require.NoError(t, paramtable.Get().Save(key, fmt.Sprint(test.forceAll)))
			t.Cleanup(func() { paramtable.Get().Reset(key) })
			index := &model.Index{CollectionID: 1, FieldID: 20, IndexID: 20, IndexName: "scalar_index", CreateTime: 100}
			s.meta.indexMeta.indexes[1][20] = index
			addIndexServingTask(s, 1, 20, commonpb.IndexState_Finished)
			addIndexServingTask(s, 2, 20, test.parentScalar)
			addIndexServingTask(s, 3, 20, commonpb.IndexState_InProgress)
			require.ElementsMatch(t, test.wantIDs, indexServingQueryIDs(s))
			assertIndexServingState(t, s, index, test.want)
		})
	}
	t.Run("another vector field prevents parent selection", func(t *testing.T) {
		s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{
			newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
			newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
		}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: commonpb.IndexState_InProgress})
		index.IndexName = "vector_index"
		collection := s.meta.GetCollection(1)
		collection.Schema.Fields = append(collection.Schema.Fields, &schemapb.FieldSchema{FieldID: 30, Name: "second_vector", DataType: schemapb.DataType_FloatVector})
		s.meta.indexMeta.indexes[1][30] = &model.Index{CollectionID: 1, FieldID: 30, IndexID: 30}
		for _, id := range []int64{1, 2} {
			addIndexServingTask(s, id, 30, commonpb.IndexState_InProgress)
		}
		addIndexServingTask(s, 3, 30, commonpb.IndexState_Finished)
		require.ElementsMatch(t, []int64{3}, indexServingQueryIDs(s))
		assertIndexServingState(t, s, index, commonpb.IndexState_InProgress)
	})
}

func TestIndexServingReadinessLineageFrontier(t *testing.T) {
	for _, test := range []struct {
		name     string
		segments []*datapb.SegmentInfo
		states   map[int64]commonpb.IndexState
		wantIDs  []int64
		want     commonpb.IndexState
	}{
		{
			name: "unready intermediate cannot recursively recover finished grandparents", segments: []*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
				newIndexLineageSegment(3, 180, commonpb.SegmentState_Dropped, 1, 2), newIndexLineageSegment(4, 150, commonpb.SegmentState_Flushed, 3),
			},
			states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished}, wantIDs: []int64{4}, want: commonpb.IndexState_InProgress,
		},
		{
			name: "growing intermediate remains outside indexed fallback", segments: []*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 180, commonpb.SegmentState_Growing, 1),
				newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 2),
			},
			states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished}, wantIDs: []int64{3}, want: commonpb.IndexState_InProgress,
		},
		{
			name: "L0 intermediate remains outside indexed fallback", segments: []*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
				{ID: 2, CollectionID: 1, NumOfRows: 180, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, CompactionFrom: []int64{1}, StartPosition: &msgpb.MsgPosition{Timestamp: 50}},
				newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 2),
			},
			states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished}, wantIDs: []int64{3}, want: commonpb.IndexState_InProgress,
		},
		{
			name: "M:N overlap chooses complete parent frontier", segments: []*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
				newIndexLineageSegment(3, 80, commonpb.SegmentState_Flushed, 1, 2), newIndexLineageSegment(4, 90, commonpb.SegmentState_Flushed, 1, 2),
			},
			states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: commonpb.IndexState_Finished, 4: commonpb.IndexState_InProgress}, wantIDs: []int64{1, 2}, want: commonpb.IndexState_Finished,
		},
		{
			name: "M:N overlap cannot recover one missing parent", segments: []*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
				newIndexLineageSegment(3, 80, commonpb.SegmentState_Flushed, 1, 2), newIndexLineageSegment(4, 90, commonpb.SegmentState_Flushed, 1, 2),
			},
			states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 3: commonpb.IndexState_Finished, 4: commonpb.IndexState_InProgress}, wantIDs: []int64{3, 4}, want: commonpb.IndexState_InProgress,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, index := newIndexReadinessTestServer(test.segments, test.states)
			index.IndexName = "frontier_index"
			require.ElementsMatch(t, test.wantIDs, indexServingQueryIDs(s))
			assertIndexServingState(t, s, index, test.want)
		})
	}
}

func TestIndexServingReadinessLineageBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*SegmentInfo)
	}{
		{name: "parent in another channel", change: func(s *SegmentInfo) { s.InsertChannel = "another_channel" }},
		{name: "parent in another partition", change: func(s *SegmentInfo) { s.PartitionID = 2 }},
		{name: "parent in another collection", change: func(s *SegmentInfo) { s.CollectionID = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{
				newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed), newIndexLineageSegment(2, 90, commonpb.SegmentState_Flushed, 1),
			}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
			index.IndexName = "boundary_index"
			parent := s.meta.segments.GetSegment(1).Clone()
			test.change(parent)
			s.meta.segments.SetSegment(1, parent)
			// Metadata may still select the parent's own live frontier, but
			// it cannot establish coverage across a lineage identity boundary.
			assertIndexServingState(t, s, index, commonpb.IndexState_InProgress)
		})
	}
	t.Run("nil handler conservatively disables ancestor coverage", func(t *testing.T) {
		s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{
			newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped), newIndexLineageSegment(2, 90, commonpb.SegmentState_Flushed, 1),
		}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
		index.IndexName = "nil_handler_index"
		s.handler = nil
		assertIndexServingState(t, s, index, commonpb.IndexState_InProgress)
		outputIndexes, _ := s.meta.indexMeta.segmentIndexes.Get(2)
		output, _ := outputIndexes.Get(10)
		output.IndexState = commonpb.IndexState_Finished
		assertIndexServingState(t, s, index, commonpb.IndexState_Finished)
	})
}

func TestIndexServingReadinessCyclicAncestorCannotCoverDescendant(t *testing.T) {
	s, _ := newIndexReadinessTestServer([]*datapb.SegmentInfo{
		newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed, 2),
		newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped, 1),
		newIndexLineageSegment(3, 90, commonpb.SegmentState_Flushed, 1),
	}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished})
	index := &model.Index{CollectionID: 1, FieldID: 20, IndexID: 20, IndexName: "cyclic_scalar", CreateTime: 100}
	s.meta.indexMeta.indexes[1][20] = index
	addIndexServingTask(s, 1, 20, commonpb.IndexState_Finished)
	addIndexServingTask(s, 3, 20, commonpb.IndexState_InProgress)
	// A cycle may leave a live fallback candidate in a conservative query
	// frontier. That candidate still cannot certify another segment's index.
	assertIndexServingState(t, s, index, commonpb.IndexState_InProgress)
	outputIndexes, _ := s.meta.indexMeta.segmentIndexes.Get(3)
	output, _ := outputIndexes.Get(20)
	output.IndexState = commonpb.IndexState_Finished
	assertIndexServingState(t, s, index, commonpb.IndexState_Finished)
}

func TestIndexServingReadinessInvisibleIndexStateNone(t *testing.T) {
	for _, createdByCompaction := range []bool{false, true} {
		t.Run(fmt.Sprintf("createdByCompaction=%t", createdByCompaction), func(t *testing.T) {
			segment := newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed)
			segment.IsInvisible = true
			segment.CreatedByCompaction = createdByCompaction
			s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{segment}, map[int64]commonpb.IndexState{1: commonpb.IndexState_IndexStateNone})
			index.IndexName = "invisible_none"
			want := commonpb.IndexState_IndexStateNone
			if createdByCompaction {
				want = commonpb.IndexState_Finished
			}
			assertIndexServingState(t, s, index, want)
		})
	}
}
