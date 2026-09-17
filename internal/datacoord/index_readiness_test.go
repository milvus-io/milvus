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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newIndexReadinessTestServer(segments []*datapb.SegmentInfo, states map[int64]commonpb.IndexState) (*Server, *model.Index) {
	paramtable.Init()
	index := &model.Index{
		CollectionID: 1, FieldID: 10, IndexID: 10, CreateTime: 100,
		IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "IVF_FLAT"}},
	}
	store := metacache.NewMetaStore(nil)
	s := &Server{ctx: context.Background(), meta: &meta{
		partitionStatsMeta: &partitionStatsMeta{partitionStatsInfos: make(map[string]map[int64]*partitionStatsInfo)},
		metaStore:          store,
		channelSync:        newChannelSync(),
		segments:           NewSegmentsInfo(store),
		indexMeta: &indexMeta{
			indexes:        map[UniqueID]map[UniqueID]*model.Index{1: {10: index}},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}}
	s.meta.AddCollection(&collectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 10, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 20, Name: "scalar", DataType: schemapb.DataType_Int64},
		},
	}})
	s.handler = newServerHandler(s)
	for _, segment := range segments {
		segment.InsertChannel = "lineage_channel"
		segment.PartitionID = 1
		if len(segment.Binlogs) == 0 {
			segment.Binlogs = []*datapb.FieldBinlog{{FieldID: 10, Binlogs: []*datapb.Binlog{{LogID: segment.ID, LogPath: fmt.Sprintf("insert_log/1/1/%d/10/%d", segment.ID, segment.ID), EntriesNum: segment.NumOfRows}}}}
		}
		s.meta.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment))
	}
	for segmentID, state := range states {
		indexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		indexes.Insert(10, &model.SegmentIndex{
			SegmentID: segmentID, CollectionID: 1, IndexID: 10,
			IndexState: state, CurrentIndexVersion: 7, FailReason: "build failed",
		})
		s.meta.indexMeta.segmentIndexes.Insert(segmentID, indexes)
	}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	return s, index
}

// Exercise the metadata collector as well as the state calculation: import
// visibility and data timestamps must survive the SegmentInfo -> indexStats
// boundary, independently of allocation expiry.
func TestIndexReadinessDataTimestamp(t *testing.T) {
	for _, test := range []struct {
		name        string
		from, to    uint64
		commit      uint64
		expiry      uint64
		importing   bool
		invisible   bool
		realTime    bool
		noIndex     bool
		state       commonpb.IndexState
		wantState   commonpb.IndexState
		wantIndexed int64
		wantPending int64
	}{
		{name: "committed import retains allocation sentinel", from: 1, to: 2, commit: 99, expiry: math.MaxUint64, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "committed import has no index task", from: 1, to: 2, commit: 99, expiry: math.MaxUint64, noIndex: true, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "committed import index finished", from: 1, to: 2, commit: 99, expiry: math.MaxUint64, state: commonpb.IndexState_Finished, wantState: commonpb.IndexState_Finished, wantIndexed: 100},
		{name: "import committed after cutoff", from: 1, to: 2, commit: 101, expiry: 1, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "uncommitted import", from: 1, to: 2, expiry: 99, importing: true, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "uncommitted import without task", from: 1, to: 2, expiry: 99, importing: true, noIndex: true, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "uncommitted failed index", from: 1, to: 2, expiry: 99, importing: true, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "uncommitted import real time", from: 1, to: 2, expiry: 99, importing: true, realTime: true, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "uncommitted finished index preserves real time rows", from: 1, to: 2, expiry: 99, importing: true, realTime: true, state: commonpb.IndexState_Finished, wantState: commonpb.IndexState_Finished, wantIndexed: 100},
		{name: "unknown data timestamp with task", expiry: math.MaxUint64, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "unknown data timestamp without task", expiry: math.MaxUint64, noIndex: true, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary segment spans cutoff", from: 50, to: 150, expiry: 200, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "earliest timestamp equals cutoff", from: 100, to: 150, expiry: 200, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary segment entirely after cutoff", from: 101, to: 150, expiry: 1, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "future segment without task is excluded", from: 101, to: 150, expiry: 1, noIndex: true, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "future failed index is excluded", from: 101, to: 150, expiry: 1, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Finished, wantPending: 100},
		{name: "real time includes future task", from: 101, to: 150, expiry: 200, realTime: true, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "real time includes future missing task", from: 101, to: 150, expiry: 200, realTime: true, noIndex: true, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "real time includes future failed index", from: 101, to: 150, expiry: 200, realTime: true, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Failed, wantPending: 100},
		{name: "eligible failed index", from: 50, to: 60, expiry: 200, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Failed, wantPending: 100},
		{name: "ordinary invisible segment without task", from: 50, to: 60, expiry: 200, invisible: true, noIndex: true, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary invisible segment building index", from: 50, to: 60, expiry: 200, invisible: true, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary invisible failed index", from: 50, to: 60, expiry: 200, invisible: true, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Failed, wantPending: 100},
		{name: "ordinary invisible segment real time without task", from: 50, to: 60, expiry: 200, invisible: true, realTime: true, noIndex: true, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary invisible segment real time building index", from: 50, to: 60, expiry: 200, invisible: true, realTime: true, state: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantPending: 100},
		{name: "ordinary invisible failed index real time", from: 50, to: 60, expiry: 200, invisible: true, realTime: true, state: commonpb.IndexState_Failed, wantState: commonpb.IndexState_Failed, wantPending: 100},
		{name: "invisible finished index preserves real time rows", from: 50, to: 60, expiry: 200, invisible: true, realTime: true, state: commonpb.IndexState_Finished, wantState: commonpb.IndexState_Finished, wantIndexed: 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			segment := &datapb.SegmentInfo{
				ID: 1, CollectionID: 1, State: commonpb.SegmentState_Flushed,
				NumOfRows: 100, LastExpireTime: test.expiry,
				CommitTimestamp: test.commit, IsImporting: test.importing, IsInvisible: test.invisible,
			}
			if test.from != 0 || test.to != 0 {
				segment.Stats = &datapb.Statistics{TimestampFrom: test.from, TimestampTo: test.to}
			}
			states := map[int64]commonpb.IndexState{}
			if !test.noIndex {
				states[1] = test.state
			}
			s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{segment}, states)
			stats := s.selectSegmentIndexesStats(context.Background(), WithCollection(1))
			info := &indexpb.IndexInfo{IndexID: index.IndexID}
			s.completeIndexInfo(info, index, stats, test.realTime, 100)
			require.Equal(t, test.wantState, info.GetState())
			require.EqualValues(t, 100, info.GetTotalRows())
			require.Equal(t, test.wantIndexed, info.GetIndexedRows())
			require.Equal(t, test.wantPending, info.GetPendingIndexRows())
			if test.wantState == commonpb.IndexState_Failed {
				require.Contains(t, info.GetIndexStateFailReason(), "build failed")
			}
			if test.wantIndexed > 0 {
				require.EqualValues(t, 7, info.GetMinIndexVersion())
				require.EqualValues(t, 7, info.GetMaxIndexVersion())
			}
		})
	}
}

func TestIndexReadinessVisibility(t *testing.T) {
	for _, realTime := range []bool{false, true} {
		for _, createdByCompaction := range []bool{false, true} {
			for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_InProgress, commonpb.IndexState_Failed} {
				t.Run(fmt.Sprintf("realTime=%t/createdByCompaction=%t/state=%s", realTime, createdByCompaction, state), func(t *testing.T) {
					segments := []*datapb.SegmentInfo{
						{ID: 1, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 100, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
						{ID: 2, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 100, IsInvisible: true, CreatedByCompaction: createdByCompaction, LastExpireTime: 200, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
					}
					if createdByCompaction {
						segments[1].CompactionFrom = []int64{1}
					}
					states := map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished}
					if state != commonpb.IndexState_Unissued {
						states[2] = state
					}
					s, index := newIndexReadinessTestServer(segments, states)
					info := &indexpb.IndexInfo{IndexID: index.IndexID}
					stats := s.selectSegmentIndexesStats(context.Background(), WithCollection(1))
					s.completeIndexInfo(info, index, stats, realTime, 100)
					invisibleState := commonpb.IndexState_Finished
					if !createdByCompaction {
						invisibleState = commonpb.IndexState_InProgress
						if state == commonpb.IndexState_Failed {
							invisibleState = commonpb.IndexState_Failed
						}
					}
					require.Equal(t, invisibleState, info.GetState(), "only invisible compaction outputs are excluded from readiness")
					if invisibleState == commonpb.IndexState_Failed {
						require.Contains(t, info.GetIndexStateFailReason(), "build failed")
					} else {
						require.Empty(t, info.GetIndexStateFailReason())
					}
					require.EqualValues(t, 200, info.GetTotalRows())
					require.EqualValues(t, 100, info.GetIndexedRows())
					require.EqualValues(t, 100, info.GetPendingIndexRows())

					visible := s.meta.segments.GetSegment(2).Clone()
					visible.IsInvisible = false
					s.meta.segments.SetSegment(2, visible)
					stats = s.selectSegmentIndexesStats(context.Background(), WithCollection(1))
					s.completeIndexInfo(info, index, stats, realTime, 100)
					wantState := commonpb.IndexState_InProgress
					if createdByCompaction {
						wantState = commonpb.IndexState_Finished
					} else if state == commonpb.IndexState_Failed {
						wantState = commonpb.IndexState_Failed
					}
					require.Equal(t, wantState, info.GetState(), "visible segments need their own index or complete ancestor coverage")
					if wantState == commonpb.IndexState_Failed {
						require.Contains(t, info.GetIndexStateFailReason(), "build failed")
					} else {
						require.Empty(t, info.GetIndexStateFailReason())
					}
				})
			}
		}
	}
}

func TestIndexReadinessCompactionLineage(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: 1, State: commonpb.SegmentState_Dropped, NumOfRows: 100, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
		{ID: 2, CollectionID: 1, State: commonpb.SegmentState_Dropped, NumOfRows: 100, CompactionFrom: []int64{1}, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
		{ID: 3, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 80, CompactionFrom: []int64{2}, LastExpireTime: 200, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
	}
	for _, state := range []commonpb.IndexState{commonpb.IndexState_InProgress, commonpb.IndexState_Finished} {
		t.Run(state.String(), func(t *testing.T) {
			s, index := newIndexReadinessTestServer(segments, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 3: state})
			stats := s.selectSegmentIndexesStats(context.Background(), WithCollection(1))
			info := &indexpb.IndexInfo{IndexID: index.IndexID}
			s.completeIndexInfo(info, index, stats, false, 100)
			wantState := commonpb.IndexState_Finished
			if state == commonpb.IndexState_InProgress {
				wantState = commonpb.IndexState_InProgress
			}
			require.Equal(t, wantState, info.GetState(), "Query recovery cannot expand an unready intermediate to finished grandparents")
			require.EqualValues(t, 80, info.GetTotalRows())
			if state == commonpb.IndexState_InProgress {
				require.EqualValues(t, 100, info.GetIndexedRows(), "completed ancestor coverage survives replacement")
				require.EqualValues(t, 80, info.GetPendingIndexRows())
			} else {
				require.EqualValues(t, 80, info.GetIndexedRows(), "finished replacement is counted once")
				require.Zero(t, info.GetPendingIndexRows())
			}
		})
	}
}

func TestIndexReadinessStatusAPIs(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID: 1, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 100,
		LastExpireTime: math.MaxUint64, CommitTimestamp: 99,
		Stats: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 2},
	}
	s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{segment}, map[int64]commonpb.IndexState{1: commonpb.IndexState_InProgress})
	ctx := context.Background()
	t.Run("DescribeIndex uses explicit request cutoff", func(t *testing.T) {
		t.Cleanup(func() { index.CreateTime = 100 })
		index.CreateTime = 98
		response, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: 1, Timestamp: 100})
		require.NoError(t, err)
		require.Len(t, response.GetIndexInfos(), 1)
		require.Equal(t, commonpb.IndexState_InProgress, response.GetIndexInfos()[0].GetState())
		response, err = s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: 1})
		require.NoError(t, err)
		require.Equal(t, commonpb.IndexState_Finished, response.GetIndexInfos()[0].GetState())
	})
	t.Run("GetIndexState", func(t *testing.T) {
		response, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: 1})
		require.NoError(t, err)
		require.Equal(t, commonpb.IndexState_InProgress, response.GetState())
	})
	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		response, err := s.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{CollectionID: 1})
		require.NoError(t, err)
		require.Zero(t, response.GetIndexedRows())
		require.EqualValues(t, 100, response.GetPendingIndexRows())
	})
	t.Run("GetIndexStatistics", func(t *testing.T) {
		response, err := s.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{CollectionID: 1})
		require.NoError(t, err)
		require.Len(t, response.GetIndexInfos(), 1)
		require.Equal(t, commonpb.IndexState_InProgress, response.GetIndexInfos()[0].GetState())
	})
}
