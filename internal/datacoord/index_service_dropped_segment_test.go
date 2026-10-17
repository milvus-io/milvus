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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	droppedRaceCollID   = UniqueID(1)
	droppedRacePartID   = UniqueID(2)
	droppedRaceFieldID  = UniqueID(10)
	droppedRaceIndexID  = UniqueID(100)
	droppedRaceLiveSeg  = UniqueID(1000)
	droppedRaceDropSeg  = UniqueID(1001)
	droppedRaceCreateTS = uint64(1000)
)

// newDroppedSegmentRaceServer builds a server whose collection holds one
// indexed segment and one segment whose index task is still in progress.
func newDroppedSegmentRaceServer(t *testing.T) *Server {
	s := &Server{
		meta: &meta{
			catalog:   &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
			indexMeta: newSegmentIndexMeta(&datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}),
			segments:  NewSegmentsInfo(),
		},
	}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	s.meta.indexMeta.indexes[droppedRaceCollID] = map[UniqueID]*model.Index{
		droppedRaceIndexID: {
			CollectionID: droppedRaceCollID,
			FieldID:      droppedRaceFieldID,
			IndexID:      droppedRaceIndexID,
			IndexName:    "idx",
			CreateTime:   droppedRaceCreateTS,
		},
	}
	for segID, state := range map[UniqueID]commonpb.IndexState{
		droppedRaceLiveSeg: commonpb.IndexState_Finished,
		droppedRaceDropSeg: commonpb.IndexState_InProgress,
	} {
		s.meta.segments.SetSegment(segID, NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   droppedRaceCollID,
			PartitionID:    droppedRacePartID,
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			LastExpireTime: droppedRaceCreateTS - 1,
		}))
		segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdx.Insert(droppedRaceIndexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: droppedRaceCollID,
			PartitionID:  droppedRacePartID,
			NumRows:      100,
			IndexID:      droppedRaceIndexID,
			BuildID:      segID + 10000,
			IndexState:   state,
		})
		s.meta.indexMeta.segmentIndexes.Insert(segID, segIdx)
	}
	return s
}

// failIndexAfterSnapshot reproduces issue #54065 deterministically: the
// segment snapshot has already been taken when the segment's partition is
// dropped and its index task is aborted as failed, so the index states read
// next carry the abort while the snapshot still says Flushed.
func failIndexAfterSnapshot(segID UniqueID, dropSegment func()) *mockey.Mocker {
	var origin func(m *indexMeta, collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState
	return mockey.Mock((*indexMeta).getSegmentsIndexStates).To(
		func(m *indexMeta, collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState {
			dropSegment()
			segIdx, _ := m.segmentIndexes.Get(segID)
			idx, _ := segIdx.Get(droppedRaceIndexID)
			idx.IndexState = commonpb.IndexState_Failed
			idx.FailReason = indexTaskAbortReasonSegmentDropped
			return origin(m, collectionID, segmentIDs)
		}).Origin(&origin).Build()
}

func dropSegmentInMeta(s *Server, segID UniqueID) func() {
	return func() {
		dropped := s.meta.GetSegment(context.Background(), segID).Clone()
		dropped.State = commonpb.SegmentState_Dropped
		s.meta.segments.SetSegment(segID, dropped)
	}
}

func TestIndexState_SegmentDroppedAfterSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("GetIndexState ignores the dropped segment's aborted task", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, dropSegmentInMeta(s, droppedRaceDropSeg))
		defer mocker.UnPatch()

		resp, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.IndexState_Finished, resp.GetState())
		assert.Empty(t, resp.GetFailReason())
	})

	t.Run("DescribeIndex ignores the dropped segment's aborted task", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, dropSegmentInMeta(s, droppedRaceDropSeg))
		defer mocker.UnPatch()

		resp, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		require.Len(t, resp.GetIndexInfos(), 1)
		assert.Equal(t, commonpb.IndexState_Finished, resp.GetIndexInfos()[0].GetState())
		assert.Equal(t, int64(100), resp.GetIndexInfos()[0].GetTotalRows())
	})

	t.Run("segment removed from meta after snapshot is ignored", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, func() {
			s.meta.segments.DropSegment(droppedRaceDropSeg)
		})
		defer mocker.UnPatch()

		resp, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Finished, resp.GetState())
	})

	t.Run("compaction output committed with the drop keeps the index in progress", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		const outputSeg = UniqueID(1002)
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, func() {
			dropSegmentInMeta(s, droppedRaceDropSeg)()
			s.meta.segments.SetSegment(outputSeg, NewSegmentInfo(&datapb.SegmentInfo{
				ID:             outputSeg,
				CollectionID:   droppedRaceCollID,
				PartitionID:    droppedRacePartID,
				NumOfRows:      100,
				State:          commonpb.SegmentState_Flushed,
				LastExpireTime: droppedRaceCreateTS - 1,
				CompactionFrom: []int64{droppedRaceDropSeg},
			}))
		})
		defer mocker.UnPatch()

		resp, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_InProgress, resp.GetState())
	})

	t.Run("segment dropped before the snapshot is not re-read", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		dropSegmentInMeta(s, droppedRaceDropSeg)()
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, func() {})
		defer mocker.UnPatch()
		getSegment := mockey.Mock((*meta).GetSegment).Return(nil).Build()
		defer getSegment.UnPatch()

		resp, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Finished, resp.GetState())
		assert.Equal(t, 0, getSegment.Times())
	})

	t.Run("failed task on a live segment still fails the index", func(t *testing.T) {
		s := newDroppedSegmentRaceServer(t)
		mocker := failIndexAfterSnapshot(droppedRaceDropSeg, func() {})
		defer mocker.UnPatch()

		resp, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: droppedRaceCollID})
		require.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Failed, resp.GetState())
		assert.Contains(t, resp.GetFailReason(), indexTaskAbortReasonSegmentDropped)
	})
}
