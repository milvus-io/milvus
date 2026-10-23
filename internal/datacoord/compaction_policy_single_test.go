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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSingleCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(SingleCompactionPolicySuite))
}

type SingleCompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            *NMockHandler
	inspector          *MockCompactionInspector

	singlePolicy *singleCompactionPolicy
}

func (s *SingleCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	singleStore := metacache.NewMetaStore(nil)
	meta := &meta{segments: NewSegmentsInfo(singleStore), metaStore: singleStore}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	singleStore.PutCollection(&collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})

	s.mockAlloc = newMockAllocator(s.T())
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	}, nil).Maybe()
	s.singlePolicy = newSingleCompactionPolicy(meta, s.mockAlloc, mockHandler)
}

func (s *SingleCompactionPolicySuite) TestTrigger() {
	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeSingle]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}

func buildTestSegment(id int64,
	collId int64,
	level datapb.SegmentLevel,
	deleteRows int64,
	totalRows int64,
	deltaLogNum int,
	isSorted bool,
	isInvisible bool,
) *SegmentInfo {
	deltaBinlogs := make([]*datapb.Binlog, 0)
	for i := 0; i < deltaLogNum; i++ {
		deltaBinlogs = append(deltaBinlogs, &datapb.Binlog{
			EntriesNum: deleteRows,
		})
	}

	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           id,
			CollectionID: collId,
			Level:        level,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    totalRows,
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: deltaBinlogs,
				},
			},
			IsSorted:    isSorted,
			IsInvisible: isInvisible,
		},
	}
}

func (s *SingleCompactionPolicySuite) TestIsDeleteRowsTooManySegment() {
	segment0 := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 0, 10000, 201, true, true)
	s.Equal(true, hasTooManyDeletions(segment0))

	segment1 := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 3000, 10000, 1, true, true)
	s.Equal(true, hasTooManyDeletions(segment1))

	segment2 := buildTestSegment(101, collID, datapb.SegmentLevel_L2, 300, 10000, 10, true, true)
	s.Equal(true, hasTooManyDeletions(segment2))
}

func (s *SingleCompactionPolicySuite) TestL2SingleCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 0, 10000, 201, true, false)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L2, 500, 10000, 10, true, false)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L2, 100, 10000, 1, true, false)
	segmentsInfo := newSegmentsInfoWithSegments(segments)

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.singlePolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
		segments:           segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
	})

	views, _, _, _, err := s.singlePolicy.triggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(2, len(views))
}

func (s *SingleCompactionPolicySuite) TestSortCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L1, 0, 10000, 201, false, true)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L2, 500, 10000, 10, false, true)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L1, 100, 10000, 1, false, false)
	segmentsInfo := newSegmentsInfoWithSegments(segments)

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.singlePolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
		segments:           segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
		Type:         datapb.CompactionType_SortCompaction,
	})

	_, invisibleSortViews, visibleSortViews, _, err := s.singlePolicy.triggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(3, len(invisibleSortViews)+len(visibleSortViews))
}

// TestTriggerSortsInvisibleSegmentsFirst checks that the sort sweep returns the
// sorts of invisible segments, which a flush or a clustering compaction just
// wrote, before the sorts of visible segments of any collection.
func (s *SingleCompactionPolicySuite) TestTriggerSortsInvisibleSegmentsFirst() {
	newSegment := func(id, collectionID int64, invisible bool) *SegmentInfo {
		label := &CompactionGroupLabel{CollectionID: collectionID, PartitionID: 10, Channel: "ch-1"}
		segment := genTestSegmentInfo(label, id, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
		segment.IsInvisible = invisible
		return segment
	}
	store := metacache.NewMetaStore(nil)
	meta := &meta{segments: NewSegmentsInfo(store), metaStore: store}
	// Each collection has a visible and an invisible unsorted segment.
	for _, segment := range []*SegmentInfo{newSegment(1, 100, false), newSegment(2, 100, true), newSegment(3, 200, false), newSegment(4, 200, true)} {
		meta.segments.SetSegment(segment.GetID(), segment)
	}
	for _, collectionID := range []int64{100, 200} {
		store.PutCollection(&collectionInfo{ID: collectionID, Schema: &schemapb.CollectionSchema{}})
	}
	s.singlePolicy.meta = meta

	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	sortedSegments := lo.Map(events[TriggerTypeSort], func(view CompactionView, _ int) int64 {
		return view.GetSegmentsView()[0].ID
	})
	s.Require().Len(sortedSegments, 4)
	s.ElementsMatch([]int64{2, 4}, sortedSegments[:2])
}

func (s *SingleCompactionPolicySuite) TestSegmentSortCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L1, 0, 10000, 201, false, true)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L1, 0, 10000, 201, true, true)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L1, 0, 10000, 201, true, true)
	segments[103].State = commonpb.SegmentState_Dropped
	segmentsInfo := newSegmentsInfoWithSegments(segments)

	compactionTaskMeta := newTestCompactionTaskMeta(s.T())
	s.singlePolicy.meta = &meta{
		compactionTaskMeta: compactionTaskMeta,
		segments:           segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
		Type:         datapb.CompactionType_SortCompaction,
	})

	sortView := s.singlePolicy.triggerSegmentSortCompaction(context.TODO(), 101)
	s.NotNil(sortView)

	sortView = s.singlePolicy.triggerSegmentSortCompaction(context.TODO(), 102)
	s.Nil(sortView)

	sortView = s.singlePolicy.triggerSegmentSortCompaction(context.TODO(), 103)
	s.Nil(sortView)
}

func (s *SingleCompactionPolicySuite) TestTriggerOneCollectionSkipExternal() {
	collID := s.testLabel.CollectionID
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:       1,
					Name:          "external_pk",
					DataType:      schemapb.DataType_Int64,
					ExternalField: "pk_col",
				},
			},
		},
	}
	mockHandler := NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil)
	policy := newSingleCompactionPolicy(s.singlePolicy.meta, s.mockAlloc, mockHandler)

	views, invisibleSortViews, visibleSortViews, triggerID, err := policy.triggerOneCollection(context.Background(), collID, false)
	s.NoError(err)
	s.Nil(views)
	s.Nil(invisibleSortViews)
	s.Nil(visibleSortViews)
	s.EqualValues(0, triggerID)
}
