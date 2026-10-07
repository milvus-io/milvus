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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSingleCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(SingleCompactionPolicySuite))
}

type SingleCompactionPolicySuite struct {
	jitter string
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            *NMockHandler
	inspector          *MockCompactionInspector

	singlePolicy *singleCompactionPolicy
}

func (s *SingleCompactionPolicySuite) TearDownTest() {
	Params.Save(Params.DataCoordCfg.SingleCompactionThresholdJitter.Key, s.jitter)
}

func (s *SingleCompactionPolicySuite) SetupTest() {
	// The boundary cases below assume the configured thresholds apply exactly.
	s.jitter = Params.DataCoordCfg.SingleCompactionThresholdJitter.GetValue()
	Params.Save(Params.DataCoordCfg.SingleCompactionThresholdJitter.Key, "0")
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{segments: NewSegmentsInfo(), collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
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
	segments[102] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 500, 10000, 10, true, false)
	segments[103] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 100, 10000, 1, true, false)
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: {
					101: segments[101],
					102: segments[102],
					103: segments[103],
				},
			},
		},
	}

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

	candidates, _, _, _, err := s.singlePolicy.triggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(2, len(candidates))
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
	segments[102] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 500, 10000, 10, false, true)
	segments[103] = buildTestSegment(101, collID, datapb.SegmentLevel_L1, 100, 10000, 1, false, false)
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: {
					101: segments[101],
					102: segments[102],
					103: segments[103],
				},
			},
		},
	}

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

	_, sortViews, _, _, err := s.singlePolicy.triggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(3, len(sortViews))
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
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: {
					101: segments[101],
					102: segments[102],
					103: segments[103],
				},
			},
		},
	}

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

	candidates, sortViews, _, triggerID, err := policy.triggerOneCollection(context.Background(), collID, false)
	s.NoError(err)
	s.Nil(candidates)
	s.Nil(sortViews)
	s.EqualValues(0, triggerID)
}

// resetSingleCompactionAdmitterForTest replaces the process-wide admitter so a
// test starts with a full bucket and fresh fairness cursors.
func resetSingleCompactionAdmitterForTest() {
	getSingleCompactionAdmitter()
	globalSingleCompactionAdmitter = newSingleCompactionAdmitter(time.Now)
}

// With a budget of two tokens and three eligible segments in each of two
// collections, one trigger round admits one segment per collection: the
// candidates of the whole round are admitted in one pass and the budget
// rotates across collections instead of being drained by the first one.
func (s *SingleCompactionPolicySuite) TestTriggerAdmitsAcrossCollections() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitTokens.Key, "2")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitTokens.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitInterval.Key)
	resetSingleCompactionAdmitterForTest()

	meta := s.singlePolicy.meta
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Unset()
	for _, collID := range []int64{100, 200} {
		coll := &collectionInfo{ID: collID, Schema: newTestSchema()}
		meta.collections.Insert(collID, coll)
		s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Maybe()
		for i := int64(1); i <= 3; i++ {
			id := collID + i
			// 201 deltalogs: over the file-count threshold, so every segment is eligible.
			meta.segments.SetSegment(id, buildTestSegment(id, collID, datapb.SegmentLevel_L2, 0, 10000, 201, true, false))
		}
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	}, nil).Maybe()

	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	views := events[TriggerTypeSingle]
	s.Len(views, 2)
	admittedCollections := make(map[int64]int)
	for _, view := range views {
		admittedCollections[view.GetGroupLabel().CollectionID]++
	}
	s.Equal(map[int64]int{100: 1, 200: 1}, admittedCollections)
}

// The budget is bounded by the room left in the inspector.
func (s *SingleCompactionPolicySuite) TestTriggerBoundedByInspectorCapacity() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitTokens.Key, "10")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SingleCompactionRateLimitTokens.Key)
	resetSingleCompactionAdmitterForTest()

	meta := s.singlePolicy.meta
	collID := int64(100)
	coll := &collectionInfo{ID: collID, Schema: newTestSchema()}
	meta.collections.Insert(collID, coll)
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Unset()
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil).Maybe()
	for i := int64(1); i <= 5; i++ {
		meta.segments.SetSegment(collID+i, buildTestSegment(collID+i, collID, datapb.SegmentLevel_L2, 0, 10000, 201, true, false))
	}
	s.singlePolicy.remainingCapacity = func() int { return 1 }

	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	s.Len(events[TriggerTypeSingle], 1)
	s.Equal(9.0, globalSingleCompactionAdmitter.tokens, "only the admitted candidate consumed a token")
}
