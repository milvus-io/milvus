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

package compaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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
	testLabel          *view.CompactionGroupLabel
	handler            *meta.NMockHandler
	inspector          *MockCompactionInspector

	singlePolicy *policy.SingleCompactionPolicy
}

func (s *SingleCompactionPolicySuite) SetupTest() {
	s.testLabel = &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := policy.GenSegmentsForMeta(s.testLabel)
	singleStore := metacache.NewMetaStore(nil)
	mt := &meta.Meta{Segments: meta.NewSegmentsInfo(singleStore), MetaStore: singleStore}
	for id, segment := range segments {
		mt.Segments.SetSegment(id, segment)
	}
	singleStore.PutCollection(&meta.CollectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})

	s.mockAlloc = meta.NewMockAllocator(s.T())
	mockHandler := meta.NewNMockHandler(s.T())
	s.handler = mockHandler
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&meta.CollectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	}, nil).Maybe()
	s.singlePolicy = policy.NewSingleCompactionPolicy(mt, nil, s.mockAlloc, mockHandler)
}

func (s *SingleCompactionPolicySuite) TestTrigger() {
	events, err := s.singlePolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[policy.TriggerTypeSingle]
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
) *meta.SegmentInfo {
	deltaBinlogs := make([]*datapb.Binlog, 0)
	for i := 0; i < deltaLogNum; i++ {
		deltaBinlogs = append(deltaBinlogs, &datapb.Binlog{
			EntriesNum: deleteRows,
		})
	}

	return &meta.SegmentInfo{
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
	segment0 := buildTestSegment(101, meta.CollID, datapb.SegmentLevel_L2, 0, 10000, 201, true, true)
	s.Equal(true, policy.HasTooManyDeletions(segment0))

	segment1 := buildTestSegment(101, meta.CollID, datapb.SegmentLevel_L2, 3000, 10000, 1, true, true)
	s.Equal(true, policy.HasTooManyDeletions(segment1))

	segment2 := buildTestSegment(101, meta.CollID, datapb.SegmentLevel_L2, 300, 10000, 10, true, true)
	s.Equal(true, policy.HasTooManyDeletions(segment2))
}

func (s *SingleCompactionPolicySuite) TestL2SingleCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID:     collID,
		Schema: meta.NewTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*meta.SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L2, 0, 10000, 201, true, false)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L2, 500, 10000, 10, true, false)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L2, 100, 10000, 1, true, false)
	segmentsInfo := meta.NewSegmentsInfoWithSegments(segments)

	compactionTaskMeta := ctask.NewTestCompactionTaskMeta(s.T())
	s.singlePolicy.Meta = &meta.Meta{
		Segments: segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
	})

	views, _, _, err := s.singlePolicy.TriggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(2, len(views))
}

func (s *SingleCompactionPolicySuite) TestSortCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID:     collID,
		Schema: meta.NewTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*meta.SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L1, 0, 10000, 201, false, true)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L2, 500, 10000, 10, false, true)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L1, 100, 10000, 1, false, false)
	segmentsInfo := meta.NewSegmentsInfoWithSegments(segments)

	compactionTaskMeta := ctask.NewTestCompactionTaskMeta(s.T())
	s.singlePolicy.Meta = &meta.Meta{
		Segments: segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
		Type:         datapb.CompactionType_SortCompaction,
	})

	_, sortViews, _, err := s.singlePolicy.TriggerOneCollection(context.TODO(), collID, false)
	s.NoError(err)
	s.Equal(3, len(sortViews))
}

func (s *SingleCompactionPolicySuite) TestSegmentSortCompaction() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID:     collID,
		Schema: meta.NewTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)

	segments := make(map[UniqueID]*meta.SegmentInfo, 0)
	segments[101] = buildTestSegment(101, collID, datapb.SegmentLevel_L1, 0, 10000, 201, false, true)
	segments[102] = buildTestSegment(102, collID, datapb.SegmentLevel_L1, 0, 10000, 201, true, true)
	segments[103] = buildTestSegment(103, collID, datapb.SegmentLevel_L1, 0, 10000, 201, true, true)
	segments[103].State = commonpb.SegmentState_Dropped
	segmentsInfo := meta.NewSegmentsInfoWithSegments(segments)

	compactionTaskMeta := ctask.NewTestCompactionTaskMeta(s.T())
	s.singlePolicy.Meta = &meta.Meta{
		Segments: segmentsInfo,
	}
	compactionTaskMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: collID,
		State:        datapb.CompactionTaskState_executing,
		Type:         datapb.CompactionType_SortCompaction,
	})

	sortView := s.singlePolicy.TriggerSegmentSortCompaction(context.TODO(), 101)
	s.NotNil(sortView)

	sortView = s.singlePolicy.TriggerSegmentSortCompaction(context.TODO(), 102)
	s.Nil(sortView)

	sortView = s.singlePolicy.TriggerSegmentSortCompaction(context.TODO(), 103)
	s.Nil(sortView)
}

func (s *SingleCompactionPolicySuite) TestTriggerOneCollectionSkipExternal() {
	collID := s.testLabel.CollectionID
	coll := &meta.CollectionInfo{
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
	mockHandler := meta.NewNMockHandler(s.T())
	mockHandler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil)
	pol := policy.NewSingleCompactionPolicy(s.singlePolicy.Meta, s.singlePolicy.IndexMeta, s.mockAlloc, mockHandler)

	views, sortViews, triggerID, err := pol.TriggerOneCollection(context.Background(), collID, false)
	s.NoError(err)
	s.Nil(views)
	s.Nil(sortViews)
	s.EqualValues(0, triggerID)
}
