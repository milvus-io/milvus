package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestCompactionTriggerManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerManagerSuite))
}

type CompactionTriggerManagerSuite struct {
	suite.Suite

	mockAlloc       *allocator.MockAllocator
	handler         Handler
	mockPlanContext *MockCompactionPlanContext
	testLabel       *CompactionGroupLabel
	meta            *meta

	triggerManager *CompactionTriggerManager
}

func (s *CompactionTriggerManagerSuite) SetupTest() {
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.handler = NewNMockHandler(s.T())
	s.mockPlanContext = NewMockCompactionPlanContext(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	segments := genSegmentsForMeta(s.testLabel)
	s.meta = &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	s.triggerManager = NewCompactionTriggerManager(s.mockAlloc, s.handler, s.mockPlanContext, s.meta)
}

func (s *CompactionTriggerManagerSuite) TestNotifyByViewIDLE() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
	s.triggerManager.handler = handler

	collSegs := s.meta.GetCompactableSegmentGroupByCollection()

	segments, found := collSegs[1]
	s.Require().True(found)

	seg1, found := lo.Find(segments, func(info *SegmentInfo) bool {
		return info.ID == int64(100) && info.GetLevel() == datapb.SegmentLevel_L0
	})
	s.Require().True(found)

	// Prepare only 1 l0 segment that doesn't meet the Trigger minimum condition
	// but ViewIDLE Trigger will still forceTrigger the plan
	latestL0Segments := GetViewsByInfo(seg1)
	expectedSegID := seg1.ID

	s.Require().Equal(1, len(latestL0Segments))
	needRefresh, levelZeroView := s.triggerManager.l0Policy.getChangedLevelZeroViews(1, latestL0Segments)
	s.True(needRefresh)
	s.Require().Equal(1, len(levelZeroView))
	cView, ok := levelZeroView[0].(*LevelZeroSegmentsView)
	s.True(ok)
	s.NotNil(cView)
	log.Info("view", zap.Any("cView", cView))

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.EqualValues(19530, task.GetTriggerID())
			// s.True(signal.isGlobal)
			// s.False(signal.isForce)
			s.EqualValues(30000, task.GetPos().GetTimestamp())
			s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
			s.Equal(s.testLabel.PartitionID, task.GetPartitionID())

			s.Equal(s.testLabel.Channel, task.GetChannel())
			s.Equal(datapb.CompactionType_Level0DeleteCompaction, task.GetType())

			expectedSegs := []int64{expectedSegID}
			s.ElementsMatch(expectedSegs, task.GetInputSegments())
			return nil
		}).Return(nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(19530, nil).Maybe()
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewIDLE, levelZeroView)
}

func (s *CompactionTriggerManagerSuite) TestNotifyByViewChange() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
	s.triggerManager.handler = handler
	collSegs := s.meta.GetCompactableSegmentGroupByCollection()

	segments, found := collSegs[1]
	s.Require().True(found)

	levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
		return info.GetLevel() == datapb.SegmentLevel_L0
	})

	latestL0Segments := GetViewsByInfo(levelZeroSegments...)
	s.Require().NotEmpty(latestL0Segments)
	needRefresh, levelZeroView := s.triggerManager.l0Policy.getChangedLevelZeroViews(1, latestL0Segments)
	s.Require().True(needRefresh)
	s.Require().Equal(1, len(levelZeroView))
	cView, ok := levelZeroView[0].(*LevelZeroSegmentsView)
	s.True(ok)
	s.NotNil(cView)
	log.Info("view", zap.Any("cView", cView))

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.EqualValues(19530, task.GetTriggerID())
			// s.True(signal.isGlobal)
			// s.False(signal.isForce)
			s.EqualValues(30000, task.GetPos().GetTimestamp())
			s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
			s.Equal(s.testLabel.PartitionID, task.GetPartitionID())
			s.Equal(s.testLabel.Channel, task.GetChannel())
			s.Equal(datapb.CompactionType_Level0DeleteCompaction, task.GetType())

			expectedSegs := []int64{100, 101, 102}
			s.ElementsMatch(expectedSegs, task.GetInputSegments())
			return nil
		}).Return(nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(19530, nil).Maybe()
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewChange, levelZeroView)
}

func (s *CompactionTriggerManagerSuite) TestGetExpectedSegmentSize() {
	var (
		collectionID = int64(1000)
		fieldID      = int64(2000)
		indexID      = int64(3000)
	)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, strconv.Itoa(100))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key, strconv.Itoa(200))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key)

	s.triggerManager.meta = &meta{
		indexMeta: &indexMeta{
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collectionID: {
					indexID + 1: &model.Index{
						CollectionID: collectionID,
						FieldID:      fieldID + 1,
						IndexID:      indexID + 1,
						IndexName:    "",
						IsDeleted:    false,
						CreateTime:   0,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{Key: common.IndexTypeKey, Value: "DISKANN"},
						},
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
					indexID + 2: &model.Index{
						CollectionID: collectionID,
						FieldID:      fieldID + 2,
						IndexID:      indexID + 2,
						IndexName:    "",
						IsDeleted:    false,
						CreateTime:   0,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{Key: common.IndexTypeKey, Value: "DISKANN"},
						},
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
				},
			},
		},
	}

	s.Run("all DISKANN", func() {
		collection := &collectionInfo{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Name:        "coll1",
				Description: "",
				Fields: []*schemapb.FieldSchema{
					{FieldID: fieldID, Name: "field0", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: fieldID + 1, Name: "field1", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
					{FieldID: fieldID + 2, Name: "field2", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
				},
				EnableDynamicField: false,
				Properties:         nil,
			},
		}

		s.Equal(int64(200*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection))
	})

	s.Run("HNSW & DISKANN", func() {
		s.triggerManager.meta = &meta{
			indexMeta: &indexMeta{
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collectionID: {
						indexID + 1: &model.Index{
							CollectionID: collectionID,
							FieldID:      fieldID + 1,
							IndexID:      indexID + 1,
							IndexName:    "",
							IsDeleted:    false,
							CreateTime:   0,
							TypeParams:   nil,
							IndexParams: []*commonpb.KeyValuePair{
								{Key: common.IndexTypeKey, Value: "HNSW"},
							},
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						indexID + 2: &model.Index{
							CollectionID: collectionID,
							FieldID:      fieldID + 2,
							IndexID:      indexID + 2,
							IndexName:    "",
							IsDeleted:    false,
							CreateTime:   0,
							TypeParams:   nil,
							IndexParams: []*commonpb.KeyValuePair{
								{Key: common.IndexTypeKey, Value: "DISKANN"},
							},
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
			},
		}
		collection := &collectionInfo{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Name:        "coll1",
				Description: "",
				Fields: []*schemapb.FieldSchema{
					{FieldID: fieldID, Name: "field0", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: fieldID + 1, Name: "field1", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
					{FieldID: fieldID + 2, Name: "field2", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
				},
				EnableDynamicField: false,
				Properties:         nil,
			},
		}

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection))
	})

	s.Run("some vector has no index", func() {
		s.triggerManager.meta = &meta{
			indexMeta: &indexMeta{
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collectionID: {
						indexID + 1: &model.Index{
							CollectionID: collectionID,
							FieldID:      fieldID + 1,
							IndexID:      indexID + 1,
							IndexName:    "",
							IsDeleted:    false,
							CreateTime:   0,
							TypeParams:   nil,
							IndexParams: []*commonpb.KeyValuePair{
								{Key: common.IndexTypeKey, Value: "HNSW"},
							},
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
			},
		}
		collection := &collectionInfo{
			ID: collectionID,
			Schema: &schemapb.CollectionSchema{
				Name:        "coll1",
				Description: "",
				Fields: []*schemapb.FieldSchema{
					{FieldID: fieldID, Name: "field0", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: fieldID + 1, Name: "field1", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
					{FieldID: fieldID + 2, Name: "field2", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
				},
				EnableDynamicField: false,
				Properties:         nil,
			},
		}

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection))
	})
}
