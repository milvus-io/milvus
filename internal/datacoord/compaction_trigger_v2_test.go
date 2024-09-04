package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestCompactionTriggerManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerManagerSuite))
}

type CompactionTriggerManagerSuite struct {
	suite.Suite

	mockAlloc       *allocator.MockAllocator
	mockHandler     *NMockHandler
	mockPlanContext *MockCompactionPlanContext
	testLabel       *CompactionGroupLabel
	meta            *meta

	trigger *CompactionTriggerManager
}

func (s *CompactionTriggerManagerSuite) SetupTest() {
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockHandler = NewNMockHandler(s.T())
	s.mockPlanContext = NewMockCompactionPlanContext(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	s.meta = &meta{
		segments: NewSegmentsInfo(),
		indexMeta: &indexMeta{
			indexes: make(map[UniqueID]map[UniqueID]*model.Index),
		},
		collections: make(map[UniqueID]*collectionInfo),
	}
	s.meta.AddCollection(&collectionInfo{ID: s.testLabel.CollectionID})

	segments := genSegmentsForMeta(s.testLabel)
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	s.trigger = NewCompactionTriggerManager(s.mockAlloc, s.mockHandler, s.mockPlanContext, s.meta)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
}

func (s *CompactionTriggerManagerSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
}

func (s *CompactionTriggerManagerSuite) TestStartClose() {
	s.trigger.Start()
	s.trigger.Close()
}

func (s *CompactionTriggerManagerSuite) TestGetSeperateViews() {
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(19530, nil).Once()
	collInfo := &collectionInfo{ID: s.testLabel.CollectionID}
	got := s.trigger.getSeperateViews(context.Background(), collInfo, NotForce)
	s.NotEmpty(got)
	s.Equal(2, len(got))

	l0Views, ok := got[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.Equal(1, len(l0Views))

	l0View, ok := l0Views[0].(*LevelZeroSegmentsView)
	s.True(ok)
	s.NotEmpty(l0View.segments)
	for _, seg := range l0View.segments {
		s.Equal(datapb.SegmentLevel_L0, seg.Level)
	}

	l2Views, ok := got[TriggerTypeDeltaTooMuch]
	s.True(ok)
	s.Equal(1, len(l2Views))

	l2View, ok := l2Views[0].(*MixSegmentView)
	s.True(ok)
	s.NotEmpty(l2View.segments)
	for _, seg := range l2View.segments {
		s.Equal(datapb.SegmentLevel_L2, seg.Level)
	}
}

func (s *CompactionTriggerManagerSuite) TestManualTrigger() {
	s.Run("skipped", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(&collectionInfo{ID: s.testLabel.CollectionID}, nil).Once()
		triggerID, err := s.trigger.ManualTrigger(context.Background(), s.testLabel.CollectionID, TriggerTypeClustering)
		s.NoError(err)
		s.EqualValues(0, triggerID)
	})

	s.Run("get collection error", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock get collection wrong")).Once()
		triggerID, err := s.trigger.ManualTrigger(context.Background(), s.testLabel.CollectionID, TriggerTypeClustering)
		s.Error(err)
		s.EqualValues(0, triggerID)
	})

	s.Run("triggered", func() {
		s.SetupTest()
		s.meta.compactionTaskMeta = &compactionTaskMeta{compactionTasks: make(map[int64]map[int64]*datapb.CompactionTask)}

		s.mockPlanContext.EXPECT().enqueueCompaction(mock.Anything).Return(nil).Once()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: newTestScalarClusteringKeySchema()}, nil).Twice()
		s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(19530, nil).Once()
		s.mockAlloc.EXPECT().AllocN(mock.Anything).RunAndReturn(
			func(count int64) (int64, int64, error) {
				return 99999, 99999 + count, nil
			}).Twice()
		triggerID, err := s.trigger.ManualTrigger(context.Background(), s.testLabel.CollectionID, TriggerTypeClustering)
		s.NoError(err)
		s.EqualValues(19530, triggerID)
	})
}

func (s *CompactionTriggerManagerSuite) TestTrigger() {
	s.Run("handler failed to get collection", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Twice()
		s.trigger.TimelyTrigger()
		s.trigger.IDLETrigger(context.Background(), AllCollection)
	})

	s.Run("collection auto compaction not enabled", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(&collectionInfo{
				ID:         s.testLabel.CollectionID,
				Properties: map[string]string{common.CollectionAutoCompactionKey: "false"},
			}, nil).Twice()
		s.trigger.TimelyTrigger()
		s.trigger.IDLETrigger(context.Background(), AllCollection)
	})

	s.Run("collection auto compaction invalid value", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(&collectionInfo{
				ID:         s.testLabel.CollectionID,
				Properties: map[string]string{common.CollectionAutoCompactionKey: "okokok"},
			}, nil).Twice()
		s.trigger.TimelyTrigger()
		s.trigger.IDLETrigger(context.Background(), 1)
	})

	s.Run("triggered", func() {
		s.SetupTest()
		s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).
			Return(&collectionInfo{ID: s.testLabel.CollectionID}, nil)
		s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(19530, nil).Times(4)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(19530, 100000000, nil).Once()
		s.mockPlanContext.EXPECT().isFull().Return(false).Times(3)
		s.mockPlanContext.EXPECT().enqueueCompaction(mock.Anything).RunAndReturn(
			func(task *datapb.CompactionTask) error {
				s.EqualValues(datapb.CompactionTaskState_pipelining, task.GetState())
				s.NotNil(task.GetInputSegments())
				s.EqualValues(19530, task.GetTriggerID())
				s.EqualValues(19530, task.GetPlanID())
				return nil
			},
		).Times(3)

		s.trigger.TimelyTrigger()
		s.trigger.IDLETrigger(context.Background(), AllCollection)
	})
}

func (s *CompactionTriggerManagerSuite) TestGetExpectedSegmentSize() {
	var (
		collectionID = int64(1)
		fieldID      = int64(2000)
		indexID      = int64(3000)
	)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, strconv.Itoa(100))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key, strconv.Itoa(200))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)

	s.trigger.meta = &meta{
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

		s.Equal(int64(200*1024*1024), getExpectedSegmentSize(s.trigger.meta, collection))
	})

	s.Run("HNSW & DISKANN", func() {
		s.trigger.meta = &meta{
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

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.trigger.meta, collection))
	})

	s.Run("some vector has no index", func() {
		s.trigger.meta = &meta{
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

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.trigger.meta, collection))
	})
}
