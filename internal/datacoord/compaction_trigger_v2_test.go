package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionTriggerManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerManagerSuite))
}

// testCompactionPolicy is a minimal CompactionPolicy stub for handleTicker tests.
type testCompactionPolicy struct {
	enabled             bool
	triggerResult       map[CompactionTriggerType][]CompactionView
	triggerErr          error
	inlineTriggerResult map[CompactionTriggerType][]CompactionView
	policyName          string
}

func (p *testCompactionPolicy) Enable() bool { return p.enabled }
func (p *testCompactionPolicy) TriggerInline(_ context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	return p.inlineTriggerResult, nil
}

func (p *testCompactionPolicy) Trigger(_ context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	return p.triggerResult, p.triggerErr
}
func (p *testCompactionPolicy) Name() string { return p.policyName }

// stubDispatchableView is a CompactionView stub for handleTicker tests that need a
// non-inline-executable view to reach the dispatch / isFull branch. All methods
// return zero values; the only method used by handleTicker is IsInlineExecutable.
type stubDispatchableView struct{}

func (stubDispatchableView) GetGroupLabel() *CompactionGroupLabel { return &CompactionGroupLabel{} }
func (stubDispatchableView) GetSegmentsView() []*SegmentView      { return nil }
func (stubDispatchableView) Append(_ ...*SegmentView)             {}
func (stubDispatchableView) String() string                       { return "stubDispatchableView" }

// Trigger returns (nil, "") so that notify()'s `if outView != nil` short-circuits
// before invoking the real Submit*ViewToScheduler path. Tests using this stub care
// only about the dispatch decision (isFull check), not the downstream submit.
func (stubDispatchableView) Trigger() (CompactionView, string)           { return nil, "" }
func (stubDispatchableView) ForceTrigger() (CompactionView, string)      { return nil, "" }
func (stubDispatchableView) ForceTriggerAll() ([]CompactionView, string) { return nil, "" }
func (stubDispatchableView) GetTriggerID() int64                         { return 0 }
func (stubDispatchableView) IsInlineExecutable() bool                    { return false }

type CompactionTriggerManagerSuite struct {
	suite.Suite

	mockAlloc      *allocator.MockAllocator
	handler        Handler
	inspector      *MockCompactionInspector
	testLabel      *CompactionGroupLabel
	meta           *meta
	versionManager *MockVersionManager

	triggerManager *CompactionTriggerManager
}

func (s *CompactionTriggerManagerSuite) SetupTest() {
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.handler = NewNMockHandler(s.T())
	s.inspector = NewMockCompactionInspector(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	segments := genSegmentsForMeta(s.testLabel)
	s.meta = &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}
	s.meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	versionManager := NewMockVersionManager(s.T())
	versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.7.0")).Maybe()
	s.versionManager = versionManager
	s.triggerManager = NewCompactionTriggerManager(s.mockAlloc, s.handler, s.inspector, s.meta, s.versionManager)
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
	levelZeroViews := s.triggerManager.l0Policy.groupL0ViewsByPartChan(1, latestL0Segments, 10000)
	s.Require().Equal(1, len(levelZeroViews))
	cView, ok := levelZeroViews[0].(*LevelZeroCompactionView)
	s.True(ok)
	s.NotNil(cView)
	log.Info("view", zap.Any("cView", cView))

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.inspector.EXPECT().enqueueCompaction(mock.Anything).
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
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewIDLE, levelZeroViews)
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
	levelZeroViews := s.triggerManager.l0Policy.groupL0ViewsByPartChan(1, latestL0Segments, 10000)
	s.Require().Equal(1, len(levelZeroViews))
	cView, ok := levelZeroViews[0].(*LevelZeroCompactionView)
	s.True(ok)
	s.NotNil(cView)
	log.Info("view", zap.Any("cView", cView))

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.inspector.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.EqualValues(19530, task.GetTriggerID())
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
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewChange, levelZeroViews)
}

func (s *CompactionTriggerManagerSuite) TestManualTriggerSkipExternal() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&collectionInfo{
		ID: 1,
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
	}, nil)
	s.triggerManager.handler = handler

	_, err := s.triggerManager.ManualTrigger(context.Background(), 1, true, false, 0)
	s.Error(err)
	s.Contains(err.Error(), "external collection")
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

		s.Equal(int64(200*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection.ID, collection.Schema))
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

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection.ID, collection.Schema))
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

		s.Equal(int64(100*1024*1024), getExpectedSegmentSize(s.triggerManager.meta, collection.ID, collection.Schema))
	})
}

func (s *CompactionTriggerManagerSuite) TestManualTriggerL0Compaction() {
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
	s.triggerManager.handler = handler

	collSegs := s.meta.GetCompactableSegmentGroupByCollection()
	segments, found := collSegs[1]
	s.Require().True(found)

	levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
		return info.GetLevel() == datapb.SegmentLevel_L0
	})
	s.Require().NotEmpty(levelZeroSegments)

	// Mock allocator for trigger ID
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(19530), nil).Maybe()

	// Mock inspector to expect compaction enqueue
	s.inspector.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.EqualValues(19530, task.GetTriggerID())
			s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
			s.Equal(s.testLabel.PartitionID, task.GetPartitionID())
			s.Equal(s.testLabel.Channel, task.GetChannel())
			s.Equal(datapb.CompactionType_Level0DeleteCompaction, task.GetType())

			expectedSegs := []int64{100, 101, 102}
			s.ElementsMatch(expectedSegs, task.GetInputSegments())
			return nil
		}).Return(nil).Once()

	// Test L0 manual trigger
	triggerID, err := s.triggerManager.ManualTrigger(context.Background(), s.testLabel.CollectionID, false, true, 0)
	s.NoError(err)
	s.Equal(int64(12345), triggerID)
}

func (s *CompactionTriggerManagerSuite) TestManualTriggerInvalidParams() {
	// Test with both clustering and L0 compaction false
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
	s.triggerManager.handler = handler
	triggerID, err := s.triggerManager.ManualTrigger(context.Background(), s.testLabel.CollectionID, false, false, 0)
	s.NoError(err)
	s.Equal(int64(0), triggerID)
}

func (s *CompactionTriggerManagerSuite) TestSubmitBackfillViewToScheduler() {
	collectionSchema := &schemapb.CollectionSchema{
		Name: "test_coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25_fn", Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{100}},
		},
	}
	backfillFunc := collectionSchema.Functions[0]

	makeBackfillView := func(triggerID int64) *BackfillSegmentsView {
		segView := &SegmentView{
			ID:        200,
			label:     s.testLabel,
			NumOfRows: 1000,
		}
		return &BackfillSegmentsView{
			label:     s.testLabel,
			segments:  []*SegmentView{segView},
			triggerID: triggerID,
			funcDiff:  &FuncDiff{Added: []*schemapb.FunctionSchema{backfillFunc}},
			schema:    collectionSchema, // frozen at scan time
		}
	}

	s.Run("AllocN fails", func() {
		s.SetupTest()
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(int64(0), int64(0), errors.New("alloc error")).Once()
		view := makeBackfillView(111)
		// Should return early with no panic — no other mock calls expected.
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("GetCollection fails", func() {
		s.SetupTest()
		const planID = int64(500)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, errors.New("get collection error")).Once()
		s.triggerManager.handler = handler

		view := makeBackfillView(111)
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("collection is nil", func() {
		s.SetupTest()
		const planID = int64(501)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, nil).Once()
		s.triggerManager.handler = handler

		view := makeBackfillView(111)
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("collection is external", func() {
		s.SetupTest()
		const planID = int64(502)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID: s.testLabel.CollectionID,
				// IsExternal() checks for fields with ExternalField set, not Schema.ExternalSource.
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 1, Name: "ext_field", ExternalField: "s3://bucket/external"},
					},
				},
			}, nil).Once()
		s.triggerManager.handler = handler

		view := makeBackfillView(111)
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("funcDiff is nil", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const planID = int64(504)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:     s.testLabel.CollectionID,
				Schema: collectionSchema,
			}, nil).Once()
		s.triggerManager.handler = handler

		// Create a BackfillSegmentsView with funcDiff=nil — should log warning and return, not panic.
		view := &BackfillSegmentsView{
			label:     s.testLabel,
			segments:  []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 1000}},
			triggerID: 111,
			funcDiff:  nil,
		}
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("view is not BackfillSegmentsView", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const planID = int64(503)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:     s.testLabel.CollectionID,
				Schema: collectionSchema,
			}, nil).Once()
		s.triggerManager.handler = handler

		// Use LevelZeroCompactionView which is NOT a *BackfillSegmentsView.
		nonBackfillView := &LevelZeroCompactionView{
			label:      s.testLabel,
			l0Segments: []*SegmentView{},
		}
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), nonBackfillView)
	})

	s.Run("enqueueCompaction fails", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const (
			planID    = int64(504)
			triggerID = int64(999)
		)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:     s.testLabel.CollectionID,
				Schema: collectionSchema,
			}, nil).Once()
		s.triggerManager.handler = handler
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).Return(errors.New("enqueue error")).Once()

		view := makeBackfillView(triggerID)
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})

	s.Run("success", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const (
			planID    = int64(600)
			triggerID = int64(1001)
		)
		s.mockAlloc.EXPECT().AllocN(int64(1)).Return(planID, planID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:     s.testLabel.CollectionID,
				Schema: collectionSchema,
			}, nil).Once()
		s.triggerManager.handler = handler
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.EqualValues(planID, task.GetPlanID())
				s.EqualValues(triggerID, task.GetTriggerID())
				s.Equal(datapb.CompactionType_BackfillCompaction, task.GetType())
				s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
				s.Equal(s.testLabel.PartitionID, task.GetPartitionID())
				s.Equal(s.testLabel.Channel, task.GetChannel())
				s.Equal(collectionSchema, task.GetSchema())
				s.Require().Len(task.GetDiffFunctions(), 1)
				s.Equal(backfillFunc.GetName(), task.GetDiffFunctions()[0].GetName())
				s.ElementsMatch([]int64{200}, task.GetInputSegments())
				return nil
			}).Once()

		view := makeBackfillView(triggerID)
		s.triggerManager.SubmitBackfillViewToScheduler(context.Background(), view)
	})
}

func (s *CompactionTriggerManagerSuite) TestHandleTicker() {
	s.Run("policy not found for ticker type", func() {
		s.SetupTest()
		// TickerType 99 is not registered in policies map
		s.triggerManager.handleTicker(context.Background(), TickerType(99))
		// Should return without panic
	})

	s.Run("policy disabled", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{enabled: false, policyName: "test-disabled"}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
		// Returns early — no inspector or trigger calls
	})

	s.Run("inspector full skips Trigger dispatch", func() {
		// When isFull() returns true, Trigger() is not called. TriggerInline() runs
		// unconditionally before the isFull() gate, so metadata-only updates always
		// proceed; only physical compaction tasks are gated by inspector capacity.
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-full",
			// Views in Trigger() result are NOT dispatched because isFull() returns true.
			triggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBackfill: {stubDispatchableView{}},
			},
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		s.inspector.EXPECT().isFull().Return(true).Once()
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
	})

	s.Run("policy trigger error returns before isFull check", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-err",
			triggerErr: errors.New("trigger error"),
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		// isFull() IS called now (step 2, before Trigger). Trigger() errors → no notify().
		s.inspector.EXPECT().isFull().Return(false).Once()
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
	})

	s.Run("policy trigger returns no events skips isFull check", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-empty",
			// Nil result — Trigger() returns nothing, notify() is never called.
			triggerResult: nil,
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		// isFull() IS called now (step 2 gate before Trigger). Trigger returns nil → no notify.
		s.inspector.EXPECT().isFull().Return(false).Once()
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
	})

	s.Run("policy trigger returns events dispatched when inspector not full", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-events",
			triggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBackfill: {stubDispatchableView{}},
			},
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		s.inspector.EXPECT().isFull().Return(false).Once()
		// stubDispatchableView.Trigger() returns nil, so notify() short-circuits
		// before reaching SubmitBackfillViewToScheduler. We only care here that
		// isFull was consulted and the dispatch path was entered.
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
	})

	s.Run("inline-executable backfill view applied regardless of inspector full", func() {
		// Regression for backfillCompactionPolicy: pure column additions emit an
		// inline-executable BackfillSegmentsView (inlineMetaOnly=true). The trigger
		// manager must apply it via meta.UpdateSegment without consulting isFull —
		// otherwise schema versions never converge under inspector pressure.
		s.SetupTest()

		// Wire a mock catalog so meta.UpdateSegment can call catalog.AlterSegments.
		mockCatalog := mocks.NewDataCoordCatalog(s.T())
		mockCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Once()
		s.triggerManager.meta.catalog = mockCatalog

		segmentID := int64(424242)
		s.triggerManager.meta.segments.SetSegment(segmentID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  s.testLabel.CollectionID,
				State:         commonpb.SegmentState_Flushed,
				SchemaVersion: 1,
			},
		})

		// Inline views come from TriggerInline(), not Trigger(). Trigger() returns nothing.
		// The inline view is applied before isFull() is even consulted, so schema version
		// convergence is guaranteed regardless of inspector pressure.
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-meta-update",
			inlineTriggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBackfill: {
					&BackfillSegmentsView{
						label:               s.testLabel,
						segments:            []*SegmentView{{ID: segmentID, label: s.testLabel}},
						inlineMetaOnly:      true,
						targetSchemaVersion: 5,
					},
				},
			},
			// triggerResult is nil: no compaction tasks to dispatch
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		// isFull() is consulted for the Trigger() gate — simulate a full inspector to
		// prove inline views are unaffected by inspector pressure.
		s.inspector.EXPECT().isFull().Return(true).Once()
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)

		updated := s.triggerManager.meta.segments.GetSegment(segmentID)
		s.Require().NotNil(updated)
		s.Equal(int32(5), updated.GetSchemaVersion(),
			"inline view must be applied to bump segment schema version")
	})

	s.Run("applyInlineView with wrong view type logs warning and returns", func() {
		// When TriggerInline() returns a non-BackfillSegmentsView in the inline events,
		// applyInlineView must log a warning and return without panic.
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-wrong-inline-type",
			inlineTriggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBackfill: {
					// stubDispatchableView is NOT *BackfillSegmentsView — triggers the wrong-type branch.
					stubDispatchableView{},
				},
			},
			// No compaction tasks to dispatch.
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		// isFull() is consulted for the Trigger() gate.
		s.inspector.EXPECT().isFull().Return(true).Once()
		// Should complete without panic.
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)
	})

	s.Run("applyInlineView UpdateSegment fails logs error and continues", func() {
		// When meta.UpdateSegment fails (catalog error), applyInlineView logs the error
		// and continues rather than aborting the whole inline pass.
		s.SetupTest()

		// Wire a catalog that rejects AlterSegments.
		mockCatalog := mocks.NewDataCoordCatalog(s.T())
		mockCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("catalog error")).Once()
		s.triggerManager.meta.catalog = mockCatalog

		segmentID := int64(424243)
		s.triggerManager.meta.segments.SetSegment(segmentID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  s.testLabel.CollectionID,
				State:         commonpb.SegmentState_Flushed,
				SchemaVersion: 1,
			},
		})

		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-update-segment-fail",
			inlineTriggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBackfill: {
					&BackfillSegmentsView{
						label:               s.testLabel,
						segments:            []*SegmentView{{ID: segmentID, label: s.testLabel}},
						inlineMetaOnly:      true,
						targetSchemaVersion: 9,
					},
				},
			},
		}
		s.triggerManager.policies[BackfillTicker] = mockPolicy
		s.inspector.EXPECT().isFull().Return(true).Once()
		// Should not panic despite the catalog error.
		s.triggerManager.handleTicker(context.Background(), BackfillTicker)

		// Schema version must remain unchanged because UpdateSegment failed.
		updated := s.triggerManager.meta.segments.GetSegment(segmentID)
		s.Require().NotNil(updated)
		s.Equal(int32(1), updated.GetSchemaVersion(),
			"schema version must stay 1 when UpdateSegment fails")
	})
}
