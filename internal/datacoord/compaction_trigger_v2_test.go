package datacoord

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
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

func TestEstimateResultSegmentCount(t *testing.T) {
	tests := []struct {
		name       string
		totalSize  float64
		targetSize float64
		want       int64
	}{
		{name: "exact multiple", totalSize: 300, targetSize: 100, want: 3},
		{name: "fractional rounds up", totalSize: 301, targetSize: 100, want: 4},
		{name: "zero total size", totalSize: 0, targetSize: 100, want: 1},
		{name: "zero target size", totalSize: 100, targetSize: 0, want: 1},
		{name: "large ratio", totalSize: 10_000, targetSize: 64, want: 157},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := estimateResultSegmentCount(test.totalSize, test.targetSize)
			if got != test.want {
				t.Fatalf("estimateResultSegmentCount(%v, %v) = %d, want %d", test.totalSize, test.targetSize, got, test.want)
			}
		})
	}
}

func TestCompactionIDBlockTakeFromMetadataTail(t *testing.T) {
	t.Run("returns fixed segment ID range", func(t *testing.T) {
		block := &compactionIDBlock{
			segments: &datapb.IDRange{Begin: 100, End: 103},
			next:     103,
			end:      104,
		}

		planID, err := block.take()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		segmentIDRange := block.segmentIDRange()

		if planID != 103 {
			t.Fatalf("planID = %d, want 103", planID)
		}
		if segmentIDRange.GetBegin() != 100 || segmentIDRange.GetEnd() != 103 {
			t.Fatalf("segment ID range = [%d, %d), want [100, 103)", segmentIDRange.GetBegin(), segmentIDRange.GetEnd())
		}
	})

	t.Run("rejects future metadata over-consumption", func(t *testing.T) {
		block := &compactionIDBlock{
			segments: &datapb.IDRange{Begin: 100, End: 103},
			next:     103,
			end:      104,
		}

		_, err := block.take()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, err = block.take()
		if err == nil {
			t.Fatal("expected metadata ID exhaustion error")
		}
		segmentIDRange := block.segmentIDRange()
		if segmentIDRange.GetBegin() != 100 || segmentIDRange.GetEnd() != 103 {
			t.Fatalf("segment ID range = [%d, %d), want [100, 103)", segmentIDRange.GetBegin(), segmentIDRange.GetEnd())
		}
	})
}

func TestCreateCompactionIDBlockRejectsTooLargeBatch(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "10")
	defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)

	mockAlloc := allocator.NewMockAllocator(t)
	_, err := createCompactionIDBlock(mockAlloc, math.MaxUint32/10+1, 1)
	if err == nil {
		t.Fatal("expected too-large allocation error")
	}
	if !strings.Contains(err.Error(), "compaction too large to allocate IDs in a single batch") {
		t.Fatalf("error = %q, want too-large allocation message", err.Error())
	}
}

func TestCreateCompactionIDBlockUsesIDExpansionFactor(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "2")
	defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocN(int64(7)).Return(int64(100), int64(107), nil).Once()

	block, err := createCompactionIDBlock(mockAlloc, 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	segmentIDRange := block.segmentIDRange()
	if segmentIDRange.GetBegin() != 100 || segmentIDRange.GetEnd() != 106 {
		t.Fatalf("segment ID range = [%d, %d), want [100, 106)", segmentIDRange.GetBegin(), segmentIDRange.GetEnd())
	}
}

func TestCompactionViewsExposeTotalSizeAndCollectionTTL(t *testing.T) {
	ttl := 3 * time.Hour
	segments := []*SegmentView{{ID: 1, Size: 10}, {ID: 2, Size: 2.5}}
	tests := []struct {
		name    string
		view    CompactionView
		wantTTL time.Duration
	}{
		{name: "single", view: &MixSegmentView{segments: segments, collectionTTL: ttl}, wantTTL: ttl},
		{name: "clustering", view: &ClusteringSegmentsView{segments: segments, collectionTTL: ttl}, wantTTL: ttl},
		{name: "force merge", view: &ForceMergeSegmentView{segments: segments, collectionTTL: ttl}, wantTTL: ttl},
		{name: "level zero", view: &LevelZeroCompactionView{l0Segments: segments}},
		{name: "bump schema version", view: &BumpSchemaVersionView{segments: segments}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.view.GetTotalSize(); got != 12.5 {
				t.Fatalf("GetTotalSize() = %v, want 12.5", got)
			}
			if got := test.view.GetCollectionTTL(); got != test.wantTTL {
				t.Fatalf("GetCollectionTTL() = %v, want %v", got, test.wantTTL)
			}
		})
	}
}

// testCompactionPolicy is a minimal CompactionPolicy stub for handleTicker tests.
type testCompactionPolicy struct {
	enabled       bool
	triggerResult map[CompactionTriggerType][]CompactionView
	triggerErr    error
	policyName    string
}

func (p *testCompactionPolicy) Enable() bool { return p.enabled }
func (p *testCompactionPolicy) Trigger(_ context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	return p.triggerResult, p.triggerErr
}
func (p *testCompactionPolicy) Name() string { return p.policyName }

// stubDispatchableView is a CompactionView stub for handleTicker tests that need
// a view to reach the dispatch / isFull branch.
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
func (stubDispatchableView) GetTotalSize() float64                       { return 0 }
func (stubDispatchableView) GetCollectionTTL() time.Duration             { return 0 }

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

func (s *CompactionTriggerManagerSuite) TestSubmitSingleViewToScheduler() {
	makeMixView := func(segments []*SegmentView) *MixSegmentView {
		return &MixSegmentView{
			label:         s.testLabel,
			segments:      segments,
			collectionTTL: 100,
			triggerID:     1001,
		}
	}

	s.Run("mix compaction allocates estimated result segments", func() {
		s.SetupTest()
		pt := paramtable.Get()
		pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "1")
		defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)
		pt.Save(pt.DataCoordCfg.SegmentMaxSize.Key, "100")
		defer pt.Reset(pt.DataCoordCfg.SegmentMaxSize.Key)

		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		collectionSchema := &schemapb.CollectionSchema{
			Name: "test_coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
		s.triggerManager.handler = handler

		const (
			startID = int64(500)
			endID   = int64(504)
			planID  = int64(503)
		)
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.EqualValues(planID, task.GetPlanID())
				s.EqualValues(1001, task.GetTriggerID())
				s.Equal(datapb.CompactionType_MixCompaction, task.GetType())
				s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
				s.Equal(s.testLabel.PartitionID, task.GetPartitionID())
				s.Equal(s.testLabel.Channel, task.GetChannel())
				s.Equal(collectionSchema, task.GetSchema())
				s.ElementsMatch([]int64{200, 201}, task.GetInputSegments())
				s.EqualValues(300, task.GetTotalRows())
				s.Equal(&datapb.IDRange{Begin: startID, End: planID}, task.GetPreAllocatedSegmentIDs())
				s.Equal(task.GetStartTime(), task.GetLastStateStartTime())
				return nil
			}).Return(nil).Once()

		view := makeMixView([]*SegmentView{
			{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024},
			{ID: 201, label: s.testLabel, NumOfRows: 200, Size: 150 * 1024 * 1024},
		})
		s.triggerManager.SubmitSingleViewToScheduler(context.Background(), view, TriggerTypeSingle)
	})

	s.Run("sort compaction allocates estimated result segments", func() {
		s.SetupTest()
		pt := paramtable.Get()
		pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "1")
		defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)
		pt.Save(pt.DataCoordCfg.SegmentMaxSize.Key, "100")
		defer pt.Reset(pt.DataCoordCfg.SegmentMaxSize.Key)

		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		collectionSchema := &schemapb.CollectionSchema{
			Name: "test_coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
		s.triggerManager.handler = handler

		const (
			startID = int64(600)
			endID   = int64(604)
			planID  = int64(603)
		)
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.Equal(datapb.CompactionType_SortCompaction, task.GetType())
				s.Equal(&datapb.IDRange{Begin: startID, End: planID}, task.GetPreAllocatedSegmentIDs())
				return nil
			}).Return(nil).Once()

		view := makeMixView([]*SegmentView{
			{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 300 * 1024 * 1024},
		})
		s.triggerManager.SubmitSingleViewToScheduler(context.Background(), view, TriggerTypeSort)
	})

	s.Run("storage version upgrade keeps size based estimation", func() {
		s.SetupTest()
		pt := paramtable.Get()
		pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "1")
		defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)
		pt.Save(pt.DataCoordCfg.SegmentMaxSize.Key, "100")
		defer pt.Reset(pt.DataCoordCfg.SegmentMaxSize.Key)

		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		collectionSchema := &schemapb.CollectionSchema{
			Name: "test_coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
		s.triggerManager.handler = handler

		const (
			startID = int64(700)
			endID   = int64(704)
			planID  = int64(703)
		)
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.Equal(datapb.CompactionType_MixCompaction, task.GetType())
				s.Equal(&datapb.IDRange{Begin: startID, End: planID}, task.GetPreAllocatedSegmentIDs())
				return nil
			}).Return(nil).Once()

		view := makeMixView([]*SegmentView{
			{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 300 * 1024 * 1024},
		})
		s.triggerManager.SubmitSingleViewToScheduler(context.Background(), view, TriggerTypeStorageVersionUpgrade)
	})
}

func (s *CompactionTriggerManagerSuite) TestSubmitClusteringViewToScheduler() {
	s.SetupTest()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "2")
	defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)
	pt.Save(pt.DataCoordCfg.SegmentMaxSize.Key, "100")
	defer pt.Reset(pt.DataCoordCfg.SegmentMaxSize.Key)
	pt.Save(pt.DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1")
	defer pt.Reset(pt.DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)

	s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
	clusteringKey := &schemapb.FieldSchema{FieldID: 2, Name: "cluster_key", DataType: schemapb.DataType_Int64, IsClusteringKey: true}
	collectionSchema := &schemapb.CollectionSchema{
		Name: "test_coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			clusteringKey,
			{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
		Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
	s.triggerManager.handler = handler

	const (
		startID       = int64(800)
		segmentIDEnd  = int64(806)
		planID        = int64(806)
		analyzeTaskID = int64(807)
		endID         = int64(808)
	)
	s.mockAlloc.EXPECT().AllocN(int64(8)).Return(startID, endID, nil).Once()
	s.inspector.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.Equal(datapb.CompactionType_ClusteringCompaction, task.GetType())
			s.EqualValues(planID, task.GetPlanID())
			s.EqualValues(analyzeTaskID, task.GetAnalyzeTaskID())
			s.Equal(&datapb.IDRange{Begin: startID, End: segmentIDEnd}, task.GetPreAllocatedSegmentIDs())
			s.Equal(task.GetStartTime(), task.GetLastStateStartTime())
			return nil
		}).Return(nil).Once()

	view := &ClusteringSegmentsView{
		label:              s.testLabel,
		segments:           []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024}, {ID: 201, label: s.testLabel, NumOfRows: 200, Size: 150 * 1024 * 1024}},
		clusteringKeyField: clusteringKey,
		collectionTTL:      100,
		triggerID:          1001,
	}
	s.triggerManager.SubmitClusteringViewToScheduler(context.Background(), view)
}

func (s *CompactionTriggerManagerSuite) TestSubmitForceMergeViewToScheduler() {
	s.SetupTest()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "1")
	defer pt.Reset(pt.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)

	collectionSchema := &schemapb.CollectionSchema{
		Name: "test_coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
		Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
	s.triggerManager.handler = handler

	const (
		startID      = int64(500)
		segmentIDEnd = int64(502)
		planID       = int64(502)
		endID        = int64(503)
	)
	s.mockAlloc.EXPECT().AllocN(int64(3)).Return(startID, endID, nil).Once()
	s.inspector.EXPECT().enqueueCompaction(mock.Anything).
		RunAndReturn(func(task *datapb.CompactionTask) error {
			s.EqualValues(planID, task.GetPlanID())
			s.Equal(datapb.CompactionType_MixCompaction, task.GetType())
			s.Equal(&datapb.IDRange{Begin: startID, End: segmentIDEnd}, task.GetPreAllocatedSegmentIDs())
			return nil
		}).Return(nil).Once()

	view := &ForceMergeSegmentView{
		label:              s.testLabel,
		segments:           []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024}},
		triggerID:          1001,
		targetSegmentSize:  100 * 1024 * 1024,
		targetSegmentCount: 999,
	}
	s.triggerManager.SubmitForceMergeViewToScheduler(context.Background(), view)
}

func (s *CompactionTriggerManagerSuite) TestSubmitViewToSchedulerDefensiveReturns() {
	makeCollectionSchema := func(external bool) *schemapb.CollectionSchema {
		field := &schemapb.FieldSchema{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
		if external {
			field.ExternalField = "pk_col"
		}
		return &schemapb.CollectionSchema{
			Name:   "test_coll",
			Fields: []*schemapb.FieldSchema{field},
		}
	}

	makeMixView := func() *MixSegmentView {
		return &MixSegmentView{
			label:         s.testLabel,
			segments:      []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024}},
			collectionTTL: 100,
			triggerID:     1001,
		}
	}

	makeClusteringView := func(clusteringKey *schemapb.FieldSchema) *ClusteringSegmentsView {
		return &ClusteringSegmentsView{
			label:              s.testLabel,
			segments:           []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024}},
			clusteringKeyField: clusteringKey,
			collectionTTL:      100,
			triggerID:          1001,
		}
	}

	makeForceMergeView := func() *ForceMergeSegmentView {
		return &ForceMergeSegmentView{
			label:              s.testLabel,
			segments:           []*SegmentView{{ID: 200, label: s.testLabel, NumOfRows: 100, Size: 150 * 1024 * 1024}},
			triggerID:          1001,
			targetSegmentSize:  100 * 1024 * 1024,
			targetSegmentCount: 999,
		}
	}

	s.Run("single GetCollection error returns before allocation", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, errors.New("get collection error")).Once()
		s.triggerManager.handler = handler

		s.triggerManager.SubmitSingleViewToScheduler(context.Background(), makeMixView(), TriggerTypeSingle)
	})

	s.Run("single AllocN error returns before enqueue", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: makeCollectionSchema(false)}, nil).Once()
		s.triggerManager.handler = handler
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("alloc error")).Once()

		s.triggerManager.SubmitSingleViewToScheduler(context.Background(), makeMixView(), TriggerTypeSingle)
	})

	s.Run("clustering GetCollection error returns before allocation", func() {
		s.SetupTest()
		clusteringKey := &schemapb.FieldSchema{FieldID: 2, Name: "cluster_key", DataType: schemapb.DataType_Int64, IsClusteringKey: true}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, errors.New("get collection error")).Once()
		s.triggerManager.handler = handler

		s.triggerManager.SubmitClusteringViewToScheduler(context.Background(), makeClusteringView(clusteringKey))
	})

	s.Run("clustering AllocN error returns before enqueue", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		clusteringKey := &schemapb.FieldSchema{FieldID: 2, Name: "cluster_key", DataType: schemapb.DataType_Int64, IsClusteringKey: true}
		collectionSchema := makeCollectionSchema(false)
		collectionSchema.Fields = append(collectionSchema.Fields, clusteringKey)
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
		s.triggerManager.handler = handler
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("alloc error")).Once()

		s.triggerManager.SubmitClusteringViewToScheduler(context.Background(), makeClusteringView(clusteringKey))
	})

	s.Run("force merge GetCollection error returns before allocation", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, errors.New("get collection error")).Once()
		s.triggerManager.handler = handler

		s.triggerManager.SubmitForceMergeViewToScheduler(context.Background(), makeForceMergeView())
	})

	s.Run("force merge nil collection returns before allocation", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).Return(nil, nil).Once()
		s.triggerManager.handler = handler

		s.triggerManager.SubmitForceMergeViewToScheduler(context.Background(), makeForceMergeView())
	})

	s.Run("force merge external collection returns before allocation", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: makeCollectionSchema(true)}, nil).Once()
		s.triggerManager.handler = handler

		s.triggerManager.SubmitForceMergeViewToScheduler(context.Background(), makeForceMergeView())
	})

	s.Run("force merge AllocN error returns before enqueue", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: makeCollectionSchema(false)}, nil).Once()
		s.triggerManager.handler = handler
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("alloc error")).Once()

		s.triggerManager.SubmitForceMergeViewToScheduler(context.Background(), makeForceMergeView())
	})
}

func (s *CompactionTriggerManagerSuite) TestSubmitBumpSchemaVersionViewToScheduler() {
	Params.Save(Params.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CompactionPreAllocateIDExpansionFactor.Key)
	Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "100")
	defer Params.Reset(Params.DataCoordCfg.SegmentMaxSize.Key)
	Params.Save(Params.DataCoordCfg.DiskSegmentMaxSize.Key, "100")
	defer Params.Reset(Params.DataCoordCfg.DiskSegmentMaxSize.Key)

	collectionSchema := &schemapb.CollectionSchema{
		Name: "test_coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25_fn", Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{100}},
		},
	}
	makeBumpSchemaVersionView := func(triggerID int64) *BumpSchemaVersionView {
		segView := &SegmentView{
			ID:        200,
			label:     s.testLabel,
			NumOfRows: 1000,
			Size:      300 * 1024 * 1024,
		}
		return &BumpSchemaVersionView{
			label:     s.testLabel,
			segments:  []*SegmentView{segView},
			triggerID: triggerID,
			schema:    collectionSchema,
		}
	}

	s.Run("AllocN fails", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: collectionSchema}, nil).Once()
		s.triggerManager.handler = handler
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(int64(0), int64(0), errors.New("alloc error")).Once()
		view := makeBumpSchemaVersionView(111)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("GetCollection fails", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, errors.New("get collection error")).Once()
		s.triggerManager.handler = handler

		view := makeBumpSchemaVersionView(111)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("collection is nil", func() {
		s.SetupTest()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(nil, nil).Once()
		s.triggerManager.handler = handler

		view := makeBumpSchemaVersionView(111)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("collection is external", func() {
		s.SetupTest()
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

		view := makeBumpSchemaVersionView(111)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("view is not BumpSchemaVersionView", func() {
		s.SetupTest()

		// Use LevelZeroCompactionView which is NOT a *BumpSchemaVersionView.
		nonBumpSchemaVersionView := &LevelZeroCompactionView{
			label:      s.testLabel,
			l0Segments: []*SegmentView{},
		}
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), nonBumpSchemaVersionView)
	})

	s.Run("enqueueCompaction fails", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const (
			startID   = int64(504)
			endID     = int64(508)
			triggerID = int64(999)
		)
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:     s.testLabel.CollectionID,
				Schema: collectionSchema,
			}, nil).Once()
		s.triggerManager.handler = handler
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).Return(errors.New("enqueue error")).Once()

		view := makeBumpSchemaVersionView(triggerID)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("success", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const (
			startID   = int64(600)
			endID     = int64(604)
			planID    = int64(603)
			triggerID = int64(1001)
		)
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{
				ID:         s.testLabel.CollectionID,
				Schema:     collectionSchema,
				Properties: map[string]string{common.CollectionTTLConfigKey: "3600"},
			}, nil).Once()
		s.triggerManager.handler = handler
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.EqualValues(planID, task.GetPlanID())
				s.EqualValues(triggerID, task.GetTriggerID())
				s.Equal(datapb.CompactionType_BumpSchemaVersionCompaction, task.GetType())
				s.Equal(s.testLabel.CollectionID, task.GetCollectionID())
				s.Equal(s.testLabel.PartitionID, task.GetPartitionID())
				s.Equal(s.testLabel.Channel, task.GetChannel())
				s.Equal(collectionSchema, task.GetSchema())
				s.ElementsMatch([]int64{200}, task.GetInputSegments())
				s.Empty(task.GetResultSegments())
				s.NotZero(task.GetStartTime())
				s.NotZero(task.GetLastStateStartTime())
				s.NotNil(task.GetPreAllocatedSegmentIDs())
				s.EqualValues(startID, task.GetPreAllocatedSegmentIDs().GetBegin())
				s.EqualValues(planID, task.GetPreAllocatedSegmentIDs().GetEnd())
				s.EqualValues(time.Hour.Nanoseconds(), task.GetCollectionTtl())
				return nil
			}).Once()

		view := makeBumpSchemaVersionView(triggerID)
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
	})

	s.Run("policy to submit keeps frozen schema", func() {
		s.SetupTest()
		s.meta.indexMeta = &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)}
		const (
			startID   = int64(601)
			endID     = int64(605)
			planID    = int64(604)
			triggerID = int64(1002)
		)
		frozenSchema := &schemapb.CollectionSchema{
			Name:    "test_coll",
			Version: 2,
			Fields:  []*schemapb.FieldSchema{{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		}
		liveSchema := &schemapb.CollectionSchema{
			Name:    "test_coll",
			Version: 3,
			Fields:  []*schemapb.FieldSchema{{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		}
		s.mockAlloc.EXPECT().AllocN(int64(4)).Return(startID, endID, nil).Once()
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.testLabel.CollectionID).
			Return(&collectionInfo{ID: s.testLabel.CollectionID, Schema: liveSchema}, nil).Once()
		s.triggerManager.handler = handler
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).
			RunAndReturn(func(task *datapb.CompactionTask) error {
				s.Equal(datapb.CompactionType_BumpSchemaVersionCompaction, task.GetType())
				s.EqualValues(2, task.GetSchema().GetVersion())
				s.Equal(frozenSchema, task.GetSchema())
				s.NotNil(task.GetPreAllocatedSegmentIDs())
				s.EqualValues(startID, task.GetPreAllocatedSegmentIDs().GetBegin())
				s.EqualValues(planID, task.GetPreAllocatedSegmentIDs().GetEnd())
				return nil
			}).Once()

		view := makeBumpSchemaVersionView(triggerID)
		view.schema = frozenSchema
		s.triggerManager.SubmitBumpSchemaVersionViewToScheduler(context.Background(), view)
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
		s.triggerManager.policies[BumpSchemaVersionTicker] = mockPolicy
		s.triggerManager.handleTicker(context.Background(), BumpSchemaVersionTicker)
		// Returns early — no inspector or trigger calls
	})

	s.Run("inspector full skips Trigger dispatch", func() {
		// When isFull() returns true, Trigger() is not called and no schema-bump task is submitted.
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-full",
			// Views in Trigger() result are NOT dispatched because isFull() returns true.
			triggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBumpSchemaVersion: {stubDispatchableView{}},
			},
		}
		s.triggerManager.policies[BumpSchemaVersionTicker] = mockPolicy
		s.inspector.EXPECT().isFull().Return(true).Once()
		s.triggerManager.handleTicker(context.Background(), BumpSchemaVersionTicker)
	})

	s.Run("policy trigger error returns before isFull check", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-err",
			triggerErr: errors.New("trigger error"),
		}
		s.triggerManager.policies[BumpSchemaVersionTicker] = mockPolicy
		// isFull() IS called now (step 2, before Trigger). Trigger() errors → no notify().
		s.inspector.EXPECT().isFull().Return(false).Once()
		s.triggerManager.handleTicker(context.Background(), BumpSchemaVersionTicker)
	})

	s.Run("policy trigger returns no events skips isFull check", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-empty",
			// Nil result — Trigger() returns nothing, notify() is never called.
			triggerResult: nil,
		}
		s.triggerManager.policies[BumpSchemaVersionTicker] = mockPolicy
		// isFull() IS called now (step 2 gate before Trigger). Trigger returns nil → no notify.
		s.inspector.EXPECT().isFull().Return(false).Once()
		s.triggerManager.handleTicker(context.Background(), BumpSchemaVersionTicker)
	})

	s.Run("policy trigger returns events dispatched when inspector not full", func() {
		s.SetupTest()
		mockPolicy := &testCompactionPolicy{
			enabled:    true,
			policyName: "test-events",
			triggerResult: map[CompactionTriggerType][]CompactionView{
				TriggerTypeBumpSchemaVersion: {stubDispatchableView{}},
			},
		}
		s.triggerManager.policies[BumpSchemaVersionTicker] = mockPolicy
		s.inspector.EXPECT().isFull().Return(false).Once()
		// stubDispatchableView.Trigger() returns nil, so notify() short-circuits
		// before reaching SubmitBumpSchemaVersionViewToScheduler. We only care here that
		// isFull was consulted and the dispatch path was entered.
		s.triggerManager.handleTicker(context.Background(), BumpSchemaVersionTicker)
	})
}
