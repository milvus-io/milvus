package datacoord

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

func TestCompactionTriggerManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerManagerSuite))
}

type CompactionTriggerManagerSuite struct {
	suite.Suite

	mockAlloc       *NMockAllocator
	handler         Handler
	mockPlanContext *MockCompactionPlanContext
	testLabel       *CompactionGroupLabel
	meta            *meta

	triggerManager *CompactionTriggerManager
}

func (s *CompactionTriggerManagerSuite) SetupTest() {
	s.mockAlloc = NewNMockAllocator(s.T())
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

func (s *CompactionTriggerManagerSuite) TestNotifyToFullScheduler() {
	s.mockPlanContext.EXPECT().isFull().Return(true)
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

	// s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().isFull().Return(false)
	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(19530, nil).Maybe()
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewChange, levelZeroView)
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

	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().isFull().Return(false)
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
	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(19530, nil).Maybe()
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

	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().isFull().Return(false)
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
	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(19530, nil).Maybe()
	s.triggerManager.notify(context.Background(), TriggerTypeLevelZeroViewChange, levelZeroView)
}
