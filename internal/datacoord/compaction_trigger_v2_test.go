package datacoord

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestCompactionTriggerManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerManagerSuite))
}

type CompactionTriggerManagerSuite struct {
	suite.Suite

	mockAlloc       *NMockAllocator
	mockPlanContext *MockCompactionPlanContext
	testLabel       *CompactionGroupLabel

	m *CompactionTriggerManager
}

func (s *CompactionTriggerManagerSuite) SetupTest() {
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockPlanContext = NewMockCompactionPlanContext(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	meta := &meta{segments: &SegmentsInfo{
		segments: genSegmentsForMeta(s.testLabel),
	}}

	s.m = NewCompactionTriggerManager(meta, s.mockAlloc, s.mockPlanContext)
}

func (s *CompactionTriggerManagerSuite) TestNotify() {
	viewManager := NewCompactionViewManager(s.m.meta, s.m, s.m.allocator)
	collSegs := s.m.meta.GetCompactableSegmentGroupByCollection()

	segments, found := collSegs[1]
	s.Require().True(found)

	levelZeroSegments := lo.Filter(segments, func(info *SegmentInfo, _ int) bool {
		return info.GetLevel() == datapb.SegmentLevel_L0
	})

	segmentViews, levelZeroView := viewManager.GetLatestLevelZeroSegmentWithSignals(1, levelZeroSegments)
	s.Require().NotEmpty(segmentViews)
	s.Require().Equal(1, len(levelZeroView))
	cView, ok := levelZeroView[0].(*LevelZeroSegmentsView)
	s.True(ok)
	s.NotNil(cView)
	log.Info("view", zap.Any("cView", cView))

	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil)
	s.mockPlanContext.EXPECT().execCompactionPlan(mock.Anything, mock.Anything).
		Run(func(signal *compactionSignal, plan *datapb.CompactionPlan) {
			s.EqualValues(19530, signal.id)
			s.True(signal.isGlobal)
			s.False(signal.isForce)
			s.EqualValues(30000, signal.pos.GetTimestamp())
			s.Equal(s.testLabel.CollectionID, signal.collectionID)
			s.Equal(s.testLabel.PartitionID, signal.partitionID)

			s.NotNil(plan)
			s.Equal(s.testLabel.Channel, plan.GetChannel())
			s.Equal(datapb.CompactionType_Level0DeleteCompaction, plan.GetType())

			expectedSegs := []int64{100, 101, 102}
			gotSegs := lo.Map(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
				return b.GetSegmentID()
			})

			s.ElementsMatch(expectedSegs, gotSegs)
			log.Info("generated plan", zap.Any("plan", plan))
		}).Return(nil).Once()

	s.m.Notify(19530, TriggerTypeLevelZeroView, levelZeroView)
}
