package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestCompactionViewManagerSuite(t *testing.T) {
	suite.Run(t, new(CompactionViewManagerSuite))
}

type CompactionViewManagerSuite struct {
	suite.Suite

	mockAlloc          *NMockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel

	m *CompactionViewManager
}

const MB = 1024 * 1024 * 1024

func genSegmentsForMeta(label *CompactionGroupLabel) map[int64]*SegmentInfo {
	segArgs := []struct {
		ID    UniqueID
		Level datapb.SegmentLevel
		State commonpb.SegmentState
		PosT  Timestamp

		LogSize  int64
		LogCount int
	}{
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 4 * MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 4 * MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 4 * MB, 1},
		{103, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 50000, 4 * MB, 1},
		{200, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 50000, 0, 0},
		{201, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 30000, 0, 0},
		{300, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 10000, 0, 0},
		{301, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 20000, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(label, arg.ID, arg.Level, arg.State)
		if info.Level == datapb.SegmentLevel_L0 || info.State == commonpb.SegmentState_Flushed {
			info.Deltalogs = genTestDeltalogs(arg.LogCount, arg.LogSize)
			info.DmlPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}
		if info.State == commonpb.SegmentState_Growing {
			info.StartPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}

		segments[arg.ID] = info
	}

	return segments
}

func (s *CompactionViewManagerSuite) SetupTest() {
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockTriggerManager = NewMockTriggerManager(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	meta := &meta{segments: &SegmentsInfo{
		segments: genSegmentsForMeta(s.testLabel),
	}}

	s.m = NewCompactionViewManager(meta, s.mockTriggerManager, s.mockAlloc)
}

func (s *CompactionViewManagerSuite) TestCheckLoop() {
	s.Run("Test start and close", func() {
		s.m.Start()
		s.m.Close()
	})

	s.Run("Test not enable auto compaction", func() {
		paramtable.Get().Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableAutoCompaction.Key)

		s.m.Start()
		s.m.closeWg.Wait()
	})
}

func (s *CompactionViewManagerSuite) TestCheck() {
	paramtable.Get().Save(Params.DataCoordCfg.EnableLevelZeroSegment.Key, "true")
	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableLevelZeroSegment.Key)

	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil).Times(3)
	// nothing int the view, just store in the first check
	s.Empty(s.m.view.collections)
	s.m.Check()
	for _, views := range s.m.view.collections {
		for _, view := range views {
			s.Equal(datapb.SegmentLevel_L0, view.Level)
			s.Equal(commonpb.SegmentState_Flushed, view.State)
			log.Info("String", zap.String("segment", view.String()))
			log.Info("LevelZeroString", zap.String("segment", view.LevelZeroString()))
		}
	}

	// change of meta
	addInfo := genTestSegmentInfo(s.testLabel, 19530, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
	addInfo.Deltalogs = genTestDeltalogs(1, 10)
	s.m.meta.Lock()
	s.m.meta.segments.segments[addInfo.GetID()] = addInfo
	s.m.meta.Unlock()

	s.mockTriggerManager.EXPECT().Notify(mock.Anything, mock.Anything, mock.Anything).
		Run(func(taskID UniqueID, tType CompactionTriggerType, views []CompactionView) {
			s.EqualValues(1, taskID)
			s.Equal(TriggerTypeLevelZeroView, tType)
			s.Equal(1, len(views))
			v, ok := views[0].(*LevelZeroSegmentsView)
			s.True(ok)
			s.NotNil(v)

			expectedSegs := []int64{100, 101, 102, 103, 19530}
			gotSegs := lo.Map(v.segments, func(s *SegmentView, _ int) int64 { return s.ID })
			s.ElementsMatch(expectedSegs, gotSegs)

			s.EqualValues(30000, v.earliestGrowingSegmentPos.GetTimestamp())
			log.Info("All views", zap.String("l0 view", v.String()))
		}).Once()

	s.m.Check()

	// clear meta
	s.m.meta.Lock()
	s.m.meta.segments.segments = make(map[int64]*SegmentInfo)
	s.m.meta.Unlock()
	s.m.Check()
	s.Empty(s.m.view.collections)
}

func genTestSegmentInfo(label *CompactionGroupLabel, ID UniqueID, level datapb.SegmentLevel, state commonpb.SegmentState) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            ID,
			CollectionID:  label.CollectionID,
			PartitionID:   label.PartitionID,
			InsertChannel: label.Channel,
			Level:         level,
			State:         state,
		},
	}
}

func genTestDeltalogs(logCount int, logSize int64) []*datapb.FieldBinlog {
	var binlogs []*datapb.Binlog

	for i := 0; i < logCount; i++ {
		binlog := &datapb.Binlog{
			LogSize: logSize,
		}
		binlogs = append(binlogs, binlog)
	}

	return []*datapb.FieldBinlog{
		{Binlogs: binlogs},
	}
}
