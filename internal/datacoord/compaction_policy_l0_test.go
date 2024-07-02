// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datacoord

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
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
	handler            Handler
	mockPlanContext    *MockCompactionPlanContext

	m *CompactionTriggerManager
}

const MB = 1024 * 1024

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
	s.handler = NewNMockHandler(s.T())
	s.mockPlanContext = NewMockCompactionPlanContext(s.T())

	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}

	s.m = NewCompactionTriggerManager(s.mockAlloc, s.handler, s.mockPlanContext, meta)
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

	s.Run("Test not enable levelZero segment", func() {
		paramtable.Get().Save(Params.DataCoordCfg.EnableLevelZeroSegment.Key, "false")
		defer paramtable.Get().Reset(Params.DataCoordCfg.EnableLevelZeroSegment.Key)

		s.m.Start()
		s.m.closeWg.Wait()
	})
}

//func (s *CompactionViewManagerSuite) TestCheckLoopIDLETicker() {
//	paramtable.Get().Save(Params.DataCoordCfg.GlobalCompactionInterval.Key, "0.1")
//	defer paramtable.Get().Reset(Params.DataCoordCfg.GlobalCompactionInterval.Key)
//	paramtable.Get().Save(Params.DataCoordCfg.EnableLevelZeroSegment.Key, "true")
//	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableLevelZeroSegment.Key)
//
//	events := s.m.Check(context.Background())
//	s.NotEmpty(events)
//	s.Require().NotEmpty(s.m.view.collections)
//
//	notified := make(chan struct{})
//	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil).Once()
//	s.mockTriggerManager.EXPECT().Notify(mock.Anything, mock.Anything, mock.Anything).
//		Run(func(taskID UniqueID, tType CompactionTriggerType, views []CompactionView) {
//			s.Equal(TriggerTypeLevelZeroViewIDLE, tType)
//			v, ok := views[0].(*LevelZeroSegmentsView)
//			s.True(ok)
//			s.NotNil(v)
//			log.Info("All views", zap.String("l0 view", v.String()))
//
//			notified <- struct{}{}
//		}).Once()
//
//	s.m.Start()
//	<-notified
//	s.m.Close()
//}
//
//func (s *CompactionViewManagerSuite) TestCheckLoopRefreshViews() {
//	paramtable.Get().Save(Params.DataCoordCfg.GlobalCompactionInterval.Key, "0.1")
//	defer paramtable.Get().Reset(Params.DataCoordCfg.GlobalCompactionInterval.Key)
//	paramtable.Get().Save(Params.DataCoordCfg.EnableLevelZeroSegment.Key, "true")
//	defer paramtable.Get().Reset(Params.DataCoordCfg.EnableLevelZeroSegment.Key)
//
//	s.Require().Empty(s.m.view.collections)
//
//	notified := make(chan struct{})
//	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil).Once()
//	s.mockTriggerManager.EXPECT().Notify(mock.Anything, mock.Anything, mock.Anything).
//		Run(func(taskID UniqueID, tType CompactionTriggerType, views []CompactionView) {
//			s.Equal(TriggerTypeLevelZeroViewChange, tType)
//			v, ok := views[0].(*LevelZeroSegmentsView)
//			s.True(ok)
//			s.NotNil(v)
//			log.Info("All views", zap.String("l0 view", v.String()))
//
//			notified <- struct{}{}
//		}).Once()
//
//	s.m.Start()
//	<-notified
//
//	// clear view
//	s.m.viewGuard.Lock()
//	s.m.view.collections = make(map[int64][]*SegmentView)
//	s.m.viewGuard.Unlock()
//
//	// clear meta
//	s.m.meta.Lock()
//	s.m.meta.segments.segments = make(map[int64]*SegmentInfo)
//	s.m.meta.Unlock()
//
//	<-time.After(time.Second)
//	s.m.Close()
//}
//
//func (s *CompactionViewManagerSuite) TestTriggerEventForIDLEView() {
//	s.Require().Empty(s.m.view.collections)
//	s.m.triggerEventForIDLEView()
//
//	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil).Once()
//	s.mockTriggerManager.EXPECT().Notify(mock.Anything, mock.Anything, mock.Anything).
//		Run(func(taskID UniqueID, tType CompactionTriggerType, views []CompactionView) {
//			s.EqualValues(1, taskID)
//			s.Equal(TriggerTypeLevelZeroViewIDLE, tType)
//			s.Equal(1, len(views))
//			v, ok := views[0].(*LevelZeroSegmentsView)
//			s.True(ok)
//			s.NotNil(v)
//
//			expectedSegs := []int64{100, 101, 102, 103}
//			gotSegs := lo.Map(v.segments, func(s *SegmentView, _ int) int64 { return s.ID })
//			s.ElementsMatch(expectedSegs, gotSegs)
//
//			s.EqualValues(30000, v.earliestGrowingSegmentPos.GetTimestamp())
//			log.Info("All views", zap.String("l0 view", v.String()))
//		}).Once()
//
//	events := s.m.Check(context.Background())
//	s.NotEmpty(events)
//	s.Require().NotEmpty(s.m.view.collections)
//	s.m.triggerEventForIDLEView()
//}
//
//func (s *CompactionViewManagerSuite) TestNotifyTrigger() {
//	s.mockAlloc.EXPECT().allocID(mock.Anything).Return(1, nil).Once()
//	s.mockTriggerManager.EXPECT().Notify(mock.Anything, mock.Anything, mock.Anything).
//		Run(func(taskID UniqueID, tType CompactionTriggerType, views []CompactionView) {
//			s.EqualValues(1, taskID)
//			s.Equal(TriggerTypeLevelZeroViewChange, tType)
//			s.Equal(1, len(views))
//			v, ok := views[0].(*LevelZeroSegmentsView)
//			s.True(ok)
//			s.NotNil(v)
//
//			expectedSegs := []int64{100, 101, 102, 103}
//			gotSegs := lo.Map(v.segments, func(s *SegmentView, _ int) int64 { return s.ID })
//			s.ElementsMatch(expectedSegs, gotSegs)
//
//			s.EqualValues(30000, v.earliestGrowingSegmentPos.GetTimestamp())
//			log.Info("All views", zap.String("l0 view", v.String()))
//		}).Once()
//
//	ctx := context.Background()
//	s.Require().Empty(s.m.view.collections)
//	events := s.m.Check(ctx)
//
//	s.m.notifyTrigger(ctx, events)
//}
//
//func (s *CompactionViewManagerSuite) TestCheck() {
//	// nothing in the view before the test
//	ctx := context.Background()
//	s.Require().Empty(s.m.view.collections)
//	events := s.m.Check(ctx)
//
//	s.m.viewGuard.Lock()
//	views := s.m.view.GetSegmentViewBy(s.testLabel.CollectionID, nil)
//	s.m.viewGuard.Unlock()
//	s.Equal(4, len(views))
//	for _, view := range views {
//		s.EqualValues(s.testLabel, view.label)
//		s.Equal(datapb.SegmentLevel_L0, view.Level)
//		s.Equal(commonpb.SegmentState_Flushed, view.State)
//		log.Info("String", zap.String("segment", view.String()))
//		log.Info("LevelZeroString", zap.String("segment", view.LevelZeroString()))
//	}
//
//	s.NotEmpty(events)
//	s.Equal(1, len(events))
//	refreshed, ok := events[TriggerTypeLevelZeroViewChange]
//	s.Require().True(ok)
//	s.Equal(1, len(refreshed))
//
//	// same meta
//	emptyEvents := s.m.Check(ctx)
//	s.Empty(emptyEvents)
//
//	// clear meta
//	s.m.meta.Lock()
//	s.m.meta.segments.segments = make(map[int64]*SegmentInfo)
//	s.m.meta.Unlock()
//	emptyEvents = s.m.Check(ctx)
//	s.Empty(emptyEvents)
//	s.Empty(s.m.view.collections)
//
//	s.Run("check collection for zero l0 segments", func() {
//		s.SetupTest()
//		ctx := context.Background()
//		s.Require().Empty(s.m.view.collections)
//		events := s.m.Check(ctx)
//
//		s.m.viewGuard.Lock()
//		views := s.m.view.GetSegmentViewBy(s.testLabel.CollectionID, nil)
//		s.m.viewGuard.Unlock()
//		s.Require().Equal(4, len(views))
//		for _, view := range views {
//			s.EqualValues(s.testLabel, view.label)
//			s.Equal(datapb.SegmentLevel_L0, view.Level)
//			s.Equal(commonpb.SegmentState_Flushed, view.State)
//			log.Info("String", zap.String("segment", view.String()))
//			log.Info("LevelZeroString", zap.String("segment", view.LevelZeroString()))
//		}
//
//		s.NotEmpty(events)
//		s.Equal(1, len(events))
//		refreshed, ok := events[TriggerTypeLevelZeroViewChange]
//		s.Require().True(ok)
//		s.Equal(1, len(refreshed))
//
//		// All l0 segments are dropped in the collection
//		// and there're still some L1 segments
//		s.m.meta.Lock()
//		s.m.meta.segments.segments = map[int64]*SegmentInfo{
//			2000: genTestSegmentInfo(s.testLabel, 2000, datapb.SegmentLevel_L0, commonpb.SegmentState_Dropped),
//			2001: genTestSegmentInfo(s.testLabel, 2001, datapb.SegmentLevel_L0, commonpb.SegmentState_Dropped),
//			2003: genTestSegmentInfo(s.testLabel, 2003, datapb.SegmentLevel_L0, commonpb.SegmentState_Dropped),
//			3000: genTestSegmentInfo(s.testLabel, 2003, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed),
//		}
//		s.m.meta.Unlock()
//		events = s.m.Check(ctx)
//		s.Empty(events)
//		s.m.viewGuard.Lock()
//		views = s.m.view.GetSegmentViewBy(s.testLabel.CollectionID, nil)
//		s.m.viewGuard.Unlock()
//		s.Equal(0, len(views))
//	})
//}

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
			LogSize:    logSize,
			MemorySize: logSize,
		}
		binlogs = append(binlogs, binlog)
	}

	return []*datapb.FieldBinlog{
		{Binlogs: binlogs},
	}
}
