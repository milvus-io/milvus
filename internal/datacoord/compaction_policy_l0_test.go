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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

func TestL0CompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(L0CompactionPolicySuite))
}

type L0CompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *NMockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            Handler
	mockPlanContext    *MockCompactionPlanContext

	l0_policy *l0CompactionPolicy
}

const MB = 1024 * 1024

func (s *L0CompactionPolicySuite) TestTrigger() {
	s.Require().Empty(s.l0_policy.view.collections)

	events, err := s.l0_policy.Trigger()
	s.NoError(err)
	gotViews, ok := events[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	cView := gotViews[0]
	s.Equal(s.testLabel, cView.GetGroupLabel())
	s.Equal(4, len(cView.GetSegmentsView()))
	for _, view := range cView.GetSegmentsView() {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}
	log.Info("cView", zap.String("string", cView.String()))

	// Test for idle trigger
	for i := 0; i < 2; i++ {
		events, err = s.l0_policy.Trigger()
		s.NoError(err)
		s.Equal(0, len(events))
	}
	s.EqualValues(2, s.l0_policy.emptyLoopCount.Load())

	events, err = s.l0_policy.Trigger()
	s.NoError(err)
	s.EqualValues(0, s.l0_policy.emptyLoopCount.Load())
	s.Equal(1, len(events))
	gotViews, ok = events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))
	cView = gotViews[0]
	s.Equal(s.testLabel, cView.GetGroupLabel())
	s.Equal(4, len(cView.GetSegmentsView()))
	for _, view := range cView.GetSegmentsView() {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}
	log.Info("cView", zap.String("string", cView.String()))

	segArgs := []struct {
		ID   UniqueID
		PosT Timestamp

		DelatLogSize  int64
		DeltaLogCount int
	}{
		{500, 10000, 4 * MB, 1},
		{501, 10000, 4 * MB, 1},
		{502, 10000, 4 * MB, 1},
		{503, 50000, 4 * MB, 1},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(s.testLabel, arg.ID, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
		info.Deltalogs = genTestBinlogs(arg.DeltaLogCount, arg.DelatLogSize)
		info.DmlPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		segments[arg.ID] = info
	}
	meta := &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	s.l0_policy.meta = meta

	events, err = s.l0_policy.Trigger()
	s.NoError(err)
	gotViews, ok = events[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.Equal(1, len(gotViews))
}

func (s *L0CompactionPolicySuite) TestGenerateEventForLevelZeroViewChange() {
	s.Require().Empty(s.l0_policy.view.collections)

	events := s.l0_policy.generateEventForLevelZeroViewChange()
	s.NotEmpty(events)
	s.NotEmpty(s.l0_policy.view.collections)

	gotViews, ok := events[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	storedViews, ok := s.l0_policy.view.collections[s.testLabel.CollectionID]
	s.True(ok)
	s.NotNil(storedViews)
	s.Equal(4, len(storedViews))

	for _, view := range storedViews {
		s.Equal(s.testLabel, view.label)
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}
}

func genSegmentsForMeta(label *CompactionGroupLabel) map[int64]*SegmentInfo {
	segArgs := []struct {
		ID    UniqueID
		Level datapb.SegmentLevel
		State commonpb.SegmentState
		PosT  Timestamp

		InsertLogSize  int64
		InsertLogCount int

		DelatLogSize  int64
		DeltaLogCount int
	}{
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * MB, 0, 4 * MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * MB, 0, 4 * MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0 * MB, 0, 4 * MB, 1},
		{103, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 50000, 0 * MB, 0, 4 * MB, 1},
		{200, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 50000, 10 * MB, 1, 0, 0},
		{201, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 30000, 10 * MB, 1, 0, 0},
		{300, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 10000, 10 * MB, 1, 0, 0},
		{301, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 20000, 10 * MB, 1, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(label, arg.ID, arg.Level, arg.State)
		if info.Level == datapb.SegmentLevel_L0 || info.State == commonpb.SegmentState_Flushed {
			info.Deltalogs = genTestBinlogs(arg.DeltaLogCount, arg.DelatLogSize)
			info.DmlPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}
		info.Binlogs = genTestBinlogs(arg.InsertLogCount, arg.InsertLogSize)
		if info.State == commonpb.SegmentState_Growing {
			info.StartPosition = &msgpb.MsgPosition{Timestamp: arg.PosT}
		}

		segments[arg.ID] = info
	}

	return segments
}

func (s *L0CompactionPolicySuite) SetupTest() {
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

	s.l0_policy = newL0CompactionPolicy(meta)
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

func genTestBinlogs(logCount int, logSize int64) []*datapb.FieldBinlog {
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
