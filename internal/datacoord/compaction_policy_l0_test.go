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

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestL0CompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(L0CompactionPolicySuite))
}

func (s *L0CompactionPolicySuite) SetupTest() {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	targetView := &LevelZeroSegmentsView{
		label: label,
		segments: []*SegmentView{
			genTestL0SegmentView(100, label, 10000),
			genTestL0SegmentView(101, label, 10000),
			genTestL0SegmentView(102, label, 10000),
		},
		earliestGrowingSegmentPos: &msgpb.MsgPosition{Timestamp: 10000},
	}

	s.l0Policy = &L0CompactionPolicy{}
	s.v = targetView
	log.Info("LevelZeroSegmentsView", zap.String("view", targetView.String()))
}

type L0CompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            Handler
	mockPlanContext    *MockCompactionPlanContext

	l0Policy *L0CompactionPolicy
	v        *LevelZeroSegmentsView
}

const MB = 1024 * 1024

func (s *L0CompactionPolicySuite) TestParams() {
	// By default, it's enabled
	s.Require().True(s.l0Policy.Enabled())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.Key, "false")
	s.False(s.l0Policy.Enabled())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.False(s.l0Policy.Enabled())

	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.Key)
	s.False(s.l0Policy.Enabled())

	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
	s.True(s.l0Policy.Enabled())
}

func genTestL0SegmentView(ID UniqueID, label *CompactionGroupLabel, posTime Timestamp) *SegmentView {
	return &SegmentView{
		ID:     ID,
		label:  label,
		dmlPos: &msgpb.MsgPosition{Timestamp: posTime},
		Level:  datapb.SegmentLevel_L0,
		State:  commonpb.SegmentState_Flushed,
	}
}

func (s *L0CompactionPolicySuite) TestWrongInputViewType() {
	policy := &L0CompactionPolicy{}
	got, reason := policy.Trigger(nil)
	s.Nil(got)
	s.Empty(reason)

	got, reason = policy.ForceTrigger(nil)
	s.Nil(got)
	s.Empty(reason)

	s.v.segments = nil
	got, reason = policy.ForceTrigger(s.v)
	s.Nil(got)
	s.Empty(reason)
}

func (s *L0CompactionPolicySuite) TestTrigger() {
	label := s.v.label
	views := []*SegmentView{
		genTestL0SegmentView(100, label, 20000),
		genTestL0SegmentView(101, label, 10000),
		genTestL0SegmentView(102, label, 30000),
		genTestL0SegmentView(103, label, 40000),
	}

	s.v.segments = views
	tests := []struct {
		description string

		prepSizeEach  float64
		prepCountEach int64
		prepEarliestT Timestamp

		expectedSegs []UniqueID
	}{
		{
			"No valid segments by earliest growing segment pos",
			64,
			20,
			10000,
			nil,
		},
		{
			"Not qualified",
			1,
			1,
			30000,
			nil,
		},
		{
			"Trigger by > TriggerDeltaSize",
			8 * 1024 * 1024,
			1,
			30000,
			[]UniqueID{100, 101},
		},
		{
			"Trigger by > TriggerDeltaCount",
			1,
			10,
			30000,
			[]UniqueID{100, 101},
		},
		{
			"Trigger by > maxDeltaSize",
			128 * 1024 * 1024,
			1,
			30000,
			[]UniqueID{100},
		},
		{
			"Trigger by > maxDeltaCount",
			1,
			24,
			30000,
			[]UniqueID{100},
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.v.earliestGrowingSegmentPos.Timestamp = test.prepEarliestT
			for _, view := range s.v.GetSegmentsView() {
				if view.dmlPos.Timestamp < test.prepEarliestT {
					view.DeltalogCount = test.prepCountEach
					view.DeltaSize = test.prepSizeEach
					view.DeltaRowCount = 1
				}
			}
			log.Info("LevelZeroSegmentsView", zap.String("view", s.v.String()))

			gotView, reason := s.l0Policy.Trigger(s.v)
			if len(test.expectedSegs) == 0 {
				s.Nil(gotView)
			} else {
				levelZeroView, ok := gotView.(*LevelZeroSegmentsView)
				s.True(ok)
				s.NotNil(levelZeroView)

				gotSegIDs := lo.Map(levelZeroView.GetSegmentsView(), func(v *SegmentView, _ int) int64 {
					return v.ID
				})
				s.ElementsMatch(gotSegIDs, test.expectedSegs)
				log.Info("output view", zap.String("view", levelZeroView.String()), zap.String("trigger reason", reason))
			}
		})
	}
}

func (s *L0CompactionPolicySuite) TestMinCountSizeTrigger() {
	label := s.v.GetGroupLabel()
	tests := []struct {
		description string
		segIDs      []int64
		segCounts   []int64
		segSize     []float64

		expectedIDs []int64
	}{
		{"donot trigger", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{1, 1, 1}, nil},
		{"trigger by count=15", []int64{100, 101, 102}, []int64{5, 5, 5}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=10", []int64{100, 101, 102}, []int64{5, 3, 2}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=50", []int64{100, 101, 102}, []int64{32, 10, 8}, []float64{1, 1, 1}, []int64{100}},
		{"trigger by size=24MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=8MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{3 * 1024 * 1024, 3 * 1024 * 1024, 2 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=128MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{100 * 1024 * 1024, 20 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			views := []*SegmentView{}
			for idx, ID := range test.segIDs {
				seg := genTestL0SegmentView(ID, label, 10000)
				seg.DeltaSize = test.segSize[idx]
				seg.DeltalogCount = test.segCounts[idx]

				views = append(views, seg)
			}

			picked, reason := minCountSizeTrigger(views)
			s.ElementsMatch(lo.Map(picked, func(view *SegmentView, _ int) int64 {
				return view.ID
			}), test.expectedIDs)

			if len(picked) > 0 {
				s.NotEmpty(reason)
			}

			log.Info("test minCountSizeTrigger", zap.String("trigger reason", reason))
		})
	}
}

func (s *L0CompactionPolicySuite) TestForceTrigger() {
	label := s.v.GetGroupLabel()
	tests := []struct {
		description string
		segIDs      []int64
		segCounts   []int64
		segSize     []float64

		expectedIDs []int64
	}{
		{"force trigger", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=15", []int64{100, 101, 102}, []int64{5, 5, 5}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=10", []int64{100, 101, 102}, []int64{5, 3, 2}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=50", []int64{100, 101, 102}, []int64{32, 10, 8}, []float64{1, 1, 1}, []int64{100}},
		{"trigger by size=24MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=8MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{3 * 1024 * 1024, 3 * 1024 * 1024, 2 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=128MB", []int64{100, 101, 102}, []int64{1, 1, 1}, []float64{100 * 1024 * 1024, 20 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			view := &LevelZeroSegmentsView{
				label: label,
				earliestGrowingSegmentPos: &msgpb.MsgPosition{
					Timestamp: 999999,
				},
			}

			for idx, ID := range test.segIDs {
				seg := genTestL0SegmentView(ID, label, 10000)
				seg.DeltaSize = test.segSize[idx]
				seg.DeltalogCount = test.segCounts[idx]
				view.Append(seg)
			}

			picked, reason := s.l0Policy.ForceTrigger(view)
			s.NotNil(picked)
			s.NotEmpty(reason)
			s.ElementsMatch(lo.Map(picked.GetSegmentsView(), func(view *SegmentView, _ int) int64 {
				return view.ID
			}), test.expectedIDs)
			log.Info("test forceTrigger", zap.String("trigger reason", reason))
		})
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
		{400, datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed, 20000, 10 * MB, 1, 10 * MB, 1},
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
