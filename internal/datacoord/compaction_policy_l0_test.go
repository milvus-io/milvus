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
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestL0CompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(L0CompactionPolicySuite))
}

type L0CompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            Handler
	inspector          *MockCompactionInspector

	l0_policy *l0CompactionPolicy
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

const MB = 1024 * 1024

func (s *L0CompactionPolicySuite) TestActiveToIdle() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.L0CompactionTriggerInterval.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.L0CompactionTriggerInterval.Key)

	s.l0_policy.OnCollectionUpdate(1)
	s.Require().EqualValues(1, s.l0_policy.activeCollections.GetActiveCollections()[0])

	<-time.After(3 * time.Second)

	for range 3 {
		gotViews, err := s.l0_policy.Trigger()
		s.NoError(err)
		s.NotNil(gotViews)
		s.NotEmpty(gotViews)
		_, ok := gotViews[TriggerTypeLevelZeroViewChange]
		s.True(ok)
	}

	s.Empty(s.l0_policy.activeCollections.GetActiveCollections())
	gotViews, err := s.l0_policy.Trigger()
	s.NoError(err)
	s.NotNil(gotViews)
	s.NotEmpty(gotViews)
	_, ok := gotViews[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
}

func (s *L0CompactionPolicySuite) TestTriggerIdle() {
	s.Require().Empty(s.l0_policy.activeCollections.GetActiveCollections())

	events, err := s.l0_policy.Trigger()
	s.NoError(err)
	s.NotEmpty(events)

	gotViews, ok := events[TriggerTypeLevelZeroViewChange]
	s.False(ok)
	s.Empty(gotViews)

	gotViews, ok = events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	cView := gotViews[0]
	s.Equal(s.testLabel, cView.GetGroupLabel())
	s.Equal(4, len(cView.GetSegmentsView()))
	for _, view := range cView.GetSegmentsView() {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}

	// test for skip collection
	s.l0_policy.AddSkipCollection(1)
	s.l0_policy.AddSkipCollection(1)
	// Test for skip collection
	events, err = s.l0_policy.Trigger()
	s.NoError(err)
	s.Empty(events)

	// Test for skip collection with ref count
	s.l0_policy.RemoveSkipCollection(1)
	events, err = s.l0_policy.Trigger()
	s.NoError(err)
	s.Empty(events)

	s.l0_policy.RemoveSkipCollection(1)
	events, err = s.l0_policy.Trigger()
	s.NoError(err)
	s.Equal(1, len(events))
	gotViews, ok = events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	log.Info("cView", zap.String("string", cView.String()))
}

func (s *L0CompactionPolicySuite) TestTriggerViewChange() {
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

	s.l0_policy.OnCollectionUpdate(s.testLabel.CollectionID)
	events, err := s.l0_policy.Trigger()
	s.NoError(err)
	s.Equal(1, len(events))
	gotViews, ok := events[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.Equal(1, len(gotViews))

	gotViews, ok = events[TriggerTypeLevelZeroViewIDLE]
	s.False(ok)
	s.Empty(gotViews)
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
