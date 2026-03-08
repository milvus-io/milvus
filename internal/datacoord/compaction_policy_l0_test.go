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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	collection         *collectionInfo

	l0_policy *l0CompactionPolicy
}

func (s *L0CompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	s.collection = &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	}
	meta.collections.Insert(s.testLabel.CollectionID, s.collection)
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.l0_policy = newL0CompactionPolicy(meta, s.mockAlloc)
}

const MB = 1024 * 1024

func (s *L0CompactionPolicySuite) TestActiveToIdle() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.L0CompactionTriggerInterval.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.L0CompactionTriggerInterval.Key)

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.l0_policy.OnCollectionUpdate(1)
	s.Require().EqualValues(1, s.l0_policy.activeCollections.GetActiveCollections()[0])

	<-time.After(3 * time.Second)

	for range 3 {
		gotViews, err := s.l0_policy.Trigger(context.Background())
		s.NoError(err)
		s.NotNil(gotViews)
		s.NotEmpty(gotViews)
		_, ok := gotViews[TriggerTypeLevelZeroViewChange]
		s.True(ok)
	}

	s.Empty(s.l0_policy.activeCollections.GetActiveCollections())
	gotViews, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.NotNil(gotViews)
	s.NotEmpty(gotViews)
	_, ok := gotViews[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
}

func (s *L0CompactionPolicySuite) TestTriggerIdle() {
	s.Require().Empty(s.l0_policy.activeCollections.GetActiveCollections())
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	events, err := s.l0_policy.Trigger(context.Background())
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
	s.Equal(3, len(cView.GetSegmentsView()))
	for _, view := range cView.GetSegmentsView() {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}

	// test for skip collection
	s.l0_policy.AddSkipCollection(1)
	s.l0_policy.AddSkipCollection(1)
	// Test for skip collection
	events, err = s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events)

	// Test for skip collection with ref count
	s.l0_policy.RemoveSkipCollection(1)
	events, err = s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events)

	s.l0_policy.RemoveSkipCollection(1)
	events, err = s.l0_policy.Trigger(context.Background())
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
	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	s.l0_policy.meta = meta
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)

	s.l0_policy.OnCollectionUpdate(s.testLabel.CollectionID)
	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.Equal(1, len(events))
	gotViews, ok := events[TriggerTypeLevelZeroViewChange]
	s.True(ok)
	s.Equal(1, len(gotViews))

	gotViews, ok = events[TriggerTypeLevelZeroViewIDLE]
	s.False(ok)
	s.Empty(gotViews)
}

func (s *L0CompactionPolicySuite) TestTriggerSkipExternalCollection() {
	defer func() {
		s.collection.Schema = &schemapb.CollectionSchema{}
	}()
	s.collection.Schema = &schemapb.CollectionSchema{
		ExternalSource: "s3://foo",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:       1,
				Name:          "external_pk",
				DataType:      schemapb.DataType_Int64,
				ExternalField: "pk_col",
			},
		},
	}

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil).Maybe()
	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events)
}

func (s *L0CompactionPolicySuite) TestManualTrigger() {
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)
	s.l0_policy.triggerOneCollection(context.Background(), s.testLabel.CollectionID)
}

func (s *L0CompactionPolicySuite) TestPositionFiltering() {
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
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0, 0, 4 * MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 20000, 0, 0, 4 * MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 30000, 0, 0, 4 * MB, 1},
		{103, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 40000, 0, 0, 4 * MB, 1},
		{104, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 50000, 0, 0, 4 * MB, 1},
		{200, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 35000, 10 * MB, 1, 0, 0},
		{201, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 60000, 10 * MB, 1, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(s.testLabel, arg.ID, arg.Level, arg.State)
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

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	s.l0_policy.meta = meta
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)

	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.NotEmpty(events)

	gotViews, ok := events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.Equal(1, len(gotViews))

	cView := gotViews[0]
	s.Equal(s.testLabel, cView.GetGroupLabel())
	segViews := cView.GetSegmentsView()
	s.Equal(3, len(segViews))

	for _, view := range segViews {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
		s.LessOrEqual(view.dmlPos.GetTimestamp(), uint64(35000))
	}

	includedIDs := []int64{100, 101, 102}
	for _, id := range includedIDs {
		found := false
		for _, view := range segViews {
			if view.ID == id {
				found = true
				break
			}
		}
		s.True(found, "segment %d should be included", id)
	}

	excludedIDs := []int64{103, 104}
	for _, id := range excludedIDs {
		for _, view := range segViews {
			s.NotEqual(id, view.ID, "segment %d should be excluded due to position > earliest growing position", id)
		}
	}
}

func (s *L0CompactionPolicySuite) TestPositionFilteringWithNoGrowingSegments() {
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
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0, 0, 4 * MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 20000, 0, 0, 4 * MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 30000, 0, 0, 4 * MB, 1},
		{300, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 10000, 10 * MB, 1, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(s.testLabel, arg.ID, arg.Level, arg.State)
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

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	s.l0_policy.meta = meta
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)

	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.NotEmpty(events)

	gotViews, ok := events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.Equal(1, len(gotViews))

	cView := gotViews[0]
	segViews := cView.GetSegmentsView()
	s.Equal(3, len(segViews))

	for _, view := range segViews {
		s.Equal(datapb.SegmentLevel_L0, view.Level)
	}
}

func (s *L0CompactionPolicySuite) TestPositionFilteringEdgeCase() {
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
		{100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0, 0, 4 * MB, 1},
		{101, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 30000, 0, 0, 4 * MB, 1},
		{102, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 30001, 0, 0, 4 * MB, 1},
		{200, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 30000, 10 * MB, 1, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(s.testLabel, arg.ID, arg.Level, arg.State)
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

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	meta.collections.Insert(s.testLabel.CollectionID, &collectionInfo{
		ID:     s.testLabel.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	s.l0_policy.meta = meta
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)

	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.NotEmpty(events)

	gotViews, ok := events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.Equal(1, len(gotViews))

	cView := gotViews[0]
	segViews := cView.GetSegmentsView()
	s.Equal(2, len(segViews))

	includedIDs := []int64{100, 101}
	for _, id := range includedIDs {
		found := false
		for _, view := range segViews {
			if view.ID == id {
				found = true
				break
			}
		}
		s.True(found, "segment %d with position <= 30000 should be included", id)
	}

	for _, view := range segViews {
		s.NotEqual(int64(102), view.ID, "segment 102 with position 30001 should be excluded")
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

func (s *L0CompactionPolicySuite) TestMultiChannelPositionFiltering() {
	label1 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	label2 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-2",
	}

	segArgs := []struct {
		ID    UniqueID
		Label *CompactionGroupLabel
		Level datapb.SegmentLevel
		State commonpb.SegmentState
		PosT  Timestamp

		InsertLogSize  int64
		InsertLogCount int

		DelatLogSize  int64
		DeltaLogCount int
	}{
		{100, label1, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 10000, 0, 0, 4 * MB, 1},
		{101, label1, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 20000, 0, 0, 4 * MB, 1},
		{102, label1, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 50000, 0, 0, 4 * MB, 1},
		{200, label1, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 30000, 10 * MB, 1, 0, 0},
		{300, label2, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 15000, 0, 0, 4 * MB, 1},
		{301, label2, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 25000, 0, 0, 4 * MB, 1},
		{302, label2, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 60000, 0, 0, 4 * MB, 1},
		{400, label2, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 40000, 10 * MB, 1, 0, 0},
	}

	segments := make(map[int64]*SegmentInfo)
	for _, arg := range segArgs {
		info := genTestSegmentInfo(arg.Label, arg.ID, arg.Level, arg.State)
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

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	meta.collections.Insert(label1.CollectionID, &collectionInfo{
		ID:     label1.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})
	s.l0_policy.meta = meta
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(1, nil)

	events, err := s.l0_policy.Trigger(context.Background())
	s.NoError(err)
	s.NotEmpty(events)

	gotViews, ok := events[TriggerTypeLevelZeroViewIDLE]
	s.True(ok)
	s.Equal(2, len(gotViews))

	for _, cView := range gotViews {
		if cView.GetGroupLabel().Channel == "ch-1" {
			segViews := cView.GetSegmentsView()
			s.Equal(2, len(segViews))
			for _, view := range segViews {
				s.LessOrEqual(view.dmlPos.GetTimestamp(), uint64(30000))
			}
		} else if cView.GetGroupLabel().Channel == "ch-2" {
			segViews := cView.GetSegmentsView()
			s.Equal(2, len(segViews))
			for _, view := range segViews {
				s.LessOrEqual(view.dmlPos.GetTimestamp(), uint64(40000))
			}
		}
	}
}

func (s *L0CompactionPolicySuite) TestGroupL0ViewsByPartChan() {
	label1 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	label2 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  20,
		Channel:      "ch-1",
	}

	segments := []*SegmentView{
		{
			ID:     100,
			label:  label1,
			Level:  datapb.SegmentLevel_L0,
			dmlPos: &msgpb.MsgPosition{Timestamp: 10000},
		},
		{
			ID:     101,
			label:  label1,
			Level:  datapb.SegmentLevel_L0,
			dmlPos: &msgpb.MsgPosition{Timestamp: 20000},
		},
		{
			ID:     200,
			label:  label2,
			Level:  datapb.SegmentLevel_L0,
			dmlPos: &msgpb.MsgPosition{Timestamp: 15000},
		},
	}

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	for _, segView := range segments {
		info := genTestSegmentInfo(segView.label, segView.ID, segView.Level, commonpb.SegmentState_Flushed)
		info.DmlPosition = segView.dmlPos
		info.Deltalogs = genTestBinlogs(1, 4*MB)
		meta.segments.SetSegment(segView.ID, info)
	}
	meta.collections.Insert(label1.CollectionID, &collectionInfo{
		ID:     label1.CollectionID,
		Schema: &schemapb.CollectionSchema{},
	})

	s.l0_policy.meta = meta
	views := s.l0_policy.groupL0ViewsByPartChan(1, segments, 999)

	s.Equal(2, len(views))

	for _, view := range views {
		if view.GetGroupLabel().PartitionID == 10 {
			s.Equal(2, len(view.GetSegmentsView()))
		} else if view.GetGroupLabel().PartitionID == 20 {
			s.Equal(1, len(view.GetSegmentsView()))
		}
	}
}

func (s *L0CompactionPolicySuite) TestLevelZeroCompactionViewString() {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	view := &LevelZeroCompactionView{
		triggerID: 123,
		label:     label,
		l0Segments: []*SegmentView{
			{ID: 100, Level: datapb.SegmentLevel_L0, DeltaSize: 4 * MB, DeltalogCount: 1},
			{ID: 101, Level: datapb.SegmentLevel_L0, DeltaSize: 8 * MB, DeltalogCount: 2},
		},
		latestDeletePos: &msgpb.MsgPosition{Timestamp: 30000},
	}

	s.Contains(view.String(), "L0SegCount=2")
	s.Contains(view.String(), "posT=<30000>")
	s.Contains(view.String(), "label=<coll=1, part=10, channel=ch-1>")
}

func (s *L0CompactionPolicySuite) TestLevelZeroCompactionViewAppend() {
	view := &LevelZeroCompactionView{
		triggerID:       123,
		label:           s.testLabel,
		l0Segments:      nil,
		latestDeletePos: &msgpb.MsgPosition{Timestamp: 30000},
	}

	s.Nil(view.l0Segments)

	seg1 := &SegmentView{ID: 100, Level: datapb.SegmentLevel_L0}
	view.Append(seg1)
	s.Equal(1, len(view.l0Segments))
	s.Equal(int64(100), view.l0Segments[0].ID)

	seg2 := &SegmentView{ID: 101, Level: datapb.SegmentLevel_L0}
	seg3 := &SegmentView{ID: 102, Level: datapb.SegmentLevel_L0}
	view.Append(seg2, seg3)
	s.Equal(3, len(view.l0Segments))
}
