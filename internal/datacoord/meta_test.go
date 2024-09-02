// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

// MetaReloadSuite tests meta reload & meta creation related logic
type MetaReloadSuite struct {
	testutils.PromMetricsSuite

	catalog *mocks2.DataCoordCatalog
	meta    *meta
}

func (suite *MetaReloadSuite) SetupTest() {
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	suite.catalog = catalog
}

func (suite *MetaReloadSuite) resetMock() {
	suite.catalog.ExpectedCalls = nil
}

func (suite *MetaReloadSuite) TestReloadFromKV() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	suite.Run("ListSegments_fail", func() {
		defer suite.resetMock()
		suite.catalog.EXPECT().ListSegments(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return([]*model.SegmentIndex{}, nil)
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil)
		suite.Error(err)
	})

	suite.Run("ListChannelCheckpoint_fail", func() {
		defer suite.resetMock()

		suite.catalog.EXPECT().ListSegments(mock.Anything).Return([]*datapb.SegmentInfo{}, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, errors.New("mock"))
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return([]*model.SegmentIndex{}, nil)
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

		_, err := newMeta(ctx, suite.catalog, nil)
		suite.Error(err)
	})

	suite.Run("ok", func() {
		defer suite.resetMock()
		suite.catalog.EXPECT().ListIndexes(mock.Anything).Return([]*model.Index{}, nil)
		suite.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return([]*model.SegmentIndex{}, nil)
		suite.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
		suite.catalog.EXPECT().ListSegments(mock.Anything).Return([]*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				State:        commonpb.SegmentState_Flushed,
			},
		}, nil)
		suite.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(map[string]*msgpb.MsgPosition{
			"ch": {
				ChannelName: "cn",
				MsgID:       []byte{},
				Timestamp:   1000,
			},
		}, nil)

		_, err := newMeta(ctx, suite.catalog, nil)
		suite.NoError(err)

		suite.MetricsEqual(metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushedSegmentLabel, datapb.SegmentLevel_Legacy.String()), 1)
	})
}

type MetaBasicSuite struct {
	testutils.PromMetricsSuite

	collID      int64
	partIDs     []int64
	channelName string

	meta *meta
}

func (suite *MetaBasicSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *MetaBasicSuite) SetupTest() {
	suite.collID = 1
	suite.partIDs = []int64{100, 101}
	suite.channelName = "c1"

	meta, err := newMemoryMeta()

	suite.Require().NoError(err)
	suite.meta = meta
}

func (suite *MetaBasicSuite) getCollectionInfo(partIDs ...int64) *collectionInfo {
	testSchema := newTestSchema()
	return &collectionInfo{
		ID:             suite.collID,
		Schema:         testSchema,
		Partitions:     partIDs,
		StartPositions: []*commonpb.KeyDataPair{},
	}
}

func (suite *MetaBasicSuite) TestCollection() {
	meta := suite.meta

	info := suite.getCollectionInfo(suite.partIDs...)
	meta.AddCollection(info)

	collInfo := meta.GetCollection(suite.collID)
	suite.Require().NotNil(collInfo)

	// check partition info
	suite.EqualValues(suite.collID, collInfo.ID)
	suite.EqualValues(info.Schema, collInfo.Schema)
	suite.EqualValues(len(suite.partIDs), len(collInfo.Partitions))
	suite.ElementsMatch(info.Partitions, collInfo.Partitions)

	suite.MetricsEqual(metrics.DataCoordNumCollections.WithLabelValues(), 1)
}

func (suite *MetaBasicSuite) TestCompleteCompactionMutation() {
	latestSegments := NewSegmentsInfo()
	for segID, segment := range map[UniqueID]*SegmentInfo{
		1: {SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000, 10001)},
			Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000, 20001)},
			// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 30000), getFieldBinlogIDs(0, 30001)},
			NumOfRows: 2,
		}},
		2: {SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
			Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
			// latest segment has 2 deltalogs, one submit for compaction, one is appended before compaction done
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 31000), getFieldBinlogIDs(0, 31001)},
			NumOfRows: 2,
		}},
	} {
		latestSegments.SetSegment(segID, segment)
	}

	mockChMgr := mocks.NewChunkManager(suite.T())
	m := &meta{
		catalog:      &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments:     latestSegments,
		chunkManager: mockChMgr,
	}

	compactToSeg := &datapb.CompactionSegment{
		SegmentID:           3,
		InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
		Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
		NumOfRows:           2,
	}

	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{compactToSeg},
	}
	task := &datapb.CompactionTask{
		InputSegments: []UniqueID{1, 2},
		Type:          datapb.CompactionType_MixCompaction,
	}

	infos, mutation, err := m.CompleteCompactionMutation(task, result)
	assert.NoError(suite.T(), err)
	suite.Equal(1, len(infos))
	info := infos[0]
	suite.NoError(err)
	suite.NotNil(info)
	suite.NotNil(mutation)

	// check newSegment
	suite.EqualValues(3, info.GetID())
	suite.Equal(datapb.SegmentLevel_L1, info.GetLevel())
	suite.Equal(commonpb.SegmentState_Flushed, info.GetState())

	binlogs := info.GetBinlogs()
	for _, fbinlog := range binlogs {
		for _, blog := range fbinlog.GetBinlogs() {
			suite.Empty(blog.GetLogPath())
			suite.EqualValues(50000, blog.GetLogID())
		}
	}

	statslogs := info.GetStatslogs()
	for _, fbinlog := range statslogs {
		for _, blog := range fbinlog.GetBinlogs() {
			suite.Empty(blog.GetLogPath())
			suite.EqualValues(50001, blog.GetLogID())
		}
	}

	// check compactFrom segments
	for _, segID := range []int64{1, 2} {
		seg := m.GetSegment(segID)
		suite.Equal(commonpb.SegmentState_Dropped, seg.GetState())
		suite.NotEmpty(seg.GetDroppedAt())

		suite.EqualValues(segID, seg.GetID())
		suite.ElementsMatch(latestSegments.segments[segID].GetBinlogs(), seg.GetBinlogs())
		suite.ElementsMatch(latestSegments.segments[segID].GetStatslogs(), seg.GetStatslogs())
		suite.ElementsMatch(latestSegments.segments[segID].GetDeltalogs(), seg.GetDeltalogs())
	}

	// check mutation metrics
	suite.Equal(2, len(mutation.stateChange[datapb.SegmentLevel_L1.String()]))
	suite.EqualValues(-2, mutation.rowCountChange)
	suite.EqualValues(2, mutation.rowCountAccChange)
}

func (suite *MetaBasicSuite) TestSetSegment() {
	meta := suite.meta
	catalog := mocks2.NewDataCoordCatalog(suite.T())
	meta.catalog = catalog
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suite.Run("normal", func() {
		segmentID := int64(1000)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()

		err = meta.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("not_updated", func() {
		segmentID := int64(1001)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return false
		}

		err = meta.UpdateSegment(segmentID, noOp)
		suite.NoError(err)
	})

	suite.Run("catalog_error", func() {
		segmentID := int64(1002)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			MaxRowNum:     30000,
			CollectionID:  suite.collID,
			InsertChannel: suite.channelName,
			State:         commonpb.SegmentState_Flushed,
		})
		err := meta.AddSegment(ctx, segment)
		suite.Require().NoError(err)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(errors.New("mocked")).Once()

		err = meta.UpdateSegment(segmentID, noOp)
		suite.Error(err)
	})

	suite.Run("segment_not_found", func() {
		segmentID := int64(1003)

		noOp := func(segment *SegmentInfo) bool {
			return true
		}

		err := meta.UpdateSegment(segmentID, noOp)
		suite.Error(err)
		suite.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func TestMeta(t *testing.T) {
	suite.Run(t, new(MetaBasicSuite))
	suite.Run(t, new(MetaReloadSuite))
}

func TestMeta_Basic(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"

	// mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta()
	assert.NoError(t, err)

	testSchema := newTestSchema()

	collInfo := &collectionInfo{
		ID:             collID,
		Schema:         testSchema,
		Partitions:     []UniqueID{partID0, partID1},
		StartPositions: []*commonpb.KeyDataPair{},
		DatabaseName:   util.DefaultDBName,
	}
	collInfoWoPartition := &collectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	}

	count := atomic.Int64{}
	AllocID := func() int64 {
		return count.Add(1)
	}

	t.Run("Test Segment", func(t *testing.T) {
		meta.AddCollection(collInfoWoPartition)
		// create seg0 for partition0, seg0/seg1 for partition1
		segID0_0 := AllocID()
		segInfo0_0 := buildSegment(collID, partID0, segID0_0, channelName)
		segID1_0 := AllocID()
		segInfo1_0 := buildSegment(collID, partID1, segID1_0, channelName)
		segID1_1 := AllocID()
		segInfo1_1 := buildSegment(collID, partID1, segID1_1, channelName)

		// check AddSegment
		err = meta.AddSegment(context.TODO(), segInfo0_0)
		assert.NoError(t, err)
		err = meta.AddSegment(context.TODO(), segInfo1_0)
		assert.NoError(t, err)
		err = meta.AddSegment(context.TODO(), segInfo1_1)
		assert.NoError(t, err)

		// check GetSegment
		info0_0 := meta.GetHealthySegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.True(t, proto.Equal(info0_0, segInfo0_0))
		info1_0 := meta.GetHealthySegment(segID1_0)
		assert.NotNil(t, info1_0)
		assert.True(t, proto.Equal(info1_0, segInfo1_0))

		// check GetSegmentsOfCollection
		segIDs := meta.GetSegmentsIDOfCollection(collID)
		assert.EqualValues(t, 3, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check GetSegmentsOfPartition
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID0)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID1)
		assert.EqualValues(t, 2, len(segIDs))
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check DropSegment
		err = meta.DropSegment(segID1_0)
		assert.NoError(t, err)
		segIDs = meta.GetSegmentsIDOfPartition(collID, partID1)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID1_1)

		err = meta.SetState(segID0_0, commonpb.SegmentState_Sealed)
		assert.NoError(t, err)
		err = meta.SetState(segID0_0, commonpb.SegmentState_Flushed)
		assert.NoError(t, err)

		info0_0 = meta.GetHealthySegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.EqualValues(t, commonpb.SegmentState_Flushed, info0_0.State)
	})

	t.Run("Test segment with kv fails", func(t *testing.T) {
		// inject error for `Save`
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
		catalog := datacoord.NewCatalog(metakv, "", "")
		meta, err := newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)

		err = meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.Error(t, err)

		metakv2 := mockkv.NewMetaKv(t)
		metakv2.EXPECT().Save(mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().MultiSave(mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().Remove(mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().MultiRemove(mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
		metakv2.EXPECT().MultiSaveAndRemoveWithPrefix(mock.Anything, mock.Anything).Return(errors.New("failed"))
		catalog = datacoord.NewCatalog(metakv2, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)
		// nil, since no segment yet
		err = meta.DropSegment(0)
		assert.NoError(t, err)
		// nil, since Save error not injected
		err = meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.NoError(t, err)
		// error injected
		err = meta.DropSegment(0)
		assert.Error(t, err)

		catalog = datacoord.NewCatalog(metakv, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)
		assert.NotNil(t, meta)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300

		// no segment
		nums := meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0 := AllocID()
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(context.TODO(), segInfo0)
		assert.NoError(t, err)

		// add seg2 with 300 rows
		segID1 := AllocID()
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(context.TODO(), segInfo1)
		assert.NoError(t, err)

		// check partition/collection statistics
		nums = meta.GetNumRowsOfPartition(collID, partID0)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
		nums = meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
	})

	t.Run("Test GetSegmentsChanPart", func(t *testing.T) {
		result := meta.GetSegmentsChanPart(func(*SegmentInfo) bool { return true })
		assert.Equal(t, 2, len(result))
		for _, entry := range result {
			assert.Equal(t, "c1", entry.channelName)
			if entry.partitionID == UniqueID(100) {
				assert.Equal(t, 3, len(entry.segments))
			}
			if entry.partitionID == UniqueID(101) {
				assert.Equal(t, 1, len(entry.segments))
			}
		}
		result = meta.GetSegmentsChanPart(func(seg *SegmentInfo) bool { return seg.GetCollectionID() == 10 })
		assert.Equal(t, 0, len(result))
	})

	t.Run("GetClonedCollectionInfo", func(t *testing.T) {
		// collection does not exist
		ret := meta.GetClonedCollectionInfo(-1)
		assert.Nil(t, ret)

		collInfo.Properties = map[string]string{
			common.CollectionTTLConfigKey: "3600",
		}
		meta.AddCollection(collInfo)
		ret = meta.GetClonedCollectionInfo(collInfo.ID)
		equalCollectionInfo(t, collInfo, ret)

		collInfo.StartPositions = []*commonpb.KeyDataPair{
			{
				Key:  "k",
				Data: []byte("v"),
			},
		}
		meta.AddCollection(collInfo)
		ret = meta.GetClonedCollectionInfo(collInfo.ID)
		equalCollectionInfo(t, collInfo, ret)
	})

	t.Run("Test GetCollectionBinlogSize", func(t *testing.T) {
		const size0 = 1024
		const size1 = 2048

		// add seg0 with size0
		segID0 := AllocID()
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.size.Store(size0)
		err = meta.AddSegment(context.TODO(), segInfo0)
		assert.NoError(t, err)

		// add seg1 with size1
		segID1 := AllocID()
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.size.Store(size1)
		err = meta.AddSegment(context.TODO(), segInfo1)
		assert.NoError(t, err)

		// check TotalBinlogSize
		quotaInfo := meta.GetQuotaInfo()
		assert.Len(t, quotaInfo.CollectionBinlogSize, 1)
		assert.Equal(t, int64(size0+size1), quotaInfo.CollectionBinlogSize[collID])
		assert.Equal(t, int64(size0+size1), quotaInfo.TotalBinlogSize)

		meta.collections[collID] = collInfo
		quotaInfo = meta.GetQuotaInfo()
		assert.Len(t, quotaInfo.CollectionBinlogSize, 1)
		assert.Equal(t, int64(size0+size1), quotaInfo.CollectionBinlogSize[collID])
		assert.Equal(t, int64(size0+size1), quotaInfo.TotalBinlogSize)
	})

	t.Run("Test GetCollectionBinlogSize", func(t *testing.T) {
		meta := createMeta(&datacoord.Catalog{}, withIndexMeta(createIndexMeta(&datacoord.Catalog{})))
		ret := meta.GetCollectionIndexFilesSize()
		assert.Equal(t, uint64(0), ret)

		meta.collections = map[UniqueID]*collectionInfo{
			100: {
				ID:           100,
				DatabaseName: "db",
			},
		}
		ret = meta.GetCollectionIndexFilesSize()
		assert.Equal(t, uint64(11), ret)
	})

	t.Run("Test AddAllocation", func(t *testing.T) {
		meta, _ := newMemoryMeta()
		err := meta.AddAllocation(1, &Allocation{
			SegmentID:  1,
			NumOfRows:  1,
			ExpireTime: 0,
		})
		assert.Error(t, err)
	})
}

func TestGetUnFlushedSegments(t *testing.T) {
	meta, err := newMemoryMeta()
	assert.NoError(t, err)
	s1 := &datapb.SegmentInfo{
		ID:           0,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Growing,
	}
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(s1))
	assert.NoError(t, err)
	s2 := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	}
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(s2))
	assert.NoError(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.NoError(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}

func TestUpdateSegmentsInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Growing,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
		}}
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
			AddBinlogsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDsWithEntry(1, 10, 1)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 1)},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogPath: "", LogID: 2}}}},
			),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(1)
		expected := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushing, NumOfRows: 10,
			StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}},
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(1, 0, 1)},
			Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(1, 0, 1)},
			Deltalogs:     []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000}}}},
		}}

		assert.Equal(t, updated.StartPosition, expected.StartPosition)
		assert.Equal(t, updated.DmlPosition, expected.DmlPosition)
		assert.Equal(t, updated.DmlPosition, expected.DmlPosition)
		assert.Equal(t, len(updated.Binlogs[0].Binlogs), len(expected.Binlogs[0].Binlogs))
		assert.Equal(t, len(updated.Statslogs[0].Binlogs), len(expected.Statslogs[0].Binlogs))
		assert.Equal(t, len(updated.Deltalogs[0].Binlogs), len(expected.Deltalogs[0].Binlogs))
		assert.Equal(t, updated.State, expected.State)
		assert.Equal(t, updated.size.Load(), expected.size.Load())
		assert.Equal(t, updated.NumOfRows, expected.NumOfRows)
	})

	t.Run("update compacted segment", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		// segment not found
		err = meta.UpdateSegmentsInfo(
			UpdateCompactedOperator(1),
		)
		assert.NoError(t, err)

		// normal
		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushed,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
		}}
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateCompactedOperator(1),
		)
		assert.NoError(t, err)
	})
	t.Run("update non-existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			AddBinlogsOperator(1, nil, nil, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateBinlogsOperator(1, nil, nil, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateDmlPosition(1, nil),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateDmlPosition(1, &msgpb.MsgPosition{MsgID: []byte{1}}),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateImportedRows(1, 0),
		)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateIsImporting(1, true),
		)
		assert.NoError(t, err)
	})

	t.Run("update checkpoints and start position of non existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}}
		err = meta.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		err = meta.UpdateSegmentsInfo(
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 2, NumOfRows: 10}}),
		)

		assert.NoError(t, err)
		assert.Nil(t, meta.GetHealthySegment(2))
	})

	t.Run("test save etcd failed", func(t *testing.T) {
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("mocked fail")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("mocked fail")).Maybe()
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
		catalog := datacoord.NewCatalog(metakv, "", "")
		meta, err := newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)

		segmentInfo := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        1,
				NumOfRows: 0,
				State:     commonpb.SegmentState_Growing,
			},
		}
		meta.segments.SetSegment(1, segmentInfo)

		err = meta.UpdateSegmentsInfo(
			UpdateStatusOperator(1, commonpb.SegmentState_Flushing),
			AddBinlogsOperator(1,
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
				[]*datapb.FieldBinlog{getFieldBinlogIDs(1, 2)},
				[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogPath: "", LogID: 2}}}},
			),
			UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}}),
			UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}),
		)

		assert.Error(t, err)
		assert.Equal(t, "mocked fail", err.Error())
		segmentInfo = meta.GetHealthySegment(1)
		assert.EqualValues(t, 0, segmentInfo.NumOfRows)
		assert.Equal(t, commonpb.SegmentState_Growing, segmentInfo.State)
		assert.Nil(t, segmentInfo.Binlogs)
		assert.Nil(t, segmentInfo.StartPosition)
	})
}

func Test_meta_SetSegmentsCompacting(t *testing.T) {
	type fields struct {
		client   kv.MetaKv
		segments *SegmentsInfo
	}
	type args struct {
		segmentID  UniqueID
		compacting bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test set segment compacting",
			fields{
				NewMetaMemoryKV(),
				&SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
							isCompacting: false,
						},
					},
					compactionTo: make(map[int64]UniqueID),
				},
			},
			args{
				segmentID:  1,
				compacting: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				catalog:  &datacoord.Catalog{MetaKv: tt.fields.client},
				segments: tt.fields.segments,
			}
			m.SetSegmentsCompacting([]UniqueID{tt.args.segmentID}, tt.args.compacting)
			segment := m.GetHealthySegment(tt.args.segmentID)
			assert.Equal(t, tt.args.compacting, segment.isCompacting)
		})
	}
}

func Test_meta_GetSegmentsOfCollection(t *testing.T) {
	storedSegments := NewSegmentsInfo()

	for segID, segment := range map[int64]*SegmentInfo{
		1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 1,
				State:        commonpb.SegmentState_Flushed,
			},
		},
		2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 1,
				State:        commonpb.SegmentState_Growing,
			},
		},
		3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:           3,
				CollectionID: 2,
				State:        commonpb.SegmentState_Flushed,
			},
		},
	} {
		storedSegments.SetSegment(segID, segment)
	}
	expectedSeg := map[int64]commonpb.SegmentState{1: commonpb.SegmentState_Flushed, 2: commonpb.SegmentState_Growing}
	m := &meta{segments: storedSegments}
	got := m.GetSegmentsOfCollection(1)
	assert.Equal(t, len(expectedSeg), len(got))
	for _, gotInfo := range got {
		expected, ok := expectedSeg[gotInfo.ID]
		assert.True(t, ok)
		assert.Equal(t, expected, gotInfo.GetState())
	}

	got = m.GetSegmentsOfCollection(-1)
	assert.Equal(t, 3, len(got))

	got = m.GetSegmentsOfCollection(10)
	assert.Equal(t, 0, len(got))
}

func Test_meta_GetSegmentsWithChannel(t *testing.T) {
	storedSegments := NewSegmentsInfo()
	for segID, segment := range map[int64]*SegmentInfo{
		1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				InsertChannel: "h1",
				State:         commonpb.SegmentState_Flushed,
			},
		},
		2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				InsertChannel: "h2",
				State:         commonpb.SegmentState_Growing,
			},
		},
		3: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            3,
				CollectionID:  2,
				State:         commonpb.SegmentState_Flushed,
				InsertChannel: "h1",
			},
		},
	} {
		storedSegments.SetSegment(segID, segment)
	}
	m := &meta{segments: storedSegments}
	got := m.GetSegmentsByChannel("h1")
	assert.Equal(t, 2, len(got))
	assert.ElementsMatch(t, []int64{1, 3}, lo.Map(
		got,
		func(s *SegmentInfo, i int) int64 {
			return s.ID
		},
	))

	got = m.GetSegmentsByChannel("h3")
	assert.Equal(t, 0, len(got))

	got = m.SelectSegments(WithCollection(1), WithChannel("h1"), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment != nil && segment.GetState() == commonpb.SegmentState_Flushed
	}))
	assert.Equal(t, 1, len(got))
	assert.ElementsMatch(t, []int64{1}, lo.Map(
		got,
		func(s *SegmentInfo, i int) int64 {
			return s.ID
		},
	))

	m.segments.DropSegment(3)
	_, ok := m.segments.secondaryIndexes.coll2Segments[2]
	assert.False(t, ok)
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.coll2Segments))
	assert.Equal(t, 2, len(m.segments.secondaryIndexes.channel2Segments))

	segments, ok := m.segments.secondaryIndexes.channel2Segments["h1"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	segments, ok = m.segments.secondaryIndexes.channel2Segments["h2"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(2), segments[2].ID)

	m.segments.DropSegment(2)
	segments, ok = m.segments.secondaryIndexes.coll2Segments[1]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.coll2Segments))
	assert.Equal(t, 1, len(m.segments.secondaryIndexes.channel2Segments))

	segments, ok = m.segments.secondaryIndexes.channel2Segments["h1"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, int64(1), segments[1].ID)
	_, ok = m.segments.secondaryIndexes.channel2Segments["h2"]
	assert.False(t, ok)
}

func TestMeta_HasSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID: 1,
					},
					currRows: 100,
				},
			},
		},
	}

	has, err := m.HasSegments([]UniqueID{1})
	assert.Equal(t, true, has)
	assert.NoError(t, err)

	has, err = m.HasSegments([]UniqueID{2})
	assert.Equal(t, false, has)
	assert.Error(t, err)
}

func TestMeta_GetAllSegments(t *testing.T) {
	m := &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    1,
						State: commonpb.SegmentState_Growing,
					},
				},
				2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:    2,
						State: commonpb.SegmentState_Dropped,
					},
				},
			},
		},
	}

	seg1 := m.GetHealthySegment(1)
	seg1All := m.GetSegment(1)
	seg2 := m.GetHealthySegment(2)
	seg2All := m.GetSegment(2)
	assert.NotNil(t, seg1)
	assert.NotNil(t, seg1All)
	assert.Nil(t, seg2)
	assert.NotNil(t, seg2All)
}

func TestMeta_isSegmentHealthy_issue17823_panic(t *testing.T) {
	var seg *SegmentInfo

	assert.False(t, isSegmentHealthy(seg))
}

func equalCollectionInfo(t *testing.T, a *collectionInfo, b *collectionInfo) {
	assert.Equal(t, a.ID, b.ID)
	assert.Equal(t, a.Partitions, b.Partitions)
	assert.Equal(t, a.Schema, b.Schema)
	assert.Equal(t, a.Properties, b.Properties)
	assert.Equal(t, a.StartPositions, b.StartPositions)
}

func TestChannelCP(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	pos := &msgpb.MsgPosition{
		ChannelName: mockPChannel,
		MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Timestamp:   1000,
	}

	t.Run("UpdateChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		// nil position
		err = meta.UpdateChannelCheckpoint(mockVChannel, nil)
		assert.Error(t, err)

		err = meta.UpdateChannelCheckpoint(mockVChannel, pos)
		assert.NoError(t, err)
	})

	t.Run("UpdateChannelCheckpoints", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.channelCPs.checkpoints))

		err = meta.UpdateChannelCheckpoints(nil)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.channelCPs.checkpoints))

		err = meta.UpdateChannelCheckpoints([]*msgpb.MsgPosition{pos, {
			ChannelName: "",
		}})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.channelCPs.checkpoints))
	})

	t.Run("GetChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		position := meta.GetChannelCheckpoint(mockVChannel)
		assert.Nil(t, position)

		err = meta.UpdateChannelCheckpoint(mockVChannel, pos)
		assert.NoError(t, err)
		position = meta.GetChannelCheckpoint(mockVChannel)
		assert.NotNil(t, position)
		assert.True(t, position.ChannelName == pos.ChannelName)
		assert.True(t, position.Timestamp == pos.Timestamp)
	})

	t.Run("DropChannelCheckpoint", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		err = meta.DropChannelCheckpoint(mockVChannel)
		assert.NoError(t, err)

		err = meta.UpdateChannelCheckpoint(mockVChannel, pos)
		assert.NoError(t, err)
		err = meta.DropChannelCheckpoint(mockVChannel)
		assert.NoError(t, err)
	})
}

func Test_meta_GcConfirm(t *testing.T) {
	m := &meta{}
	catalog := mocks2.NewDataCoordCatalog(t)
	m.catalog = catalog

	catalog.On("GcConfirm",
		mock.Anything,
		mock.AnythingOfType("int64"),
		mock.AnythingOfType("int64")).
		Return(false)

	assert.False(t, m.GcConfirm(context.TODO(), 100, 10000))
}

func Test_meta_ReloadCollectionsFromRootcoords(t *testing.T) {
	t.Run("fail to list database", func(t *testing.T) {
		m := &meta{
			collections: make(map[UniqueID]*collectionInfo),
		}
		mockBroker := broker.NewMockBroker(t)
		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(nil, errors.New("list database failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to show collections", func(t *testing.T) {
		m := &meta{
			collections: make(map[UniqueID]*collectionInfo),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("show collections failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to describe collection", func(t *testing.T) {
		m := &meta{
			collections: make(map[UniqueID]*collectionInfo),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(nil, errors.New("describe collection failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("fail to show partitions", func(t *testing.T) {
		m := &meta{
			collections: make(map[UniqueID]*collectionInfo),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{}, nil)
		mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return(nil, errors.New("show partitions failed, mocked"))
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		m := &meta{
			collections: make(map[UniqueID]*collectionInfo),
		}
		mockBroker := broker.NewMockBroker(t)

		mockBroker.EXPECT().ListDatabases(mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			DbNames: []string{"db1"},
		}, nil)
		mockBroker.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"coll1"},
			CollectionIds:   []int64{1000},
		}, nil)
		mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			CollectionID: 1000,
		}, nil)
		mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return([]int64{2000}, nil)
		err := m.reloadCollectionsFromRootcoord(context.TODO(), mockBroker)
		assert.NoError(t, err)
		c := m.GetCollection(UniqueID(1000))
		assert.NotNil(t, c)
	})
}
