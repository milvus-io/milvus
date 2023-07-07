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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"

	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
)

func TestMetaReloadFromKV(t *testing.T) {
	t.Run("ListSegments fail", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.On("ListSegments",
			mock.Anything,
		).Return(nil, errors.New("error"))

		_, err := newMeta(context.TODO(), catalog, nil)
		assert.Error(t, err)
	})

	t.Run("ListChannelCheckpoint fail", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.On("ListSegments",
			mock.Anything,
		).Return([]*datapb.SegmentInfo{}, nil)
		catalog.On("ListChannelCheckpoint",
			mock.Anything,
		).Return(nil, errors.New("error"))
		_, err := newMeta(context.TODO(), catalog, nil)
		assert.Error(t, err)
	})

	t.Run("ListIndexes fail", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.On("ListSegments",
			mock.Anything,
		).Return([]*datapb.SegmentInfo{}, nil)
		catalog.On("ListChannelCheckpoint",
			mock.Anything,
		).Return(map[string]*msgpb.MsgPosition{}, nil)
		catalog.On("ListIndexes",
			mock.Anything,
		).Return(nil, errors.New("error"))
		_, err := newMeta(context.TODO(), catalog, nil)
		assert.Error(t, err)
	})

	t.Run("ListSegmentIndexes fails", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.On("ListSegments",
			mock.Anything,
		).Return([]*datapb.SegmentInfo{}, nil)
		catalog.On("ListChannelCheckpoint",
			mock.Anything,
		).Return(map[string]*msgpb.MsgPosition{}, nil)
		catalog.On("ListIndexes",
			mock.Anything,
		).Return([]*model.Index{}, nil)
		catalog.On("ListSegmentIndexes",
			mock.Anything,
		).Return(nil, errors.New("error"))
		_, err := newMeta(context.TODO(), catalog, nil)
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.On("ListSegments",
			mock.Anything,
		).Return([]*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				State:        commonpb.SegmentState_Flushed,
			},
		}, nil)

		catalog.On("ListChannelCheckpoint",
			mock.Anything,
		).Return(map[string]*msgpb.MsgPosition{
			"ch": {
				ChannelName: "cn",
				MsgID:       []byte{},
				Timestamp:   1000,
			},
		}, nil)

		catalog.On("ListIndexes",
			mock.Anything,
		).Return([]*model.Index{
			{
				CollectionID: 1,
				IndexID:      1,
				IndexName:    "dix",
				CreateTime:   1,
			},
		}, nil)

		catalog.On("ListSegmentIndexes",
			mock.Anything,
		).Return([]*model.SegmentIndex{
			{
				SegmentID: 1,
				IndexID:   1,
			},
		}, nil)

		_, err := newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)
	})
}

func TestMeta_Basic(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"
	ctx := context.Background()

	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta()
	assert.NoError(t, err)

	testSchema := newTestSchema()

	Params.Init()

	collInfo := &collectionInfo{
		ID:             collID,
		Schema:         testSchema,
		Partitions:     []UniqueID{partID0, partID1},
		StartPositions: []*commonpb.KeyDataPair{},
	}
	collInfoWoPartition := &collectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	}

	t.Run("Test Collection", func(t *testing.T) {
		meta.AddCollection(collInfo)
		// check has collection
		collInfo := meta.GetCollection(collID)
		assert.NotNil(t, collInfo)

		// check partition info
		assert.EqualValues(t, collID, collInfo.ID)
		assert.EqualValues(t, testSchema, collInfo.Schema)
		assert.EqualValues(t, 2, len(collInfo.Partitions))
		assert.EqualValues(t, partID0, collInfo.Partitions[0])
		assert.EqualValues(t, partID1, collInfo.Partitions[1])
	})

	t.Run("Test Segment", func(t *testing.T) {
		meta.AddCollection(collInfoWoPartition)
		// create seg0 for partition0, seg0/seg1 for partition1
		segID0_0, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo0_0 := buildSegment(collID, partID0, segID0_0, channelName, true)
		segID1_0, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo1_0 := buildSegment(collID, partID1, segID1_0, channelName, false)
		segID1_1, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo1_1 := buildSegment(collID, partID1, segID1_1, channelName, false)

		// check AddSegment
		err = meta.AddSegment(segInfo0_0)
		assert.NoError(t, err)
		err = meta.AddSegment(segInfo1_0)
		assert.NoError(t, err)
		err = meta.AddSegment(segInfo1_1)
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

		info0_0 = meta.GetHealthySegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.Equal(t, true, info0_0.GetIsImporting())
		err = meta.UnsetIsImporting(segID0_0)
		assert.NoError(t, err)
		info0_0 = meta.GetHealthySegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.Equal(t, false, info0_0.GetIsImporting())

		// UnsetIsImporting on segment that does not exist.
		err = meta.UnsetIsImporting(segID1_0)
		assert.Error(t, err)

		info1_1 := meta.GetHealthySegment(segID1_1)
		assert.NotNil(t, info1_1)
		assert.Equal(t, false, info1_1.GetIsImporting())
		err = meta.UnsetIsImporting(segID1_1)
		assert.NoError(t, err)
		info1_1 = meta.GetHealthySegment(segID1_1)
		assert.NotNil(t, info1_1)
		assert.Equal(t, false, info1_1.GetIsImporting())

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

		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.Error(t, err)

		metakv2 := mockkv.NewMetaKv(t)
		metakv2.EXPECT().Save(mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().MultiSave(mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().Remove(mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().MultiRemove(mock.Anything).Return(errors.New("failed")).Maybe()
		metakv2.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		metakv2.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil).Maybe()
		catalog = datacoord.NewCatalog(metakv2, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)
		// nil, since no segment yet
		err = meta.DropSegment(0)
		assert.NoError(t, err)
		// nil, since Save error not injected
		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.NoError(t, err)
		// error injected
		err = meta.DropSegment(0)
		assert.Error(t, err)

		catalog = datacoord.NewCatalog(metakv, "", "")
		meta, err = newMeta(context.TODO(), catalog, nil)
		assert.NoError(t, err)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300

		// no segment
		nums := meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo0 := buildSegment(collID, partID0, segID0, channelName, false)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(segInfo0)
		assert.NoError(t, err)

		// add seg2 with 300 rows
		segID1, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo1 := buildSegment(collID, partID0, segID1, channelName, false)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(segInfo1)
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
		segID0, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo0 := buildSegment(collID, partID0, segID0, channelName, false)
		segInfo0.size.Store(size0)
		err = meta.AddSegment(segInfo0)
		assert.NoError(t, err)

		// add seg1 with size1
		segID1, err := mockAllocator.allocID(ctx)
		assert.NoError(t, err)
		segInfo1 := buildSegment(collID, partID0, segID1, channelName, false)
		segInfo1.size.Store(size1)
		err = meta.AddSegment(segInfo1)
		assert.NoError(t, err)

		// check TotalBinlogSize
		total, collectionBinlogSize := meta.GetCollectionBinlogSize()
		assert.Len(t, collectionBinlogSize, 1)
		assert.Equal(t, int64(size0+size1), collectionBinlogSize[collID])
		assert.Equal(t, int64(size0+size1), total)
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
	err = meta.AddSegment(NewSegmentInfo(s1))
	assert.NoError(t, err)
	s2 := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	}
	err = meta.AddSegment(NewSegmentInfo(s2))
	assert.NoError(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.NoError(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}

func TestUpdateFlushSegmentsInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, getInsertLogPath("binlog0", 1))},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, getStatsLogPath("statslog0", 1))}}}
		err = meta.AddSegment(segment1)
		assert.NoError(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, true, false, true, []*datapb.FieldBinlog{getFieldBinlogPathsWithEntry(1, 10, getInsertLogPath("binlog1", 1))},
			[]*datapb.FieldBinlog{getFieldBinlogPaths(1, getStatsLogPath("statslog1", 1))},
			[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogPath: getDeltaLogPath("deltalog1", 1)}}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.NoError(t, err)

		updated := meta.GetHealthySegment(1)
		expected := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushing, NumOfRows: 10,
			StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}},
			Binlogs:       []*datapb.FieldBinlog{getFieldBinlogPaths(1, "binlog0", "binlog1")},
			Statslogs:     []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statslog0", "statslog1")},
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

	t.Run("update non-existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, false, false, nil, nil, nil, nil, nil)
		assert.NoError(t, err)
	})

	t.Run("update checkpoints and start position of non existed segment", func(t *testing.T) {
		meta, err := newMemoryMeta()
		assert.NoError(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}}
		err = meta.AddSegment(segment1)
		assert.NoError(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, false, false, nil, nil, nil, []*datapb.CheckPoint{{SegmentID: 2, NumOfRows: 10}},

			[]*datapb.SegmentStartPosition{{SegmentID: 2, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
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

		err = meta.UpdateFlushSegmentsInfo(1, true, false, false, []*datapb.FieldBinlog{getFieldBinlogPaths(1, getInsertLogPath("binlog", 1))},
			[]*datapb.FieldBinlog{getFieldBinlogPaths(1, getInsertLogPath("statslog", 1))},
			[]*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{EntriesNum: 1, TimestampFrom: 100, TimestampTo: 200, LogSize: 1000, LogPath: getDeltaLogPath("deltalog", 1)}}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &msgpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.Error(t, err)
		assert.Equal(t, "mocked fail", err.Error())
		segmentInfo = meta.GetHealthySegment(1)
		assert.EqualValues(t, 0, segmentInfo.NumOfRows)
		assert.Equal(t, commonpb.SegmentState_Growing, segmentInfo.State)
		assert.Nil(t, segmentInfo.Binlogs)
		assert.Nil(t, segmentInfo.StartPosition)
	})
}

func TestMeta_alterMetaStore(t *testing.T) {
	toAlter := []*datapb.SegmentInfo{
		{
			CollectionID: 100,
			PartitionID:  10,
			ID:           1,
			NumOfRows:    10,
		},
	}

	newSeg := &datapb.SegmentInfo{
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{},
			},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{},
			},
		},
		CollectionID: 100,
		PartitionID:  10,
		ID:           2,
		NumOfRows:    15,
	}

	m := &meta{
		catalog: &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments: &SegmentsInfo{map[int64]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:        1,
				Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
				Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
				Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
			}},
		}},
	}

	err := m.alterMetaStoreAfterCompaction(&SegmentInfo{SegmentInfo: newSeg}, lo.Map(toAlter, func(t *datapb.SegmentInfo, _ int) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: t}
	}))
	assert.NoError(t, err)
}

func TestMeta_PrepareCompleteCompactionMutation(t *testing.T) {
	prepareSegments := &SegmentsInfo{
		map[UniqueID]*SegmentInfo{
			1: {SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Binlogs:      []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
				Statslogs:    []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
				Deltalogs:    []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
				NumOfRows:    1,
			}},
			2: {SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				Binlogs:      []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
				Statslogs:    []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
				Deltalogs:    []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
				NumOfRows:    1,
			}},
		},
	}

	m := &meta{
		catalog:  &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments: prepareSegments,
	}

	inCompactionLogs := []*datapb.CompactionSegmentBinlogs{
		{
			SegmentID:           1,
			FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1", "log2")},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog1", "statlog2")},
			Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog1", "deltalog2")},
		},
		{
			SegmentID:           2,
			FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3", "log4")},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog3", "statlog4")},
			Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog3", "deltalog4")},
		},
	}

	inCompactionResult := &datapb.CompactionResult{
		SegmentID:           3,
		InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log5")},
		Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "statlog5")},
		Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(0, "deltalog5")},
		NumOfRows:           2,
	}
	beforeCompact, afterCompact, newSegment, metricMutation, err := m.PrepareCompleteCompactionMutation(inCompactionLogs, inCompactionResult)
	assert.NoError(t, err)
	assert.NotNil(t, beforeCompact)
	assert.NotNil(t, afterCompact)
	assert.NotNil(t, newSegment)
	assert.Equal(t, 3, len(metricMutation.stateChange))
	assert.Equal(t, int64(0), metricMutation.rowCountChange)
	assert.Equal(t, int64(2), metricMutation.rowCountAccChange)

	require.Equal(t, 2, len(beforeCompact))
	assert.Equal(t, commonpb.SegmentState_Flushed, beforeCompact[0].GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, beforeCompact[1].GetState())
	assert.Zero(t, beforeCompact[0].GetDroppedAt())
	assert.Zero(t, beforeCompact[1].GetDroppedAt())

	require.Equal(t, 2, len(afterCompact))
	assert.Equal(t, commonpb.SegmentState_Dropped, afterCompact[0].GetState())
	assert.Equal(t, commonpb.SegmentState_Dropped, afterCompact[1].GetState())
	assert.NotZero(t, afterCompact[0].GetDroppedAt())
	assert.NotZero(t, afterCompact[1].GetDroppedAt())

	assert.Equal(t, inCompactionResult.SegmentID, newSegment.GetID())
	assert.Equal(t, UniqueID(100), newSegment.GetCollectionID())
	assert.Equal(t, UniqueID(10), newSegment.GetPartitionID())
	assert.Equal(t, inCompactionResult.NumOfRows, newSegment.GetNumOfRows())
	assert.Equal(t, commonpb.SegmentState_Flushing, newSegment.GetState())

	assert.EqualValues(t, inCompactionResult.GetInsertLogs(), newSegment.GetBinlogs())
	assert.EqualValues(t, inCompactionResult.GetField2StatslogPaths(), newSegment.GetStatslogs())
	assert.EqualValues(t, inCompactionResult.GetDeltalogs(), newSegment.GetDeltalogs())
	assert.NotZero(t, newSegment.lastFlushTime)
}

func Test_meta_SetSegmentCompacting(t *testing.T) {
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
					map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    1,
								State: commonpb.SegmentState_Flushed,
							},
							isCompacting: false,
						},
					},
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
			m.SetSegmentCompacting(tt.args.segmentID, tt.args.compacting)
			segment := m.GetHealthySegment(tt.args.segmentID)
			assert.Equal(t, tt.args.compacting, segment.isCompacting)
		})
	}
}

func Test_meta_SetSegmentImporting(t *testing.T) {
	type fields struct {
		client   kv.MetaKv
		segments *SegmentsInfo
	}
	type args struct {
		segmentID UniqueID
		importing bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test set segment importing",
			fields{
				NewMetaMemoryKV(),
				&SegmentsInfo{
					map[int64]*SegmentInfo{
						1: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:          1,
								State:       commonpb.SegmentState_Flushed,
								IsImporting: false,
							},
						},
					},
				},
			},
			args{
				segmentID: 1,
				importing: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				catalog:  &datacoord.Catalog{MetaKv: tt.fields.client},
				segments: tt.fields.segments,
			}
			m.SetSegmentCompacting(tt.args.segmentID, tt.args.importing)
			segment := m.GetHealthySegment(tt.args.segmentID)
			assert.Equal(t, tt.args.importing, segment.isCompacting)
		})
	}
}

func Test_meta_GetSegmentsOfCollection(t *testing.T) {
	type fields struct {
		segments *SegmentsInfo
	}
	type args struct {
		collectionID UniqueID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		expect []*SegmentInfo
	}{
		{
			"test get segments",
			fields{
				&SegmentsInfo{
					map[int64]*SegmentInfo{
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
					},
				},
			},
			args{
				collectionID: 1,
			},
			[]*SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1,
						CollectionID: 1,
						State:        commonpb.SegmentState_Flushed,
					},
				},
				{
					SegmentInfo: &datapb.SegmentInfo{
						ID:           2,
						CollectionID: 1,
						State:        commonpb.SegmentState_Growing,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &meta{
				segments: tt.fields.segments,
			}
			got := m.GetSegmentsOfCollection(tt.args.collectionID)
			assert.ElementsMatch(t, tt.expect, got)
		})
	}
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
		MsgID:       []byte{},
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
	catalog := mocks.NewDataCoordCatalog(t)
	m.catalog = catalog

	catalog.On("GcConfirm",
		mock.Anything,
		mock.AnythingOfType("int64"),
		mock.AnythingOfType("int64")).
		Return(false)

	assert.False(t, m.GcConfirm(context.TODO(), 100, 10000))
}
