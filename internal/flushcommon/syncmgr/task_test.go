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

package syncmgr

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type SyncTaskSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channelName  string

	metacache    *metacache.MockMetaCache
	allocator    *allocator.MockGIDAllocator
	schema       *schemapb.CollectionSchema
	chunkManager *mocks.ChunkManager
	broker       *broker.MockBroker
}

func (s *SyncTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())

	s.collectionID = rand.Int63n(100) + 1000
	s.partitionID = rand.Int63n(100) + 2000
	s.segmentID = rand.Int63n(1000) + 10000
	s.channelName = fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID)
	s.schema = &schemapb.CollectionSchema{
		Name: "sync_task_test_col",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

func (s *SyncTaskSuite) SetupTest() {
	s.allocator = allocator.NewMockGIDAllocator()
	s.allocator.AllocF = func(count uint32) (int64, int64, error) {
		return time.Now().Unix(), int64(count), nil
	}
	s.allocator.AllocOneF = func() (allocator.UniqueID, error) {
		return time.Now().Unix(), nil
	}

	s.chunkManager = mocks.NewChunkManager(s.T())
	s.chunkManager.EXPECT().RootPath().Return("/tmp").Maybe()
	s.chunkManager.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	s.broker = broker.NewMockBroker(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().Collection().Return(s.collectionID).Maybe()
	s.metacache.EXPECT().Schema().Return(s.schema).Maybe()

	initcore.InitLocalArrowFileSystem("/tmp")
}

func (s *SyncTaskSuite) getEmptyInsertBuffer() *storage.InsertData {
	buf, err := storage.NewInsertData(s.schema)
	s.Require().NoError(err)

	return buf
}

func (s *SyncTaskSuite) getInsertBuffer() *storage.InsertData {
	buf := s.getEmptyInsertBuffer()

	// generate data
	for i := 0; i < 10; i++ {
		data := make(map[storage.FieldID]any)
		data[common.RowIDField] = int64(i + 1)
		data[common.TimeStampField] = int64(i + 1)
		data[100] = int64(i + 1)
		vector := lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		})
		data[101] = vector
		err := buf.Append(data)
		s.Require().NoError(err)
	}
	return buf
}

func (s *SyncTaskSuite) getDeleteBuffer() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := tsoutil.ComposeTSByTime(time.Now(), 0)
		buf.Append(pk, ts)
	}
	return buf
}

func (s *SyncTaskSuite) getSuiteSyncTask(pack *SyncPack) *SyncTask {
	task := NewSyncTask().
		WithSyncPack(pack.
			WithCollectionID(s.collectionID).
			WithPartitionID(s.partitionID).
			WithSegmentID(s.segmentID).
			WithChannelName(s.channelName)).
		WithAllocator(s.allocator).
		WithChunkManager(s.chunkManager).
		WithMetaCache(s.metacache).
		WithSchema(s.schema)
	return task
}

func (s *SyncTaskSuite) createSegment(storageVersion int64) *metacache.SegmentInfo {
	bfs := pkoracle.NewBloomFilterSet()
	fd, err := storage.NewFieldData(schemapb.DataType_Int64, &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "ID",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}, 16)
	s.Require().NoError(err)

	ids := []int64{1, 2, 3, 4, 5, 6, 7}
	for _, id := range ids {
		err = fd.AppendRow(id)
		s.Require().NoError(err)
	}

	bfs.UpdatePKRange(fd)
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{StorageVersion: storageVersion}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	seg.GetBloomFilterSet().Roll()

	return seg
}

func (s *SyncTaskSuite) TestRunNormal() {
	s.runTestRunNormal(storage.StorageV1)
}

func (s *SyncTaskSuite) TestRunNormalWithStorageV2() {
	s.runTestRunNormal(storage.StorageV2)
}

func (s *SyncTaskSuite) runTestRunNormal(storageVersion int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	seg := s.createSegment(storageVersion)

	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return()

	s.Run("without_data", func() {
		task := s.getSuiteSyncTask(new(SyncPack).WithCheckpoint(
			&msgpb.MsgPosition{
				ChannelName: s.channelName,
				MsgID:       []byte{1, 2, 3, 4},
				Timestamp:   100,
			}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1))
		if storageVersion == storage.StorageV2 {
			task.WithMultiPartUploadSize(0)
			task.WithSyncBufferSize(packed.DefaultWriteBufferSize)
		}

		err := task.Run(ctx)
		s.NoError(err)
	})

	s.Run("with_insert_delete_cp", func() {
		task := s.getSuiteSyncTask(
			new(SyncPack).
				WithInsertData([]*storage.InsertData{s.getInsertBuffer()}).
				WithCheckpoint(&msgpb.MsgPosition{
					ChannelName: s.channelName,
					MsgID:       []byte{1, 2, 3, 4},
					Timestamp:   100,
				}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1)).WithSchema(s.schema)
		if storageVersion == storage.StorageV2 {
			task.WithMultiPartUploadSize(0)
			task.WithSyncBufferSize(packed.DefaultWriteBufferSize)
		}

		err := task.Run(ctx)
		s.NoError(err)
	})

	s.Run("with_flush", func() {
		task := s.getSuiteSyncTask(
			new(SyncPack).
				WithInsertData([]*storage.InsertData{s.getInsertBuffer()}).
				WithFlush().
				WithCheckpoint(&msgpb.MsgPosition{
					ChannelName: s.channelName,
					MsgID:       []byte{1, 2, 3, 4},
					Timestamp:   100,
				}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1)).WithSchema(s.schema)
		if storageVersion == storage.StorageV2 {
			task.WithMultiPartUploadSize(0)
			task.WithSyncBufferSize(packed.DefaultWriteBufferSize)
		}
		err := task.Run(ctx)
		s.NoError(err)
	})

	s.Run("with_drop", func() {
		s.metacache.EXPECT().RemoveSegments(mock.Anything, mock.Anything).Return(nil).Once()
		task := s.getSuiteSyncTask(new(SyncPack).
			WithDeleteData(s.getDeleteBuffer()).
			WithDrop().
			WithCheckpoint(&msgpb.MsgPosition{
				ChannelName: s.channelName,
				MsgID:       []byte{1, 2, 3, 4},
				Timestamp:   100,
			}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1)).WithSchema(s.schema)
		if storageVersion == storage.StorageV2 {
			task.WithMultiPartUploadSize(0)
			task.WithSyncBufferSize(packed.DefaultWriteBufferSize)
		}
		err := task.Run(ctx)
		s.NoError(err)
	})
}

func (s *SyncTaskSuite) TestRunL0Segment() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)

	s.Run("pure_delete_l0_flush", func() {
		bfs := pkoracle.NewBloomFilterSet()
		seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{Level: datapb.SegmentLevel_L0}, bfs, nil)
		s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
		s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
		task := s.getSuiteSyncTask(new(SyncPack).
			WithDeleteData(s.getDeleteBuffer()).
			WithFlush().
			WithCheckpoint(&msgpb.MsgPosition{
				ChannelName: s.channelName,
				MsgID:       []byte{1, 2, 3, 4},
				Timestamp:   100,
			}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1)).WithSchema(s.schema)

		err := task.Run(ctx)
		s.NoError(err)
	})

	s.Run("pure_delete_l0_no_flush_storage_v2", func() {
		// should not affect l0 segment with storage v2
		bfs := pkoracle.NewBloomFilterSet()
		seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{Level: datapb.SegmentLevel_L0, StorageVersion: storage.StorageV2}, bfs, nil)
		s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
		s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
		task := s.getSuiteSyncTask(new(SyncPack).
			WithDeleteData(s.getDeleteBuffer()).
			WithFlush().
			WithCheckpoint(&msgpb.MsgPosition{
				ChannelName: s.channelName,
				MsgID:       []byte{1, 2, 3, 4},
				Timestamp:   100,
			}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1)).WithSchema(s.schema)

		err := task.Run(ctx)
		s.NoError(err)
	})
}

func (s *SyncTaskSuite) TestRunError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("segment_not_found", func() {
		s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(nil, false)
		flag := false
		handler := func(_ error) { flag = true }
		task := s.getSuiteSyncTask(new(SyncPack)).WithFailureCallback(handler)

		err := task.Run(ctx)

		s.Error(err)
		s.True(flag)
	})

	s.metacache.ExpectedCalls = nil
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, pkoracle.NewBloomFilterSet(), nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().Collection().Return(s.collectionID).Maybe()
	s.metacache.EXPECT().Schema().Return(s.schema).Maybe()

	s.Run("allocate_id_fail", func() {
		mockAllocator := allocator.NewMockAllocator(s.T())
		mockAllocator.EXPECT().Alloc(mock.Anything).Return(0, 0, errors.New("mocked"))

		task := s.getSuiteSyncTask(new(SyncPack).WithFlush()).WithAllocator(mockAllocator)

		err := task.Run(ctx)
		s.Error(err)
	})

	s.Run("metawrite_fail", func() {
		s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(errors.New("mocked"))

		task := s.getSuiteSyncTask(new(SyncPack).
			WithCheckpoint(&msgpb.MsgPosition{
				ChannelName: s.channelName,
				MsgID:       []byte{1, 2, 3, 4},
				Timestamp:   100,
			}))
		task.WithMetaWriter(BrokerMetaWriter(s.broker, 1, retry.Attempts(1)))

		err := task.Run(ctx)
		s.Error(err)
	})

	s.Run("chunk_manager_save_fail", func() {
		flag := false
		handler := func(_ error) { flag = true }
		s.chunkManager.ExpectedCalls = nil
		s.chunkManager.EXPECT().RootPath().Return("files")
		s.chunkManager.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).Return(retry.Unrecoverable(errors.New("mocked")))
		task := s.getSuiteSyncTask(new(SyncPack).WithInsertData([]*storage.InsertData{s.getInsertBuffer()})).
			WithFailureCallback(handler).
			WithWriteRetryOptions(retry.Attempts(1))

		err := task.Run(ctx)

		s.Error(err)
		s.True(flag)
	})
}

func (s *SyncTaskSuite) TestRunErrorWithStorageV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("storage v2 allocate_id_fail", func() {
		mockAllocator := allocator.NewMockAllocator(s.T())
		mockAllocator.EXPECT().Alloc(mock.Anything).Return(0, 0, errors.New("mocked"))
		segV2 := metacache.NewSegmentInfo(&datapb.SegmentInfo{Level: datapb.SegmentLevel_L0, StorageVersion: storage.StorageV2}, pkoracle.NewBloomFilterSet(), nil)
		s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(segV2, true)

		task := s.getSuiteSyncTask(new(SyncPack).WithFlush()).WithAllocator(mockAllocator)

		err := task.Run(ctx)
		s.Error(err)
	})
}

func (s *SyncTaskSuite) TestSyncTask_MarshalJSON() {
	t := &SyncTask{
		segmentID:   12345,
		batchRows:   100,
		level:       datapb.SegmentLevel_L0,
		tsFrom:      1000,
		tsTo:        2000,
		flushedSize: 1024,
		execTime:    2 * time.Second,
	}

	tm := &metricsinfo.SyncTask{
		SegmentID:    t.segmentID,
		BatchRows:    t.batchRows,
		SegmentLevel: t.level.String(),
		TSFrom:       tsoutil.PhysicalTimeFormat(t.tsFrom),
		TSTo:         tsoutil.PhysicalTimeFormat(t.tsTo),
		FlushSize:    t.flushedSize,
		RunningTime:  t.execTime.String(),
		NodeID:       paramtable.GetNodeID(),
	}
	expectedBytes, err := json.Marshal(tm)
	s.NoError(err)
	expectedJSON := string(expectedBytes)

	data, err := t.MarshalJSON()
	s.NoError(err)
	s.JSONEq(expectedJSON, string(data))
}

func TestSyncTask(t *testing.T) {
	suite.Run(t, new(SyncTaskSuite))
}
