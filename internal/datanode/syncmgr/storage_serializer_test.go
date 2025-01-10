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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type StorageV1SerializerSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channelName  string

	schema *schemapb.CollectionSchema

	mockCache      *metacache.MockMetaCache
	mockMetaWriter *MockMetaWriter

	serializer *storageV1Serializer
}

func (s *StorageV1SerializerSuite) SetupSuite() {
	s.collectionID = rand.Int63n(100) + 1000
	s.partitionID = rand.Int63n(100) + 2000
	s.segmentID = rand.Int63n(1000) + 10000
	s.channelName = fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID)
	s.schema = &schemapb.CollectionSchema{
		Name: "serializer_v1_test_col",
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

	s.mockCache = metacache.NewMockMetaCache(s.T())
	s.mockMetaWriter = NewMockMetaWriter(s.T())
}

func (s *StorageV1SerializerSuite) SetupTest() {
	s.mockCache.EXPECT().Collection().Return(s.collectionID)
	s.mockCache.EXPECT().Schema().Return(s.schema)

	var err error
	s.serializer, err = NewStorageSerializer(s.mockCache, s.mockMetaWriter)
	s.Require().NoError(err)
}

func (s *StorageV1SerializerSuite) getEmptyInsertBuffer() *storage.InsertData {
	buf, err := storage.NewInsertData(s.schema)
	s.Require().NoError(err)

	return buf
}

func (s *StorageV1SerializerSuite) getInsertBuffer() *storage.InsertData {
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

func (s *StorageV1SerializerSuite) getDeleteBuffer() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := tsoutil.ComposeTSByTime(time.Now(), 0)
		buf.Append(pk, ts)
	}
	return buf
}

func (s *StorageV1SerializerSuite) getDeleteBufferZeroTs() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		buf.Append(pk, 0)
	}
	return buf
}

func (s *StorageV1SerializerSuite) getBasicPack() *SyncPack {
	pack := &SyncPack{}

	pack.WithCollectionID(s.collectionID).
		WithPartitionID(s.partitionID).
		WithSegmentID(s.segmentID).
		WithChannelName(s.channelName).
		WithCheckpoint(&msgpb.MsgPosition{
			Timestamp:   1000,
			ChannelName: s.channelName,
		})

	return pack
}

func (s *StorageV1SerializerSuite) getBfs() *metacache.BloomFilterSet {
	bfs := metacache.NewBloomFilterSet()
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
	return bfs
}

func (s *StorageV1SerializerSuite) TestSerializeInsert() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("without_data", func() {
		pack := s.getBasicPack()
		pack.WithTimeRange(50, 100)
		pack.WithDrop()

		task, err := s.serializer.EncodeBuffer(ctx, pack)
		s.NoError(err)
		taskV1, ok := task.(*SyncTask)
		s.Require().True(ok)
		s.Equal(s.collectionID, taskV1.collectionID)
		s.Equal(s.partitionID, taskV1.partitionID)
		s.Equal(s.channelName, taskV1.channelName)
		s.Equal(&msgpb.MsgPosition{
			Timestamp:   1000,
			ChannelName: s.channelName,
		}, taskV1.checkpoint)
		s.EqualValues(50, taskV1.tsFrom)
		s.EqualValues(100, taskV1.tsTo)
		s.True(taskV1.isDrop)
	})

	s.Run("with_empty_data", func() {
		pack := s.getBasicPack()
		pack.WithTimeRange(50, 100)
		pack.WithInsertData([]*storage.InsertData{s.getEmptyInsertBuffer()}).WithBatchSize(0)

		_, err := s.serializer.EncodeBuffer(ctx, pack)
		s.Error(err)
	})

	s.Run("with_normal_data", func() {
		pack := s.getBasicPack()
		pack.WithTimeRange(50, 100)
		pack.WithInsertData([]*storage.InsertData{s.getInsertBuffer()}).WithBatchSize(10)

		s.mockCache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return().Once()

		task, err := s.serializer.EncodeBuffer(ctx, pack)
		s.NoError(err)

		taskV1, ok := task.(*SyncTask)
		s.Require().True(ok)
		s.Equal(s.collectionID, taskV1.collectionID)
		s.Equal(s.partitionID, taskV1.partitionID)
		s.Equal(s.channelName, taskV1.channelName)
		s.Equal(&msgpb.MsgPosition{
			Timestamp:   1000,
			ChannelName: s.channelName,
		}, taskV1.checkpoint)
		s.EqualValues(50, taskV1.tsFrom)
		s.EqualValues(100, taskV1.tsTo)
		s.Len(taskV1.binlogBlobs, 4)
		s.NotNil(taskV1.batchStatsBlob)
	})

	s.Run("with_flush_segment_not_found", func() {
		pack := s.getBasicPack()
		pack.WithFlush()

		s.mockCache.EXPECT().GetSegmentByID(s.segmentID).Return(nil, false).Once()
		_, err := s.serializer.EncodeBuffer(ctx, pack)
		s.Error(err)
	})

	s.Run("with_flush", func() {
		pack := s.getBasicPack()
		pack.WithTimeRange(50, 100)
		pack.WithInsertData([]*storage.InsertData{s.getInsertBuffer()}).WithBatchSize(10)
		pack.WithFlush()

		bfs := s.getBfs()
		segInfo := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
		metacache.UpdateNumOfRows(1000)(segInfo)
		s.mockCache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
			action(segInfo)
		}).Return().Once()
		s.mockCache.EXPECT().GetSegmentByID(s.segmentID).Return(segInfo, true).Once()

		task, err := s.serializer.EncodeBuffer(ctx, pack)
		s.NoError(err)

		taskV1, ok := task.(*SyncTask)
		s.Require().True(ok)
		s.Equal(s.collectionID, taskV1.collectionID)
		s.Equal(s.partitionID, taskV1.partitionID)
		s.Equal(s.channelName, taskV1.channelName)
		s.Equal(&msgpb.MsgPosition{
			Timestamp:   1000,
			ChannelName: s.channelName,
		}, taskV1.checkpoint)
		s.EqualValues(50, taskV1.tsFrom)
		s.EqualValues(100, taskV1.tsTo)
		s.Len(taskV1.binlogBlobs, 4)
		s.NotNil(taskV1.batchStatsBlob)
		s.NotNil(taskV1.mergedStatsBlob)
	})
}

func (s *StorageV1SerializerSuite) TestSerializeDelete() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("serialize_failed", func() {
		pack := s.getBasicPack()
		pack.WithDeleteData(s.getDeleteBufferZeroTs())
		pack.WithTimeRange(50, 100)

		_, err := s.serializer.EncodeBuffer(ctx, pack)
		s.Error(err)
	})

	s.Run("serialize_normal", func() {
		pack := s.getBasicPack()
		pack.WithDeleteData(s.getDeleteBuffer())
		pack.WithTimeRange(50, 100)

		task, err := s.serializer.EncodeBuffer(ctx, pack)
		s.NoError(err)

		taskV1, ok := task.(*SyncTask)
		s.Require().True(ok)
		s.Equal(s.collectionID, taskV1.collectionID)
		s.Equal(s.partitionID, taskV1.partitionID)
		s.Equal(s.channelName, taskV1.channelName)
		s.Equal(&msgpb.MsgPosition{
			Timestamp:   1000,
			ChannelName: s.channelName,
		}, taskV1.checkpoint)
		s.EqualValues(50, taskV1.tsFrom)
		s.EqualValues(100, taskV1.tsTo)
		s.NotNil(taskV1.deltaBlob)
	})
}

func (s *StorageV1SerializerSuite) TestBadSchema() {
	mockCache := metacache.NewMockMetaCache(s.T())
	mockCache.EXPECT().Collection().Return(s.collectionID).Once()
	mockCache.EXPECT().Schema().Return(&schemapb.CollectionSchema{}).Once()
	_, err := NewStorageSerializer(mockCache, s.mockMetaWriter)
	s.Error(err)
}

func TestStorageV1Serializer(t *testing.T) {
	suite.Run(t, new(StorageV1SerializerSuite))
}
