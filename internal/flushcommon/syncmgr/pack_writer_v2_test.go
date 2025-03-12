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
	"math"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestPackWriterV2Suite(t *testing.T) {
	suite.Run(t, new(PackWriterV2Suite))
}

type PackWriterV2Suite struct {
	suite.Suite

	ctx          context.Context
	mockID       atomic.Int64
	logIDAlloc   allocator.Interface
	mockBinlogIO *mock_util.MockBinlogIO

	schema    *schemapb.CollectionSchema
	cm        storage.ChunkManager
	rootPath  string
	maxRowNum int64
	chunkSize uint64
}

func (s *PackWriterV2Suite) SetupTest() {
	s.ctx = context.Background()
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	s.rootPath = "/tmp"
	initcore.InitLocalArrowFileSystem(s.rootPath)
	paramtable.Get().Init(paramtable.NewBaseTable())

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
	s.cm = storage.NewLocalChunkManager(objectstorage.RootPath(s.rootPath))
}

func (s *PackWriterV2Suite) TestPackWriterV2_Write() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	rows := 10

	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(collectionID).Maybe()
	mc.EXPECT().Schema().Return(s.schema).Maybe()
	mc.EXPECT().GetSegmentByID(segmentID).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	deletes := &storage.DeleteData{}
	for i := 0; i < rows; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := uint64(100 + i)
		deletes.Append(pk, ts)
	}

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData(genInsertData(rows, s.schema)).WithDeleteData(deletes)

	bw := NewBulkPackWriterV2(mc, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0)

	gotInserts, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.NoError(err)
	s.Equal(gotInserts[0].Binlogs[0].GetEntriesNum(), int64(rows))
	s.Equal(gotInserts[0].Binlogs[0].GetLogPath(), "/tmp/insert_log/123/456/789/0/1")
}

func (s *PackWriterV2Suite) TestWriteEmptyInsertData() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, 1)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Schema().Return(s.schema).Maybe()

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName)
	bw := NewBulkPackWriterV2(mc, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0)

	_, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.NoError(err)
}

func (s *PackWriterV2Suite) TestNoPkField() {
	s.schema = &schemapb.CollectionSchema{
		Name: "no pk field",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		},
	}
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Schema().Return(s.schema).Maybe()

	buf, _ := storage.NewInsertData(s.schema)
	data := make(map[storage.FieldID]any)
	data[common.RowIDField] = int64(1)
	data[common.TimeStampField] = int64(1)
	buf.Append(data)

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData([]*storage.InsertData{buf})
	bw := NewBulkPackWriterV2(mc, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0)

	_, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func (s *PackWriterV2Suite) TestAllocIDExhausedError() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, 1)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	rows := 10
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Schema().Return(s.schema).Maybe()

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData(genInsertData(rows, s.schema))
	bw := NewBulkPackWriterV2(mc, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0)

	_, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func (s *PackWriterV2Suite) TestWriteInsertDataError() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Schema().Return(s.schema).Maybe()

	buf, _ := storage.NewInsertData(s.schema)
	data := make(map[storage.FieldID]any)
	data[common.RowIDField] = int64(1)
	buf.Append(data)

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData([]*storage.InsertData{buf})
	bw := NewBulkPackWriterV2(mc, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0)

	_, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func genInsertData(size int, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	for i := 0; i < size; i++ {
		data := make(map[storage.FieldID]any)
		data[common.RowIDField] = int64(i + 1)
		data[common.TimeStampField] = int64(i + 1)
		data[100] = int64(i + 1)
		vector := lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		})
		data[101] = vector
		buf.Append(data)
	}
	return []*storage.InsertData{buf}
}
