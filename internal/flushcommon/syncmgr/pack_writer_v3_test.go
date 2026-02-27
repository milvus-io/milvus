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
	"path"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestPackWriterV3Suite(t *testing.T) {
	suite.Run(t, new(PackWriterV3Suite))
}

type PackWriterV3Suite struct {
	suite.Suite

	ctx          context.Context
	mockID       atomic.Int64
	logIDAlloc   allocator.Interface
	mockBinlogIO *mock_util.MockBinlogIO

	schema        *schemapb.CollectionSchema
	cm            storage.ChunkManager
	rootPath      string
	maxRowNum     int64
	chunkSize     uint64
	currentSplit  []storagecommon.ColumnGroup
	storageConfig *indexpb.StorageConfig
}

func (s *PackWriterV3Suite) SetupTest() {
	s.ctx = context.Background()
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	s.rootPath = "/tmp"
	initcore.InitLocalArrowFileSystem(s.rootPath)
	paramtable.Get().Init(paramtable.NewBaseTable())
	paramtable.Get().Save(paramtable.Get().MinioCfg.RootPath.Key, "/tmp")
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	// force use v3
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")

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
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 102,
				Name:    "struct_array",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     103,
						Name:        "vector_array",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: "128"},
						},
					},
				},
			},
		},
	}
	allFields := typeutil.GetAllFieldSchemas(s.schema)
	s.currentSplit = storagecommon.SplitColumns(allFields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
	s.cm = storage.NewLocalChunkManager(objectstorage.RootPath(s.rootPath))
	s.storageConfig = &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    s.rootPath,
	}
}

func (s *PackWriterV3Suite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().MinioCfg.RootPath.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
}

func (s *PackWriterV3Suite) TestPackWriterV3_Write() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	rows := 10

	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(collectionID).Maybe()
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()
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

	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)

	gotInserts, _, _, _, writtenManifestPath, _, err := bw.Write(context.Background(), pack)
	s.NoError(err)
	s.Equal(gotInserts[0].Binlogs[0].GetEntriesNum(), int64(rows))
	writtenBasePath, revision, err := packed.UnmarshalManfestPath(writtenManifestPath)
	s.NoError(err)
	s.Equal(basePath, writtenBasePath)
	s.Greater(revision, int64(0))
}

func (s *PackWriterV3Suite) TestWriteEmptyInsertData() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, 1)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName)
	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)

	_, _, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.NoError(err)
}

func (s *PackWriterV3Suite) TestNoPkField() {
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

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()

	buf, _ := storage.NewInsertData(s.schema)
	data := make(map[storage.FieldID]any)
	data[common.RowIDField] = int64(1)
	data[common.TimeStampField] = int64(1)
	buf.Append(data)

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData([]*storage.InsertData{buf})
	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)

	_, _, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func (s *PackWriterV3Suite) TestWriteInsertDataError() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()

	buf, _ := storage.NewInsertData(s.schema)
	data := make(map[storage.FieldID]any)
	data[common.RowIDField] = int64(1)
	buf.Append(data)

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData([]*storage.InsertData{buf})
	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)

	_, _, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func (s *PackWriterV3Suite) TestInvalidManifestPath() {
	s.logIDAlloc = allocator.NewLocalAllocator(1, math.MaxInt64)
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	rows := 10

	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()

	// Use an invalid manifest path (not a valid JSON)
	invalidManifestPath := "invalid-manifest-path"

	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithInsertData(genInsertData(rows, s.schema))
	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, invalidManifestPath)

	_, _, _, _, _, _, err := bw.Write(context.Background(), pack)
	s.Error(err)
}

func (s *PackWriterV3Suite) TestWriteWithDeleteData() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	rows := 10

	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ManifestPath: manifestPath,
	}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(collectionID).Maybe()
	mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()
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

	// Test with only delete data (no inserts)
	pack := new(SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(segmentID).WithChannelName(channelName).WithDeleteData(deletes)

	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)

	gotInserts, gotDeletes, _, _, writtenManifestPath, _, err := bw.Write(context.Background(), pack)
	s.NoError(err)
	s.Equal(0, len(gotInserts)) // No insert binlogs when only deletes
	// For V3, deltas are nil since deltalogs are stored in manifest
	s.Nil(gotDeletes)
	// Verify manifest was updated (version should be > -1)
	_, revision, err := packed.UnmarshalManfestPath(writtenManifestPath)
	s.NoError(err)
	s.Greater(revision, int64(-1))
}

func (s *PackWriterV3Suite) TestV3InheritsV2Fields() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, -1)

	mc := metacache.NewMockMetaCache(s.T())

	// Create V3 writer and verify it has access to V2 fields
	bw := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, packed.DefaultMultiPartUploadSize, s.storageConfig, s.currentSplit, manifestPath)

	// Verify V3 can access fields from embedded V2
	s.Equal(s.schema, bw.schema)
	s.Equal(s.cm, bw.chunkManager)
	s.Equal(s.logIDAlloc, bw.allocator)
	s.EqualValues(packed.DefaultWriteBufferSize, bw.bufferSize) // 0 was passed for bufferSize (default write buffer)
	s.EqualValues(packed.DefaultMultiPartUploadSize, bw.multiPartUploadSize)
	s.Equal(s.currentSplit, bw.columnGroups)
	s.Equal(manifestPath, bw.manifestPath)
}
