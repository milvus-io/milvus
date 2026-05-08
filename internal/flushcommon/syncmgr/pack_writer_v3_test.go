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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	s.rootPath = s.T().TempDir()
	initcore.CleanArrowFileSystemSingleton()
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
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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
	writtenBasePath, revision, err := packed.UnmarshalManifestPath(writtenManifestPath)
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
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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
	// For V3, delta summary is returned for compaction trigger (no path, only stats)
	s.NotNil(gotDeletes)
	s.Equal(1, len(gotDeletes.GetBinlogs()))
	s.EqualValues(int64(rows), gotDeletes.GetBinlogs()[0].GetEntriesNum())
	s.NotZero(gotDeletes.GetBinlogs()[0].GetLogID())
	s.Empty(gotDeletes.GetBinlogs()[0].GetLogPath())
	// Verify manifest was updated (version should be > -1)
	_, revision, err := packed.UnmarshalManifestPath(writtenManifestPath)
	s.NoError(err)
	s.Greater(revision, int64(-1))
}

func (s *PackWriterV3Suite) TestV3InheritsV2Fields() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(789)

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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

// genInsertDataWithPKOffset generates insert data with PKs starting at pkOffset+1.
func genInsertDataWithPKOffset(size int, pkOffset int, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	for i := 0; i < size; i++ {
		data := make(map[storage.FieldID]any)
		data[common.RowIDField] = int64(pkOffset + i + 1)
		data[common.TimeStampField] = int64(pkOffset + i + 1)
		data[100] = int64(pkOffset + i + 1) // pk field

		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i+j) * 0.01
		}
		data[101] = vector

		vectorData := []float32{float32(i) * 0.1, float32(i) * 0.2}
		vectorArray := &schemapb.VectorField{
			Dim: 128,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: vectorData},
			},
		}
		data[103] = vectorArray
		buf.Append(data)
	}
	return []*storage.InsertData{buf}
}

// TestMultiBatchStatsAccumulation verifies that bloom filter and BM25 stat files
// from multiple batches accumulate in the manifest rather than being replaced.
// This is the regression test for the bug where loon_transaction_update_stat
// replaced the previous batch's stat entry, leaving only the last batch's
// bloom filter in the manifest.
func (s *PackWriterV3Suite) TestMultiBatchStatsAccumulation() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(10001) // unique ID to avoid manifest collision with other tests
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	batchRows := 5

	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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

	bfKey := fmt.Sprintf("bloom_filter.%d", 100) // pk field ID

	// Batch 1: PKs 1..5
	pack1 := new(SyncPack).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertDataWithPKOffset(batchRows, 0, s.schema)).
		WithBatchRows(int64(batchRows))

	bw1 := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)
	_, _, _, _, manifest1, _, err := bw1.Write(context.Background(), pack1)
	s.Require().NoError(err)

	stats1, err := packed.GetManifestStats(manifest1, s.storageConfig)
	s.Require().NoError(err)
	count1 := len(stats1[bfKey].Paths)
	s.Greater(count1, 0, "batch 1 should produce bloom filter files")

	// Batch 2: PKs 6..10, starting from the manifest written by batch 1
	pack2 := new(SyncPack).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertDataWithPKOffset(batchRows, batchRows, s.schema)).
		WithBatchRows(int64(batchRows))

	bw2 := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifest1)
	_, _, _, _, manifest2, _, err := bw2.Write(context.Background(), pack2)
	s.Require().NoError(err)

	stats2, err := packed.GetManifestStats(manifest2, s.storageConfig)
	s.Require().NoError(err)
	count2 := len(stats2[bfKey].Paths)
	s.Greater(count2, count1, "batch 2 should accumulate more bloom filter files than batch 1")

	// Batch 3: PKs 11..15
	pack3 := new(SyncPack).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertDataWithPKOffset(batchRows, batchRows*2, s.schema)).
		WithBatchRows(int64(batchRows))

	bw3 := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifest2)
	_, _, _, _, manifest3, _, err := bw3.Write(context.Background(), pack3)
	s.Require().NoError(err)

	stats3, err := packed.GetManifestStats(manifest3, s.storageConfig)
	s.Require().NoError(err)
	count3 := len(stats3[bfKey].Paths)
	s.Greater(count3, count2, "batch 3 should accumulate more bloom filter files than batch 2")
	s.NotEmpty(stats3[bfKey].Metadata["memory_size"], "memory_size metadata should be set")
}

// TestMultiBatchBM25StatsAccumulation verifies that BM25 stat files from
// multiple batches accumulate in the manifest.
func (s *PackWriterV3Suite) TestMultiBatchBM25StatsAccumulation() {
	collectionID := int64(123)
	partitionID := int64(456)
	segmentID := int64(10002) // unique ID to avoid manifest collision with other tests
	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)
	batchRows := 5

	bfs := pkoracle.NewBloomFilterSet()

	k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
	basePath := path.Join(common.SegmentInsertLogPath, k)
	manifestPath := packed.MarshalManifestPath(basePath, packed.ManifestEarliest)

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

	bm25FieldID := int64(200)
	makeBM25Stats := func() map[int64]*storage.BM25Stats {
		stats := storage.NewBM25Stats()
		stats.Append(map[uint32]float32{1: 1.0, 2: 2.0})
		return map[int64]*storage.BM25Stats{bm25FieldID: stats}
	}

	bm25Key := fmt.Sprintf("bm25.%d", bm25FieldID)

	// Batch 1
	pack1 := new(SyncPack).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertDataWithPKOffset(batchRows, 0, s.schema)).
		WithBatchRows(int64(batchRows)).
		WithBM25Stats(makeBM25Stats())

	bw1 := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifestPath)
	_, _, _, _, manifest1, _, err := bw1.Write(context.Background(), pack1)
	s.Require().NoError(err)

	stats1, err := packed.GetManifestStats(manifest1, s.storageConfig)
	s.Require().NoError(err)
	count1 := len(stats1[bm25Key].Paths)
	s.Greater(count1, 0, "batch 1 should produce bm25 stat files")

	// Batch 2
	pack2 := new(SyncPack).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithSegmentID(segmentID).
		WithChannelName(channelName).
		WithInsertData(genInsertDataWithPKOffset(batchRows, batchRows, s.schema)).
		WithBatchRows(int64(batchRows)).
		WithBM25Stats(makeBM25Stats())

	bw2 := NewBulkPackWriterV3(mc, s.schema, s.cm, s.logIDAlloc, packed.DefaultWriteBufferSize, 0, s.storageConfig, s.currentSplit, manifest1)
	_, _, _, _, manifest2, _, err := bw2.Write(context.Background(), pack2)
	s.Require().NoError(err)

	stats2, err := packed.GetManifestStats(manifest2, s.storageConfig)
	s.Require().NoError(err)
	count2 := len(stats2[bm25Key].Paths)
	s.Greater(count2, count1, "batch 2 should accumulate more bm25 files than batch 1")
	s.NotEmpty(stats2[bm25Key].Metadata["memory_size"], "memory_size metadata should be set")
}
