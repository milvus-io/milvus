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

package segment

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

// setupV3TestEnv prepares a real local loon-backed StorageV3 environment:
// local arrow filesystem, local chunk manager, paramtable local storage with
// loon FFI forced on. Returns the storage config and chunk manager used by
// the growing writer.
func setupV3TestEnv(t *testing.T) (*indexpb.StorageConfig, storage.ChunkManager) {
	t.Helper()
	rootPath := t.TempDir()
	initcore.CleanArrowFileSystem()
	initcore.InitLocalArrowFileSystem(rootPath)

	pt := paramtable.Get()
	pt.Init(paramtable.NewBaseTable())
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.CommonCfg.UseLoonFFI.Key, "true")
	pt.Save(pt.MinioCfg.RootPath.Key, rootPath)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.CommonCfg.UseLoonFFI.Key)
		pt.Reset(pt.MinioCfg.RootPath.Key)
	})

	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	return &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}, cm
}

func crashTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "8"},
				},
			},
		},
	}
}

// buildCrashTestInsertMessage builds a single-row column-based insert message
// whose body timestamp deliberately differs from the WAL timetick, so tests
// can verify which value ends up in the persisted timestamp column.
func buildCrashTestInsertMessage(t *testing.T, vchannel string, collectionID, partitionID, segmentID int64, bodyTS, timeTick uint64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        1,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     []int64{1},
			Timestamps: []uint64{bodyTS},
			NumRows:    1,
			FieldsData: []*schemapb.FieldData{
				newTestLongFieldData(common.RowIDField, 1),
				newTestLongFieldData(common.TimeStampField, int64(bodyTS)),
				newTestLongFieldData(100, 1),
				{
					FieldId: 101,
					Type:    schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 8,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}},
							},
						},
					},
				},
			},
		}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timeTick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
}

func crashTestPack(collectionID, partitionID, segmentID int64, vchannel string, timeTick uint64, schema *schemapb.CollectionSchema, insert message.ImmutableMessage) *flushPack {
	return &flushPack{
		Meta: &streamingpb.SegmentAssignmentMeta{
			CollectionId:     collectionID,
			PartitionId:      partitionID,
			SegmentId:        segmentID,
			Vchannel:         vchannel,
			StorageVersion:   storage.StorageV3,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
		SegmentID:    segmentID,
		VChannel:     vchannel,
		FromTimeTick: timeTick,
		ToTimeTick:   timeTick,
		Schema:       schema,
		Rows:         1,
		Inserts:      []message.ImmutableMessage{insert},
	}
}

// TestBuildGrowingInsertData_PersistsWALTimetick verifies High-2: the
// timestamp column prepared for a growing chunk uses the WAL timetick, not
// the proxy TSO carried in the insert body, so every replica and the
// checkpoint agree on the same timestamp column.
func TestBuildGrowingInsertData_PersistsWALTimetick(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		vchannel     = "v1"
		timeTick     = uint64(20)
	)
	schema := crashTestSchema()
	insert := buildCrashTestInsertMessage(t, vchannel, collectionID, partitionID, segmentID, 999, timeTick)
	pack := crashTestPack(collectionID, partitionID, segmentID, vchannel, timeTick, schema, insert)

	insertData, _, err := buildGrowingInsertData(schema, pack)
	require.NoError(t, err)
	require.Len(t, insertData, 1)

	timestamps, err := storage.GetTimestampFromInsertData(insertData[0])
	require.NoError(t, err)
	require.Equal(t, []int64{int64(timeTick)}, timestamps.Data,
		"timestamp column must carry the WAL timetick, not the body TSO")
}

// TestFlushInsertBuffer_V3PersistsBM25Statslogs verifies High-1: BM25 stats
// collected during insert preparation reach the writer and are persisted as
// BM25 statlogs in the StorageV3 manifest. Regression for the bug where
// buildGrowingInsertData dropped the prepared BM25 stats, leaving the
// querynode's loadBm25Stats without paths.
func TestFlushInsertBuffer_V3PersistsBM25Statslogs(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		vchannel     = "v1"
		timeTick     = uint64(20)
	)
	storageConfig, cm := setupV3TestEnv(t)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}
	sparseData := testutils.GenerateSparseFloatVectors(1)

	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        1,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     []int64{1},
			Timestamps: []uint64{999},
			NumRows:    1,
			FieldsData: []*schemapb.FieldData{
				newTestLongFieldData(common.RowIDField, 1),
				newTestLongFieldData(common.TimeStampField, 999),
				newTestLongFieldData(100, 1),
				{
					FieldId: 101,
					Type:    schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"hello world"}},
							},
						},
					},
				},
				{
					FieldId: 102,
					Type:    schemapb.DataType_SparseFloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: sparseData.Dim,
							Data: &schemapb.VectorField_SparseFloatVector{
								SparseFloatVector: &schemapb.SparseFloatArray{
									Dim:      sparseData.Dim,
									Contents: sparseData.Contents,
								},
							},
						},
					},
				},
			},
		}).
		BuildMutable()
	require.NoError(t, err)
	insert := mutable.WithTimeTick(timeTick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))

	writer := &growingBulkPackWriter{
		chunkManager:  cm,
		allocator:     allocator.NewLocalAllocator(1, math.MaxInt64),
		storageConfig: storageConfig,
	}
	result, err := writer.FlushInsertBuffer(context.Background(),
		crashTestPack(collectionID, partitionID, segmentID, vchannel, timeTick, schema, insert))
	require.NoError(t, err)
	require.NotEmpty(t, result.PersistedStorage.GetBinlogs(), "flush must produce binlogs")

	// StorageV3 persists BM25 stats as manifest stat entries keyed
	// "bm25.<fieldID>" (see writeBM25Stasts); the querynode's loadBm25Stats
	// reads those paths. Assert the stats blob for the BM25 output field is
	// actually registered in the committed manifest.
	manifestPath := result.PersistedStorage.GetManifestPath()
	stats, err := packed.GetManifestStats(manifestPath, storageConfig)
	require.NoError(t, err)
	bm25Entry, ok := stats["bm25.102"]
	require.True(t, ok, "BM25 stats must be registered in the manifest for the BM25 output field")
	require.NotEmpty(t, bm25Entry.Paths, "BM25 stats must reference a persisted stats blob")
}

