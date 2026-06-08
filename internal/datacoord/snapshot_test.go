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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =========================== Test Helper Functions ===========================

func createTestSnapshotData() *SnapshotData {
	return &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			CreateTs:     1234567890,
			Name:         "test_snapshot",
			Description:  "test description",
			S3Location:   "s3://test-bucket/snapshot",
			PartitionIds: []int64{1, 2},
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "field1", DataType: schemapb.DataType_Int64},
					{FieldID: 2, Name: "field2", DataType: schemapb.DataType_FloatVector},
				},
			},
			NumShards:        2,
			NumPartitions:    2,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			Properties:       []*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:    1001,
				PartitionId:  1,
				SegmentLevel: 1,
				ChannelName:  "test_channel",
				NumOfRows:    100,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    100,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/binlog1",
								LogSize:       1024,
								LogID:         1,
								MemorySize:    2048,
							},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    10,
								TimestampFrom: 1500,
								TimestampTo:   2500,
								LogPath:       "/path/to/deltalog1",
								LogSize:       512,
								LogID:         2,
								MemorySize:    1024,
							},
						},
					},
				},
				Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    5,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/statslog1",
								LogSize:       256,
								LogID:         3,
								MemorySize:    512,
							},
						},
					},
				},
				Bm25Statslogs: []*datapb.FieldBinlog{
					{
						FieldID: 3,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum:    20,
								TimestampFrom: 1000,
								TimestampTo:   2000,
								LogPath:       "/path/to/bm25statslog1",
								LogSize:       128,
								LogID:         4,
								MemorySize:    256,
							},
						},
					},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {
						FieldID:    100,
						Version:    1,
						Files:      []string{"/text/index/file1", "/text/index/file2"},
						LogSize:    2048,
						MemorySize: 4096,
						BuildID:    5000,
					},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {
						FieldID:                200,
						Version:                2,
						Files:                  []string{"/json/index/file1"},
						LogSize:                1024,
						MemorySize:             2048,
						BuildID:                6000,
						JsonKeyStatsDataFormat: 1,
					},
				},
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{
						SegmentID: 1001,
						FieldID:   2,
						IndexID:   2001,
						BuildID:   3001,
						IndexName: "test_index",
						IndexParams: []*commonpb.KeyValuePair{
							{Key: "metric_type", Value: "IP"},
							{Key: "index_type", Value: "IVF_FLAT"},
						},
						IndexFilePaths: []string{"/index/path1", "/index/path2"},
						SerializedSize: 4096,
						IndexVersion:   1,
						NumRows:        100,
						MemSize:        8192,
					},
				},
				StartPosition: &msgpb.MsgPosition{
					ChannelName: "test_channel",
					MsgID:       []byte{1, 2, 3, 4},
					MsgGroup:    "test_group",
					Timestamp:   1000,
				},
				DmlPosition: &msgpb.MsgPosition{
					ChannelName: "test_channel",
					MsgID:       []byte{5, 6, 7, 8},
					MsgGroup:    "test_group",
					Timestamp:   2000,
				},
				StorageVersion: 2,
				IsSorted:       true,
			},
		},
		Indexes: []*indexpb.IndexInfo{
			{
				IndexID:   2001,
				FieldID:   2,
				IndexName: "test_index",
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "metric_type", Value: "IP"},
					{Key: "index_type", Value: "IVF_FLAT"},
				},
			},
		},
	}
}

type testBucketChunkManager struct {
	storage.ChunkManager
	bucket string
}

func (cm *testBucketChunkManager) BucketName() string {
	return cm.bucket
}

type denyExistChunkManager struct {
	storage.ChunkManager
	deniedPath string
}

func (cm *denyExistChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	if filePath == cm.deniedPath {
		return false, nil
	}
	return cm.ChunkManager.Exist(ctx, filePath)
}

// =========================== SnapshotWriter Tests ===========================

func TestSnapshotMetadata_ChannelSeekPositionsRoundTrip(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           101,
			Name:         "per_channel_snapshot",
			CollectionId: 100,
			CreateTs:     100,
			ChannelSeekPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "by-dev-rootcoord-dml_0_100v0",
					Timestamp:   100,
					MsgID:       []byte{1, 2, 3},
				},
				{
					ChannelName: "by-dev-rootcoord-dml_1_100v1",
					Timestamp:   1000,
					MsgID:       []byte{4, 5, 6},
				},
			},
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
	}

	metadataJSON, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(metadata)
	require.NoError(t, err)

	restored := &datapb.SnapshotMetadata{}
	err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(metadataJSON, restored)
	require.NoError(t, err)
	require.NotNil(t, restored.GetSnapshotInfo())

	positions := restored.GetSnapshotInfo().GetChannelSeekPositions()
	require.Len(t, positions, 2)
	assert.Equal(t, "by-dev-rootcoord-dml_0_100v0", positions[0].GetChannelName())
	assert.Equal(t, uint64(100), positions[0].GetTimestamp())
	assert.Equal(t, []byte{1, 2, 3}, positions[0].GetMsgID())
	assert.Equal(t, "by-dev-rootcoord-dml_1_100v1", positions[1].GetChannelName())
	assert.Equal(t, uint64(1000), positions[1].GetTimestamp())
	assert.Equal(t, []byte{4, 5, 6}, positions[1].GetMsgID())
}

func TestSnapshotMetadata_LegacySnapshotWithoutChannelSeekPositions(t *testing.T) {
	metadataJSON := []byte(`{
  "format_version": 3,
  "snapshot_info": {
    "name": "legacy_snapshot",
    "id": "99",
    "collection_id": "100",
    "partition_ids": ["1"],
    "create_ts": "12345"
  },
  "collection": {
    "schema": {
      "name": "legacy_collection"
    }
  },
  "manifest_list": [],
  "segment_ids": [],
  "build_ids": []
}`)

	metadata := &datapb.SnapshotMetadata{}
	err := protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(metadataJSON, metadata)
	require.NoError(t, err)
	require.NotNil(t, metadata.GetSnapshotInfo())
	assert.Equal(t, int64(12345), metadata.GetSnapshotInfo().GetCreateTs())
	assert.Empty(t, metadata.GetSnapshotInfo().GetChannelSeekPositions())
}

func TestSnapshotWriter_Save_RealAvro(t *testing.T) {
	// Use real ChunkManager and Avro operations, only mock storage layer
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Only mock storage layer Write operations, let Avro execute for real
	writeCallCount := 0
	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(func(ctx context.Context, filePath string, content []byte) error {
		writeCallCount++
		// Verify that written content is not empty mock data
		assert.NotEmpty(t, content)
		return nil
	}).Build()
	defer mockWrite.UnPatch()

	metadataPath, err := writer.Save(context.Background(), snapshotData)

	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)
	assert.Contains(t, metadataPath, "snapshots/100/metadata/")
	assert.Contains(t, metadataPath, ".json")
	assert.Equal(t, 2, writeCallCount) // manifest, metadata
}

func TestSnapshotWriter_Save_StorageError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	expectedError := errors.New("storage write failed")

	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).Return(expectedError).Build()
	defer mockWrite.UnPatch()

	_, err := writer.Save(context.Background(), snapshotData)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write")
}

func TestSnapshotWriter_Save_EmptySegments(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	snapshotData.Segments = nil // Empty segments

	writeCallCount := 0
	mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).To(func(ctx context.Context, filePath string, content []byte) error {
		writeCallCount++
		return nil
	}).Build()
	defer mockWrite.UnPatch()

	metadataPath, err := writer.Save(context.Background(), snapshotData)

	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)
	// When there are no segments, only metadata file is written (no manifest files)
	assert.Equal(t, 1, writeCallCount) // Only metadata file is written
}

func TestSnapshotWriter_Drop_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)

	// Mock metadata file content
	metadata := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:       1,
			CreateTs: 1234567890,
			Name:     "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{"manifest1.avro"},
	}
	opts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := opts.Marshal(metadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock file operations - no longer need ListAllChunkWithPrefix
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	mockMultiRemove := mockey.Mock((*storage.LocalChunkManager).MultiRemove).Return(nil).Build()
	defer mockMultiRemove.UnPatch()

	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).Return(nil).Build()
	defer mockRemove.UnPatch()

	err := writer.Drop(context.Background(), metadataFilePath)

	assert.NoError(t, err)
}

func TestSnapshotWriter_Drop_MetadataReadFailed(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock Read to return error (file not found)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(
		nil,
		fmt.Errorf("file not found"),
	).Build()
	defer mockRead.UnPatch()

	err := writer.Drop(context.Background(), metadataFilePath)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read metadata file")
}

func TestSnapshotWriter_Drop_EmptyPath(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)

	err := writer.Drop(context.Background(), "")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata file path cannot be empty")
}

// =========================== SnapshotReader Tests ===========================

func TestSnapshotReader_ReadSnapshot_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:       1,
			CreateTs: 1234567890,
			Name:     "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{"manifest1.avro"},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := marshalOpts.Marshal(metadata)

	// Generate valid manifest entry with all required fields (single record per file)
	manifestEntry := snapshotio.ManifestEntry{
		SegmentID:         1001,
		PartitionID:       1,
		SegmentLevel:      1,
		ChannelName:       "test_channel",
		NumOfRows:         100,
		BinlogFiles:       []snapshotio.AvroFieldBinlog{},
		DeltalogFiles:     []snapshotio.AvroFieldBinlog{},
		StatslogFiles:     []snapshotio.AvroFieldBinlog{},
		Bm25StatslogFiles: []snapshotio.AvroFieldBinlog{},
		TextIndexFiles:    []snapshotio.AvroTextIndexEntry{},
		JSONKeyIndexFiles: []snapshotio.AvroJSONKeyIndexEntry{},
		IndexFiles:        []snapshotio.AvroIndexFilePathInfo{},
		StartPosition:     &snapshotio.AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
		DmlPosition:       &snapshotio.AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
		StorageVersion:    0,
		IsSorted:          false,
	}

	// Pre-generate valid Avro data for manifest using the real schema (single record)
	manifestSchema, _ := snapshotio.ManifestSchema()
	validManifestData, _ := avro.Marshal(manifestSchema, manifestEntry)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	// Mock file operations - no longer need ListAllChunkWithPrefix since we pass the path directly
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		if filePath == "manifest1.avro" {
			return validManifestData, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	snapshot, err := reader.ReadSnapshot(context.Background(), metadataFilePath, true)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, int64(1), snapshot.SnapshotInfo.GetId())
	assert.Equal(t, "test_snapshot", snapshot.SnapshotInfo.GetName())
	assert.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(1001), snapshot.Segments[0].SegmentId)
	assert.Equal(t, "test_channel", snapshot.Segments[0].ChannelName)
	assert.Equal(t, int64(100), snapshot.Segments[0].NumOfRows)
}

func TestSnapshotReader_ReadSnapshot_EmptyPath(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	// Test with empty path - should return error immediately
	_, err := reader.ReadSnapshot(context.Background(), "", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata file path cannot be empty")
}

func TestSnapshotReader_ReadSnapshot_FileNotFound(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	metadataFilePath := "snapshots/100/metadata/nonexistent.json"

	// Mock Read to return error (file not found)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).Return(
		nil,
		fmt.Errorf("file not found"),
	).Build()
	defer mockRead.UnPatch()

	_, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read metadata file")
}

func TestSnapshotReader_ListSnapshots_Success(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	metadata1 := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:          1,
			CreateTs:    1234567890,
			Name:        "snapshot1",
			Description: "first snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadata1JSON, _ := marshalOpts.Marshal(metadata1)

	metadata2 := &datapb.SnapshotMetadata{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:          2,
			CreateTs:    1234567900,
			Name:        "snapshot2",
			Description: "second snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	metadata2JSON, _ := marshalOpts.Marshal(metadata2)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{
			"snapshots/100/metadata/00001-uuid1.json",
			"snapshots/100/metadata/00002-uuid2.json",
			"snapshots/100/metadata/version-hint.txt",
		},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/00001-uuid1.json" {
			return metadata1JSON, nil
		}
		if filePath == "snapshots/100/metadata/00002-uuid2.json" {
			return metadata2JSON, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	snapshots, err := reader.ListSnapshots(context.Background(), 100)

	assert.NoError(t, err)
	assert.Len(t, snapshots, 2)
	assert.Equal(t, "snapshot1", snapshots[0].GetName())
	assert.Equal(t, "snapshot2", snapshots[1].GetName())
}

func TestSnapshotReader_ListSnapshots_ListFailurePreservesIoError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		nil,
		nil,
		fmt.Errorf("list failed"),
	).Build()
	defer mockList.UnPatch()

	_, err := reader.ListSnapshots(context.Background(), 100)

	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}

// =========================== Data Conversion Tests ===========================

func TestFieldBinlog_RoundtripConversion(t *testing.T) {
	originalFieldBinlog := &datapb.FieldBinlog{
		FieldID:     123,
		ChildFields: []int64{123, 124, 125},
		Format:      "parquet",
		Binlogs: []*datapb.Binlog{
			{
				EntriesNum:    1000,
				TimestampFrom: 1500,
				TimestampTo:   2500,
				LogPath:       "/original/path",
				LogSize:       2048,
				LogID:         10,
				MemorySize:    4096,
			},
			{
				EntriesNum:    500,
				TimestampFrom: 2000,
				TimestampTo:   3000,
				LogPath:       "/another/path",
				LogSize:       1024,
				LogID:         11,
				MemorySize:    2048,
			},
		},
	}

	avroFieldBinlog := snapshotio.FieldBinlogToAvro(originalFieldBinlog)
	resultFieldBinlog := snapshotio.AvroToFieldBinlog(avroFieldBinlog)

	assert.Equal(t, originalFieldBinlog.FieldID, resultFieldBinlog.FieldID)
	assert.Equal(t, originalFieldBinlog.ChildFields, resultFieldBinlog.ChildFields)
	assert.Equal(t, originalFieldBinlog.Format, resultFieldBinlog.Format)
	assert.Len(t, resultFieldBinlog.Binlogs, len(originalFieldBinlog.Binlogs))

	for i, originalBinlog := range originalFieldBinlog.Binlogs {
		resultBinlog := resultFieldBinlog.Binlogs[i]
		assert.Equal(t, originalBinlog.EntriesNum, resultBinlog.EntriesNum)
		assert.Equal(t, originalBinlog.TimestampFrom, resultBinlog.TimestampFrom)
		assert.Equal(t, originalBinlog.TimestampTo, resultBinlog.TimestampTo)
		assert.Equal(t, originalBinlog.LogPath, resultBinlog.LogPath)
		assert.Equal(t, originalBinlog.LogSize, resultBinlog.LogSize)
		assert.Equal(t, originalBinlog.LogID, resultBinlog.LogID)
		assert.Equal(t, originalBinlog.MemorySize, resultBinlog.MemorySize)
	}
}

func TestIndexFilePathInfo_RoundtripConversion(t *testing.T) {
	originalIndexInfo := &indexpb.IndexFilePathInfo{
		SegmentID: 9999,
		FieldID:   88,
		IndexID:   7777,
		BuildID:   6666,
		IndexName: "comprehensive_index",
		IndexParams: []*commonpb.KeyValuePair{
			{Key: "metric_type", Value: "IP"},
			{Key: "index_type", Value: "IVF_FLAT"},
			{Key: "nlist", Value: "1024"},
		},
		IndexFilePaths:        []string{"/idx/path1", "/idx/path2", "/idx/path3"},
		SerializedSize:        16384,
		IndexVersion:          5,
		NumRows:               50000,
		CurrentIndexVersion:   5,
		MemSize:               32768,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}

	avroIndexInfo := snapshotio.IndexFilePathInfoToAvro(originalIndexInfo)
	resultIndexInfo := snapshotio.AvroToIndexFilePathInfo(avroIndexInfo)

	assert.Equal(t, originalIndexInfo.SegmentID, resultIndexInfo.SegmentID)
	assert.Equal(t, originalIndexInfo.FieldID, resultIndexInfo.FieldID)
	assert.Equal(t, originalIndexInfo.IndexID, resultIndexInfo.IndexID)
	assert.Equal(t, originalIndexInfo.BuildID, resultIndexInfo.BuildID)
	assert.Equal(t, originalIndexInfo.IndexName, resultIndexInfo.IndexName)
	assert.Equal(t, originalIndexInfo.SerializedSize, resultIndexInfo.SerializedSize)
	assert.Equal(t, originalIndexInfo.IndexVersion, resultIndexInfo.IndexVersion)
	assert.Equal(t, originalIndexInfo.NumRows, resultIndexInfo.NumRows)
	assert.Equal(t, originalIndexInfo.CurrentIndexVersion, resultIndexInfo.CurrentIndexVersion)
	assert.Equal(t, originalIndexInfo.MemSize, resultIndexInfo.MemSize)
	assert.Equal(t, originalIndexInfo.IndexStorePathVersion, resultIndexInfo.IndexStorePathVersion)

	// Verify IndexParams
	assert.Len(t, resultIndexInfo.IndexParams, len(originalIndexInfo.IndexParams))
	for i, originalParam := range originalIndexInfo.IndexParams {
		resultParam := resultIndexInfo.IndexParams[i]
		assert.Equal(t, originalParam.Key, resultParam.Key)
		assert.Equal(t, originalParam.Value, resultParam.Value)
	}

	// Verify IndexFilePaths
	assert.Equal(t, originalIndexInfo.IndexFilePaths, resultIndexInfo.IndexFilePaths)
}

func TestSnapshotReader_ReadManifestLegacyIndexFilePathInfoDefaultsBuildRooted(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				SegmentID:      1001,
				FieldID:        101,
				IndexID:        201,
				BuildID:        301,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"files/index_files/301/1/2001/1001/index_data"},
			},
		},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV1(), "index_store_path_version")
	oldSchema, err := snapshotio.ManifestSchemaByVersion(1)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(oldSchema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "legacy_manifest.avro")
	err = cm.Write(context.Background(), manifestPath, binaryData)
	require.NoError(t, err)

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 1)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetIndexFiles(), 1)
	assert.Equal(t,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		segments[0].GetIndexFiles()[0].GetIndexStorePathVersion())
}

// TestSnapshotManifest_CommitTimestampRoundtripV3 verifies that CommitTimestamp
// survives a Marshal/Unmarshal cycle with the current (V3) schema. This is the
// invariant the snapshot.go field comment promises ("preserved so that GC, TTL,
// and MVCC protections survive snapshot/restore").
func TestSnapshotManifest_CommitTimestampRoundtripV3(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	const wantCommitTs = uint64(1234567890)
	segment := &datapb.SegmentDescription{
		SegmentId:       1001,
		PartitionId:     2001,
		ChannelName:     "ch-0",
		CommitTimestamp: wantCommitTs,
	}
	entry := snapshotio.SegmentToManifestEntry(segment)
	require.Equal(t, int64(wantCommitTs), entry.CommitTimestamp)

	assert.Contains(t, snapshotio.AvroSchemaV3(), "commit_timestamp")
	schema, err := snapshotio.ManifestSchemaByVersion(3)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v3_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 3)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	assert.Equal(t, wantCommitTs, segments[0].GetCommitTimestamp())
}

// TestSnapshotManifest_LegacyV2NoCommitTimestamp verifies that a manifest
// written with the V2 schema (no commit_timestamp field) still decodes cleanly
// under the V2 reader and surfaces CommitTimestamp=0. This guarantees that
// pre-existing on-disk snapshots remain readable after the V3 bump.
func TestSnapshotManifest_LegacyV2NoCommitTimestamp(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	segment := &datapb.SegmentDescription{
		SegmentId:       1001,
		PartitionId:     2001,
		ChannelName:     "ch-0",
		CommitTimestamp: 999, // set on the struct; V2 schema must drop it
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV2(), "commit_timestamp")
	v2Schema, err := snapshotio.ManifestSchemaByVersion(2)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v2Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v2_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 2)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	assert.Equal(t, uint64(0), segments[0].GetCommitTimestamp(),
		"V2 manifest must decode with CommitTimestamp=0 (field absent in schema)")
}

func TestSnapshotManifest_FieldBinlogChildFieldsAndFormatRoundtripV4(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID:     900,
				ChildFields: []int64{100, 101},
				Format:      "parquet",
				Binlogs: []*datapb.Binlog{{
					EntriesNum: 10,
					LogPath:    "files/insert_log/1/2/3/900/1",
				}},
			},
		},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.Contains(t, snapshotio.AvroSchemaV4(), "child_fields")
	assert.Contains(t, snapshotio.AvroSchemaV4(), `"format"`)
	v4Schema, err := snapshotio.ManifestSchemaByVersion(4)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v4Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v4_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 4)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Equal(t, []int64{100, 101}, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "parquet", segments[0].GetBinlogs()[0].GetFormat())
}

func TestSnapshotManifest_LegacyV3NoChildFieldsOrFormat(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     900,
			ChildFields: []int64{100, 101},
			Format:      "parquet",
			Binlogs: []*datapb.Binlog{{
				EntriesNum: 10,
				LogPath:    "files/insert_log/1/2/3/900/1",
			}},
		}},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV3(), "child_fields")
	assert.NotContains(t, snapshotio.AvroSchemaV3(), `"format"`)
	v3Schema, err := snapshotio.ManifestSchemaByVersion(3)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v3Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v3_manifest.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 3)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Empty(t, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "", segments[0].GetBinlogs()[0].GetFormat())
}

func TestSnapshotManifest_LegacyV2NoChildFieldsOrFormat(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	segment := &datapb.SegmentDescription{
		SegmentId:   1001,
		PartitionId: 2001,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     900,
			ChildFields: []int64{100, 101},
			Format:      "parquet",
			Binlogs: []*datapb.Binlog{{
				EntriesNum: 10,
				LogPath:    "files/insert_log/1/2/3/900/1",
			}},
		}},
	}
	entry := snapshotio.SegmentToManifestEntry(segment)

	assert.NotContains(t, snapshotio.AvroSchemaV2(), "child_fields")
	assert.NotContains(t, snapshotio.AvroSchemaV2(), `"format"`)
	v2Schema, err := snapshotio.ManifestSchemaByVersion(2)
	require.NoError(t, err)
	binaryData, err := avro.Marshal(v2Schema, entry)
	require.NoError(t, err)

	manifestPath := path.Join(tempDir, "v2_no_child_fields.avro")
	require.NoError(t, cm.Write(context.Background(), manifestPath, binaryData))

	segments, err := reader.readManifestFile(context.Background(), manifestPath, 2)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Len(t, segments[0].GetBinlogs(), 1)
	assert.Empty(t, segments[0].GetBinlogs()[0].GetChildFields())
	assert.Equal(t, "", segments[0].GetBinlogs()[0].GetFormat())
}

// =========================== Integration Tests ===========================

func TestSnapshotWriter_ManifestList_Roundtrip(t *testing.T) {
	// This test is no longer relevant since we removed the manifest list layer
	// Manifest paths are now stored directly in metadata.json
	t.Skip("Manifest list layer has been removed - manifest paths are now stored directly in metadata.json")
}

func TestSnapshot_CompleteWorkflow(t *testing.T) {
	// Test complete snapshot workflow
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	reader := NewSnapshotReader(cm)

	// 1. Create test data
	snapshotData := createTestSnapshotData()

	// 2. Save snapshot
	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// 3. Read snapshot using the metadata path returned from Save
	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readSnapshot)

	// 4. Verify data consistency
	assert.Equal(t, snapshotData.SnapshotInfo.GetId(), readSnapshot.SnapshotInfo.GetId())
	assert.Equal(t, snapshotData.SnapshotInfo.GetName(), readSnapshot.SnapshotInfo.GetName())
	assert.Equal(t, snapshotData.SnapshotInfo.GetDescription(), readSnapshot.SnapshotInfo.GetDescription())

	// 5. Verify collection information
	assert.Equal(t, snapshotData.Collection.Schema.Name, readSnapshot.Collection.Schema.Name)
	assert.Len(t, readSnapshot.Collection.Schema.Fields, len(snapshotData.Collection.Schema.Fields))

	// 6. Verify segments information
	assert.Len(t, readSnapshot.Segments, len(snapshotData.Segments))
	if len(readSnapshot.Segments) > 0 {
		assert.Equal(t, snapshotData.Segments[0].SegmentId, readSnapshot.Segments[0].SegmentId)
		assert.Equal(t, snapshotData.Segments[0].PartitionId, readSnapshot.Segments[0].PartitionId)
		if len(snapshotData.Segments[0].GetBinlogs()) > 0 {
			require.Len(t, readSnapshot.Segments[0].GetBinlogs(), len(snapshotData.Segments[0].GetBinlogs()))
			assert.Equal(t, snapshotData.Segments[0].GetBinlogs()[0].GetChildFields(), readSnapshot.Segments[0].GetBinlogs()[0].GetChildFields())
			assert.Equal(t, snapshotData.Segments[0].GetBinlogs()[0].GetFormat(), readSnapshot.Segments[0].GetBinlogs()[0].GetFormat())
		}
	}

	// 7. Verify indexes information
	assert.Len(t, readSnapshot.Indexes, len(snapshotData.Indexes))
	if len(readSnapshot.Indexes) > 0 {
		assert.Equal(t, snapshotData.Indexes[0].IndexID, readSnapshot.Indexes[0].IndexID)
		assert.Equal(t, snapshotData.Indexes[0].IndexName, readSnapshot.Indexes[0].IndexName)
	}

	// 8. Cleanup - use the metadata path returned from Save
	err = writer.Drop(context.Background(), metadataPath)
	assert.NoError(t, err)
}

func TestSnapshotExporter_ExportCopiesFilesAndWritesSelfContainedMetadata(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	sourceBinlog := path.Join(tempDir, "files/insert_log/1/2/1001/field1/1")
	sourceIndex := path.Join(tempDir, "files/index_files/1001/2001/3001/index")
	assert.NoError(t, cm.Write(ctx, sourceBinlog, []byte("binlog")))
	assert.NoError(t, cm.Write(ctx, sourceIndex, []byte("index")))

	snapshotData := createTestSnapshotData()
	snapshotData.SnapshotInfo.S3Location = "s3://source/snapshots/100/metadata/1.json"
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceBinlog,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{
		SegmentID:           segment.GetSegmentId(),
		FieldID:             2,
		IndexID:             2001,
		BuildID:             3001,
		IndexFilePaths:      []string{sourceIndex},
		SerializedSize:      uint64(len("index")),
		IndexVersion:        1,
		NumRows:             segment.GetNumOfRows(),
		IndexName:           "test_index",
		CurrentIndexVersion: 1,
	}}
	snapshotData.SegmentIDs = []int64{segment.GetSegmentId()}
	snapshotData.BuildIDs = []int64{3001}

	targetRoot := path.Join(tempDir, "exported")
	copier := &recordingCrossBucketCopier{sourceCM: cm, targetCM: cm}
	metadataURI, err := newSnapshotExporter(cm, cm, copier, "", "").Export(ctx, snapshotData, targetRoot)
	assert.NoError(t, err)
	assert.Equal(t, path.Join(targetRoot, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json"), metadataURI)

	copiedBinlog := path.Join(targetRoot, exportedSnapshotFilesPath, "files/insert_log/1/2/1001/field1/1")
	copiedIndex := path.Join(targetRoot, exportedSnapshotFilesPath, "files/index_files/1001/2001/3001/index")
	binlogData, err := cm.Read(ctx, copiedBinlog)
	assert.NoError(t, err)
	assert.Equal(t, []byte("binlog"), binlogData)
	indexData, err := cm.Read(ctx, copiedIndex)
	assert.NoError(t, err)
	assert.Equal(t, []byte("index"), indexData)

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataURI, true)
	assert.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, readSnapshot.Layout)
	assert.Equal(t, metadataURI, readSnapshot.SnapshotInfo.GetS3Location())
	assert.Equal(t, copiedBinlog, readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, copiedIndex, readSnapshot.Segments[0].GetIndexFiles()[0].GetIndexFilePaths()[0])
}

type recordingCrossBucketCopier struct {
	sourceCM storage.ChunkManager
	targetCM storage.ChunkManager
	calls    []copyCall
}

type copyCall struct {
	srcBucket string
	src       string
	dstBucket string
	dst       string
}

func (c *recordingCrossBucketCopier) CopyCrossBucket(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	c.calls = append(c.calls, copyCall{
		srcBucket: srcBucket,
		src:       srcObject,
		dstBucket: dstBucket,
		dst:       dstObject,
	})
	data, err := c.sourceCM.Read(ctx, srcObject)
	if err != nil {
		return err
	}
	return c.targetCM.Write(ctx, dstObject, data)
}

func TestSnapshotExporter_ExportCrossBucketUsesTargetManagerAndCopier(t *testing.T) {
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(targetRoot))
	copier := &recordingCrossBucketCopier{
		sourceCM: sourceCM,
		targetCM: targetCM,
	}

	ctx := context.Background()
	sourceBinlog := path.Join(sourceRoot, "files/insert_log/100/1/10/1")
	require.NoError(t, sourceCM.Write(ctx, sourceBinlog, []byte("data")))

	snapshotData := createTestSnapshotData()
	snapshotData.SnapshotInfo.S3Location = "s3://local-bucket/snapshots/100/metadata/1.json"
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceBinlog,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	snapshotData.SegmentIDs = []int64{segment.GetSegmentId()}
	snapshotData.BuildIDs = nil

	var origin func(context.Context, storage.ChunkManager, *SnapshotData, map[string]string, SnapshotRewriteOptions) (string, error)
	mockWriteSnapshot := mockey.Mock(WriteSnapshotWithMapping).To(
		func(ctx context.Context, gotCM storage.ChunkManager, gotSnapshot *SnapshotData, mappings map[string]string, opts SnapshotRewriteOptions) (string, error) {
			assert.Same(t, targetCM, gotCM)
			assert.Same(t, snapshotData, gotSnapshot)
			return origin(ctx, gotCM, gotSnapshot, mappings, opts)
		}).Origin(&origin).Build()
	defer mockWriteSnapshot.UnPatch()

	targetURI := "s3://foreign-bucket/" + path.Join(targetRoot, "export-root")
	expectedMetadataURI := joinSnapshotURI(targetURI, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json")
	expectedCopiedBinlog := path.Join(targetRoot, "export-root", exportedSnapshotFilesPath, "files/insert_log/100/1/10/1")

	exporter := newSnapshotExporter(sourceCM, targetCM, copier, "local-bucket", "foreign-bucket")
	metadataURI, err := exporter.Export(ctx, snapshotData, targetURI)
	require.NoError(t, err)
	assert.Equal(t, expectedMetadataURI, metadataURI)
	assert.Equal(t, []copyCall{{
		srcBucket: "local-bucket",
		src:       sourceBinlog,
		dstBucket: "foreign-bucket",
		dst:       expectedCopiedBinlog,
	}}, copier.calls)

	readSnapshot, err := NewSnapshotReader(targetCM).ReadSnapshot(ctx, metadataURI, true)
	require.NoError(t, err)
	assert.Equal(t, metadataURI, readSnapshot.SnapshotInfo.GetS3Location())
	assert.Equal(t, copier.calls[0].dst, readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	copiedData, err := targetCM.Read(ctx, copier.calls[0].dst)
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), copiedData)
}

func TestSnapshotExporter_ExportUsesSnapshotPrimitivesStrictly(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	sourceInsert := path.Join(tempDir, "files/insert_log/100/10/1001/1")
	sourceStats := path.Join(tempDir, "files/stats_log/100/10/1001/2")
	require.NoError(t, cm.Write(ctx, sourceInsert, []byte("insert")))
	require.NoError(t, cm.Write(ctx, sourceStats, []byte("stats")))

	snapshotData := createTestSnapshotData()
	segment := snapshotData.Segments[0]
	segment.StorageVersion = 0
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceInsert,
		}},
	}}
	segment.Statslogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogID:   2,
			LogPath: sourceStats,
		}},
	}}
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	refs, err := ListSnapshotDataFiles(ctx, cm, snapshotData)
	require.NoError(t, err)
	require.Len(t, refs, 2)

	targetPath := path.Join(tempDir, "exported")
	targetRoot := normalizeSnapshotObjectPath(cm, targetPath)
	metadataURI := joinSnapshotURI(targetPath,
		SnapshotRootPath,
		fmt.Sprintf("%d", snapshotData.SnapshotInfo.GetCollectionId()),
		SnapshotMetadataSubPath,
		fmt.Sprintf("%d.json", snapshotData.SnapshotInfo.GetId()))

	var origin func(context.Context, storage.ChunkManager, *SnapshotData, map[string]string, SnapshotRewriteOptions) (string, error)
	called := false
	mockWriteSnapshot := mockey.Mock(WriteSnapshotWithMapping).To(
		func(ctx context.Context, gotCM storage.ChunkManager, gotSnapshot *SnapshotData, mappings map[string]string, opts SnapshotRewriteOptions) (string, error) {
			called = true
			assert.Same(t, cm, gotCM)
			assert.Same(t, snapshotData, gotSnapshot)
			assert.Equal(t, targetRoot, opts.TargetRoot)
			assert.Equal(t, metadataURI, opts.MetadataURI)
			assert.True(t, opts.StrictMapping)
			for _, ref := range refs {
				expectedPath := exportedSnapshotPath(cm, ref.NormalizedPath, targetRoot)
				assert.Equal(t, expectedPath, mappings[ref.Path])
				assert.Equal(t, expectedPath, mappings[ref.NormalizedPath])
			}
			return origin(ctx, gotCM, gotSnapshot, mappings, opts)
		}).Origin(&origin).Build()
	defer mockWriteSnapshot.UnPatch()

	copier := &recordingCrossBucketCopier{sourceCM: cm, targetCM: cm}
	gotMetadataURI, err := newSnapshotExporter(cm, cm, copier, "", "").Export(ctx, snapshotData, targetPath)
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, metadataURI, gotMetadataURI)

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, gotMetadataURI, true)
	require.NoError(t, err)
	require.Len(t, readSnapshot.Segments, 1)

	rewrittenInsert := readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath()
	rewrittenStats := readSnapshot.Segments[0].GetStatslogs()[0].GetBinlogs()[0].GetLogPath()
	assert.Equal(t, exportedSnapshotPath(cm, sourceInsert, targetRoot), rewrittenInsert)
	assert.Equal(t, exportedSnapshotPath(cm, sourceStats, targetRoot), rewrittenStats)

	insertData, err := cm.Read(ctx, rewrittenInsert)
	require.NoError(t, err)
	assert.Equal(t, []byte("insert"), insertData)
	statsData, err := cm.Read(ctx, rewrittenStats)
	require.NoError(t, err)
	assert.Equal(t, []byte("stats"), statsData)
}

func TestSnapshotExporter_ExportReturnsManifestLobError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	basePath := path.Join(tempDir, "files/insert_log/1/2/1001")
	assert.NoError(t, cm.Write(ctx, path.Join(basePath, "manifest"), []byte("manifest")))

	snapshotData := createTestSnapshotData()
	segment := snapshotData.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return(nil, errors.New("lob unavailable")).Build()
	defer mockGetLobFiles.UnPatch()

	copier := &recordingCrossBucketCopier{sourceCM: cm, targetCM: cm}
	_, err := newSnapshotExporter(cm, cm, copier, "", "").Export(ctx, snapshotData, path.Join(tempDir, "exported"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list LOB files for segment 1001")
	assert.Contains(t, err.Error(), "lob unavailable")
}

func TestListSnapshotDataFiles_CollectsReferencedFiles(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = 0
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/100/10/1001/1",
		}},
	}}
	segment.Statslogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/stats_log/100/10/1001/2",
		}},
	}}
	segment.Deltalogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/delta_log/100/10/1001/3",
		}},
	}}
	segment.Bm25Statslogs = []*datapb.FieldBinlog{{
		FieldID: 11,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/bm25_stats_log/100/11/1001/4",
		}},
	}}
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{
		BuildID: 7001,
		IndexFilePaths: []string{
			"files/index_files/100/10/1001/7001/index",
		},
	}}
	segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{
		12: {
			FieldID: 12,
			BuildID: 7002,
			Files: []string{
				"files/text_index/100/12/1001/7002/posting",
			},
		},
	}
	segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{
		13: {
			FieldID: 13,
			BuildID: 7003,
			Files: []string{
				"files/json_index/100/13/1001/7003/key",
			},
		},
	}

	refs, err := ListSnapshotDataFiles(context.Background(), cm, snapshot)
	require.NoError(t, err)

	byPath := make(map[string]SnapshotFileRef)
	for _, ref := range refs {
		byPath[ref.NormalizedPath] = ref
		assert.Equal(t, SnapshotFileRefKindObject, ref.Kind)
		assert.Equal(t, int64(1001), ref.SegmentID)
	}

	assert.Equal(t, SnapshotFileTypeInsertBinlog, byPath["files/insert_log/100/10/1001/1"].Type)
	assert.Equal(t, SnapshotFileTypeStatsBinlog, byPath["files/stats_log/100/10/1001/2"].Type)
	assert.Equal(t, SnapshotFileTypeDeltaBinlog, byPath["files/delta_log/100/10/1001/3"].Type)
	assert.Equal(t, SnapshotFileTypeBM25StatsBinlog, byPath["files/bm25_stats_log/100/11/1001/4"].Type)
	assert.Equal(t, SnapshotFileTypeIndexFile, byPath["files/index_files/100/10/1001/7001/index"].Type)
	assert.Equal(t, int64(7001), byPath["files/index_files/100/10/1001/7001/index"].BuildID)
	assert.Equal(t, SnapshotFileTypeTextIndexFile, byPath["files/text_index/100/12/1001/7002/posting"].Type)
	assert.Equal(t, int64(7002), byPath["files/text_index/100/12/1001/7002/posting"].BuildID)
	assert.Equal(t, SnapshotFileTypeJSONKeyIndexFile, byPath["files/json_index/100/13/1001/7003/key"].Type)
	assert.Equal(t, int64(7003), byPath["files/json_index/100/13/1001/7003/key"].BuildID)
	assert.Len(t, byPath, 7)
}

func TestListSnapshotDataFiles_StorageV3IncludesManifestRootObjectsAndLobs(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	basePath := path.Join(tempDir, "files/insert_log/100/20/1001")
	require.NoError(t, cm.Write(ctx, path.Join(basePath, "manifest"), []byte("manifest")))
	require.NoError(t, cm.Write(ctx, path.Join(basePath, "_data/cg0.parquet"), []byte("data")))
	siblingPath := basePath + "0/manifest"
	require.NoError(t, cm.Write(ctx, siblingPath, []byte("sibling")))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)
	segment.Binlogs = nil
	segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{
		12: {
			FieldID: 12,
			BuildID: 7002,
			Files: []string{
				"files/text_index/100/12/1001/7002/posting",
			},
		},
	}
	segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{
		13: {
			FieldID: 13,
			BuildID: 7003,
			Files: []string{
				"files/json_index/100/13/1001/7003/key",
			},
		},
	}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil

	foreignStorageConfig := &indexpb.StorageConfig{
		BucketName: "foreign-bucket",
		RootPath:   "foreign-root",
	}
	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).To(
		func(gotManifestPath string, gotStorageConfig *indexpb.StorageConfig) ([]packed.LobFileInfo, error) {
			assert.Equal(t, segment.GetManifestPath(), gotManifestPath)
			assert.Same(t, foreignStorageConfig, gotStorageConfig)
			return []packed.LobFileInfo{
				{Path: "files/insert_log/100/20/lobs/30/_data/lob.vx", FieldID: 30},
			}, nil
		}).Build()
	defer mockGetLobFiles.UnPatch()

	refs, err := ListSnapshotDataFiles(ctx, cm, snapshot, foreignStorageConfig)
	require.NoError(t, err)

	byPath := make(map[string]SnapshotFileRef)
	for _, ref := range refs {
		byPath[ref.NormalizedPath] = ref
	}

	assert.Equal(t, SnapshotFileRefKindPrefix, byPath[basePath].Kind)
	assert.Equal(t, SnapshotFileTypeStorageV3ManifestRoot, byPath[basePath].Type)
	assert.Equal(t, SnapshotFileRefKindObject, byPath[path.Join(basePath, "manifest")].Kind)
	assert.Equal(t, SnapshotFileTypeStorageV3ManifestObject, byPath[path.Join(basePath, "manifest")].Type)
	assert.Equal(t, SnapshotFileRefKindObject, byPath[path.Join(basePath, "_data/cg0.parquet")].Kind)
	assert.Equal(t, SnapshotFileTypeStorageV3ManifestObject, byPath[path.Join(basePath, "_data/cg0.parquet")].Type)
	assert.Equal(t, SnapshotFileRefKindObject, byPath["files/insert_log/100/20/lobs/30/_data/lob.vx"].Kind)
	assert.Equal(t, SnapshotFileTypeStorageV3LOBFile, byPath["files/insert_log/100/20/lobs/30/_data/lob.vx"].Type)
	assert.Equal(t, SnapshotFileTypeTextIndexFile, byPath["files/text_index/100/12/1001/7002/posting"].Type)
	assert.Equal(t, int64(7002), byPath["files/text_index/100/12/1001/7002/posting"].BuildID)
	assert.Equal(t, SnapshotFileTypeJSONKeyIndexFile, byPath["files/json_index/100/13/1001/7003/key"].Type)
	assert.Equal(t, int64(7003), byPath["files/json_index/100/13/1001/7003/key"].BuildID)
	assert.NotContains(t, byPath, siblingPath)
}

func TestValidateSnapshotDataFiles_ReturnsMissingFile(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	missingPath := path.Join(tempDir, "files/insert_log/100/10/1001/missing")
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: missingPath,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	err := ValidateSnapshotDataFiles(context.Background(), cm, snapshot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot file does not exist")
	assert.Contains(t, err.Error(), missingPath)
}

func TestValidateSnapshotDataFiles_SucceedsWhenObjectsExist(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()
	existingPath := path.Join(tempDir, "files/insert_log/100/10/1001/1")
	require.NoError(t, cm.Write(ctx, existingPath, []byte("insert")))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: existingPath,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	err := ValidateSnapshotDataFiles(ctx, cm, snapshot)
	require.NoError(t, err)
}

func TestValidateSnapshotDataFiles_StorageV3SucceedsWhenManifestObjectsExist(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()
	basePath := path.Join(tempDir, "files/insert_log/100/20/1001")
	require.NoError(t, cm.Write(ctx, path.Join(basePath, "manifest"), []byte("manifest")))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	err := ValidateSnapshotDataFiles(ctx, cm, snapshot)
	require.NoError(t, err)
}

func TestValidateSnapshotDataFiles_StorageV3ManifestObjectsComeFromWalk(t *testing.T) {
	tempDir := t.TempDir()
	baseCM := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()
	basePath := path.Join(tempDir, "files/insert_log/100/20/1001")
	manifestPath := path.Join(basePath, "manifest")
	require.NoError(t, baseCM.Write(ctx, manifestPath, []byte("manifest")))
	cm := &denyExistChunkManager{
		ChunkManager: baseCM,
		deniedPath:   manifestPath,
	}

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	err := ValidateSnapshotDataFiles(ctx, cm, snapshot)
	require.NoError(t, err)
}

func TestValidateSnapshotDataFiles_StorageV3RequiresManifestObjects(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	basePath := path.Join(tempDir, "files/insert_log/100/20/1001")
	require.NoError(t, os.MkdirAll(basePath, os.ModePerm))

	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	err := ValidateSnapshotDataFiles(context.Background(), cm, snapshot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot file prefix does not contain any objects")
	assert.Contains(t, err.Error(), basePath)
}

func TestValidateSnapshotDataFiles_StorageV3RejectsEmptyManifestBasePath(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("", 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	err := ValidateSnapshotDataFiles(context.Background(), cm, snapshot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage v3 segment 1001 requires manifest base path")
}

func TestRewriteSnapshotWithMapping_RewritesAllReferencesStrictly(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/100/10/1001/1",
		}},
	}}
	segment.Statslogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/stats_log/100/10/1001/2",
		}},
	}}
	segment.Deltalogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/delta_log/100/10/1001/3",
		}},
	}}
	segment.Bm25Statslogs = []*datapb.FieldBinlog{{
		FieldID: 11,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/bm25_stats_log/100/11/1001/4",
		}},
	}}
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{
		BuildID: 7001,
		IndexFilePaths: []string{
			"files/index_files/100/10/1001/7001/index",
		},
	}}
	segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{
		12: {
			FieldID: 12,
			BuildID: 7002,
			Files: []string{
				"files/text_index/100/12/1001/7002/posting",
			},
		},
	}
	segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{
		13: {
			FieldID: 13,
			BuildID: 7003,
			Files: []string{
				"files/json_index/100/13/1001/7003/key",
			},
		},
	}
	segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/20/1001", 1)

	mappings := map[string]string{
		"files/insert_log/100/10/1001/1":            "exports/files/insert_log/100/10/1001/1",
		"files/stats_log/100/10/1001/2":             "exports/files/stats_log/100/10/1001/2",
		"files/delta_log/100/10/1001/3":             "exports/files/delta_log/100/10/1001/3",
		"files/bm25_stats_log/100/11/1001/4":        "exports/files/bm25_stats_log/100/11/1001/4",
		"files/index_files/100/10/1001/7001/index":  "exports/files/index_files/100/10/1001/7001/index",
		"files/text_index/100/12/1001/7002/posting": "exports/files/text_index/100/12/1001/7002/posting",
		"files/json_index/100/13/1001/7003/key":     "exports/files/json_index/100/13/1001/7003/key",
		"files/insert_log/100/20/1001":              "exports/files/insert_log/100/20/1001",
	}

	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, mappings, SnapshotRewriteOptions{
		TargetRoot:    "exports",
		MetadataURI:   "s3://bucket/exports/snapshots/100/metadata/1.json",
		StrictMapping: true,
	})
	require.NoError(t, err)
	require.Len(t, rewritten.Segments, 1)

	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, rewritten.Layout)
	assert.Equal(t, "s3://bucket/exports/snapshots/100/metadata/1.json", rewritten.SnapshotInfo.GetS3Location())
	assert.Equal(t, "exports/files/insert_log/100/10/1001/1", rewritten.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "exports/files/stats_log/100/10/1001/2", rewritten.Segments[0].GetStatslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "exports/files/delta_log/100/10/1001/3", rewritten.Segments[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "exports/files/bm25_stats_log/100/11/1001/4", rewritten.Segments[0].GetBm25Statslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "exports/files/index_files/100/10/1001/7001/index", rewritten.Segments[0].GetIndexFiles()[0].GetIndexFilePaths()[0])
	assert.Equal(t, "exports/files/text_index/100/12/1001/7002/posting", rewritten.Segments[0].GetTextIndexFiles()[12].GetFiles()[0])
	assert.Equal(t, "exports/files/json_index/100/13/1001/7003/key", rewritten.Segments[0].GetJsonKeyIndexFiles()[13].GetFiles()[0])
	rewrittenManifestRoot, rewrittenManifestVersion, err := packed.UnmarshalManifestPath(rewritten.Segments[0].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "exports/files/insert_log/100/20/1001", rewrittenManifestRoot)
	assert.Equal(t, int64(1), rewrittenManifestVersion)
	assert.Equal(t, "files/insert_log/100/10/1001/1", snapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	originalManifestRoot, originalManifestVersion, err := packed.UnmarshalManifestPath(snapshot.Segments[0].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "files/insert_log/100/20/1001", originalManifestRoot)
	assert.Equal(t, int64(1), originalManifestVersion)
}

func TestRewriteSnapshotWithMapping_MissingMappingFailsStrictMode(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/100/10/1001/1",
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, map[string]string{}, SnapshotRewriteOptions{
		TargetRoot:    "exports",
		StrictMapping: true,
	})
	require.Error(t, err)
	assert.Nil(t, rewritten)
	assert.Contains(t, err.Error(), "missing snapshot file mapping")
	assert.Contains(t, err.Error(), "insert binlog segment 1001 field 10")
	assert.Contains(t, err.Error(), "files/insert_log/100/10/1001/1")
}

func TestRewriteSnapshotWithMapping_StorageV3ClearsStaleInsertBinlogs(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/20/1001", 1)
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/100/10/1001/stale",
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{
		{
			Path:    "files/insert_log/100/20/lobs/30/_data/lob.vx",
			FieldID: 30,
		},
	}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, map[string]string{
		"files/insert_log/100/20/1001":                 "exports/files/insert_log/100/20/1001",
		"files/insert_log/100/20/lobs/30/_data/lob.vx": "exports/files/insert_log/100/20/lobs/30/_data/lob.vx",
	}, SnapshotRewriteOptions{
		TargetRoot:    "exports",
		StrictMapping: true,
	})
	require.NoError(t, err)
	require.Len(t, rewritten.Segments, 1)

	assert.Empty(t, rewritten.Segments[0].GetBinlogs())
	rewrittenManifestRoot, rewrittenManifestVersion, err := packed.UnmarshalManifestPath(rewritten.Segments[0].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "exports/files/insert_log/100/20/1001", rewrittenManifestRoot)
	assert.Equal(t, int64(1), rewrittenManifestVersion)
	assert.Equal(t, "files/insert_log/100/10/1001/stale", snapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestRewriteSnapshotWithMapping_StorageV3IgnoresMalformedStaleInsertBinlogs(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/20/1001", 1)
	segment.Binlogs = []*datapb.FieldBinlog{
		nil,
		{
			FieldID: 10,
			Binlogs: []*datapb.Binlog{
				nil,
			},
		},
	}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, map[string]string{
		"files/insert_log/100/20/1001": "exports/files/insert_log/100/20/1001",
	}, SnapshotRewriteOptions{
		TargetRoot:    "exports",
		StrictMapping: true,
	})
	require.NoError(t, err)
	require.Len(t, rewritten.Segments, 1)
	assert.Empty(t, rewritten.Segments[0].GetBinlogs())
	assert.Len(t, snapshot.Segments[0].GetBinlogs(), 2)
}

func TestRewriteSnapshotWithMapping_MissingStorageV3LOBMappingFailsStrictMode(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshot := createTestSnapshotData()
	segment := snapshot.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/20/1001", 1)
	segment.Binlogs = nil
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{
		{
			Path:    "files/insert_log/100/20/lobs/30/_data/lob.vx",
			FieldID: 30,
		},
	}, nil).Build()
	defer mockGetLobFiles.UnPatch()

	rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, map[string]string{
		"files/insert_log/100/20/1001": "exports/files/insert_log/100/20/1001",
	}, SnapshotRewriteOptions{
		TargetRoot:    "exports",
		StrictMapping: true,
	})
	require.Error(t, err)
	assert.Nil(t, rewritten)
	assert.Contains(t, err.Error(), "missing snapshot file mapping")
	assert.Contains(t, err.Error(), "storage v3 lob file segment 1001 field 30")
	assert.Contains(t, err.Error(), "files/insert_log/100/20/lobs/30/_data/lob.vx")
}

func TestRewriteSnapshotWithMapping_RejectsNilSnapshotEntries(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	t.Run("nil top-level index info", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		snapshot.Indexes = []*indexpb.IndexInfo{nil}

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "snapshot index at index 0 cannot be nil")
	})

	t.Run("nil segment", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		snapshot.Segments = []*datapb.SegmentDescription{nil}

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "snapshot segment at index 0 cannot be nil")
	})

	t.Run("nil field binlog", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		segment := snapshot.Segments[0]
		segment.Binlogs = []*datapb.FieldBinlog{nil}
		segment.Statslogs = nil
		segment.Deltalogs = nil
		segment.Bm25Statslogs = nil
		segment.IndexFiles = nil
		segment.TextIndexFiles = nil
		segment.JsonKeyIndexFiles = nil

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "insert binlog segment 1001 field binlog at index 0 cannot be nil")
	})

	t.Run("nil binlog", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		segment := snapshot.Segments[0]
		segment.Binlogs = []*datapb.FieldBinlog{{
			FieldID: 10,
			Binlogs: []*datapb.Binlog{
				nil,
			},
		}}
		segment.Statslogs = nil
		segment.Deltalogs = nil
		segment.Bm25Statslogs = nil
		segment.IndexFiles = nil
		segment.TextIndexFiles = nil
		segment.JsonKeyIndexFiles = nil

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "insert binlog segment 1001 field 10 binlog at index 0 cannot be nil")
	})

	t.Run("nil index file entry", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		segment := snapshot.Segments[0]
		segment.Binlogs = nil
		segment.Statslogs = nil
		segment.Deltalogs = nil
		segment.Bm25Statslogs = nil
		segment.IndexFiles = []*indexpb.IndexFilePathInfo{nil}
		segment.TextIndexFiles = nil
		segment.JsonKeyIndexFiles = nil

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "index file segment 1001 entry at index 0 cannot be nil")
	})

	t.Run("nil text index entry", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		segment := snapshot.Segments[0]
		segment.Binlogs = nil
		segment.Statslogs = nil
		segment.Deltalogs = nil
		segment.Bm25Statslogs = nil
		segment.IndexFiles = nil
		segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{12: nil}
		segment.JsonKeyIndexFiles = nil

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "text index segment 1001 field 12 cannot be nil")
	})

	t.Run("nil json key index entry", func(t *testing.T) {
		snapshot := createTestSnapshotData()
		segment := snapshot.Segments[0]
		segment.Binlogs = nil
		segment.Statslogs = nil
		segment.Deltalogs = nil
		segment.Bm25Statslogs = nil
		segment.IndexFiles = nil
		segment.TextIndexFiles = nil
		segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{13: nil}

		rewritten, err := RewriteSnapshotWithMapping(cm, snapshot, nil, SnapshotRewriteOptions{
			TargetRoot: "exports",
		})
		require.Error(t, err)
		assert.Nil(t, rewritten)
		assert.Contains(t, err.Error(), "json key index segment 1001 field 13 cannot be nil")
	})
}

func TestValidateSnapshotObjectPath(t *testing.T) {
	baseCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	cm := &testBucketChunkManager{
		ChunkManager: baseCM,
		bucket:       "test-bucket",
	}

	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{
			name: "relative key",
			path: "files/snapshots/100/metadata/1.json",
		},
		{
			name: "same bucket object key",
			path: "export-root/snapshots/100/metadata/1.json",
		},
		{
			name: "absolute local path",
			path: "/tmp/snapshots/100/metadata/1.json",
		},
		{
			name: "matching bucket",
			path: "s3://test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "gcp bucket URI",
			path: "gs://test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "azure endpoint URI",
			path: "azure://blob.core.windows.net/test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name: "https endpoint URI",
			path: "https://storage.example.com/test-bucket/files/snapshots/100/metadata/1.json",
		},
		{
			name:    "empty path",
			path:    "",
			wantErr: "snapshot_s3_location is required",
		},
		{
			name:    "credentials",
			path:    "s3://access:secret@test-bucket/files/snapshots/100/metadata/1.json",
			wantErr: "snapshot_s3_location must not embed credentials in the URI",
		},
		{
			name:    "unsupported scheme",
			path:    "ftp://test-bucket/files/snapshots/100/metadata/1.json",
			wantErr: "snapshot_s3_location must be an object key or supported snapshot URI",
		},
		{
			name:    "missing bucket",
			path:    "s3:///files/snapshots/100/metadata/1.json",
			wantErr: "snapshot_s3_location URI must include a bucket or endpoint host",
		},
		{
			name:    "bucket mismatch",
			path:    "s3://other-bucket/files/snapshots/100/metadata/1.json",
			wantErr: `snapshot_s3_location bucket "other-bucket" does not match configured bucket "test-bucket"`,
		},
		{
			name:    "endpoint URI bucket mismatch",
			path:    "https://storage.example.com/other-bucket/files/snapshots/100/metadata/1.json",
			wantErr: `snapshot_s3_location bucket "other-bucket" does not match configured bucket "test-bucket"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSnapshotObjectPath(cm, "snapshot_s3_location", tt.path)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestSnapshotWriter_SaveAllowsEmptyRoot(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshotData := createTestSnapshotData()

	metadataPath, err := NewSnapshotWriter(cm).SaveToRoot(
		context.Background(),
		snapshotData,
		"",
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
	)

	require.NoError(t, err)
	assert.Equal(t, "snapshots/100/metadata/1.json", metadataPath)
	exist, err := cm.Exist(context.Background(), metadataPath)
	require.NoError(t, err)
	assert.True(t, exist)
}

func TestRedactSnapshotObjectPath(t *testing.T) {
	assert.Equal(t,
		"s3://redacted@test-bucket/files/snapshots/100/metadata/1.json",
		redactSnapshotObjectPath("s3://access:secret@test-bucket/files/snapshots/100/metadata/1.json"))
	assert.Equal(t,
		"files/snapshots/100/metadata/1.json",
		redactSnapshotObjectPath("files/snapshots/100/metadata/1.json"))
}

func TestSnapshotReader_ReadSnapshot_RejectsSelfContainedPathOutsideRoot(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	snapshotData := createTestSnapshotData()
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: path.Join(tempDir, "outside/insert_log/1"),
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	segment.IndexFiles = nil

	targetRoot := path.Join(tempDir, "bundle")
	metadataPath, err := NewSnapshotWriter(cm).SaveToRoot(ctx, snapshotData, targetRoot, datapb.SnapshotLayout_SnapshotLayoutSelfContained)
	assert.NoError(t, err)

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	assert.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.Contains(t, err.Error(), "outside snapshot root")
}

func TestRebaseSelfContainedSnapshotMetadata_RewritesManifestLists(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{
			"export-root/snapshots/100/manifests/1/1001.avro",
		},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath("export-root/files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := RebaseSelfContainedSnapshotMetadata(cm, metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "restored/x/snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(metadata.GetStoragev2ManifestList()[0].GetManifest())
	require.NoError(t, err)
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001", gotBasePath)
	assert.Equal(t, int64(7), gotVersion)
}

func TestRebaseSelfContainedSnapshotMetadata_RewritesConfiguredBucketS3URI(t *testing.T) {
	baseCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	cm := &testBucketChunkManager{
		ChunkManager: baseCM,
		bucket:       "test-bucket",
	}
	metadata := &datapb.SnapshotMetadata{
		ManifestList: []string{
			"s3://test-bucket/export-root/snapshots/100/manifests/1/1001.avro",
		},
	}

	err := RebaseSelfContainedSnapshotMetadata(cm, metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "restored/x/snapshots/100/manifests/1/1001.avro", metadata.GetManifestList()[0])
}

func TestRebaseSelfContainedSnapshotMetadata_SkipsInvalidManifestWhenRootMissing(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	metadata := &datapb.SnapshotMetadata{
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  "legacy-manifest-path",
			},
		},
	}

	err := RebaseSelfContainedSnapshotMetadata(cm, metadata, "", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, "legacy-manifest-path", metadata.GetStoragev2ManifestList()[0].GetManifest())
}

func TestRebaseSelfContainedSnapshotMetadata_LeavesUnmatchedManifestPathUnchanged(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	manifestPath := packed.MarshalManifestPath("other-root/files/insert_log/100/1/1001", 7)
	metadata := &datapb.SnapshotMetadata{
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  manifestPath,
			},
		},
	}

	err := RebaseSelfContainedSnapshotMetadata(cm, metadata, "export-root", "restored/x")
	require.NoError(t, err)
	assert.Equal(t, manifestPath, metadata.GetStoragev2ManifestList()[0].GetManifest())
}

func TestRebaseSelfContainedSnapshotData_RewritesSegmentReferences(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	snapshot := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "relocated"},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:    1001,
				PartitionId:  1,
				SegmentLevel: datapb.SegmentLevel_L1,
				ChannelName:  "by-dev-rootcoord-dml_0_100v0",
				NumOfRows:    100,
				Binlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   1,
						LogPath: "export-root/files/insert_log/100/1/1001/1",
					}},
				}},
				Statslogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   2,
						LogPath: "export-root/files/stats_log/100/1/1001/2",
					}},
				}},
				Deltalogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogID:   3,
						LogPath: "export-root/files/delta_log/100/1/1001/3",
					}},
				}},
				Bm25Statslogs: []*datapb.FieldBinlog{{
					FieldID: 3,
					Binlogs: []*datapb.Binlog{{
						LogID:   4,
						LogPath: "export-root/files/bm25_stats_log/100/1/1001/4",
					}},
				}},
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					SegmentID:             1001,
					FieldID:               2,
					BuildID:               3001,
					IndexFilePaths:        []string{"export-root/files/index_files/100/1/1001/3001/index"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				}},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {
						FieldID: 100,
						BuildID: 5000,
						Files:   []string{"export-root/files/text_index/100/1/1001/5000/text"},
					},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {
						FieldID: 200,
						BuildID: 6000,
						Files:   []string{"export-root/files/json_key_index/100/1/1001/6000/json"},
					},
				},
				ManifestPath: packed.MarshalManifestPath("export-root/files/insert_log/100/1/1001", 7),
			},
		},
	}

	err := RebaseSelfContainedSnapshotData(cm, snapshot, "export-root", "restored/x")
	require.NoError(t, err)

	segment := snapshot.Segments[0]
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001/1", segment.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/stats_log/100/1/1001/2", segment.GetStatslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/delta_log/100/1/1001/3", segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/bm25_stats_log/100/1/1001/4", segment.GetBm25Statslogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "restored/x/files/index_files/100/1/1001/3001/index", segment.GetIndexFiles()[0].GetIndexFilePaths()[0])
	assert.Equal(t, "restored/x/files/text_index/100/1/1001/5000/text", segment.GetTextIndexFiles()[100].GetFiles()[0])
	assert.Equal(t, "restored/x/files/json_key_index/100/1/1001/6000/json", segment.GetJsonKeyIndexFiles()[200].GetFiles()[0])

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, "restored/x/files/insert_log/100/1/1001", gotBasePath)
	assert.Equal(t, int64(7), gotVersion)
}

func TestSnapshotReader_ReadSnapshot_RebasesRelocatedSelfContainedBundleBeforeManifestRead(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := NewSnapshotReader(cm)
	ctx := context.Background()

	oldRoot := "export-root"
	newRoot := "restored/x"
	oldMetadataPath := path.Join(oldRoot, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json")
	newMetadataPath := path.Join(newRoot, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json")
	oldManifestPath := path.Join(oldRoot, SnapshotRootPath, "100", SnapshotManifestsSubPath, "1", "1001.avro")
	newManifestPath := path.Join(newRoot, SnapshotRootPath, "100", SnapshotManifestsSubPath, "1", "1001.avro")
	oldBinlogPath := path.Join(oldRoot, "files", "insert_log", "100", "1", "1001", "1")
	newBinlogPath := path.Join(newRoot, "files", "insert_log", "100", "1", "1001", "1")

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   oldMetadataPath,
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{oldManifestPath},
		SegmentIds:   []int64{1001},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	segment := &datapb.SegmentDescription{
		SegmentId:    1001,
		PartitionId:  1,
		SegmentLevel: datapb.SegmentLevel_L1,
		ChannelName:  "by-dev-rootcoord-dml_0_100v0",
		NumOfRows:    100,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{
				LogID:   1,
				LogPath: oldBinlogPath,
			}},
		}},
		Statslogs:         []*datapb.FieldBinlog{},
		Deltalogs:         []*datapb.FieldBinlog{},
		Bm25Statslogs:     []*datapb.FieldBinlog{},
		TextIndexFiles:    map[int64]*datapb.TextIndexStats{},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{},
		IndexFiles:        []*indexpb.IndexFilePathInfo{},
		StartPosition:     &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		DmlPosition:       &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		StorageVersion:    2,
		CommitTimestamp:   10,
	}
	manifestEntry := convertSegmentToManifestEntry(segment)
	manifestSchema, err := getManifestSchema()
	require.NoError(t, err)
	manifestBytes, err := avro.Marshal(manifestSchema, manifestEntry)
	require.NoError(t, err)

	readPaths := make([]string, 0, 2)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		readPaths = append(readPaths, filePath)
		switch filePath {
		case newMetadataPath:
			return metadataJSON, nil
		case newManifestPath:
			return manifestBytes, nil
		case oldManifestPath:
			return nil, fmt.Errorf("old manifest path should not be read: %s", filePath)
		default:
			return nil, fmt.Errorf("unexpected file path: %s", filePath)
		}
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(ctx, newMetadataPath, true)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Segments, 1)
	assert.Equal(t, newMetadataPath, got.SnapshotInfo.GetS3Location())
	assert.Equal(t, newBinlogPath, got.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Contains(t, readPaths, newManifestPath)
	assert.NotContains(t, readPaths, oldManifestPath)
}

func TestSnapshotReader_ReadSnapshot_RejectsRelocatedMetadataWithoutSnapshotsAnchor(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   "export-root/snapshots/100/metadata/1.json",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		ManifestList: []string{"export-root/snapshots/100/manifests/1/1001.avro"},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	metadataPathWithoutAnchor := "restored/x/meta.json"
	readPaths := make([]string, 0, 2)
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		readPaths = append(readPaths, filePath)
		if filePath == metadataPathWithoutAnchor {
			return metadataJSON, nil
		}
		if filePath == metadata.GetManifestList()[0] {
			return nil, fmt.Errorf("manifest should not be read")
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(context.Background(), metadataPathWithoutAnchor, true)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "cannot derive snapshot root from metadata path")
	assert.NotContains(t, readPaths, metadata.GetManifestList()[0])
}

func TestSnapshotReader_ReadSnapshot_RejectsSelfContainedMetadataWithoutSnapshotInfo(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := NewSnapshotReader(cm)

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		ManifestList: []string{"restored/x/snapshots/100/manifests/1/1001.avro"},
		Layout:       datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	metadataPath := "restored/x/snapshots/100/metadata/1.json"
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataPath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	var got *SnapshotData
	require.NotPanics(t, func() {
		got, err = reader.ReadSnapshot(context.Background(), metadataPath, false)
	})
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "snapshot info")
}

func TestSnapshot_JSONStatsPaths_RoundTripPreservesSnapshotRestorePaths(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	reader := NewSnapshotReader(cm)

	snapshotData := createTestSnapshotData()
	snapshotData.Segments[0].ManifestPath = `{"ver":7,"base_path":"files/insert_log/1/2/1001"}`
	snapshotData.Segments[0].JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{
		200: {
			FieldID:                200,
			Version:                2,
			Files:                  []string{"files/json_stats/3/6000/2/100/1/1001/200/shared_key_index/.managed.json_0"},
			LogSize:                1024,
			MemorySize:             2048,
			BuildID:                6000,
			JsonKeyStatsDataFormat: 3,
		},
		201: {
			FieldID: 201,
			Version: 3,
			Files: []string{
				"files/insert_log/1/2/1001/_stats/json_stats.201/shared_key_index/.managed.json_1",
				"files/insert_log/1/2/1001/_stats/json_stats.201/shredding_data/data.parquet",
			},
			LogSize:                2048,
			MemorySize:             4096,
			BuildID:                6001,
			JsonKeyStatsDataFormat: 3,
		},
	}

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.Len(t, readSnapshot.Segments, 1)

	got := readSnapshot.Segments[0].GetJsonKeyIndexFiles()
	assert.Equal(t, snapshotData.Segments[0].GetJsonKeyIndexFiles()[200].GetFiles(), got[200].GetFiles())
	assert.Equal(t, snapshotData.Segments[0].GetJsonKeyIndexFiles()[201].GetFiles(), got[201].GetFiles())
	assert.Equal(t, snapshotData.Segments[0].GetManifestPath(), readSnapshot.Segments[0].GetManifestPath())
}

// =========================== New Fields Tests ===========================

func TestSnapshot_NewFields_Serialization(t *testing.T) {
	// Test that all new fields (Bm25Statslogs, TextIndexFiles, JsonKeyIndexFiles,
	// StartPosition, DmlPosition, StorageVersion, IsSorted) are properly serialized
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	reader := NewSnapshotReader(cm)

	snapshotData := createTestSnapshotData()

	// 1. Save snapshot
	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	// 2. Read snapshot back using the metadata path
	readSnapshot, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readSnapshot)
	assert.Len(t, readSnapshot.Segments, 1)

	original := snapshotData.Segments[0]
	restored := readSnapshot.Segments[0]

	// 3. Verify Bm25Statslogs field
	assert.Equal(t, len(original.Bm25Statslogs), len(restored.Bm25Statslogs), "Bm25Statslogs count should match")
	if len(original.Bm25Statslogs) > 0 {
		assert.Equal(t, original.Bm25Statslogs[0].FieldID, restored.Bm25Statslogs[0].FieldID)
		assert.Equal(t, original.Bm25Statslogs[0].ChildFields, restored.Bm25Statslogs[0].ChildFields)
		assert.Equal(t, original.Bm25Statslogs[0].Format, restored.Bm25Statslogs[0].Format)
		assert.Equal(t, len(original.Bm25Statslogs[0].Binlogs), len(restored.Bm25Statslogs[0].Binlogs))
		if len(original.Bm25Statslogs[0].Binlogs) > 0 {
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogPath, restored.Bm25Statslogs[0].Binlogs[0].LogPath)
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogSize, restored.Bm25Statslogs[0].Binlogs[0].LogSize)
		}
	}

	// 3.5 Verify insert binlog metadata fields
	require.NotEmpty(t, restored.Binlogs)
	assert.Equal(t, original.Binlogs[0].ChildFields, restored.Binlogs[0].ChildFields)
	assert.Equal(t, original.Binlogs[0].Format, restored.Binlogs[0].Format)

	// 3.6 Verify stats/delta binlog metadata fields
	require.NotEmpty(t, restored.Statslogs)
	require.NotEmpty(t, restored.Deltalogs)
	assert.Equal(t, original.Statslogs[0].ChildFields, restored.Statslogs[0].ChildFields)
	assert.Equal(t, original.Statslogs[0].Format, restored.Statslogs[0].Format)
	assert.Equal(t, original.Deltalogs[0].ChildFields, restored.Deltalogs[0].ChildFields)
	assert.Equal(t, original.Deltalogs[0].Format, restored.Deltalogs[0].Format)

	// 4. Verify TextIndexFiles field (map type)
	assert.Equal(t, len(original.TextIndexFiles), len(restored.TextIndexFiles), "TextIndexFiles count should match")
	for fieldID, origStats := range original.TextIndexFiles {
		restStats, exists := restored.TextIndexFiles[fieldID]
		assert.True(t, exists, "TextIndexFiles field %d should exist", fieldID)
		assert.Equal(t, origStats.FieldID, restStats.FieldID)
		assert.Equal(t, origStats.Version, restStats.Version)
		assert.Equal(t, origStats.Files, restStats.Files)
		assert.Equal(t, origStats.LogSize, restStats.LogSize)
		assert.Equal(t, origStats.MemorySize, restStats.MemorySize)
		assert.Equal(t, origStats.BuildID, restStats.BuildID)
	}

	// 5. Verify JsonKeyIndexFiles field (map type)
	assert.Equal(t, len(original.JsonKeyIndexFiles), len(restored.JsonKeyIndexFiles), "JsonKeyIndexFiles count should match")
	for fieldID, origStats := range original.JsonKeyIndexFiles {
		restStats, exists := restored.JsonKeyIndexFiles[fieldID]
		assert.True(t, exists, "JsonKeyIndexFiles field %d should exist", fieldID)
		assert.Equal(t, origStats.FieldID, restStats.FieldID)
		assert.Equal(t, origStats.Version, restStats.Version)
		assert.Equal(t, origStats.Files, restStats.Files)
		assert.Equal(t, origStats.LogSize, restStats.LogSize)
		assert.Equal(t, origStats.MemorySize, restStats.MemorySize)
		assert.Equal(t, origStats.BuildID, restStats.BuildID)
		assert.Equal(t, origStats.JsonKeyStatsDataFormat, restStats.JsonKeyStatsDataFormat)
	}

	// 6. Verify StartPosition field
	assert.NotNil(t, restored.StartPosition, "StartPosition should not be nil")
	assert.Equal(t, original.StartPosition.ChannelName, restored.StartPosition.ChannelName)
	assert.Equal(t, original.StartPosition.MsgID, restored.StartPosition.MsgID)
	assert.Equal(t, original.StartPosition.MsgGroup, restored.StartPosition.MsgGroup)
	assert.Equal(t, original.StartPosition.Timestamp, restored.StartPosition.Timestamp)

	// 7. Verify DmlPosition field
	assert.NotNil(t, restored.DmlPosition, "DmlPosition should not be nil")
	assert.Equal(t, original.DmlPosition.ChannelName, restored.DmlPosition.ChannelName)
	assert.Equal(t, original.DmlPosition.MsgID, restored.DmlPosition.MsgID)
	assert.Equal(t, original.DmlPosition.MsgGroup, restored.DmlPosition.MsgGroup)
	assert.Equal(t, original.DmlPosition.Timestamp, restored.DmlPosition.Timestamp)

	// 8. Verify StorageVersion field
	assert.Equal(t, original.StorageVersion, restored.StorageVersion, "StorageVersion should match")

	// 9. Verify IsSorted field
	assert.Equal(t, original.IsSorted, restored.IsSorted, "IsSorted should match")

	// 10. Cleanup - use the metadata path returned from Save
	err = writer.Drop(context.Background(), metadataPath)
	assert.NoError(t, err)
}

func TestSnapshot_ConversionFunctions(t *testing.T) {
	// Test conversion functions for new types

	// Test MsgPosition conversion
	t.Run("MsgPosition conversion", func(t *testing.T) {
		original := &msgpb.MsgPosition{
			ChannelName: "test_channel",
			MsgID:       []byte{1, 2, 3, 4},
			MsgGroup:    "test_group",
			Timestamp:   12345,
		}
		avro := snapshotio.MsgPositionToAvro(original)
		restored := snapshotio.AvroToMsgPosition(avro)

		assert.Equal(t, original.ChannelName, restored.ChannelName)
		assert.Equal(t, original.MsgID, restored.MsgID)
		assert.Equal(t, original.MsgGroup, restored.MsgGroup)
		assert.Equal(t, original.Timestamp, restored.Timestamp)
	})

	// Test MsgPosition nil handling
	t.Run("MsgPosition nil handling", func(t *testing.T) {
		avro := snapshotio.MsgPositionToAvro(nil)
		assert.NotNil(t, avro)
		assert.Equal(t, "", avro.ChannelName)
		assert.Equal(t, []byte{}, avro.MsgID)

		restored := snapshotio.AvroToMsgPosition(nil)
		assert.Nil(t, restored)
	})

	// Test TextIndexStats conversion
	t.Run("TextIndexStats conversion", func(t *testing.T) {
		original := &datapb.TextIndexStats{
			FieldID:    100,
			Version:    1,
			Files:      []string{"/file1", "/file2"},
			LogSize:    1024,
			MemorySize: 2048,
			BuildID:    5000,
		}
		avro := snapshotio.TextIndexStatsToAvro(original)
		restored := snapshotio.AvroToTextIndexStats(avro)

		assert.Equal(t, original.FieldID, restored.FieldID)
		assert.Equal(t, original.Version, restored.Version)
		assert.Equal(t, original.Files, restored.Files)
		assert.Equal(t, original.LogSize, restored.LogSize)
		assert.Equal(t, original.MemorySize, restored.MemorySize)
		assert.Equal(t, original.BuildID, restored.BuildID)
	})

	// Test JsonKeyStats conversion
	t.Run("JsonKeyStats conversion", func(t *testing.T) {
		original := &datapb.JsonKeyStats{
			FieldID:                200,
			Version:                2,
			Files:                  []string{"/json/file1"},
			LogSize:                512,
			MemorySize:             1024,
			BuildID:                6000,
			JsonKeyStatsDataFormat: 1,
		}
		avro := snapshotio.JSONKeyStatsToAvro(original)
		restored := snapshotio.AvroToJSONKeyStats(avro)

		assert.Equal(t, original.FieldID, restored.FieldID)
		assert.Equal(t, original.Version, restored.Version)
		assert.Equal(t, original.Files, restored.Files)
		assert.Equal(t, original.LogSize, restored.LogSize)
		assert.Equal(t, original.MemorySize, restored.MemorySize)
		assert.Equal(t, original.BuildID, restored.BuildID)
		assert.Equal(t, original.JsonKeyStatsDataFormat, restored.JsonKeyStatsDataFormat)
	})

	// Test TextIndexMap conversion (map to array and back)
	t.Run("TextIndexMap conversion", func(t *testing.T) {
		originalMap := map[int64]*datapb.TextIndexStats{
			100: {FieldID: 100, Version: 1, Files: []string{"/file1"}},
			200: {FieldID: 200, Version: 2, Files: []string{"/file2"}},
		}
		avroArray := snapshotio.TextIndexMapToAvro(originalMap)
		restoredMap := snapshotio.AvroToTextIndexMap(avroArray)

		assert.Equal(t, len(originalMap), len(restoredMap))
		for fieldID, origStats := range originalMap {
			restStats, exists := restoredMap[fieldID]
			assert.True(t, exists)
			assert.Equal(t, origStats.FieldID, restStats.FieldID)
			assert.Equal(t, origStats.Version, restStats.Version)
			assert.Equal(t, origStats.Files, restStats.Files)
		}
	})

	// Test JsonKeyIndexMap conversion (map to array and back)
	t.Run("JsonKeyIndexMap conversion", func(t *testing.T) {
		originalMap := map[int64]*datapb.JsonKeyStats{
			100: {FieldID: 100, Version: 1, Files: []string{"/json1"}, JsonKeyStatsDataFormat: 1},
			200: {FieldID: 200, Version: 2, Files: []string{"/json2"}, JsonKeyStatsDataFormat: 2},
		}
		avroArray := snapshotio.JSONKeyIndexMapToAvro(originalMap)
		restoredMap := snapshotio.AvroToJSONKeyIndexMap(avroArray)

		assert.Equal(t, len(originalMap), len(restoredMap))
		for fieldID, origStats := range originalMap {
			restStats, exists := restoredMap[fieldID]
			assert.True(t, exists)
			assert.Equal(t, origStats.FieldID, restStats.FieldID)
			assert.Equal(t, origStats.Version, restStats.Version)
			assert.Equal(t, origStats.Files, restStats.Files)
			assert.Equal(t, origStats.JsonKeyStatsDataFormat, restStats.JsonKeyStatsDataFormat)
		}
	})
}

// =========================== StorageV2 Manifest Tests ===========================

func TestSnapshotWriter_Save_WithStorageV2Manifest(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Add manifest_path to segment
	snapshotData.Segments[0].ManifestPath = "s3://bucket/collection/partition/segment1/manifest.json"

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// Read back the metadata and verify StorageV2ManifestList
	reader := NewSnapshotReader(cm)
	metadata, err := reader.readMetadataFile(context.Background(), metadataPath)
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Len(t, metadata.GetStoragev2ManifestList(), 1)

	// Verify the manifest list content
	assert.Equal(t, int64(1001), metadata.GetStoragev2ManifestList()[0].GetSegmentId())
	assert.Equal(t, "s3://bucket/collection/partition/segment1/manifest.json", metadata.GetStoragev2ManifestList()[0].GetManifest())
}

func TestSnapshotReader_ReadSnapshot_WithStorageV2Manifest(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	// Write a snapshot with StorageV2 manifest
	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()
	snapshotData.Segments[0].ManifestPath = "s3://bucket/collection/partition/segment1/manifest.json"

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	// Read back the snapshot using the metadata path
	reader := NewSnapshotReader(cm)
	readData, err := reader.ReadSnapshot(context.Background(), metadataPath, true)
	assert.NoError(t, err)
	assert.NotNil(t, readData)
	assert.Len(t, readData.Segments, 1)

	// Verify manifest_path is restored
	assert.Equal(t, int64(1001), readData.Segments[0].GetSegmentId())
	assert.Equal(t, "s3://bucket/collection/partition/segment1/manifest.json", readData.Segments[0].GetManifestPath())
}

func TestSnapshotReader_ReadSnapshot_RebasesStorageV2ManifestListBeforeFillingSegments(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	reader := NewSnapshotReader(cm)
	ctx := context.Background()

	oldRoot := "export-root"
	newRoot := "restored/x"
	newMetadataPath := path.Join(newRoot, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json")
	oldMetadataPath := path.Join(oldRoot, SnapshotRootPath, "100", SnapshotMetadataSubPath, "1.json")
	oldManifestPath := path.Join(oldRoot, SnapshotRootPath, "100", SnapshotManifestsSubPath, "1", "1001.avro")
	newManifestPath := path.Join(newRoot, SnapshotRootPath, "100", SnapshotManifestsSubPath, "1", "1001.avro")
	oldStorageV2BasePath := path.Join(oldRoot, "files", "insert_log", "100", "1", "1001")
	newStorageV2BasePath := path.Join(newRoot, "files", "insert_log", "100", "1", "1001")

	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "relocated",
			S3Location:   oldMetadataPath,
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		ManifestList: []string{oldManifestPath},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{
				SegmentId: 1001,
				Manifest:  packed.MarshalManifestPath(oldStorageV2BasePath, 9),
			},
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	metadataJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(metadata)
	require.NoError(t, err)

	segment := &datapb.SegmentDescription{
		SegmentId:         1001,
		PartitionId:       1,
		SegmentLevel:      datapb.SegmentLevel_L1,
		ChannelName:       "by-dev-rootcoord-dml_0_100v0",
		NumOfRows:         100,
		Binlogs:           []*datapb.FieldBinlog{},
		Statslogs:         []*datapb.FieldBinlog{},
		Deltalogs:         []*datapb.FieldBinlog{},
		Bm25Statslogs:     []*datapb.FieldBinlog{},
		TextIndexFiles:    map[int64]*datapb.TextIndexStats{},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{},
		IndexFiles:        []*indexpb.IndexFilePathInfo{},
		StartPosition:     &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		DmlPosition:       &msgpb.MsgPosition{ChannelName: "by-dev-rootcoord-dml_0_100v0"},
		StorageVersion:    2,
		CommitTimestamp:   10,
	}
	manifestEntry := convertSegmentToManifestEntry(segment)
	manifestSchema, err := getManifestSchema()
	require.NoError(t, err)
	manifestBytes, err := avro.Marshal(manifestSchema, manifestEntry)
	require.NoError(t, err)

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		switch filePath {
		case newMetadataPath:
			return metadataJSON, nil
		case newManifestPath:
			return manifestBytes, nil
		default:
			return nil, fmt.Errorf("unexpected file path: %s", filePath)
		}
	}).Build()
	defer mockRead.UnPatch()

	got, err := reader.ReadSnapshot(ctx, newMetadataPath, true)
	require.NoError(t, err)
	require.Len(t, got.Segments, 1)

	gotBasePath, gotVersion, err := packed.UnmarshalManifestPath(got.Segments[0].GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, newStorageV2BasePath, gotBasePath)
	assert.Equal(t, int64(9), gotVersion)
}

func TestSnapshotWriter_Save_EmptyManifestPath(t *testing.T) {
	tempDir := t.TempDir()
	defer t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	defer cm.RemoveWithPrefix(context.Background(), "")

	writer := NewSnapshotWriter(cm)
	snapshotData := createTestSnapshotData()

	// Segment without manifest_path (StorageV1 segment)
	snapshotData.Segments[0].ManifestPath = ""

	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// Read back the metadata
	reader := NewSnapshotReader(cm)
	metadata, err := reader.readMetadataFile(context.Background(), metadataPath)
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	// StorageV2ManifestList should be empty since no segment has manifest_path
	assert.Len(t, metadata.GetStoragev2ManifestList(), 0)
}

// =========================== Format Version Tests ===========================

func TestValidateFormatVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     int
		wantErr     bool
		errContains string
	}{
		{
			name:    "version_0_legacy",
			version: 0,
			wantErr: false,
		},
		{
			name:    "version_1_current",
			version: 1,
			wantErr: false,
		},
		{
			name:    "version_2_legacy",
			version: 2,
			wantErr: false,
		},
		{
			name:    "version_3_current",
			version: 3,
			wantErr: false,
		},
		{
			name:    "version_4_current",
			version: 4,
			wantErr: false,
		},
		{
			name:        "version_100_future",
			version:     100,
			wantErr:     true,
			errContains: "too new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := snapshotio.ValidateFormatVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetManifestSchemaByVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     int
		wantErr     bool
		errContains string
	}{
		{
			name:    "version_0_legacy",
			version: 0,
			wantErr: false,
		},
		{
			name:    "version_1_current",
			version: 1,
			wantErr: false,
		},
		{
			name:    "version_2_legacy",
			version: 2,
			wantErr: false,
		},
		{
			name:    "version_3_current",
			version: 3,
			wantErr: false,
		},
		{
			name:    "version_4_current",
			version: 4,
			wantErr: false,
		},
		{
			name:        "version_99_unsupported",
			version:     99,
			wantErr:     true,
			errContains: "unsupported manifest schema version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := snapshotio.ManifestSchemaByVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, schema)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, schema)
			}
		})
	}
}

func TestSnapshotMetadata_FormatVersion(t *testing.T) {
	// Test that FormatVersion is correctly serialized/deserialized
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: int32(SnapshotFormatVersion),
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1,
			Name: "test",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}

	// Serialize to JSON using protojson
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := marshalOpts.Marshal(metadata)
	assert.NoError(t, err)

	// Verify JSON contains format_version field (protojson uses snake_case with UseProtoNames)
	assert.Contains(t, string(data), `"format_version":`)

	// Deserialize back
	restored := &datapb.SnapshotMetadata{}
	unmarshalOpts := protojson.UnmarshalOptions{DiscardUnknown: true}
	err = unmarshalOpts.Unmarshal(data, restored)
	assert.NoError(t, err)
	assert.Equal(t, int32(SnapshotFormatVersion), restored.GetFormatVersion())
}

func TestSnapshotReader_ReadSnapshot_LegacyVersion(t *testing.T) {
	// Test reading legacy snapshot without FormatVersion field (version 0)
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	// Create legacy metadata without FormatVersion (simulating old snapshots)
	// Note: protojson supports snake_case (proto names) and camelCase (JSON names)
	// We use snake_case here to simulate a snapshot created with protojson defaults
	legacyMetadata := map[string]interface{}{
		"snapshot_info": map[string]interface{}{
			"id":   1,
			"name": "legacy_snapshot",
		},
		"collection":    map[string]interface{}{},
		"indexes":       []interface{}{},
		"manifest_list": []string{},
		// Note: No "format_version" field - this is the legacy case (version 0)
	}
	metadataJSON, _ := json.Marshal(legacyMetadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	// Reading legacy snapshot without manifest should succeed
	// withSegments=false to avoid needing manifest files
	snapshot, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)
	assert.NoError(t, err) // Should succeed with version 0 (legacy)
	assert.NotNil(t, snapshot)
	assert.Equal(t, "legacy_snapshot", snapshot.SnapshotInfo.GetName())
}

func TestSnapshotReader_ReadSnapshot_FutureVersion(t *testing.T) {
	// Test that reading a snapshot with future version fails
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	reader := NewSnapshotReader(cm)

	futureMetadata := &datapb.SnapshotMetadata{
		FormatVersion: 999, // Future version
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1,
			Name: "future_snapshot",
		},
		Collection:   &datapb.CollectionDescription{},
		Indexes:      []*indexpb.IndexInfo{},
		ManifestList: []string{},
	}
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	metadataJSON, _ := marshalOpts.Marshal(futureMetadata)

	metadataFilePath := "snapshots/100/metadata/00001-uuid.json"

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == metadataFilePath {
			return metadataJSON, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	_, err := reader.ReadSnapshot(context.Background(), metadataFilePath, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too new")
}
