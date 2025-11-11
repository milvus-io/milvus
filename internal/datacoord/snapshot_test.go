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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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

// =========================== SnapshotWriter Tests ===========================

func TestSnapshotWriter_Save_RealAvro(t *testing.T) {
	// Use real ChunkManager and Avro operations, only mock storage layer
	tempDir := t.TempDir()
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
	cm := &storage.LocalChunkManager{}
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
	cm := &storage.LocalChunkManager{}
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
	assert.Equal(t, 2, writeCallCount) // Must write 2 files even without segments
}

func TestSnapshotWriter_Drop_Success(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	writer := NewSnapshotWriter(cm)

	// Mock metadata file content
	metadata := &SnapshotMetadata{
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
	metadataJSON, _ := json.Marshal(metadata)

	// Mock file operations
	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{"snapshots/100/metadata/00001-uuid.json"},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/00001-uuid.json" {
			return metadataJSON, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	mockMultiRemove := mockey.Mock((*storage.LocalChunkManager).MultiRemove).Return(nil).Build()
	defer mockMultiRemove.UnPatch()

	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).Return(nil).Build()
	defer mockRemove.UnPatch()

	err := writer.Drop(context.Background(), 100, 1)

	assert.NoError(t, err)
}

func TestSnapshotWriter_Drop_SnapshotNotFound(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	writer := NewSnapshotWriter(cm)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	err := writer.Drop(context.Background(), 100, 1)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// =========================== SnapshotReader Tests ===========================

func TestSnapshotReader_ReadSnapshot_LatestSnapshot_Success(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	reader := NewSnapshotReader(cm)

	metadata := &SnapshotMetadata{
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
	metadataJSON, _ := json.Marshal(metadata)

	// Generate valid manifest entries with all required fields including new ones
	manifestEntries := []ManifestEntry{
		{
			Status:            "ADDED",
			SnapshotID:        1,
			SegmentID:         1001,
			PartitionID:       1,
			SegmentLevel:      1,
			ChannelName:       "test_channel",
			NumOfRows:         100,
			BinlogFiles:       []AvroFieldBinlog{},
			DeltalogFiles:     []AvroFieldBinlog{},
			StatslogFiles:     []AvroFieldBinlog{},
			Bm25StatslogFiles: []AvroFieldBinlog{},
			TextIndexFiles:    []AvroTextIndexEntry{},
			JsonKeyIndexFiles: []AvroJsonKeyIndexEntry{},
			IndexFiles:        []AvroIndexFilePathInfo{},
			StartPosition:     &AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
			DmlPosition:       &AvroMsgPosition{ChannelName: "", MsgID: []byte{}, MsgGroup: "", Timestamp: 0},
			StorageVersion:    0,
			IsSorted:          false,
		},
	}

	// Pre-generate valid Avro data for manifest using the real schema
	manifestSchema, _ := getManifestSchema()
	validManifestData, _ := avro.Marshal(manifestSchema, manifestEntries)

	// Mock file operations
	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{"snapshots/100/metadata/00001-uuid.json"},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/00001-uuid.json" {
			return metadataJSON, nil
		}
		if filePath == "manifest1.avro" {
			return validManifestData, nil
		}
		return nil, fmt.Errorf("unexpected file path: %s", filePath)
	}).Build()
	defer mockRead.UnPatch()

	snapshot, err := reader.ReadSnapshot(context.Background(), 100, 1, true)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, int64(1), snapshot.SnapshotInfo.GetId())
	assert.Equal(t, "test_snapshot", snapshot.SnapshotInfo.GetName())
	assert.Len(t, snapshot.Segments, 1)
	assert.Equal(t, int64(1001), snapshot.Segments[0].SegmentId)
	assert.Equal(t, "test_channel", snapshot.Segments[0].ChannelName)
	assert.Equal(t, int64(100), snapshot.Segments[0].NumOfRows)
}

func TestSnapshotReader_ReadSnapshot_SnapshotNotFound(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	reader := NewSnapshotReader(cm)

	mockList := mockey.Mock(storage.ListAllChunkWithPrefix).Return(
		[]string{},
		[]time.Time{},
		nil,
	).Build()
	defer mockList.UnPatch()

	_, err := reader.ReadSnapshot(context.Background(), 100, 1, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotReader_ListSnapshots_Success(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	reader := NewSnapshotReader(cm)

	metadata1 := &SnapshotMetadata{
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
	metadata1JSON, _ := json.Marshal(metadata1)

	metadata2 := &SnapshotMetadata{
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
	metadata2JSON, _ := json.Marshal(metadata2)

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

// =========================== Data Conversion Tests ===========================

func TestFieldBinlog_RoundtripConversion(t *testing.T) {
	originalFieldBinlog := &datapb.FieldBinlog{
		FieldID: 123,
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

	avroFieldBinlog := convertFieldBinlogToAvro(originalFieldBinlog)
	resultFieldBinlog := convertAvroToFieldBinlog(avroFieldBinlog)

	assert.Equal(t, originalFieldBinlog.FieldID, resultFieldBinlog.FieldID)
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
		IndexFilePaths:      []string{"/idx/path1", "/idx/path2", "/idx/path3"},
		SerializedSize:      16384,
		IndexVersion:        5,
		NumRows:             50000,
		CurrentIndexVersion: 5,
		MemSize:             32768,
	}

	avroIndexInfo := convertIndexFilePathInfoToAvro(originalIndexInfo)
	resultIndexInfo := convertAvroToIndexFilePathInfo(avroIndexInfo)

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

// =========================== Integration Tests ===========================

func TestSnapshotWriter_ManifestList_Roundtrip(t *testing.T) {
	// This test is no longer relevant since we removed the manifest list layer
	// Manifest paths are now stored directly in metadata.json
	t.Skip("Manifest list layer has been removed - manifest paths are now stored directly in metadata.json")
}

func TestSnapshot_CompleteWorkflow(t *testing.T) {
	// Test complete snapshot workflow
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	reader := NewSnapshotReader(cm)

	// 1. Create test data
	snapshotData := createTestSnapshotData()

	// 2. Save snapshot
	metadataPath, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)
	assert.NotEmpty(t, metadataPath)

	// 3. Read snapshot
	readSnapshot, err := reader.ReadSnapshot(context.Background(), 100, 1, true)
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
	}

	// 7. Verify indexes information
	assert.Len(t, readSnapshot.Indexes, len(snapshotData.Indexes))
	if len(readSnapshot.Indexes) > 0 {
		assert.Equal(t, snapshotData.Indexes[0].IndexID, readSnapshot.Indexes[0].IndexID)
		assert.Equal(t, snapshotData.Indexes[0].IndexName, readSnapshot.Indexes[0].IndexName)
	}

	// 8. Cleanup
	err = writer.Drop(context.Background(), 100, 1)
	assert.NoError(t, err)
}

// =========================== New Fields Tests ===========================

func TestSnapshot_NewFields_Serialization(t *testing.T) {
	// Test that all new fields (Bm25Statslogs, TextIndexFiles, JsonKeyIndexFiles,
	// StartPosition, DmlPosition, StorageVersion, IsSorted) are properly serialized
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)
	reader := NewSnapshotReader(cm)

	snapshotData := createTestSnapshotData()

	// 1. Save snapshot
	_, err := writer.Save(context.Background(), snapshotData)
	assert.NoError(t, err)

	// 2. Read snapshot back
	readSnapshot, err := reader.ReadSnapshot(context.Background(), 100, 1, true)
	assert.NoError(t, err)
	assert.NotNil(t, readSnapshot)
	assert.Len(t, readSnapshot.Segments, 1)

	original := snapshotData.Segments[0]
	restored := readSnapshot.Segments[0]

	// 3. Verify Bm25Statslogs field
	assert.Equal(t, len(original.Bm25Statslogs), len(restored.Bm25Statslogs), "Bm25Statslogs count should match")
	if len(original.Bm25Statslogs) > 0 {
		assert.Equal(t, original.Bm25Statslogs[0].FieldID, restored.Bm25Statslogs[0].FieldID)
		assert.Equal(t, len(original.Bm25Statslogs[0].Binlogs), len(restored.Bm25Statslogs[0].Binlogs))
		if len(original.Bm25Statslogs[0].Binlogs) > 0 {
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogPath, restored.Bm25Statslogs[0].Binlogs[0].LogPath)
			assert.Equal(t, original.Bm25Statslogs[0].Binlogs[0].LogSize, restored.Bm25Statslogs[0].Binlogs[0].LogSize)
		}
	}

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

	// 10. Cleanup
	err = writer.Drop(context.Background(), 100, 1)
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
		avro := convertMsgPositionToAvro(original)
		restored := convertAvroToMsgPosition(avro)

		assert.Equal(t, original.ChannelName, restored.ChannelName)
		assert.Equal(t, original.MsgID, restored.MsgID)
		assert.Equal(t, original.MsgGroup, restored.MsgGroup)
		assert.Equal(t, original.Timestamp, restored.Timestamp)
	})

	// Test MsgPosition nil handling
	t.Run("MsgPosition nil handling", func(t *testing.T) {
		avro := convertMsgPositionToAvro(nil)
		assert.NotNil(t, avro)
		assert.Equal(t, "", avro.ChannelName)
		assert.Equal(t, []byte{}, avro.MsgID)

		restored := convertAvroToMsgPosition(nil)
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
		avro := convertTextIndexStatsToAvro(original)
		restored := convertAvroToTextIndexStats(avro)

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
		avro := convertJsonKeyStatsToAvro(original)
		restored := convertAvroToJsonKeyStats(avro)

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
		avroArray := convertTextIndexMapToAvro(originalMap)
		restoredMap := convertAvroToTextIndexMap(avroArray)

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
		avroArray := convertJsonKeyIndexMapToAvro(originalMap)
		restoredMap := convertAvroToJsonKeyIndexMap(avroArray)

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
