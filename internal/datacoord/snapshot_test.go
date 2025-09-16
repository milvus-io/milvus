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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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
				BinlogFiles: []*datapb.FieldBinlog{
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
				DeltalogFiles: []*datapb.FieldBinlog{
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
	assert.Equal(t, 4, writeCallCount) // manifest, manifest-list, metadata, version-hint
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
	assert.Equal(t, 4, writeCallCount) // Must write 4 files even without segments
}

func TestSnapshotWriter_Drop_Success(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	writer := NewSnapshotWriter(cm)

	// Mock metadata file content
	metadata := &SnapshotMetadata{
		SnapshotID:   1,
		TimestampMs:  1234567890,
		ManifestList: "manifest-list-path",
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
		},
		Properties: map[string]string{
			"name": "test_snapshot",
		},
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

		if filePath == "manifest-list-path" {
			// Return valid Avro serialized data for manifest list
			manifestEntries := []ManifestListEntry{
				{
					ManifestPath:   "manifest1.avro",
					ManifestLength: 1024,
				},
			}
			avroSchema := `{"type":"array","items":{"type":"record","name":"ManifestListEntry","fields":[{"name":"manifest_path","type":"string"},{"name":"manifest_length","type":"long"}]}}`
			schema, _ := avro.Parse(avroSchema)
			data, _ := avro.Marshal(schema, manifestEntries)
			return data, nil
		}
		return []byte("mock-data"), nil
	}).Build()
	defer mockRead.UnPatch()

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
		SnapshotID:   1,
		TimestampMs:  1234567890,
		ManifestList: "manifest-list-path",
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
		},
		Properties: map[string]string{
			"name": "test_snapshot",
		},
	}
	metadataJSON, _ := json.Marshal(metadata)

	// Mock manifest list entries
	manifestEntries := []ManifestListEntry{
		{
			ManifestPath:   "manifest1.avro",
			ManifestLength: 1024,
		},
	}

	// Pre-generate valid Avro data BEFORE setting up mocks
	avroSchema := `{"type":"array","items":{"type":"record","name":"ManifestListEntry","fields":[{"name":"manifest_path","type":"string"},{"name":"manifest_length","type":"long"}]}}`
	schema, _ := avro.Parse(avroSchema)
	validAvroData, _ := avro.Marshal(schema, manifestEntries)

	// Mock file operations
	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/version-hint.txt" {
			return []byte("00001-uuid.json"), nil
		}
		if filePath == "snapshots/100/metadata/00001-uuid.json" {
			return metadataJSON, nil
		}
		if filePath == "manifest-list-path" {
			// Return pre-generated valid Avro data
			return validAvroData, nil
		}
		return []byte("mock-manifest-data"), nil
	}).Build()
	defer mockRead.UnPatch()

	// Mock Avro operations for the actual test
	mockParse := mockey.Mock(avro.Parse).Return(schema, nil).Build()
	defer mockParse.UnPatch()
	mockUnmarshal := mockey.Mock(avro.Unmarshal).To(func(schema avro.Schema, data []byte, v interface{}) error {
		if entries, ok := v.(*[]ManifestListEntry); ok {
			*entries = manifestEntries
		}
		return nil
	}).Build()
	defer mockUnmarshal.UnPatch()

	snapshot, err := reader.ReadSnapshot(context.Background(), 100, 0, true)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, int64(1), snapshot.SnapshotInfo.GetId())
	assert.Equal(t, "test_snapshot", snapshot.SnapshotInfo.GetName())
}

func TestSnapshotReader_ReadSnapshot_SnapshotNotFound(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	reader := NewSnapshotReader(cm)

	mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(func(ctx context.Context, filePath string) ([]byte, error) {
		if filePath == "snapshots/100/metadata/version-hint.txt" {
			return []byte("00001-uuid.json"), nil
		}
		return nil, errors.New("file not found")
	}).Build()
	defer mockRead.UnPatch()

	_, err := reader.ReadSnapshot(context.Background(), 100, 1, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotReader_ListSnapshots_Success(t *testing.T) {
	cm := &storage.LocalChunkManager{}
	reader := NewSnapshotReader(cm)

	metadata1 := &SnapshotMetadata{
		SnapshotID:  1,
		TimestampMs: 1234567890,
		Properties: map[string]string{
			"name":        "snapshot1",
			"description": "first snapshot",
		},
	}
	metadata1JSON, _ := json.Marshal(metadata1)

	metadata2 := &SnapshotMetadata{
		SnapshotID:  2,
		TimestampMs: 1234567900,
		Properties: map[string]string{
			"name":        "snapshot2",
			"description": "second snapshot",
		},
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

func TestConvertInterfaceSliceToStringSlice_Comprehensive(t *testing.T) {
	testCases := []struct {
		name     string
		input    []interface{}
		expected []string
	}{
		{
			name:     "normal strings",
			input:    []interface{}{"string1", "string2", "string3"},
			expected: []string{"string1", "string2", "string3"},
		},
		{
			name:     "empty slice",
			input:    []interface{}{},
			expected: []string{},
		},
		{
			name:     "with non-strings",
			input:    []interface{}{"string1", 123, "string2"},
			expected: []string{"string1", "", "string2"},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: []string{},
		},
		{
			name:     "mixed types",
			input:    []interface{}{"string1", 123, true, nil, "string2"},
			expected: []string{"string1", "", "", "", "string2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := convertInterfaceSliceToStringSlice(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

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
	// Use real ChunkManager
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	writer := NewSnapshotWriter(cm)

	// Write real manifest list
	manifestPath := "test-manifest.avro"
	manifestLength := int64(1024)
	listPath, err := writer.writeManifestList(context.Background(), tempDir, 1, "uuid", manifestPath, manifestLength)
	assert.NoError(t, err)

	// Read and verify
	entries, err := writer.readManifestList(context.Background(), listPath)
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, manifestPath, entries[0].ManifestPath)
	assert.Equal(t, manifestLength, entries[0].ManifestLength)
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
