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

package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLOBSegmentMetadata(t *testing.T) {
	metadata := NewLOBSegmentMetadata()
	require.NotNil(t, metadata)
	assert.Equal(t, LOBMetadataVersion, metadata.Version)
	assert.NotNil(t, metadata.LOBFields)
	assert.Len(t, metadata.LOBFields, 0)
	assert.Equal(t, 0, metadata.TotalLOBFiles)
	assert.Equal(t, int64(0), metadata.TotalLOBRecords)
	assert.Equal(t, int64(0), metadata.TotalLOBBytes)
}

func TestLOBSegmentMetadata_HasLOBFields(t *testing.T) {
	metadata := NewLOBSegmentMetadata()
	assert.False(t, metadata.HasLOBFields())

	fieldMeta := &LOBFieldMetadata{
		FieldID: 101,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
		},
	}
	metadata.LOBFields[fieldMeta.FieldID] = fieldMeta

	assert.True(t, metadata.HasLOBFields())
}

func TestLOBSegmentMetadata_IsLOBField(t *testing.T) {
	metadata := NewLOBSegmentMetadata()

	fieldMeta := &LOBFieldMetadata{
		FieldID: 101,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
		},
	}
	metadata.LOBFields[fieldMeta.FieldID] = fieldMeta

	assert.True(t, metadata.IsLOBField(101))
	assert.False(t, metadata.IsLOBField(102))
	assert.False(t, metadata.IsLOBField(999))
}

func TestLOBSegmentMetadata_GetFieldMetadata(t *testing.T) {
	metadata := NewLOBSegmentMetadata()

	fieldMeta := &LOBFieldMetadata{
		FieldID: 101,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
		},
		SizeThreshold: 65536,
		RecordCount:   100,
		TotalBytes:    1024000,
	}
	metadata.LOBFields[fieldMeta.FieldID] = fieldMeta

	// Existing field
	retrieved, ok := metadata.GetFieldMetadata(101)
	require.True(t, ok)
	assert.Equal(t, int64(101), retrieved.FieldID)
	assert.Equal(t, []string{"1001"}, retrieved.LOBFiles)
	assert.Equal(t, int64(65536), retrieved.SizeThreshold)
	assert.Equal(t, int64(100), retrieved.RecordCount)
	assert.Equal(t, int64(1024000), retrieved.TotalBytes)

	// Non-existing field
	retrieved, ok = metadata.GetFieldMetadata(999)
	assert.False(t, ok)
	assert.Nil(t, retrieved)
}

func TestEncodeLOBMetadata(t *testing.T) {
	metadata := NewLOBSegmentMetadata()

	fieldMeta := &LOBFieldMetadata{
		FieldID: 101,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
			{FilePath: "1002", LobFileID: 1002, RowCount: 100, ValidRecordCount: 100},
		},
		SizeThreshold: 65536,
		RecordCount:   100,
		TotalBytes:    1024000,
	}
	metadata.LOBFields[fieldMeta.FieldID] = fieldMeta
	metadata.TotalLOBFiles = 2
	metadata.TotalLOBRecords = 100
	metadata.TotalLOBBytes = 1024000

	encoded, err := EncodeLOBMetadata(metadata)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Verify it's valid JSON
	var jsonMap map[string]interface{}
	err = json.Unmarshal([]byte(encoded), &jsonMap)
	require.NoError(t, err)

	// Check key fields
	assert.Equal(t, float64(LOBMetadataVersion), jsonMap["version"])
	assert.NotNil(t, jsonMap["lob_fields"])
	assert.Equal(t, float64(2), jsonMap["total_lob_files"])
	assert.Equal(t, float64(100), jsonMap["total_lob_records"])
	assert.Equal(t, float64(1024000), jsonMap["total_lob_bytes"])
}

func TestDecodeLOBMetadata(t *testing.T) {
	jsonStr := `{
		"version": 1,
		"lob_fields": {
			"101": {
				"field_id": 101,
				"lob_files": [
					{
						"file_path": "1001",
						"lob_file_id": 1001,
						"row_count": 100,
						"valid_record_count": 100
					},
					{
						"file_path": "1002",
						"lob_file_id": 1002,
						"row_count": 100,
						"valid_record_count": 100
					}
				],
			"size_threshold": 65536,
				"record_count": 100,
				"total_bytes": 1024000
			}
		},
		"total_lob_files": 2,
		"total_lob_records": 100,
		"total_lob_bytes": 1024000
	}`

	metadata, err := DecodeLOBMetadata(jsonStr)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	assert.Equal(t, 1, metadata.Version)
	assert.Len(t, metadata.LOBFields, 1)
	assert.Equal(t, 2, metadata.TotalLOBFiles)
	assert.Equal(t, int64(100), metadata.TotalLOBRecords)
	assert.Equal(t, int64(1024000), metadata.TotalLOBBytes)

	fieldMeta, ok := metadata.GetFieldMetadata(101)
	require.True(t, ok)
	assert.Equal(t, int64(101), fieldMeta.FieldID)
	assert.Len(t, fieldMeta.LOBFiles, 2)
	assert.Equal(t, "1001", fieldMeta.LOBFiles[0].FilePath)
	assert.Equal(t, int64(1001), fieldMeta.LOBFiles[0].LobFileID)
	assert.Equal(t, int64(100), fieldMeta.LOBFiles[0].RowCount)
	assert.Equal(t, int64(100), fieldMeta.LOBFiles[0].ValidRecordCount)
	assert.Equal(t, "1002", fieldMeta.LOBFiles[1].FilePath)
	assert.Equal(t, int64(1002), fieldMeta.LOBFiles[1].LobFileID)
	assert.Equal(t, int64(100), fieldMeta.LOBFiles[1].RowCount)
	assert.Equal(t, int64(100), fieldMeta.LOBFiles[1].ValidRecordCount)
	assert.Equal(t, int64(65536), fieldMeta.SizeThreshold)
	assert.Equal(t, int64(100), fieldMeta.RecordCount)
	assert.Equal(t, int64(1024000), fieldMeta.TotalBytes)
}

func TestDecodeLOBMetadata_InvalidJSON(t *testing.T) {
	invalidJSON := `{invalid json`

	metadata, err := DecodeLOBMetadata(invalidJSON)
	require.Error(t, err)
	assert.Nil(t, metadata)
	assert.Contains(t, err.Error(), "failed to unmarshal LOB metadata")
}

func TestLOBMetadata_RoundTrip(t *testing.T) {
	// Create metadata
	original := NewLOBSegmentMetadata()

	field1 := &LOBFieldMetadata{
		FieldID: 101,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
		},
		SizeThreshold: 65536,
		RecordCount:   50,
		TotalBytes:    500000,
	}

	field2 := &LOBFieldMetadata{
		FieldID: 102,
		LOBFiles: []*LOBFileInfo{
			{FilePath: "2001", LobFileID: 2001, RowCount: 100, ValidRecordCount: 100},
			{FilePath: "2002", LobFileID: 2002, RowCount: 100, ValidRecordCount: 100},
		},
		SizeThreshold: 131072,
		RecordCount:   100,
		TotalBytes:    1000000,
	}

	original.LOBFields[field1.FieldID] = field1
	original.LOBFields[field2.FieldID] = field2

	// Encode
	encoded, err := EncodeLOBMetadata(original)
	require.NoError(t, err)

	// Decode
	decoded, err := DecodeLOBMetadata(encoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Version, decoded.Version)
	assert.Equal(t, original.TotalLOBFiles, decoded.TotalLOBFiles)
	assert.Equal(t, original.TotalLOBRecords, decoded.TotalLOBRecords)
	assert.Equal(t, original.TotalLOBBytes, decoded.TotalLOBBytes)
	assert.Len(t, decoded.LOBFields, 2)

	// Check field 101
	decodedField1, ok := decoded.GetFieldMetadata(101)
	require.True(t, ok)
	assert.Equal(t, field1.FieldID, decodedField1.FieldID)
	assert.Equal(t, field1.LOBFiles, decodedField1.LOBFiles)
	assert.Equal(t, field1.SizeThreshold, decodedField1.SizeThreshold)
	assert.Equal(t, field1.RecordCount, decodedField1.RecordCount)
	assert.Equal(t, field1.TotalBytes, decodedField1.TotalBytes)

	// Check field 102
	decodedField2, ok := decoded.GetFieldMetadata(102)
	require.True(t, ok)
	assert.Equal(t, field2.FieldID, decodedField2.FieldID)
	assert.Equal(t, field2.LOBFiles, decodedField2.LOBFiles)
	assert.Equal(t, field2.SizeThreshold, decodedField2.SizeThreshold)
	assert.Equal(t, field2.RecordCount, decodedField2.RecordCount)
	assert.Equal(t, field2.TotalBytes, decodedField2.TotalBytes)
}

func TestLOBFieldMetadata_EmptyLOBFiles(t *testing.T) {
	metadata := NewLOBSegmentMetadata()

	fieldMeta := &LOBFieldMetadata{
		FieldID:       101,
		LOBFiles:      []*LOBFileInfo{},
		SizeThreshold: 65536,
		RecordCount:   0,
		TotalBytes:    0,
	}

	metadata.LOBFields[fieldMeta.FieldID] = fieldMeta

	assert.Equal(t, 0, metadata.TotalLOBFiles)
	assert.Equal(t, int64(0), metadata.TotalLOBRecords)
	assert.Equal(t, int64(0), metadata.TotalLOBBytes)
}

func BenchmarkEncodeLOBMetadata(b *testing.B) {
	metadata := NewLOBSegmentMetadata()

	for i := 0; i < 10; i++ {
		fieldMeta := &LOBFieldMetadata{
			FieldID: int64(100 + i),
			LOBFiles: []*LOBFileInfo{
				{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "1002", LobFileID: 1002, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "1003", LobFileID: 1003, RowCount: 100, ValidRecordCount: 100},
			},
			SizeThreshold: 65536,
			RecordCount:   100,
			TotalBytes:    1024000,
		}
		metadata.LOBFields[fieldMeta.FieldID] = fieldMeta
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeLOBMetadata(metadata)
	}
}

func BenchmarkDecodeLOBMetadata(b *testing.B) {
	metadata := NewLOBSegmentMetadata()

	for i := 0; i < 10; i++ {
		fieldMeta := &LOBFieldMetadata{
			FieldID: int64(100 + i),
			LOBFiles: []*LOBFileInfo{
				{FilePath: "1001", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "1002", LobFileID: 1002, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "1003", LobFileID: 1003, RowCount: 100, ValidRecordCount: 100},
			},
			SizeThreshold: 65536,
			RecordCount:   100,
			TotalBytes:    1024000,
		}
		metadata.LOBFields[fieldMeta.FieldID] = fieldMeta
	}

	encoded, _ := EncodeLOBMetadata(metadata)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeLOBMetadata(encoded)
	}
}
