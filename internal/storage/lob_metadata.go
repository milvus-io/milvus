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
	"fmt"
)

const (
	// LOBMetadataKey is the key used to store LOB metadata in Parquet footer
	LOBMetadataKey = "milvus.lob.metadata"

	// LOBFieldMetadataKeyPrefix is the prefix for field-specific LOB metadata
	// Format: "milvus.lob.field.<fieldID>"
	LOBFieldMetadataKeyPrefix = "milvus.lob.field."
)

// LOBSegmentMetadata contains LOB metadata for an entire segment
// This is stored in the Parquet footer of the main segment file
type LOBSegmentMetadata struct {
	// Version of the LOB metadata format
	Version int `json:"version"`

	// LOBFields maps fieldID to LOB field metadata
	LOBFields map[int64]*LOBFieldMetadata `json:"lob_fields"`

	// TotalLOBFiles is the total number of LOB files for this segment
	TotalLOBFiles int `json:"total_lob_files"`

	// TotalLOBRecords is the total number of records stored in LOB files
	TotalLOBRecords int64 `json:"total_lob_records"`

	// TotalLOBBytes is the total size of LOB data in bytes
	TotalLOBBytes int64 `json:"total_lob_bytes"`
}

// LOBFieldMetadata contains metadata for a single field that uses LOB storage
type LOBFieldMetadata struct {
	// FieldID is the field identifier
	FieldID int64 `json:"field_id"`

	// LOBFiles is the list of LOB file paths for this field
	// Each entry is a relative path: insert_log/{collection_id}/{partition_id}/{segment_id}/{field_id}/lob/{lob_file_id}.parquet
	LOBFiles []string `json:"lob_files"`

	// SizeThreshold is the size threshold (in bytes) used for this field
	SizeThreshold int64 `json:"size_threshold"`

	// RecordCount is the number of records stored in LOB files for this field
	RecordCount int64 `json:"record_count"`

	// TotalBytes is the total size of LOB data for this field
	TotalBytes int64 `json:"total_bytes"`
}

// EncodeLOBMetadata encodes LOB segment metadata to JSON string
func EncodeLOBMetadata(metadata *LOBSegmentMetadata) (string, error) {
	data, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal LOB metadata: %w", err)
	}
	return string(data), nil
}

// DecodeLOBMetadata decodes LOB segment metadata from JSON string
func DecodeLOBMetadata(jsonStr string) (*LOBSegmentMetadata, error) {
	var metadata LOBSegmentMetadata
	if err := json.Unmarshal([]byte(jsonStr), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LOB metadata: %w", err)
	}
	return &metadata, nil
}

// GetLOBFieldMetadataKey returns the metadata key for a specific field
func GetLOBFieldMetadataKey(fieldID int64) string {
	return fmt.Sprintf("%s%d", LOBFieldMetadataKeyPrefix, fieldID)
}

// NewLOBSegmentMetadata creates a new LOB segment metadata
func NewLOBSegmentMetadata() *LOBSegmentMetadata {
	return &LOBSegmentMetadata{
		Version:   1,
		LOBFields: make(map[int64]*LOBFieldMetadata),
	}
}

// AddFieldMetadata adds metadata for a field
func (m *LOBSegmentMetadata) AddFieldMetadata(fieldMeta *LOBFieldMetadata) {
	m.LOBFields[fieldMeta.FieldID] = fieldMeta
	m.TotalLOBFiles += len(fieldMeta.LOBFiles)
	m.TotalLOBRecords += fieldMeta.RecordCount
	m.TotalLOBBytes += fieldMeta.TotalBytes
}

// HasLOBFields returns true if there are any LOB fields
func (m *LOBSegmentMetadata) HasLOBFields() bool {
	return len(m.LOBFields) > 0
}

// GetFieldMetadata returns metadata for a specific field
func (m *LOBSegmentMetadata) GetFieldMetadata(fieldID int64) (*LOBFieldMetadata, bool) {
	meta, ok := m.LOBFields[fieldID]
	return meta, ok
}

// IsLOBField checks if a field uses LOB storage
func (m *LOBSegmentMetadata) IsLOBField(fieldID int64) bool {
	_, ok := m.LOBFields[fieldID]
	return ok
}
