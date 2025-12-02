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
	"sync"
)

const LOBMetadataVersion = 1

// LOBSegmentMetadata contains LOB metadata for an entire segment.
type LOBSegmentMetadata struct {
	mu sync.RWMutex

	// the version of the LOB metadata format
	Version int `json:"version"`

	// maps fieldID to LOB field metadata
	LOBFields map[int64]*LOBFieldMetadata `json:"lob_fields"`

	// the total number of LOB files for this segment
	TotalLOBFiles int `json:"total_lob_files"`

	// the total number of records stored in LOB files
	TotalLOBRecords int64 `json:"total_lob_records"`

	// the total size of LOB data in bytes
	TotalLOBBytes int64 `json:"total_lob_bytes"`
}

// LOBFieldMetadata contains metadata for a single field that uses LOB storage
type LOBFieldMetadata struct {
	// the field identifier
	FieldID int64 `json:"field_id"`

	// the list of LOB file paths for this field
	// Each entry is a relative path: insert_log/{collection_id}/{partition_id}/{segment_id}/{field_id}/lob/{lob_file_id}
	LOBFiles []string `json:"lob_files"`

	// the size threshold (in bytes) used for this field
	SizeThreshold int64 `json:"size_threshold"`

	// the number of records stored in LOB files for this field
	RecordCount int64 `json:"record_count"`

	// the total size of LOB data for this field
	TotalBytes int64 `json:"total_bytes"`
}

func EncodeLOBMetadata(metadata *LOBSegmentMetadata) (string, error) {
	data, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal LOB metadata: %w", err)
	}
	return string(data), nil
}

func DecodeLOBMetadata(jsonStr string) (*LOBSegmentMetadata, error) {
	var metadata LOBSegmentMetadata
	if err := json.Unmarshal([]byte(jsonStr), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LOB metadata: %w", err)
	}
	return &metadata, nil
}

func NewLOBSegmentMetadata() *LOBSegmentMetadata {
	return &LOBSegmentMetadata{
		Version:   LOBMetadataVersion,
		LOBFields: make(map[int64]*LOBFieldMetadata),
	}
}

// AddFieldMetadata adds metadata for a field
func (m *LOBSegmentMetadata) AddFieldMetadata(fieldMeta *LOBFieldMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LOBFields[fieldMeta.FieldID] = fieldMeta
	m.TotalLOBFiles += len(fieldMeta.LOBFiles)
	m.TotalLOBRecords += fieldMeta.RecordCount
	m.TotalLOBBytes += fieldMeta.TotalBytes
}

func (m *LOBSegmentMetadata) HasLOBFields() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.LOBFields) > 0
}

func (m *LOBSegmentMetadata) GetFieldMetadata(fieldID int64) (*LOBFieldMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.LOBFields[fieldID]
	return meta, ok
}

// IsLOBField checks if a field uses LOB storage
func (m *LOBSegmentMetadata) IsLOBField(fieldID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.LOBFields[fieldID]
	return ok
}
