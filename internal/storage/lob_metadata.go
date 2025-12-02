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
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
)

const LOBMetadataVersion = 1

// LOBFileInfo contains metadata for a single LOB file
type LOBFileInfo struct {
	// the relative path to the LOB file
	FilePath string `json:"file_path"`

	// the unique ID for this LOB file
	LobFileID int64 `json:"lob_file_id"`

	// the number of rows stored in this file (immutable after creation)
	RowCount int64 `json:"row_count"`

	// the number of rows actually referenced by this segment (updated during compaction)
	// initially equals RowCount, decreases when rows are deleted or filtered during compaction
	ValidRecordCount int64 `json:"valid_record_count"`
}

// LOBSegmentMetadata contains LOB metadata for an entire segment.
type LOBSegmentMetadata struct {
	// the version of the LOB metadata format
	Version int `json:"version"`

	// segment identifiers for path reconstruction
	SegmentID    int64 `json:"segment_id"`
	PartitionID  int64 `json:"partition_id"`
	CollectionID int64 `json:"collection_id"`

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

	// the list of LOB files for this field (with path, ID, and row_count)
	LOBFiles []*LOBFileInfo `json:"lob_files"`

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

func (m *LOBSegmentMetadata) HasLOBFields() bool {
	return len(m.LOBFields) > 0
}

func (m *LOBSegmentMetadata) GetFieldMetadata(fieldID int64) (*LOBFieldMetadata, bool) {
	meta, ok := m.LOBFields[fieldID]
	return meta, ok
}

// IsLOBField checks if a field uses LOB storage
func (m *LOBSegmentMetadata) IsLOBField(fieldID int64) bool {
	_, ok := m.LOBFields[fieldID]
	return ok
}

// BuildLOBFilePath reconstructs the full LOB file path from a fileID string
func (m *LOBSegmentMetadata) BuildLOBFilePath(rootPath string, fieldID int64, fileIDStr string) (string, error) {
	fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid LOB file ID: %s", fileIDStr)
	}
	return metautil.BuildLOBLogPath(rootPath, m.CollectionID, m.PartitionID, fieldID, fileID), nil
}
