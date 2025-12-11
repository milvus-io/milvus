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
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/hamba/avro/v2"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// S3 snapshot storage path constants.
// These constants define the directory structure for storing snapshots in object storage (S3/MinIO).
//
// Directory structure:
//
//	snapshots/{collection_id}/
//	├── metadata/           # Contains JSON metadata files for each snapshot
//	│   └── {snapshot_id}.json     # Snapshot metadata including collection schema and manifest references
//	└── manifests/          # Contains Avro manifest files for segment details
//	    └── {snapshot_id}/         # Directory for each snapshot
//	        └── {segment_id}.avro  # Individual segment manifest file
const (
	// SnapshotRootPath is the root directory for all snapshots in object storage.
	SnapshotRootPath = "snapshots"

	// SnapshotMetadataSubPath is the subdirectory under collection path for metadata JSON files.
	SnapshotMetadataSubPath = "metadata"

	// SnapshotManifestsSubPath is the subdirectory under collection path for manifest Avro files.
	SnapshotManifestsSubPath = "manifests"
)

var (
	// manifestSchemaOnce ensures the Avro schema is parsed only once for performance optimization.
	manifestSchemaOnce sync.Once
	// manifestSchema holds the parsed Avro schema used for serializing/deserializing ManifestEntry records.
	manifestSchema avro.Schema
	// manifestSchemaErr stores any error that occurred during schema parsing.
	manifestSchemaErr error
)

// getManifestSchema returns the cached Avro schema for manifest files.
// The schema is parsed only once using sync.Once to avoid repeated parsing overhead.
// This schema defines the structure of ManifestEntry records stored in Avro format.
func getManifestSchema() (avro.Schema, error) {
	manifestSchemaOnce.Do(func() {
		manifestSchema, manifestSchemaErr = avro.Parse(getProperAvroSchema())
	})
	return manifestSchema, manifestSchemaErr
}

// =============================================================================
// Section 1: Core Data Structures
// =============================================================================
// These structs represent the in-memory representation of snapshot data,
// bridging Milvus's internal protobuf messages with the storage format.

// SnapshotData encapsulates all the information needed to create or restore a snapshot.
// It serves as the main data transfer object between Milvus's internal logic and
// the snapshot storage layer.
//
// Usage:
//   - For snapshot creation: Populate all fields from DataCoord's metadata
//   - For snapshot restore: Fields are populated by SnapshotReader from storage
//
// Fields:
//   - SnapshotInfo: Core snapshot metadata (ID, name, timestamp, etc.)
//   - Collection: Full collection schema and properties
//   - Segments: Detailed segment information including binlog paths
//   - Indexes: Index definitions for the collection
//   - SegmentIDs/IndexIDs: Pre-computed ID lists for fast reload without reading Avro files
type SnapshotData struct {
	// SnapshotInfo contains core snapshot metadata from protobuf definition.
	SnapshotInfo *datapb.SnapshotInfo
	// Collection contains the full collection schema and properties.
	Collection *datapb.CollectionDescription
	// Segments contains detailed segment information including all binlog file paths.
	Segments []*datapb.SegmentDescription
	// Indexes contains index definitions for the collection.
	Indexes []*indexpb.IndexInfo

	// SegmentIDs is a pre-computed list of segment IDs for fast reload.
	// Populated from metadata.json when includeSegments=false to avoid reading heavy Avro files.
	// This enables quick DataCoord startup by deferring full segment loading.
	SegmentIDs []int64
	// IndexIDs is a pre-computed list of index IDs for fast reload.
	// Similar purpose as SegmentIDs for optimizing startup performance.
	IndexIDs []int64
}

// =============================================================================
// Section 2: Avro Serialization Structures
// =============================================================================
// These structs define the Avro-compatible format for storing segment manifest data.
// Avro format is chosen for efficient binary serialization and schema evolution support.
// Each struct mirrors a protobuf message but uses Avro-compatible field types and tags.

// ManifestEntry represents a single segment record in a manifest Avro file.
// Each manifest file contains an array of ManifestEntry records.
//
// File structure: Each segment gets its own manifest file at:
//
//	snapshots/{collection_id}/manifests/{snapshot_id}/{segment_id}.avro
type ManifestEntry struct {
	// SegmentID is the unique identifier of the segment.
	SegmentID int64 `avro:"segment_id"`
	// PartitionID is the partition this segment belongs to.
	PartitionID int64 `avro:"partition_id"`
	// SegmentLevel indicates the compaction level (L0, L1, L2, etc.).
	SegmentLevel int64 `avro:"segment_level"`
	// BinlogFiles contains insert binlog file paths organized by field.
	BinlogFiles []AvroFieldBinlog `avro:"binlog_files"`
	// DeltalogFiles contains delete binlog file paths organized by field.
	DeltalogFiles []AvroFieldBinlog `avro:"deltalog_files"`
	// IndexFiles contains index file information for vector and scalar indexes.
	IndexFiles []AvroIndexFilePathInfo `avro:"index_files"`
	// ChannelName is the DML channel this segment subscribes to.
	ChannelName string `avro:"channel_name"`
	// NumOfRows is the total number of rows in this segment.
	NumOfRows int64 `avro:"num_of_rows"`
	// StatslogFiles contains statistics binlog file paths.
	StatslogFiles []AvroFieldBinlog `avro:"statslog_files"`
	// Bm25StatslogFiles contains BM25 statistics for text search.
	Bm25StatslogFiles []AvroFieldBinlog `avro:"bm25_statslog_files"`
	// TextIndexFiles contains text index file information for full-text search.
	TextIndexFiles []AvroTextIndexEntry `avro:"text_index_files"`
	// JsonKeyIndexFiles contains JSON key index file information.
	JsonKeyIndexFiles []AvroJsonKeyIndexEntry `avro:"json_key_index_files"`
	// StartPosition is the message queue position when segment was created.
	StartPosition *AvroMsgPosition `avro:"start_position"`
	// DmlPosition is the last consumed message queue position.
	DmlPosition *AvroMsgPosition `avro:"dml_position"`
	// StorageVersion indicates the storage format version (0=v1, 1=v2).
	StorageVersion int64 `avro:"storage_version"`
	// IsSorted indicates whether the segment data is sorted by primary key.
	IsSorted bool `avro:"is_sorted"`
}

// AvroFieldBinlog represents datapb.FieldBinlog in Avro-compatible format.
// Groups binlog files by field ID, allowing efficient access to field-specific data.
type AvroFieldBinlog struct {
	// FieldID is the unique identifier of the field these binlogs belong to.
	FieldID int64 `avro:"field_id"`
	// Binlogs contains the list of binlog files for this field.
	Binlogs []AvroBinlog `avro:"binlogs"`
}

// AvroBinlog represents datapb.Binlog in Avro-compatible format.
// Contains metadata about a single binlog file in object storage.
type AvroBinlog struct {
	// EntriesNum is the number of entries (rows) in this binlog file.
	EntriesNum int64 `avro:"entries_num"`
	// TimestampFrom is the minimum timestamp of entries in this file.
	TimestampFrom int64 `avro:"timestamp_from"`
	// TimestampTo is the maximum timestamp of entries in this file.
	TimestampTo int64 `avro:"timestamp_to"`
	// LogPath is the full path to the binlog file in object storage.
	LogPath string `avro:"log_path"`
	// LogSize is the size of the binlog file in bytes.
	LogSize int64 `avro:"log_size"`
	// LogID is the unique identifier of this binlog file.
	LogID int64 `avro:"log_id"`
	// MemorySize is the estimated memory consumption when loaded.
	MemorySize int64 `avro:"memory_size"`
}

// AvroIndexFilePathInfo represents indexpb.IndexFilePathInfo in Avro-compatible format.
// Contains all information needed to locate and load an index for a segment.
type AvroIndexFilePathInfo struct {
	// SegmentID is the segment this index belongs to.
	SegmentID int64 `avro:"segment_id"`
	// FieldID is the field this index is built on.
	FieldID int64 `avro:"field_id"`
	// IndexID is the unique identifier of the index definition.
	IndexID int64 `avro:"index_id"`
	// BuildID is the unique identifier of this index build task.
	BuildID int64 `avro:"build_id"`
	// IndexName is the user-defined name of the index.
	IndexName string `avro:"index_name"`
	// IndexParams contains index-specific parameters (e.g., nlist, m for IVF).
	IndexParams []AvroKeyValuePair `avro:"index_params"`
	// IndexFilePaths contains paths to all index files in object storage.
	IndexFilePaths []string `avro:"index_file_paths"`
	// SerializedSize is the total size of all index files in bytes.
	SerializedSize int64 `avro:"serialized_size"`
	// IndexVersion is the version of the index format.
	IndexVersion int64 `avro:"index_version"`
	// NumRows is the number of vectors indexed.
	NumRows int64 `avro:"num_rows"`
	// CurrentIndexVersion is the current index algorithm version.
	CurrentIndexVersion int32 `avro:"current_index_version"`
	// MemSize is the estimated memory consumption when loaded.
	MemSize int64 `avro:"mem_size"`
}

// AvroKeyValuePair represents commonpb.KeyValuePair in Avro-compatible format.
// Used for storing arbitrary key-value configuration parameters.
type AvroKeyValuePair struct {
	Key   string `avro:"key"`
	Value string `avro:"value"`
}

// AvroMsgPosition represents msgpb.MsgPosition in Avro-compatible format.
// Records a position in the message queue for checkpoint/recovery purposes.
type AvroMsgPosition struct {
	// ChannelName is the message queue channel name.
	ChannelName string `avro:"channel_name"`
	// MsgID is the message queue specific message identifier.
	MsgID []byte `avro:"msg_id"`
	// MsgGroup is the consumer group name.
	MsgGroup string `avro:"msg_group"`
	// Timestamp is the message timestamp.
	Timestamp int64 `avro:"timestamp"`
}

// AvroTextIndexStats represents datapb.TextIndexStats in Avro-compatible format.
// Contains statistics and file paths for text/full-text search indexes.
type AvroTextIndexStats struct {
	// FieldID is the field this text index is built on.
	FieldID int64 `avro:"field_id"`
	// Version is the index version for tracking updates.
	Version int64 `avro:"version"`
	// Files contains paths to text index files.
	Files []string `avro:"files"`
	// LogSize is the total size of index files.
	LogSize int64 `avro:"log_size"`
	// MemorySize is the estimated memory when loaded.
	MemorySize int64 `avro:"memory_size"`
	// BuildID is the index build task identifier.
	BuildID int64 `avro:"build_id"`
}

// AvroJsonKeyStats represents datapb.JsonKeyStats in Avro-compatible format.
// Contains statistics and file paths for JSON key indexes.
type AvroJsonKeyStats struct {
	// FieldID is the JSON field this index is built on.
	FieldID int64 `avro:"field_id"`
	// Version is the index version for tracking updates.
	Version int64 `avro:"version"`
	// Files contains paths to JSON key index files.
	Files []string `avro:"files"`
	// LogSize is the total size of index files.
	LogSize int64 `avro:"log_size"`
	// MemorySize is the estimated memory when loaded.
	MemorySize int64 `avro:"memory_size"`
	// BuildID is the index build task identifier.
	BuildID int64 `avro:"build_id"`
	// JsonKeyStatsDataFormat indicates the data format version.
	JsonKeyStatsDataFormat int64 `avro:"json_key_stats_data_format"`
}

// AvroTextIndexEntry wraps AvroTextIndexStats with its field ID.
// Avro doesn't support maps with non-string keys, so we convert map[int64]*TextIndexStats
// to an array of AvroTextIndexEntry for serialization.
type AvroTextIndexEntry struct {
	// FieldID is the map key (duplicated from Stats.FieldID for clarity).
	FieldID int64 `avro:"field_id"`
	// Stats contains the text index statistics.
	Stats *AvroTextIndexStats `avro:"stats"`
}

// AvroJsonKeyIndexEntry wraps AvroJsonKeyStats with its field ID.
// Similar to AvroTextIndexEntry, this converts map[int64]*JsonKeyStats to array format.
type AvroJsonKeyIndexEntry struct {
	// FieldID is the map key (duplicated from Stats.FieldID for clarity).
	FieldID int64 `avro:"field_id"`
	// Stats contains the JSON key index statistics.
	Stats *AvroJsonKeyStats `avro:"stats"`
}

// =============================================================================
// Section 3: JSON Metadata Structures
// =============================================================================
// These structs define the JSON format for snapshot metadata files.
// JSON is used for metadata because it's human-readable and easily debuggable.

// StorageV2SegmentManifest maps a segment ID to its StorageV2 manifest file path.
// StorageV2 is Milvus's newer storage format that uses Lance/Arrow for better performance.
type StorageV2SegmentManifest struct {
	// SegmentID is the unique identifier of the segment.
	SegmentID int64 `json:"segmentID"`
	// Manifest is the path to the StorageV2 manifest file for this segment.
	Manifest string `json:"manifest"`
}

// SnapshotMetadata is the root structure written to the metadata JSON file.
// This file serves as the entry point for reading a snapshot, containing:
//   - Snapshot identification and status information
//   - Collection schema and properties
//   - References to segment manifest files
//   - Pre-computed ID lists for optimized loading
//
// File path: snapshots/{collection_id}/metadata/{snapshot_id}.json
type SnapshotMetadata struct {
	// SnapshotInfo contains core snapshot metadata (ID, name, status, timestamps).
	SnapshotInfo *datapb.SnapshotInfo `json:"snapshot-info"`
	// Collection contains the full collection schema and properties at snapshot time.
	Collection *datapb.CollectionDescription `json:"collection"`
	// Indexes contains index definitions for the collection.
	Indexes []*indexpb.IndexInfo `json:"indexes"`
	// ManifestList contains paths to segment manifest Avro files.
	ManifestList []string `json:"manifest-list"`
	// StorageV2ManifestList contains segment-to-manifest mappings for StorageV2 format.
	StorageV2ManifestList []*StorageV2SegmentManifest `json:"storagev2-manifest-list"`

	// SegmentIDs is a pre-computed list of segment IDs for fast reload.
	// Stored in metadata to avoid reading heavy Avro manifest files during DataCoord startup.
	// Optional: omitted if empty (omitempty).
	SegmentIDs []int64 `json:"segment-ids,omitempty"`
	// IndexIDs is a pre-computed list of index IDs for fast reload.
	// Similar purpose as SegmentIDs for startup optimization.
	// Optional: omitted if empty (omitempty).
	IndexIDs []int64 `json:"index-ids,omitempty"`
}

// =============================================================================
// Section 4: SnapshotWriter - Write Operations
// =============================================================================
// SnapshotWriter handles all write operations for creating and deleting snapshots.
// It implements a 2-phase commit (2PC) protocol for atomic snapshot creation:
//   Phase 1: Write manifest and metadata files to object storage
//   Phase 2: Commit snapshot info to etcd (handled by SnapshotManager)
//
// This design ensures:
//   - Atomic snapshot creation (either fully committed or fully rolled back)
//   - GC-safe orphan cleanup (uncommitted files can be identified and removed)
//   - Consistent snapshot state across distributed components

/*
S3 Storage Path Structure:

snapshots/{collection_id}/
├── metadata/
│   └── {snapshot_id}.json         # Snapshot metadata (JSON format)
│
└── manifests/
    └── {snapshot_id}/             # Directory per snapshot
        ├── {segment_id_1}.avro    # Segment 1 manifest (Avro format)
        ├── {segment_id_2}.avro    # Segment 2 manifest (Avro format)
        └── ...

Example paths:
- s3://bucket/snapshots/12345/metadata/456789.json
- s3://bucket/snapshots/12345/manifests/456789/100001.avro

File Format Details:
- metadata/*.json: JSON format containing snapshot metadata, collection schema,
                   indexes, and references to manifest files
- manifests/{snapshot_id}/*.avro: Avro format containing detailed segment information
                                   (binlogs, deltalogs, indexes, etc.)

Access Pattern (Read):
1. Read metadata JSON file directly using known path (from SnapshotInfo.S3Location)
2. Parse metadata to get manifest file paths list
3. Read manifest Avro files to get detailed segment information

Access Pattern (Write - 2PC):
1. Create snapshot record in etcd with PENDING state (snapshot ID is assigned)
2. Write segment manifest files to manifests/{snapshot_id}/*.avro
3. Write metadata file to metadata/{snapshot_id}.json
4. Commit snapshot info to etcd with S3Location = metadata path and COMPLETED state
5. On rollback, GC cleans up orphan files using the snapshot ID
*/

// SnapshotWriter handles writing snapshot data to object storage.
// Thread-safe: Can be used concurrently for multiple snapshots.
type SnapshotWriter struct {
	// chunkManager provides object storage operations (S3/MinIO/local filesystem).
	chunkManager storage.ChunkManager
}

// NewSnapshotWriter creates a new SnapshotWriter instance.
//
// Parameters:
//   - cm: ChunkManager for object storage operations
//
// Returns:
//   - *SnapshotWriter: Ready-to-use writer instance
func NewSnapshotWriter(cm storage.ChunkManager) *SnapshotWriter {
	return &SnapshotWriter{
		chunkManager: cm,
	}
}

// GetSnapshotPaths computes the storage paths for a snapshot given collection ID and snapshot ID.
// This is a pure function used by both writer and GC for consistent path computation.
//
// The snapshot ID serves as a unique identifier for each snapshot, enabling:
//   - Atomic cleanup of orphan files if commit fails
//   - Isolation between concurrent snapshot operations
//   - Easy identification of snapshot-related files
//   - Intuitive debugging (paths directly correlate with snapshot records)
//
// Parameters:
//   - rootPath: Root path from ChunkManager (e.g., chunkManager.RootPath())
//   - collectionID: The collection this snapshot belongs to
//   - snapshotID: Unique identifier of the snapshot
//
// Returns:
//   - manifestDir: Directory for segment manifest files ({rootPath}/snapshots/{collection_id}/manifests/{snapshot_id}/)
//   - metadataPath: Full path to metadata JSON file ({rootPath}/snapshots/{collection_id}/metadata/{snapshot_id}.json)
func GetSnapshotPaths(rootPath string, collectionID int64, snapshotID int64) (manifestDir, metadataPath string) {
	basePath := path.Join(rootPath, SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	snapshotIDStr := strconv.FormatInt(snapshotID, 10)
	// Manifest directory: snapshots/{collection_id}/manifests/{snapshot_id}/
	manifestDir = path.Join(basePath, SnapshotManifestsSubPath, snapshotIDStr)
	// Metadata file: snapshots/{collection_id}/metadata/{snapshot_id}.json
	metadataPath = path.Join(basePath, SnapshotMetadataSubPath, fmt.Sprintf("%s.json", snapshotIDStr))
	return manifestDir, metadataPath
}

// GetSegmentManifestPath returns the full path for a single segment's manifest file.
//
// Parameters:
//   - manifestDir: The manifest directory from GetSnapshotPaths
//   - segmentID: The segment's unique identifier
//
// Returns:
//   - Full path: {manifestDir}/{segmentID}.avro
func GetSegmentManifestPath(manifestDir string, segmentID int64) string {
	return path.Join(manifestDir, fmt.Sprintf("%d.avro", segmentID))
}

// Save saves snapshot data to object storage.
// This is Phase 1 of the 2-phase commit protocol for atomic snapshot creation.
//
// The snapshot ID is extracted from snapshot.SnapshotInfo.GetId() and used for path generation.
// This makes paths more intuitive for debugging as they directly correlate with snapshot records.
//
// Each segment is written to a separate manifest file, enabling:
//   - Fine-grained GC of orphan segment files
//   - Parallel segment file writing (future optimization)
//   - Incremental snapshot support (future feature)
//
// The snapshot ID is stored in etcd during Phase 2, enabling GC to identify and cleanup
// orphan files if Phase 2 fails.
//
// Write order:
//  1. Write segment manifest Avro files (can fail independently)
//  2. Collect StorageV2 manifest paths if applicable
//  3. Write metadata JSON file (references all manifests)
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - snapshot: Complete snapshot data to persist (must have valid SnapshotInfo with ID)
//
// Returns:
//   - string: Path to metadata file (to be stored in SnapshotInfo.S3Location)
//   - error: Any write failure (partial writes may exist and need GC cleanup)
func (w *SnapshotWriter) Save(ctx context.Context, snapshot *SnapshotData) (string, error) {
	// Validate input parameters
	if snapshot == nil {
		return "", fmt.Errorf("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", fmt.Errorf("snapshot info cannot be nil")
	}
	collectionID := snapshot.SnapshotInfo.GetCollectionId()
	if collectionID <= 0 {
		return "", fmt.Errorf("invalid collection ID: %d", collectionID)
	}
	if snapshot.Collection == nil {
		return "", fmt.Errorf("collection description cannot be nil")
	}

	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", fmt.Errorf("invalid snapshot ID: %d", snapshotID)
	}
	manifestDir, metadataPath := GetSnapshotPaths(w.chunkManager.RootPath(), collectionID, snapshotID)

	// Step 1: Write each segment to a separate manifest file
	// Each segment gets its own Avro file for fine-grained control and GC
	manifestPaths := make([]string, 0, len(snapshot.Segments))
	for _, segment := range snapshot.Segments {
		manifestPath := GetSegmentManifestPath(manifestDir, segment.GetSegmentId())
		if err := w.writeSegmentManifest(ctx, manifestPath, segment); err != nil {
			return "", fmt.Errorf("failed to write manifest for segment %d: %w", segment.GetSegmentId(), err)
		}
		manifestPaths = append(manifestPaths, manifestPath)
	}

	log.Info("Successfully wrote segment manifest files",
		zap.Int("numSegments", len(snapshot.Segments)),
		zap.String("manifestDir", manifestDir))

	// Step 2: Collect StorageV2 manifest paths from segments
	// StorageV2 segments have an additional manifest file for Lance/Arrow format
	storagev2Manifests := make([]*StorageV2SegmentManifest, 0)
	for _, segment := range snapshot.Segments {
		if segment.GetManifestPath() != "" {
			storagev2Manifests = append(storagev2Manifests, &StorageV2SegmentManifest{
				SegmentID: segment.GetSegmentId(),
				Manifest:  segment.GetManifestPath(),
			})
		}
	}

	// Step 3: Write metadata file with all manifest paths
	// This file is the entry point for reading the snapshot
	if err := w.writeMetadataFile(ctx, metadataPath, snapshot, manifestPaths, storagev2Manifests); err != nil {
		return "", fmt.Errorf("failed to write metadata file: %w", err)
	}

	log.Info("Successfully wrote metadata file",
		zap.String("metadataPath", metadataPath))

	return metadataPath, nil
}

// writeSegmentManifest writes a single segment's metadata to an Avro manifest file.
// The manifest file contains all information needed to reconstruct the segment,
// including binlog paths, index files, statistics, and message queue positions.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - manifestPath: Full path where the Avro file will be written
//   - segment: Complete segment description from DataCoord metadata
//
// Returns:
//   - error: Any serialization or write failure
func (w *SnapshotWriter) writeSegmentManifest(ctx context.Context, manifestPath string, segment *datapb.SegmentDescription) error {
	// Convert protobuf segment to Avro-compatible ManifestEntry
	entry := convertSegmentToManifestEntry(segment)

	// Serialize to Avro binary format
	// Note: Single entry wrapped in array for schema consistency
	avroSchema, err := getManifestSchema()
	if err != nil {
		return fmt.Errorf("failed to get manifest schema: %w", err)
	}

	binaryData, err := avro.Marshal(avroSchema, []ManifestEntry{entry})
	if err != nil {
		return fmt.Errorf("failed to serialize entry to avro: %w", err)
	}

	// Write to object storage
	return w.chunkManager.Write(ctx, manifestPath, binaryData)
}

// writeMetadataFile writes the snapshot metadata to a JSON file.
// This file serves as the entry point for reading a snapshot and contains:
//   - Snapshot identification and status
//   - Collection schema and properties
//   - References to segment manifest files
//   - Pre-computed ID lists for fast startup
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - metadataPath: Full path where the JSON file will be written
//   - snapshot: Source snapshot data
//   - manifestPaths: List of paths to segment manifest Avro files
//   - storagev2Manifests: StorageV2 manifest mappings for Lance/Arrow segments
//
// Returns:
//   - error: Any serialization or write failure
func (w *SnapshotWriter) writeMetadataFile(ctx context.Context, metadataPath string, snapshot *SnapshotData, manifestPaths []string, storagev2Manifests []*StorageV2SegmentManifest) error {
	// Assemble metadata structure with all snapshot information
	metadata := &SnapshotMetadata{
		SnapshotInfo:          snapshot.SnapshotInfo,
		Collection:            snapshot.Collection,
		Indexes:               snapshot.Indexes,
		ManifestList:          manifestPaths,
		StorageV2ManifestList: storagev2Manifests,
		SegmentIDs:            snapshot.SegmentIDs,
		IndexIDs:              snapshot.IndexIDs,
	}

	// Serialize to pretty-printed JSON for human readability
	jsonData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	// Write to object storage
	return w.chunkManager.Write(ctx, metadataPath, jsonData)
}

// Drop removes a snapshot and all its associated files from object storage.
// This is used when explicitly deleting a snapshot or during GC cleanup.
//
// Deletion order (important for consistency):
//  1. Read metadata file to discover all manifest file paths
//  2. Remove all manifest files (segment data references)
//  3. Remove the metadata file (snapshot entry point)
//
// Note: This does NOT remove the actual segment data files (binlogs, indexes, etc.)
// as they may be shared with other snapshots or the live collection.
// Segment data cleanup is handled by the main GC process.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - metadataFilePath: Full path to the snapshot's metadata.json file
//     (typically from SnapshotInfo.S3Location)
//
// Returns:
//   - error: Any read or delete failure
func (w *SnapshotWriter) Drop(ctx context.Context, metadataFilePath string) error {
	// Validate input parameter
	if metadataFilePath == "" {
		return fmt.Errorf("metadata file path cannot be empty")
	}

	// Step 1: Read metadata file to discover manifest file paths
	metadata, err := w.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	snapshotID := metadata.SnapshotInfo.GetId()

	// Step 2: Remove all segment manifest files
	if len(metadata.ManifestList) > 0 {
		if err := w.chunkManager.MultiRemove(ctx, metadata.ManifestList); err != nil {
			return fmt.Errorf("failed to remove manifest files: %w", err)
		}
		log.Info("Successfully removed manifest files",
			zap.Int("count", len(metadata.ManifestList)),
			zap.Int64("snapshotID", snapshotID))
	}

	// Step 3: Remove the metadata file (entry point)
	if err := w.chunkManager.Remove(ctx, metadataFilePath); err != nil {
		return fmt.Errorf("failed to remove metadata file: %w", err)
	}
	log.Info("Successfully removed metadata file",
		zap.String("metadataFilePath", metadataFilePath))

	log.Info("Successfully dropped snapshot",
		zap.Int64("snapshotID", snapshotID))
	return nil
}

// readMetadataFile reads and deserializes a snapshot metadata JSON file.
// This is a helper method used by Drop to discover manifest files.
func (w *SnapshotWriter) readMetadataFile(ctx context.Context, filePath string) (*SnapshotMetadata, error) {
	// Read raw JSON content from object storage
	data, err := w.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Deserialize JSON to SnapshotMetadata struct
	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// =============================================================================
// Section 5: SnapshotReader - Read Operations
// =============================================================================
// SnapshotReader handles all read operations for loading snapshot data.
// It supports two loading modes:
//   - Full load (includeSegments=true): Load complete segment details from Avro files
//   - Fast load (includeSegments=false): Load only metadata and ID lists from JSON
//
// The fast load mode is used during DataCoord startup for quick snapshot awareness
// without the overhead of reading potentially large Avro manifest files.

// SnapshotReader handles reading snapshot data from object storage.
// Thread-safe: Can be used concurrently for multiple read operations.
type SnapshotReader struct {
	// chunkManager provides object storage operations (S3/MinIO/local filesystem).
	chunkManager storage.ChunkManager
}

// NewSnapshotReader creates a new SnapshotReader instance.
//
// Parameters:
//   - cm: ChunkManager for object storage operations
//
// Returns:
//   - *SnapshotReader: Ready-to-use reader instance
func NewSnapshotReader(cm storage.ChunkManager) *SnapshotReader {
	return &SnapshotReader{
		chunkManager: cm,
	}
}

// ReadSnapshot reads a snapshot from object storage by its metadata file path.
// This is the primary method for loading snapshot data, supporting two modes:
//
// Full load (includeSegments=true):
//   - Reads metadata JSON file
//   - Reads all segment manifest Avro files
//   - Returns complete SnapshotData with all segment details
//   - Used for snapshot restore operations
//
// Fast load (includeSegments=false):
//   - Reads only metadata JSON file
//   - Returns SnapshotData with pre-computed ID lists but no segment details
//   - Used for DataCoord startup to quickly register snapshots
//   - Segment details can be loaded later on-demand
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - metadataFilePath: Full path to the snapshot's metadata.json file
//   - includeSegments: Whether to load full segment details from Avro files
//
// Returns:
//   - *SnapshotData: Loaded snapshot data (segment details may be nil if not included)
//   - error: Any read or parse failure
func (r *SnapshotReader) ReadSnapshot(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
	// Validate input
	if metadataFilePath == "" {
		return nil, fmt.Errorf("metadata file path cannot be empty")
	}

	// Step 1: Read metadata file (always required)
	metadata, err := r.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Step 2: Optionally read segment details from manifest files
	var allSegments []*datapb.SegmentDescription
	if includeSegments {
		// Read each segment's manifest file
		for _, manifestPath := range metadata.ManifestList {
			segments, err := r.readManifestFile(ctx, manifestPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest file %s: %w", manifestPath, err)
			}
			allSegments = append(allSegments, segments...)
		}

		// Step 2.5: Populate StorageV2 manifest paths for Lance/Arrow segments
		if len(metadata.StorageV2ManifestList) > 0 {
			manifestMap := make(map[int64]string)
			for _, m := range metadata.StorageV2ManifestList {
				manifestMap[m.SegmentID] = m.Manifest
			}
			for _, segment := range allSegments {
				if manifestPath, ok := manifestMap[segment.GetSegmentId()]; ok {
					segment.ManifestPath = manifestPath
				}
			}
		}
	}

	// Step 3: Assemble SnapshotData from loaded components
	snapshotData := &SnapshotData{
		SnapshotInfo: metadata.SnapshotInfo,
		Collection:   metadata.Collection,
		Segments:     allSegments, // nil if includeSegments=false
		Indexes:      metadata.Indexes,
		// Pre-computed ID lists available even without full segment loading
		SegmentIDs: metadata.SegmentIDs,
		IndexIDs:   metadata.IndexIDs,
	}

	return snapshotData, nil
}

// readMetadataFile reads and deserializes a snapshot metadata JSON file.
// Helper method used by ReadSnapshot.
func (r *SnapshotReader) readMetadataFile(ctx context.Context, filePath string) (*SnapshotMetadata, error) {
	// Read raw JSON content from object storage
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Deserialize JSON to SnapshotMetadata struct
	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// readManifestFile reads and parses a segment manifest Avro file.
// Each manifest file contains an array of ManifestEntry records (typically one per file).
// Only segments with status "ADDED" are included in the returned list.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - filePath: Full path to the Avro manifest file
//
// Returns:
//   - []*datapb.SegmentDescription: List of segment descriptions from this manifest
//   - error: Any read or parse failure
func (r *SnapshotReader) readManifestFile(ctx context.Context, filePath string) ([]*datapb.SegmentDescription, error) {
	// Read raw Avro content from object storage
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	// Use cached Avro schema for deserialization
	avroSchema, err := getManifestSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest schema: %w", err)
	}

	// Deserialize Avro binary data to ManifestEntry array
	var manifestEntries []ManifestEntry
	err = avro.Unmarshal(avroSchema, data, &manifestEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to parse avro data: %w", err)
	}

	// Convert ManifestEntry records to protobuf SegmentDescription
	var segments []*datapb.SegmentDescription
	for _, record := range manifestEntries {
		// Convert basic segment fields
		segment := &datapb.SegmentDescription{
			SegmentId:      record.SegmentID,
			PartitionId:    record.PartitionID,
			SegmentLevel:   datapb.SegmentLevel(record.SegmentLevel),
			ChannelName:    record.ChannelName,
			NumOfRows:      record.NumOfRows,
			StartPosition:  convertAvroToMsgPosition(record.StartPosition),
			DmlPosition:    convertAvroToMsgPosition(record.DmlPosition),
			StorageVersion: record.StorageVersion,
			IsSorted:       record.IsSorted,
		}

		// Convert binlog files (insert data)
		for _, binlogFile := range record.BinlogFiles {
			segment.Binlogs = append(segment.Binlogs, convertAvroToFieldBinlog(binlogFile))
		}

		// Convert deltalog files (delete data)
		for _, deltalogFile := range record.DeltalogFiles {
			segment.Deltalogs = append(segment.Deltalogs, convertAvroToFieldBinlog(deltalogFile))
		}

		// Convert statslog files (statistics)
		for _, statslogFile := range record.StatslogFiles {
			segment.Statslogs = append(segment.Statslogs, convertAvroToFieldBinlog(statslogFile))
		}

		// Convert BM25 statslog files (full-text search statistics)
		for _, bm25StatslogFile := range record.Bm25StatslogFiles {
			segment.Bm25Statslogs = append(segment.Bm25Statslogs, convertAvroToFieldBinlog(bm25StatslogFile))
		}

		// Convert index files (vector and scalar indexes)
		for _, indexFile := range record.IndexFiles {
			segment.IndexFiles = append(segment.IndexFiles, convertAvroToIndexFilePathInfo(indexFile))
		}

		// Convert text index files (array back to map)
		segment.TextIndexFiles = convertAvroToTextIndexMap(record.TextIndexFiles)

		// Convert JSON key index files (array back to map)
		segment.JsonKeyIndexFiles = convertAvroToJsonKeyIndexMap(record.JsonKeyIndexFiles)

		segments = append(segments, segment)
	}

	return segments, nil
}

// ListSnapshots discovers and returns all available snapshots for a collection.
// This scans the metadata directory in object storage to find all snapshot metadata files.
//
// Note: This method reads all metadata files which can be slow for collections
// with many snapshots. For normal operations, prefer using cached snapshot info
// from etcd via SnapshotManager.
//
// Use cases:
//   - Recovery: Rebuilding snapshot cache after etcd data loss
//   - Admin tools: Listing snapshots directly from object storage
//   - Debugging: Verifying snapshot consistency between etcd and S3
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - collectionID: The collection to list snapshots for
//
// Returns:
//   - []*datapb.SnapshotInfo: List of snapshot info for all discovered snapshots
//   - error: Any list or read failure
func (r *SnapshotReader) ListSnapshots(ctx context.Context, collectionID int64) ([]*datapb.SnapshotInfo, error) {
	// Validate input parameters
	if collectionID <= 0 {
		return nil, fmt.Errorf("invalid collection ID: %d", collectionID)
	}

	// Construct metadata directory path
	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	// List all files in the metadata directory
	files, _, err := storage.ListAllChunkWithPrefix(ctx, r.chunkManager, metadataDir, false)
	if err != nil {
		return nil, fmt.Errorf("failed to list metadata files: %w", err)
	}

	// Read each metadata file and extract SnapshotInfo
	var snapshots []*datapb.SnapshotInfo
	for _, file := range files {
		// Skip non-JSON files
		if !strings.HasSuffix(file, ".json") {
			continue
		}

		// Read and parse metadata file
		metadata, err := r.readMetadataFile(ctx, file)
		if err != nil {
			// Log warning but continue - don't fail entire list for one bad file
			log.Warn("Failed to parse metadata file, skipping",
				zap.String("file", file),
				zap.Error(err))
			continue
		}

		// Add to results
		snapshots = append(snapshots, metadata.SnapshotInfo)
	}

	return snapshots, nil
}

// =============================================================================
// Section 6: Conversion Helper Functions
// =============================================================================
// These helper functions handle bidirectional conversion between protobuf messages
// and Avro-compatible structs. The conversion is necessary because:
//   - Avro requires specific field types and tags different from protobuf
//   - Some protobuf types (maps with int64 keys) aren't directly supported in Avro
//   - Timestamp types need conversion between uint64 (proto) and int64 (Avro)

// --- Protobuf to Avro Conversion (for writing snapshots) ---

// convertSegmentToManifestEntry converts a protobuf SegmentDescription to Avro ManifestEntry.
// This is the main conversion function used when writing segment manifests.
func convertSegmentToManifestEntry(segment *datapb.SegmentDescription) ManifestEntry {
	// Convert insert binlog files to Avro format
	var avroBinlogFiles []AvroFieldBinlog
	for _, binlog := range segment.GetBinlogs() {
		avroFieldBinlog := convertFieldBinlogToAvro(binlog)
		avroBinlogFiles = append(avroBinlogFiles, avroFieldBinlog)
	}

	// Convert delete binlog files to Avro format
	var avroDeltalogFiles []AvroFieldBinlog
	for _, deltalog := range segment.GetDeltalogs() {
		avroFieldBinlog := convertFieldBinlogToAvro(deltalog)
		avroDeltalogFiles = append(avroDeltalogFiles, avroFieldBinlog)
	}

	// Convert statistics binlog files to Avro format
	var avroStatslogFiles []AvroFieldBinlog
	for _, statslog := range segment.GetStatslogs() {
		avroFieldBinlog := convertFieldBinlogToAvro(statslog)
		avroStatslogFiles = append(avroStatslogFiles, avroFieldBinlog)
	}

	// Convert BM25 statistics files to Avro format
	var avroBm25StatslogFiles []AvroFieldBinlog
	for _, bm25Statslog := range segment.GetBm25Statslogs() {
		avroFieldBinlog := convertFieldBinlogToAvro(bm25Statslog)
		avroBm25StatslogFiles = append(avroBm25StatslogFiles, avroFieldBinlog)
	}

	// Convert index files to Avro format
	var avroIndexFiles []AvroIndexFilePathInfo
	for _, indexFile := range segment.GetIndexFiles() {
		avroIndexFile := convertIndexFilePathInfoToAvro(indexFile)
		avroIndexFiles = append(avroIndexFiles, avroIndexFile)
	}

	// Convert text index map to Avro array format
	avroTextIndexFiles := convertTextIndexMapToAvro(segment.GetTextIndexFiles())

	// Convert JSON key index map to Avro array format
	avroJsonKeyIndexFiles := convertJsonKeyIndexMapToAvro(segment.GetJsonKeyIndexFiles())

	// Assemble the ManifestEntry with all converted fields
	return ManifestEntry{
		SegmentID:         segment.GetSegmentId(),
		PartitionID:       segment.GetPartitionId(),
		SegmentLevel:      int64(segment.GetSegmentLevel()),
		BinlogFiles:       avroBinlogFiles,
		DeltalogFiles:     avroDeltalogFiles,
		IndexFiles:        avroIndexFiles,
		ChannelName:       segment.GetChannelName(),
		NumOfRows:         segment.GetNumOfRows(),
		StatslogFiles:     avroStatslogFiles,
		Bm25StatslogFiles: avroBm25StatslogFiles,
		TextIndexFiles:    avroTextIndexFiles,
		JsonKeyIndexFiles: avroJsonKeyIndexFiles,
		StartPosition:     convertMsgPositionToAvro(segment.GetStartPosition()),
		DmlPosition:       convertMsgPositionToAvro(segment.GetDmlPosition()),
		StorageVersion:    segment.GetStorageVersion(),
		IsSorted:          segment.GetIsSorted(),
	}
}

// convertFieldBinlogToAvro converts protobuf FieldBinlog to Avro format.
// Handles timestamp conversion from uint64 (proto) to int64 (Avro).
func convertFieldBinlogToAvro(fb *datapb.FieldBinlog) AvroFieldBinlog {
	avroFieldBinlog := AvroFieldBinlog{
		FieldID: fb.GetFieldID(),
		Binlogs: make([]AvroBinlog, len(fb.GetBinlogs())),
	}

	for i, binlog := range fb.GetBinlogs() {
		avroFieldBinlog.Binlogs[i] = AvroBinlog{
			EntriesNum:    binlog.GetEntriesNum(),
			TimestampFrom: int64(binlog.GetTimestampFrom()), // uint64 -> int64
			TimestampTo:   int64(binlog.GetTimestampTo()),   // uint64 -> int64
			LogPath:       binlog.GetLogPath(),
			LogSize:       binlog.GetLogSize(),
			LogID:         binlog.GetLogID(),
			MemorySize:    binlog.GetMemorySize(),
		}
	}

	return avroFieldBinlog
}

// convertIndexFilePathInfoToAvro converts protobuf IndexFilePathInfo to Avro format.
// Handles size field conversion from uint64 (proto) to int64 (Avro).
func convertIndexFilePathInfoToAvro(info *indexpb.IndexFilePathInfo) AvroIndexFilePathInfo {
	avroInfo := AvroIndexFilePathInfo{
		SegmentID:           info.GetSegmentID(),
		FieldID:             info.GetFieldID(),
		IndexID:             info.GetIndexID(),
		BuildID:             info.GetBuildID(),
		IndexName:           info.GetIndexName(),
		IndexFilePaths:      info.GetIndexFilePaths(),
		SerializedSize:      int64(info.GetSerializedSize()), // uint64 -> int64
		IndexVersion:        info.GetIndexVersion(),
		NumRows:             info.GetNumRows(),
		CurrentIndexVersion: info.GetCurrentIndexVersion(),
		MemSize:             int64(info.GetMemSize()), // uint64 -> int64
		IndexParams:         make([]AvroKeyValuePair, len(info.GetIndexParams())),
	}

	// Convert key-value parameters
	for i, param := range info.GetIndexParams() {
		avroInfo.IndexParams[i] = AvroKeyValuePair{
			Key:   param.GetKey(),
			Value: param.GetValue(),
		}
	}

	return avroInfo
}

// --- Avro to Protobuf Conversion (for reading snapshots) ---

// convertAvroToFieldBinlog converts Avro FieldBinlog back to protobuf format.
// Handles timestamp conversion from int64 (Avro) to uint64 (proto).
func convertAvroToFieldBinlog(avroFB AvroFieldBinlog) *datapb.FieldBinlog {
	fieldBinlog := &datapb.FieldBinlog{
		FieldID: avroFB.FieldID,
		Binlogs: make([]*datapb.Binlog, len(avroFB.Binlogs)),
	}

	for i, avroBinlog := range avroFB.Binlogs {
		fieldBinlog.Binlogs[i] = &datapb.Binlog{
			EntriesNum:    avroBinlog.EntriesNum,
			TimestampFrom: uint64(avroBinlog.TimestampFrom), // int64 -> uint64
			TimestampTo:   uint64(avroBinlog.TimestampTo),   // int64 -> uint64
			LogPath:       avroBinlog.LogPath,
			LogSize:       avroBinlog.LogSize,
			LogID:         avroBinlog.LogID,
			MemorySize:    avroBinlog.MemorySize,
		}
	}

	return fieldBinlog
}

// convertAvroToIndexFilePathInfo converts Avro IndexFilePathInfo back to protobuf format.
// Handles size field conversion from int64 (Avro) to uint64 (proto).
func convertAvroToIndexFilePathInfo(avroInfo AvroIndexFilePathInfo) *indexpb.IndexFilePathInfo {
	info := &indexpb.IndexFilePathInfo{
		SegmentID:           avroInfo.SegmentID,
		FieldID:             avroInfo.FieldID,
		IndexID:             avroInfo.IndexID,
		BuildID:             avroInfo.BuildID,
		IndexName:           avroInfo.IndexName,
		IndexFilePaths:      avroInfo.IndexFilePaths,
		SerializedSize:      uint64(avroInfo.SerializedSize), // int64 -> uint64
		IndexVersion:        avroInfo.IndexVersion,
		NumRows:             avroInfo.NumRows,
		CurrentIndexVersion: avroInfo.CurrentIndexVersion,
		MemSize:             uint64(avroInfo.MemSize), // int64 -> uint64
	}

	// Convert key-value parameters
	for _, param := range avroInfo.IndexParams {
		info.IndexParams = append(info.IndexParams, &commonpb.KeyValuePair{
			Key:   param.Key,
			Value: param.Value,
		})
	}

	return info
}

// --- Message Position Conversion ---

// convertMsgPositionToAvro converts protobuf MsgPosition to Avro format.
// Returns a zero-value struct for nil input (Avro doesn't support null for required fields).
func convertMsgPositionToAvro(pos *msgpb.MsgPosition) *AvroMsgPosition {
	if pos == nil {
		// Return empty struct instead of nil (Avro schema requires non-null)
		return &AvroMsgPosition{
			ChannelName: "",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   0,
		}
	}
	return &AvroMsgPosition{
		ChannelName: pos.GetChannelName(),
		MsgID:       pos.GetMsgID(),
		MsgGroup:    pos.GetMsgGroup(),
		Timestamp:   int64(pos.GetTimestamp()), // uint64 -> int64
	}
}

// convertAvroToMsgPosition converts Avro MsgPosition back to protobuf format.
func convertAvroToMsgPosition(avroPos *AvroMsgPosition) *msgpb.MsgPosition {
	if avroPos == nil {
		return nil
	}
	return &msgpb.MsgPosition{
		ChannelName: avroPos.ChannelName,
		MsgID:       avroPos.MsgID,
		MsgGroup:    avroPos.MsgGroup,
		Timestamp:   uint64(avroPos.Timestamp), // int64 -> uint64
	}
}

// --- Text Index Conversion ---

// convertTextIndexStatsToAvro converts protobuf TextIndexStats to Avro format.
func convertTextIndexStatsToAvro(stats *datapb.TextIndexStats) *AvroTextIndexStats {
	if stats == nil {
		return nil
	}
	return &AvroTextIndexStats{
		FieldID:    stats.GetFieldID(),
		Version:    stats.GetVersion(),
		Files:      stats.GetFiles(),
		LogSize:    stats.GetLogSize(),
		MemorySize: stats.GetMemorySize(),
		BuildID:    stats.GetBuildID(),
	}
}

// convertAvroToTextIndexStats converts Avro TextIndexStats back to protobuf format.
func convertAvroToTextIndexStats(avroStats *AvroTextIndexStats) *datapb.TextIndexStats {
	if avroStats == nil {
		return nil
	}
	return &datapb.TextIndexStats{
		FieldID:    avroStats.FieldID,
		Version:    avroStats.Version,
		Files:      avroStats.Files,
		LogSize:    avroStats.LogSize,
		MemorySize: avroStats.MemorySize,
		BuildID:    avroStats.BuildID,
	}
}

// convertTextIndexMapToAvro converts protobuf map[int64]*TextIndexStats to Avro array format.
// Avro doesn't support maps with non-string keys, so we convert to array of entries.
func convertTextIndexMapToAvro(indexMap map[int64]*datapb.TextIndexStats) []AvroTextIndexEntry {
	var entries []AvroTextIndexEntry
	for fieldID, stats := range indexMap {
		entries = append(entries, AvroTextIndexEntry{
			FieldID: fieldID,
			Stats:   convertTextIndexStatsToAvro(stats),
		})
	}
	return entries
}

// convertAvroToTextIndexMap converts Avro array back to protobuf map format.
func convertAvroToTextIndexMap(entries []AvroTextIndexEntry) map[int64]*datapb.TextIndexStats {
	indexMap := make(map[int64]*datapb.TextIndexStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = convertAvroToTextIndexStats(entry.Stats)
	}
	return indexMap
}

// --- JSON Key Index Conversion ---

// convertJsonKeyStatsToAvro converts protobuf JsonKeyStats to Avro format.
func convertJsonKeyStatsToAvro(stats *datapb.JsonKeyStats) *AvroJsonKeyStats {
	if stats == nil {
		return nil
	}
	return &AvroJsonKeyStats{
		FieldID:                stats.GetFieldID(),
		Version:                stats.GetVersion(),
		Files:                  stats.GetFiles(),
		LogSize:                stats.GetLogSize(),
		MemorySize:             stats.GetMemorySize(),
		BuildID:                stats.GetBuildID(),
		JsonKeyStatsDataFormat: stats.GetJsonKeyStatsDataFormat(),
	}
}

// convertAvroToJsonKeyStats converts Avro JsonKeyStats back to protobuf format.
func convertAvroToJsonKeyStats(avroStats *AvroJsonKeyStats) *datapb.JsonKeyStats {
	if avroStats == nil {
		return nil
	}
	return &datapb.JsonKeyStats{
		FieldID:                avroStats.FieldID,
		Version:                avroStats.Version,
		Files:                  avroStats.Files,
		LogSize:                avroStats.LogSize,
		MemorySize:             avroStats.MemorySize,
		BuildID:                avroStats.BuildID,
		JsonKeyStatsDataFormat: avroStats.JsonKeyStatsDataFormat,
	}
}

// convertJsonKeyIndexMapToAvro converts protobuf map[int64]*JsonKeyStats to Avro array format.
// Avro doesn't support maps with non-string keys, so we convert to array of entries.
func convertJsonKeyIndexMapToAvro(indexMap map[int64]*datapb.JsonKeyStats) []AvroJsonKeyIndexEntry {
	var entries []AvroJsonKeyIndexEntry
	for fieldID, stats := range indexMap {
		entries = append(entries, AvroJsonKeyIndexEntry{
			FieldID: fieldID,
			Stats:   convertJsonKeyStatsToAvro(stats),
		})
	}
	return entries
}

// convertAvroToJsonKeyIndexMap converts Avro array back to protobuf map format.
func convertAvroToJsonKeyIndexMap(entries []AvroJsonKeyIndexEntry) map[int64]*datapb.JsonKeyStats {
	indexMap := make(map[int64]*datapb.JsonKeyStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = convertAvroToJsonKeyStats(entry.Stats)
	}
	return indexMap
}

// =============================================================================
// Section 7: Avro Schema Definition
// =============================================================================

// getProperAvroSchema returns the Avro schema definition for ManifestEntry records.
// This schema defines the complete structure of segment manifest files, including:
//   - Basic segment metadata (ID, partition, level, channel, row count)
//   - Binlog file references (insert, delete, stats, BM25 stats)
//   - Index file references (vector indexes, text indexes, JSON key indexes)
//   - Message queue positions (start and DML positions)
//   - Storage version and sorting information
//
// Schema design notes:
//   - Uses int64 for timestamps (Avro doesn't have uint64)
//   - Uses arrays instead of maps (Avro maps require string keys)
//   - Nested record types are defined inline and referenced by name
//   - All fields are required (no null unions for simplicity)
func getProperAvroSchema() string {
	return `{
		"type": "array", "items": {
			"type": "record",
			"name": "ManifestEntry",
			"fields": [
				{"name": "segment_id", "type": "long"},
				{"name": "partition_id", "type": "long"},
				{"name": "segment_level", "type": "long"},
				{"name": "channel_name", "type": "string"},
				{"name": "num_of_rows", "type": "long"},
				{
					"name": "start_position",
					"type": {
						"type": "record",
						"name": "AvroMsgPosition",
						"fields": [
							{"name": "channel_name", "type": "string"},
							{"name": "msg_id", "type": "bytes"},
							{"name": "msg_group", "type": "string"},
							{"name": "timestamp", "type": "long"}
						]
					}
				},
				{
					"name": "dml_position",
					"type": "AvroMsgPosition"
				},
				{"name": "storage_version", "type": "long"},
				{"name": "is_sorted", "type": "boolean"},
				{
					"name": "binlog_files",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "AvroFieldBinlog",
							"fields": [
								{"name": "field_id", "type": "long"},
								{
									"name": "binlogs",
									"type": {
										"type": "array",
										"items": {
											"type": "record",
											"name": "AvroBinlog",
											"fields": [
												{"name": "entries_num", "type": "long"},
												{"name": "timestamp_from", "type": "long"},
												{"name": "timestamp_to", "type": "long"},
												{"name": "log_path", "type": "string"},
												{"name": "log_size", "type": "long"},
												{"name": "log_id", "type": "long"},
												{"name": "memory_size", "type": "long"}
											]
										}
									}
								}
							]
						}
					}
				},
				{
					"name": "deltalog_files",
					"type": {
						"type": "array",
						"items": "AvroFieldBinlog"
					}
				},
				{
					"name": "statslog_files",
					"type": {
						"type": "array",
						"items": "AvroFieldBinlog"
					}
				},
				{
					"name": "bm25_statslog_files",
					"type": {
						"type": "array",
						"items": "AvroFieldBinlog"
					}
				},
				{
					"name": "index_files",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "AvroIndexFilePathInfo",
							"fields": [
								{"name": "segment_id", "type": "long"},
								{"name": "field_id", "type": "long"},
								{"name": "index_id", "type": "long"},
								{"name": "build_id", "type": "long"},
								{"name": "index_name", "type": "string"},
								{
									"name": "index_params",
									"type": {
										"type": "array",
										"items": {
											"type": "record",
											"name": "AvroKeyValuePair",
											"fields": [
												{"name": "key", "type": "string"},
												{"name": "value", "type": "string"}
											]
										}
									}
								},
								{"name": "index_file_paths", "type": {"type": "array", "items": "string"}},
								{"name": "serialized_size", "type": "long"},
								{"name": "index_version", "type": "long"},
								{"name": "num_rows", "type": "long"},
								{"name": "current_index_version", "type": "int"},
								{"name": "mem_size", "type": "long"}
							]
						}
					}
				},
				{
					"name": "text_index_files",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "AvroTextIndexEntry",
							"fields": [
								{"name": "field_id", "type": "long"},
								{
									"name": "stats",
									"type": {
										"type": "record",
										"name": "AvroTextIndexStats",
										"fields": [
											{"name": "field_id", "type": "long"},
											{"name": "version", "type": "long"},
											{"name": "files", "type": {"type": "array", "items": "string"}},
											{"name": "log_size", "type": "long"},
											{"name": "memory_size", "type": "long"},
											{"name": "build_id", "type": "long"}
										]
									}
								}
							]
						}
					}
				},
				{
					"name": "json_key_index_files",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "AvroJsonKeyIndexEntry",
							"fields": [
								{"name": "field_id", "type": "long"},
								{
									"name": "stats",
									"type": {
										"type": "record",
										"name": "AvroJsonKeyStats",
										"fields": [
											{"name": "field_id", "type": "long"},
											{"name": "version", "type": "long"},
											{"name": "files", "type": {"type": "array", "items": "string"}},
											{"name": "log_size", "type": "long"},
											{"name": "memory_size", "type": "long"},
											{"name": "build_id", "type": "long"},
											{"name": "json_key_stats_data_format", "type": "long"}
										]
									}
								}
							]
						}
					}
				}
			]
		}
	}`
}
