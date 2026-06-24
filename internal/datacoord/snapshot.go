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
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	// SnapshotFormatVersion is the current snapshot format version.
	// This version covers both metadata JSON and Avro manifest formats.
	// Increment when making incompatible schema changes.
	// Version 0: Legacy snapshots without version field (treated as version 1)
	// Version 1: Initial version with format-version field in metadata
	// Version 2: Adds index_store_path_version to vector/scalar index files
	// Version 3: Adds commit_timestamp to ManifestEntry (import/CDC segments)
	// Version 4: Adds child_fields and format to AvroFieldBinlog
	SnapshotFormatVersion = snapshotio.SnapshotFormatVersion
)

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
//   - SegmentIDs/BuildIDs: Pre-computed ID lists for fast reload without reading Avro files
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
	// BuildIDs is a pre-computed list of index build IDs for precise GC protection.
	// Each buildID uniquely identifies an index build task, enabling GC to check
	// if specific index files are referenced by a snapshot without path parsing.
	BuildIDs []int64
}

// =============================================================================
// Section 2: SnapshotWriter - Write Operations
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
		return "", merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", merr.WrapErrServiceInternalMsg("snapshot info cannot be nil")
	}
	collectionID := snapshot.SnapshotInfo.GetCollectionId()
	if collectionID <= 0 {
		return "", merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}
	if snapshot.Collection == nil {
		return "", merr.WrapErrServiceInternalMsg("collection description cannot be nil")
	}

	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", merr.WrapErrServiceInternalMsg("invalid snapshot ID: %d", snapshotID)
	}
	manifestDir, metadataPath := GetSnapshotPaths(w.chunkManager.RootPath(), collectionID, snapshotID)

	// Step 1: Write each segment to a separate manifest file
	// Each segment gets its own Avro file for fine-grained control and GC
	manifestPaths := make([]string, 0, len(snapshot.Segments))
	for _, segment := range snapshot.Segments {
		manifestPath := GetSegmentManifestPath(manifestDir, segment.GetSegmentId())
		if err := w.writeSegmentManifest(ctx, manifestPath, segment); err != nil {
			return "", merr.WrapErrServiceInternalErr(err, "failed to write manifest for segment %d", segment.GetSegmentId())
		}
		manifestPaths = append(manifestPaths, manifestPath)
	}

	mlog.Info(ctx, "Successfully wrote segment manifest files",
		mlog.Int("numSegments", len(snapshot.Segments)),
		mlog.String("manifestDir", manifestDir))

	// Step 2: Collect StorageV2 manifest paths from segments
	// StorageV2 segments have an additional manifest file for Lance/Arrow format
	storagev2Manifests := make([]*datapb.StorageV2SegmentManifest, 0)
	for _, segment := range snapshot.Segments {
		if segment.GetManifestPath() != "" {
			storagev2Manifests = append(storagev2Manifests, &datapb.StorageV2SegmentManifest{
				SegmentId: segment.GetSegmentId(),
				Manifest:  segment.GetManifestPath(),
			})
		}
	}

	// Step 3: Write metadata file with all manifest paths
	// This file is the entry point for reading the snapshot
	if err := w.writeMetadataFile(ctx, metadataPath, snapshot, manifestPaths, storagev2Manifests); err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "failed to write metadata file")
	}

	mlog.Info(ctx, "Successfully wrote metadata file",
		mlog.String("metadataPath", metadataPath))

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
	binaryData, err := snapshotio.MarshalSegmentManifest(segment)
	if err != nil {
		return err
	}
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
func (w *SnapshotWriter) writeMetadataFile(ctx context.Context, metadataPath string, snapshot *SnapshotData, manifestPaths []string, storagev2Manifests []*datapb.StorageV2SegmentManifest) error {
	// Assemble metadata structure with all snapshot information
	metadata := &datapb.SnapshotMetadata{
		FormatVersion:         int32(SnapshotFormatVersion),
		SnapshotInfo:          snapshot.SnapshotInfo,
		Collection:            snapshot.Collection,
		Indexes:               snapshot.Indexes,
		ManifestList:          manifestPaths,
		Storagev2ManifestList: storagev2Manifests,
		SegmentIds:            snapshot.SegmentIDs,
		BuildIds:              snapshot.BuildIDs,
	}

	// Use protojson for serialization to correctly handle protobuf oneof fields
	opts := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	jsonData, err := opts.Marshal(metadata)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to marshal metadata to JSON")
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
		return merr.WrapErrServiceInternalMsg("metadata file path cannot be empty")
	}

	// Step 1: Read metadata file to discover manifest file paths
	metadata, err := w.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	snapshotID := metadata.GetSnapshotInfo().GetId()

	// Step 2: Remove all segment manifest files
	manifestList := metadata.GetManifestList()
	if len(manifestList) > 0 {
		if err := w.chunkManager.MultiRemove(ctx, manifestList); err != nil {
			return merr.WrapErrServiceInternalErr(err, "failed to remove manifest files")
		}
		mlog.Info(ctx, "Successfully removed manifest files",
			mlog.Int("count", len(manifestList)),
			mlog.Int64("snapshotID", snapshotID))
	}

	// Step 3: Remove the metadata file (entry point)
	if err := w.chunkManager.Remove(ctx, metadataFilePath); err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to remove metadata file")
	}
	mlog.Info(ctx, "Successfully removed metadata file",
		mlog.String("metadataFilePath", metadataFilePath))

	mlog.Info(ctx, "Successfully dropped snapshot",
		mlog.Int64("snapshotID", snapshotID))
	return nil
}

// readMetadataFile reads and deserializes a snapshot metadata JSON file.
// This is a helper method used by Drop to discover manifest files.
func (w *SnapshotWriter) readMetadataFile(ctx context.Context, filePath string) (*datapb.SnapshotMetadata, error) {
	// Read raw JSON content from object storage
	data, err := w.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	return snapshotio.ParseSnapshotMetadata(data)
}

// =============================================================================
// Section 3: SnapshotReader - Read Operations
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
		return nil, merr.WrapErrServiceInternalMsg("metadata file path cannot be empty")
	}

	// Step 1: Read metadata file (always required)
	metadata, err := r.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	// Step 2: Optionally read segment details from manifest files
	var allSegments []*datapb.SegmentDescription
	if includeSegments {
		// Read each segment's manifest file using the format version from metadata
		for _, manifestPath := range metadata.GetManifestList() {
			segments, err := r.readManifestFile(ctx, manifestPath, int(metadata.GetFormatVersion()))
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "failed to read manifest file %s", manifestPath)
			}
			allSegments = append(allSegments, segments...)
		}

		// Step 2.5: Populate StorageV2 manifest paths for Lance/Arrow segments
		if len(metadata.GetStoragev2ManifestList()) > 0 {
			manifestMap := make(map[int64]string)
			for _, m := range metadata.GetStoragev2ManifestList() {
				manifestMap[m.GetSegmentId()] = m.GetManifest()
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
		SnapshotInfo: metadata.GetSnapshotInfo(),
		Collection:   metadata.GetCollection(),
		Segments:     allSegments, // nil if includeSegments=false
		Indexes:      metadata.GetIndexes(),
		// Pre-computed ID lists available even without full segment loading
		SegmentIDs: metadata.GetSegmentIds(),
		BuildIDs:   metadata.GetBuildIds(),
	}

	return snapshotData, nil
}

// readMetadataFile reads and deserializes a snapshot metadata JSON file.
// Helper method used by ReadSnapshot.
func (r *SnapshotReader) readMetadataFile(ctx context.Context, filePath string) (*datapb.SnapshotMetadata, error) {
	// Read raw JSON content from object storage
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	return snapshotio.ParseSnapshotMetadataWithVersionCheck(data)
}

// readManifestFile reads and parses a segment manifest Avro file.
// Each manifest file contains an array of ManifestEntry records (typically one per file).
// Only segments with status "ADDED" are included in the returned list.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - filePath: Full path to the Avro manifest file
//   - formatVersion: The snapshot format version (determines which schema to use)
//
// Returns:
//   - []*datapb.SegmentDescription: List of segment descriptions from this manifest
//   - error: Any read or parse failure
func (r *SnapshotReader) readManifestFile(ctx context.Context, filePath string, formatVersion int) ([]*datapb.SegmentDescription, error) {
	// Read raw Avro content from object storage
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to read manifest file")
	}

	segment, err := snapshotio.ParseSegmentManifest(data, formatVersion)
	if err != nil {
		return nil, err
	}
	return []*datapb.SegmentDescription{segment}, nil
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
		return nil, merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}

	// Construct metadata directory path
	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	// List all files in the metadata directory
	files, _, err := storage.ListAllChunkWithPrefix(ctx, r.chunkManager, metadataDir, false)
	if err != nil {
		return nil, merr.Wrap(storage.ToMilvusIoError(metadataDir, err), "failed to list metadata files")
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
			mlog.Warn(ctx, "Failed to parse metadata file, skipping",
				mlog.String("file", file),
				mlog.Err(err))
			continue
		}

		// Add to results
		snapshots = append(snapshots, metadata.GetSnapshotInfo())
	}

	return snapshots, nil
}
