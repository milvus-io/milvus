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

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// S3 snapshot storage path constants
const (
	// SnapshotRootPath is the root directory for all snapshots in S3
	SnapshotRootPath = "snapshots"

	// SnapshotMetadataSubPath is the subdirectory for metadata files
	SnapshotMetadataSubPath = "metadata"

	// SnapshotManifestsSubPath is the subdirectory for manifest files
	SnapshotManifestsSubPath = "manifests"
)

var (
	// Cached Avro schemas for better performance
	manifestSchemaOnce sync.Once
	manifestSchema     avro.Schema
	manifestSchemaErr  error
)

// getManifestSchema returns the cached manifest schema
func getManifestSchema() (avro.Schema, error) {
	manifestSchemaOnce.Do(func() {
		manifestSchema, manifestSchemaErr = avro.Parse(getProperAvroSchema())
	})
	return manifestSchema, manifestSchemaErr
}

// --- 1. Go Struct Definitions Based on Protobuf Messages ---

// SnapshotData encapsulates all the information needed to create a snapshot.
// This is the object you need to build from Milvus's internal logic.
type SnapshotData struct {
	SnapshotInfo *datapb.SnapshotInfo          // Use proto definition
	Collection   *datapb.CollectionDescription // Use proto definition
	Segments     []*datapb.SegmentDescription  // Use proto definition
	Indexes      []*indexpb.IndexInfo          // Use proto definition
}

// --- 2. Avro and JSON Serialization Struct Definitions ---

// ManifestEntry is a single record written to a Manifest File.
// It describes the details of a Segment.
type ManifestEntry struct {
	Status            string                  `avro:"status"`
	SnapshotID        int64                   `avro:"snapshot_id"`
	SegmentID         int64                   `avro:"segment_id"`
	PartitionID       int64                   `avro:"partition_id"`
	SegmentLevel      int64                   `avro:"segment_level"`
	BinlogFiles       []AvroFieldBinlog       `avro:"binlog_files"`
	DeltalogFiles     []AvroFieldBinlog       `avro:"deltalog_files"`
	IndexFiles        []AvroIndexFilePathInfo `avro:"index_files"`
	ChannelName       string                  `avro:"channel_name"`
	NumOfRows         int64                   `avro:"num_of_rows"`
	StatslogFiles     []AvroFieldBinlog       `avro:"statslog_files"`
	Bm25StatslogFiles []AvroFieldBinlog       `avro:"bm25_statslog_files"`
	TextIndexFiles    []AvroTextIndexEntry    `avro:"text_index_files"`
	JsonKeyIndexFiles []AvroJsonKeyIndexEntry `avro:"json_key_index_files"`
	StartPosition     *AvroMsgPosition        `avro:"start_position"`
	DmlPosition       *AvroMsgPosition        `avro:"dml_position"`
	StorageVersion    int64                   `avro:"storage_version"`
	IsSorted          bool                    `avro:"is_sorted"`
}

// AvroFieldBinlog represents datapb.FieldBinlog in Avro-compatible format
type AvroFieldBinlog struct {
	FieldID int64        `avro:"field_id"`
	Binlogs []AvroBinlog `avro:"binlogs"`
}

// AvroBinlog represents datapb.Binlog in Avro-compatible format
type AvroBinlog struct {
	EntriesNum    int64  `avro:"entries_num"`
	TimestampFrom int64  `avro:"timestamp_from"`
	TimestampTo   int64  `avro:"timestamp_to"`
	LogPath       string `avro:"log_path"`
	LogSize       int64  `avro:"log_size"`
	LogID         int64  `avro:"log_id"`
	MemorySize    int64  `avro:"memory_size"`
}

// AvroIndexFilePathInfo represents indexpb.IndexFilePathInfo in Avro-compatible format
type AvroIndexFilePathInfo struct {
	SegmentID           int64              `avro:"segment_id"`
	FieldID             int64              `avro:"field_id"`
	IndexID             int64              `avro:"index_id"`
	BuildID             int64              `avro:"build_id"`
	IndexName           string             `avro:"index_name"`
	IndexParams         []AvroKeyValuePair `avro:"index_params"`
	IndexFilePaths      []string           `avro:"index_file_paths"`
	SerializedSize      int64              `avro:"serialized_size"`
	IndexVersion        int64              `avro:"index_version"`
	NumRows             int64              `avro:"num_rows"`
	CurrentIndexVersion int32              `avro:"current_index_version"`
	MemSize             int64              `avro:"mem_size"`
}

// AvroKeyValuePair represents commonpb.KeyValuePair in Avro-compatible format
type AvroKeyValuePair struct {
	Key   string `avro:"key"`
	Value string `avro:"value"`
}

// AvroMsgPosition represents msgpb.MsgPosition in Avro-compatible format
type AvroMsgPosition struct {
	ChannelName string `avro:"channel_name"`
	MsgID       []byte `avro:"msg_id"`
	MsgGroup    string `avro:"msg_group"`
	Timestamp   int64  `avro:"timestamp"`
}

// AvroTextIndexStats represents datapb.TextIndexStats in Avro-compatible format
type AvroTextIndexStats struct {
	FieldID    int64    `avro:"field_id"`
	Version    int64    `avro:"version"`
	Files      []string `avro:"files"`
	LogSize    int64    `avro:"log_size"`
	MemorySize int64    `avro:"memory_size"`
	BuildID    int64    `avro:"build_id"`
}

// AvroJsonKeyStats represents datapb.JsonKeyStats in Avro-compatible format
type AvroJsonKeyStats struct {
	FieldID                int64    `avro:"field_id"`
	Version                int64    `avro:"version"`
	Files                  []string `avro:"files"`
	LogSize                int64    `avro:"log_size"`
	MemorySize             int64    `avro:"memory_size"`
	BuildID                int64    `avro:"build_id"`
	JsonKeyStatsDataFormat int64    `avro:"json_key_stats_data_format"`
}

// AvroTextIndexEntry represents a single entry in the text index map (converted from map to array)
type AvroTextIndexEntry struct {
	FieldID int64               `avro:"field_id"`
	Stats   *AvroTextIndexStats `avro:"stats"`
}

// AvroJsonKeyIndexEntry represents a single entry in the json key index map (converted from map to array)
type AvroJsonKeyIndexEntry struct {
	FieldID int64             `avro:"field_id"`
	Stats   *AvroJsonKeyStats `avro:"stats"`
}

// SnapshotMetadata is the structure that is ultimately written to metadata.json.
type SnapshotMetadata struct {
	SnapshotInfo *datapb.SnapshotInfo          `json:"snapshot-info"`
	Collection   *datapb.CollectionDescription `json:"collection"`
	Indexes      []*indexpb.IndexInfo          `json:"indexes"`
	ManifestList []string                      `json:"manifest-list"`
}

// --- 3. Core Writer ---

/*
S3 Storage Path Structure:

snapshots/{collection_id}/
├── metadata/
│   ├── 00001-{uuid}.json          # Snapshot version 1 metadata (JSON format)
│   ├── 00002-{uuid}.json          # Snapshot version 2 metadata (JSON format)
│   └── ...
│
└── manifests/
    ├── data-file-manifest-{uuid}.avro         # Manifest File containing segment details (Avro format)
    └── ...

Example paths:
- s3://bucket/snapshots/12345/metadata/00001-a1b2c3d4.json
- s3://bucket/snapshots/12345/manifests/data-file-manifest-i9j0k1l2.avro

File Format Details:
- metadata/*.json: JSON format containing snapshot metadata, collection schema, indexes, and manifest file paths array
- manifests/data-file-manifest-*.avro: Avro format containing detailed segment information

Access Pattern:
1. Find the metadata JSON file by snapshot ID (iterate through metadata files)
2. Read the metadata JSON file to get snapshot info and manifest file paths
3. Read manifest Avro files to get segment file paths
*/

// SnapshotWriter is responsible for writing snapshots to S3.
type SnapshotWriter struct {
	chunkManager storage.ChunkManager
}

// NewSnapshotWriter creates a new writer instance.
func NewSnapshotWriter(cm storage.ChunkManager) *SnapshotWriter {
	return &SnapshotWriter{
		chunkManager: cm,
	}
}

// Save is the main entry point, executing the complete snapshot saving logic.
// Returns the S3 path of the metadata file for the saved snapshot.
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
	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", fmt.Errorf("invalid snapshot ID: %d", snapshotID)
	}
	if snapshot.Collection == nil {
		return "", fmt.Errorf("collection description cannot be nil")
	}

	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	writeUUID := uuid.New().String()

	// Step 1: Write segment information to one or more Manifest files.
	manifestFilePath, _, err := w.writeManifestFile(ctx, basePath, snapshotID, writeUUID, snapshot.Segments)
	if err != nil {
		return "", fmt.Errorf("failed to write manifest file: %w", err)
	}
	log.Info("Successfully wrote manifest file",
		zap.String("manifestFilePath", manifestFilePath))

	// Step 2: Create and write the core metadata.json file with manifest paths.
	manifestPaths := []string{manifestFilePath}
	metadataFilePath, _, err := w.writeMetadataFile(ctx, basePath, writeUUID, snapshot, manifestPaths)
	if err != nil {
		return "", fmt.Errorf("failed to write metadata file: %w", err)
	}
	log.Info("Successfully wrote metadata file",
		zap.String("metadataFilePath", metadataFilePath))

	return metadataFilePath, nil
}

// writeManifestFile writes a single Manifest file containing segment information.
func (w *SnapshotWriter) writeManifestFile(ctx context.Context, basePath string, snapshotID int64, writeUUID string, segments []*datapb.SegmentDescription) (string, int64, error) {
	// Convert segments to ManifestEntry format
	var entries []ManifestEntry
	for _, segment := range segments {
		// Convert Binlogs to Avro format
		var avroBinlogFiles []AvroFieldBinlog
		for _, binlog := range segment.GetBinlogs() {
			avroFieldBinlog := convertFieldBinlogToAvro(binlog)
			avroBinlogFiles = append(avroBinlogFiles, avroFieldBinlog)
		}

		// Convert Deltalogs to Avro format
		var avroDeltalogFiles []AvroFieldBinlog
		for _, deltalog := range segment.GetDeltalogs() {
			avroFieldBinlog := convertFieldBinlogToAvro(deltalog)
			avroDeltalogFiles = append(avroDeltalogFiles, avroFieldBinlog)
		}

		// Convert Statslogs to Avro format
		var avroStatslogFiles []AvroFieldBinlog
		for _, statslog := range segment.GetStatslogs() {
			avroFieldBinlog := convertFieldBinlogToAvro(statslog)
			avroStatslogFiles = append(avroStatslogFiles, avroFieldBinlog)
		}

		// Convert Bm25Statslogs to Avro format
		var avroBm25StatslogFiles []AvroFieldBinlog
		for _, bm25Statslog := range segment.GetBm25Statslogs() {
			avroFieldBinlog := convertFieldBinlogToAvro(bm25Statslog)
			avroBm25StatslogFiles = append(avroBm25StatslogFiles, avroFieldBinlog)
		}

		// Convert IndexFiles to Avro format
		var avroIndexFiles []AvroIndexFilePathInfo
		for _, indexFile := range segment.GetIndexFiles() {
			avroIndexFile := convertIndexFilePathInfoToAvro(indexFile)
			avroIndexFiles = append(avroIndexFiles, avroIndexFile)
		}

		// Convert TextIndexFiles (map to array) to Avro format
		avroTextIndexFiles := convertTextIndexMapToAvro(segment.GetTextIndexFiles())

		// Convert JsonKeyIndexFiles (map to array) to Avro format
		avroJsonKeyIndexFiles := convertJsonKeyIndexMapToAvro(segment.GetJsonKeyIndexFiles())

		entry := ManifestEntry{
			Status:            "ADDED",
			SnapshotID:        snapshotID,
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
		entries = append(entries, entry)
	}

	// Use the cached Avro schema
	avroSchema, err := getManifestSchema()
	if err != nil {
		return "", 0, fmt.Errorf("failed to get manifest schema: %w", err)
	}

	// Serialize to binary
	binaryData, err := avro.Marshal(avroSchema, entries)
	if err != nil {
		return "", 0, fmt.Errorf("failed to serialize entry to avro binary: %w", err)
	}

	// Generate file path
	manifestFileName := fmt.Sprintf("data-file-manifest-%s.avro", writeUUID)
	manifestFilePath := path.Join(basePath, SnapshotManifestsSubPath, manifestFileName)

	// Write to storage
	if err := w.chunkManager.Write(ctx, manifestFilePath, binaryData); err != nil {
		return "", 0, fmt.Errorf("failed to write manifest file to storage: %w", err)
	}

	return manifestFilePath, int64(len(binaryData)), nil
}

// getNextVersion gets the next version number for a snapshot metadata file.
func (w *SnapshotWriter) getNextVersion(ctx context.Context, basePath string) (int, error) {
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)
	files, _, err := storage.ListAllChunkWithPrefix(ctx, w.chunkManager, metadataDir, false)
	if err != nil {
		// If directory doesn't exist or other errors, start from version 1
		return 1, nil
	}

	maxVersion := 0
	for _, file := range files {
		if !strings.HasSuffix(file, ".json") {
			continue
		}
		filename := path.Base(file)
		// Extract version number from filename like "00001-uuid.json"
		parts := strings.Split(filename, "-")
		if len(parts) >= 1 {
			if version, err := strconv.Atoi(parts[0]); err == nil {
				if version > maxVersion {
					maxVersion = version
				}
			}
		}
	}

	return maxVersion + 1, nil
}

// writeMetadataFile writes the main metadata.json file.
// Returns the full file path and the file name.
func (w *SnapshotWriter) writeMetadataFile(ctx context.Context, basePath string, writeUUID string, snapshot *SnapshotData, manifestPaths []string) (string, string, error) {
	// Create metadata structure - save complete SnapshotInfo and Collection
	metadata := &SnapshotMetadata{
		SnapshotInfo: snapshot.SnapshotInfo,
		Collection:   snapshot.Collection,
		Indexes:      snapshot.Indexes,
		ManifestList: manifestPaths,
	}

	// Serialize to JSON
	jsonData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	// Get next version number
	version, err := w.getNextVersion(ctx, basePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get next version: %w", err)
	}

	// Generate file path with version number (padded to 5 digits)
	metadataFileName := fmt.Sprintf("%05d-%s.json", version, writeUUID)
	metadataFilePath := path.Join(basePath, SnapshotMetadataSubPath, metadataFileName)

	// Write to storage
	if err := w.chunkManager.Write(ctx, metadataFilePath, jsonData); err != nil {
		return "", "", fmt.Errorf("failed to write metadata file to storage: %w", err)
	}

	return metadataFilePath, metadataFileName, nil
}

// Drop removes a specific snapshot by collection ID and snapshot ID.
// This function performs the following steps:
// 1. Find and read the metadata file for the specified snapshot
// 2. Get all manifest file paths from metadata
// 3. Remove all manifest files referenced by the snapshot
// 4. Remove the metadata file
func (w *SnapshotWriter) Drop(ctx context.Context, collectionID int64, snapshotID int64) error {
	// Validate input parameters
	if collectionID <= 0 {
		return fmt.Errorf("invalid collection ID: %d", collectionID)
	}
	if snapshotID <= 0 {
		return fmt.Errorf("invalid snapshot ID: %d", snapshotID)
	}

	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))

	// Step 1: Find metadata file for the specific snapshot ID
	metadataFilePath, err := w.findMetadataFileBySnapshotID(ctx, basePath, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to find metadata file for snapshot %d: %w", snapshotID, err)
	}

	// Step 2: Read metadata file to get manifest paths
	metadata, err := w.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Step 3: Remove all manifest files
	if len(metadata.ManifestList) > 0 {
		if err := w.chunkManager.MultiRemove(ctx, metadata.ManifestList); err != nil {
			return fmt.Errorf("failed to remove manifest files: %w", err)
		}
		log.Info("Successfully removed manifest files",
			zap.Int("count", len(metadata.ManifestList)),
			zap.Int64("snapshotID", snapshotID))
	}

	// Step 4: Remove metadata file
	if err := w.chunkManager.Remove(ctx, metadataFilePath); err != nil {
		return fmt.Errorf("failed to remove metadata file: %w", err)
	}
	log.Info("Successfully removed metadata file",
		zap.String("metadataFilePath", metadataFilePath))

	log.Info("Successfully dropped snapshot",
		zap.Int64("snapshotID", snapshotID),
		zap.Int64("collectionID", collectionID))
	return nil
}

// findMetadataFileBySnapshotID finds metadata file for a specific snapshot ID.
func (w *SnapshotWriter) findMetadataFileBySnapshotID(ctx context.Context, basePath string, snapshotID int64) (string, error) {
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	// List all metadata files
	files, _, err := storage.ListAllChunkWithPrefix(ctx, w.chunkManager, metadataDir, true)
	if err != nil {
		return "", fmt.Errorf("failed to list metadata files: %w", err)
	}

	log.Info("debug list metadata files",
		zap.Any("files", files),
		zap.String("metadataDir", metadataDir))

	// Check each metadata file to find the one with matching snapshot ID
	for _, file := range files {
		if !strings.HasSuffix(file, ".json") {
			continue
		}

		// Read and parse metadata file
		metadata, err := w.readMetadataFile(ctx, file)
		if err != nil {
			log.Warn("Failed to parse metadata file, skipping",
				zap.String("file", file),
				zap.Error(err))
			continue
		}

		if metadata.SnapshotInfo.GetId() == snapshotID {
			return file, nil
		}
	}

	return "", fmt.Errorf("snapshot %d not found, checked %d files", snapshotID, len(files))
}

// readMetadataFile reads and parses a metadata JSON file.
func (w *SnapshotWriter) readMetadataFile(ctx context.Context, filePath string) (*SnapshotMetadata, error) {
	// Read file content
	data, err := w.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Parse JSON
	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// --- 4. Snapshot Reader ---

// SnapshotReader is responsible for reading snapshots from S3.
type SnapshotReader struct {
	chunkManager storage.ChunkManager
}

// NewSnapshotReader creates a new reader instance.
func NewSnapshotReader(cm storage.ChunkManager) *SnapshotReader {
	return &SnapshotReader{
		chunkManager: cm,
	}
}

// ReadSnapshot reads a complete snapshot by collection ID and snapshot ID.
func (r *SnapshotReader) ReadSnapshot(ctx context.Context, collectionID int64, snapshotID int64, includeSegments bool) (*SnapshotData, error) {
	// Validate input parameters
	if collectionID <= 0 {
		return nil, fmt.Errorf("invalid collection ID: %d", collectionID)
	}
	if snapshotID <= 0 {
		return nil, fmt.Errorf("invalid snapshot ID: %d (must be > 0)", snapshotID)
	}

	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))

	// Step 1: Find metadata file for specific snapshot ID
	metadataFilePath, err := r.findMetadataFileBySnapshotID(ctx, basePath, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to find metadata file for snapshot %d: %w", snapshotID, err)
	}

	// Step 2: Read metadata file
	metadata, err := r.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Step 3: Read all manifest files to get segment information
	var allSegments []*datapb.SegmentDescription
	if includeSegments {
		for _, manifestPath := range metadata.ManifestList {
			segments, err := r.readManifestFile(ctx, manifestPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest file %s: %w", manifestPath, err)
			}
			allSegments = append(allSegments, segments...)
		}
	}

	// Step 4: Build SnapshotData - use complete SnapshotInfo and Collection from metadata
	snapshotData := &SnapshotData{
		SnapshotInfo: metadata.SnapshotInfo,
		Collection:   metadata.Collection,
		Segments:     allSegments,
		Indexes:      metadata.Indexes,
	}

	return snapshotData, nil
}

// findMetadataFileBySnapshotID finds metadata file for a specific snapshot ID.
func (r *SnapshotReader) findMetadataFileBySnapshotID(ctx context.Context, basePath string, snapshotID int64) (string, error) {
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	// List all metadata files
	files, _, err := storage.ListAllChunkWithPrefix(ctx, r.chunkManager, metadataDir, true)
	if err != nil {
		return "", fmt.Errorf("failed to list metadata files: %w", err)
	}

	// Check each metadata file to find the one with matching snapshot ID
	for _, file := range files {
		if !strings.HasSuffix(file, ".json") {
			continue
		}

		// Read and parse metadata file
		metadata, err := r.readMetadataFile(ctx, file)
		if err != nil {
			log.Warn("Failed to parse metadata file, skipping",
				zap.String("file", file),
				zap.Error(err))
			continue
		}

		if metadata.SnapshotInfo.GetId() == snapshotID {
			return file, nil
		}
	}

	return "", fmt.Errorf("snapshot %d not found, checked %d files", snapshotID, len(files))
}

// readMetadataFile reads and parses a metadata JSON file.
func (r *SnapshotReader) readMetadataFile(ctx context.Context, filePath string) (*SnapshotMetadata, error) {
	// Read file content
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Parse JSON
	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	return &metadata, nil
}

// readManifestFile reads and parses a manifest Avro file.
func (r *SnapshotReader) readManifestFile(ctx context.Context, filePath string) ([]*datapb.SegmentDescription, error) {
	// Read file content
	data, err := r.chunkManager.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	// Use the cached Avro schema
	avroSchema, err := getManifestSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest schema: %w", err)
	}

	// Parse Avro data
	var manifestEntries []ManifestEntry
	err = avro.Unmarshal(avroSchema, data, &manifestEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to parse avro data: %w", err)
	}

	// Convert to SegmentDescription
	var segments []*datapb.SegmentDescription
	for _, record := range manifestEntries {
		// Only include segments with status "ADDED"
		if record.Status == "ADDED" {
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

			// Convert BinlogFiles from Avro format
			for _, binlogFile := range record.BinlogFiles {
				segment.Binlogs = append(segment.Binlogs, convertAvroToFieldBinlog(binlogFile))
			}

			// Convert DeltalogFiles from Avro format
			for _, deltalogFile := range record.DeltalogFiles {
				segment.Deltalogs = append(segment.Deltalogs, convertAvroToFieldBinlog(deltalogFile))
			}

			// Convert StatslogFiles from Avro format
			for _, statslogFile := range record.StatslogFiles {
				segment.Statslogs = append(segment.Statslogs, convertAvroToFieldBinlog(statslogFile))
			}

			// Convert Bm25StatslogFiles from Avro format
			for _, bm25StatslogFile := range record.Bm25StatslogFiles {
				segment.Bm25Statslogs = append(segment.Bm25Statslogs, convertAvroToFieldBinlog(bm25StatslogFile))
			}

			// Convert IndexFiles from Avro format
			for _, indexFile := range record.IndexFiles {
				segment.IndexFiles = append(segment.IndexFiles, convertAvroToIndexFilePathInfo(indexFile))
			}

			// Convert TextIndexFiles (array to map) from Avro format
			segment.TextIndexFiles = convertAvroToTextIndexMap(record.TextIndexFiles)

			// Convert JsonKeyIndexFiles (array to map) from Avro format
			segment.JsonKeyIndexFiles = convertAvroToJsonKeyIndexMap(record.JsonKeyIndexFiles)

			segments = append(segments, segment)
		}
	}

	return segments, nil
}

// ListSnapshots lists all available snapshots for a collection.
func (r *SnapshotReader) ListSnapshots(ctx context.Context, collectionID int64) ([]*datapb.SnapshotInfo, error) {
	// Validate input parameters
	if collectionID <= 0 {
		return nil, fmt.Errorf("invalid collection ID: %d", collectionID)
	}

	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	// List all metadata files
	files, _, err := storage.ListAllChunkWithPrefix(ctx, r.chunkManager, metadataDir, false)
	if err != nil {
		return nil, fmt.Errorf("failed to list metadata files: %w", err)
	}

	var snapshots []*datapb.SnapshotInfo
	for _, file := range files {
		if !strings.HasSuffix(file, ".json") {
			continue
		}

		// Read and parse metadata file
		metadata, err := r.readMetadataFile(ctx, file)
		if err != nil {
			log.Warn("Failed to parse metadata file, skipping",
				zap.String("file", file),
				zap.Error(err))
			continue
		}

		// Use SnapshotInfo directly from metadata
		snapshots = append(snapshots, metadata.SnapshotInfo)
	}

	return snapshots, nil
}

// --- Helper functions for protobuf to Avro conversion ---

// convertFieldBinlogToAvro converts datapb.FieldBinlog to AvroFieldBinlog
func convertFieldBinlogToAvro(fb *datapb.FieldBinlog) AvroFieldBinlog {
	avroFieldBinlog := AvroFieldBinlog{
		FieldID: fb.GetFieldID(),
		Binlogs: make([]AvroBinlog, len(fb.GetBinlogs())),
	}

	for i, binlog := range fb.GetBinlogs() {
		avroFieldBinlog.Binlogs[i] = AvroBinlog{
			EntriesNum:    binlog.GetEntriesNum(),
			TimestampFrom: int64(binlog.GetTimestampFrom()),
			TimestampTo:   int64(binlog.GetTimestampTo()),
			LogPath:       binlog.GetLogPath(),
			LogSize:       binlog.GetLogSize(),
			LogID:         binlog.GetLogID(),
			MemorySize:    binlog.GetMemorySize(),
		}
	}

	return avroFieldBinlog
}

// convertIndexFilePathInfoToAvro converts indexpb.IndexFilePathInfo to AvroIndexFilePathInfo
func convertIndexFilePathInfoToAvro(info *indexpb.IndexFilePathInfo) AvroIndexFilePathInfo {
	avroInfo := AvroIndexFilePathInfo{
		SegmentID:           info.GetSegmentID(),
		FieldID:             info.GetFieldID(),
		IndexID:             info.GetIndexID(),
		BuildID:             info.GetBuildID(),
		IndexName:           info.GetIndexName(),
		IndexFilePaths:      info.GetIndexFilePaths(),
		SerializedSize:      int64(info.GetSerializedSize()),
		IndexVersion:        info.GetIndexVersion(),
		NumRows:             info.GetNumRows(),
		CurrentIndexVersion: info.GetCurrentIndexVersion(),
		MemSize:             int64(info.GetMemSize()),
		IndexParams:         make([]AvroKeyValuePair, len(info.GetIndexParams())),
	}

	for i, param := range info.GetIndexParams() {
		avroInfo.IndexParams[i] = AvroKeyValuePair{
			Key:   param.GetKey(),
			Value: param.GetValue(),
		}
	}

	return avroInfo
}

// convertAvroToFieldBinlog converts AvroFieldBinlog back to datapb.FieldBinlog
func convertAvroToFieldBinlog(avroFB AvroFieldBinlog) *datapb.FieldBinlog {
	fieldBinlog := &datapb.FieldBinlog{
		FieldID: avroFB.FieldID,
		Binlogs: make([]*datapb.Binlog, len(avroFB.Binlogs)),
	}

	for i, avroBinlog := range avroFB.Binlogs {
		fieldBinlog.Binlogs[i] = &datapb.Binlog{
			EntriesNum:    avroBinlog.EntriesNum,
			TimestampFrom: uint64(avroBinlog.TimestampFrom),
			TimestampTo:   uint64(avroBinlog.TimestampTo),
			LogPath:       avroBinlog.LogPath,
			LogSize:       avroBinlog.LogSize,
			LogID:         avroBinlog.LogID,
			MemorySize:    avroBinlog.MemorySize,
		}
	}

	return fieldBinlog
}

// convertAvroToIndexFilePathInfo converts AvroIndexFilePathInfo back to indexpb.IndexFilePathInfo
func convertAvroToIndexFilePathInfo(avroInfo AvroIndexFilePathInfo) *indexpb.IndexFilePathInfo {
	info := &indexpb.IndexFilePathInfo{
		SegmentID:           avroInfo.SegmentID,
		FieldID:             avroInfo.FieldID,
		IndexID:             avroInfo.IndexID,
		BuildID:             avroInfo.BuildID,
		IndexName:           avroInfo.IndexName,
		IndexFilePaths:      avroInfo.IndexFilePaths,
		SerializedSize:      uint64(avroInfo.SerializedSize),
		IndexVersion:        avroInfo.IndexVersion,
		NumRows:             avroInfo.NumRows,
		CurrentIndexVersion: avroInfo.CurrentIndexVersion,
		MemSize:             uint64(avroInfo.MemSize),
	}

	// Convert IndexParams
	for _, param := range avroInfo.IndexParams {
		info.IndexParams = append(info.IndexParams, &commonpb.KeyValuePair{
			Key:   param.Key,
			Value: param.Value,
		})
	}

	return info
}

// convertMsgPositionToAvro converts msgpb.MsgPosition to AvroMsgPosition
func convertMsgPositionToAvro(pos *msgpb.MsgPosition) *AvroMsgPosition {
	if pos == nil {
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
		Timestamp:   int64(pos.GetTimestamp()),
	}
}

// convertTextIndexStatsToAvro converts datapb.TextIndexStats to AvroTextIndexStats
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

// convertJsonKeyStatsToAvro converts datapb.JsonKeyStats to AvroJsonKeyStats
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

// convertTextIndexMapToAvro converts map[int64]*TextIndexStats to []AvroTextIndexEntry
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

// convertJsonKeyIndexMapToAvro converts map[int64]*JsonKeyStats to []AvroJsonKeyIndexEntry
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

// convertAvroToMsgPosition converts AvroMsgPosition back to msgpb.MsgPosition
func convertAvroToMsgPosition(avroPos *AvroMsgPosition) *msgpb.MsgPosition {
	if avroPos == nil {
		return nil
	}
	return &msgpb.MsgPosition{
		ChannelName: avroPos.ChannelName,
		MsgID:       avroPos.MsgID,
		MsgGroup:    avroPos.MsgGroup,
		Timestamp:   uint64(avroPos.Timestamp),
	}
}

// convertAvroToTextIndexStats converts AvroTextIndexStats back to datapb.TextIndexStats
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

// convertAvroToJsonKeyStats converts AvroJsonKeyStats back to datapb.JsonKeyStats
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

// convertAvroToTextIndexMap converts []AvroTextIndexEntry back to map[int64]*TextIndexStats
func convertAvroToTextIndexMap(entries []AvroTextIndexEntry) map[int64]*datapb.TextIndexStats {
	indexMap := make(map[int64]*datapb.TextIndexStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = convertAvroToTextIndexStats(entry.Stats)
	}
	return indexMap
}

// convertAvroToJsonKeyIndexMap converts []AvroJsonKeyIndexEntry back to map[int64]*JsonKeyStats
func convertAvroToJsonKeyIndexMap(entries []AvroJsonKeyIndexEntry) map[int64]*datapb.JsonKeyStats {
	indexMap := make(map[int64]*datapb.JsonKeyStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = convertAvroToJsonKeyStats(entry.Stats)
	}
	return indexMap
}

// getProperAvroSchema returns the correct Avro schema for structured data
func getProperAvroSchema() string {
	return `{
		"type": "array", "items": {
			"type": "record",
			"name": "ManifestEntry",
			"fields": [
				{"name": "status", "type": "string"},
				{"name": "snapshot_id", "type": "long"},
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
