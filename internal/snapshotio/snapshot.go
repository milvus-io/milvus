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

package snapshotio

import (
	"fmt"
	"strings"
	"sync"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

const (
	// SnapshotFormatVersion is the current snapshot metadata and manifest format.
	SnapshotFormatVersion = 4
)

var (
	manifestSchemaV1Once sync.Once
	manifestSchemaV1     avro.Schema
	manifestSchemaV1Err  error

	manifestSchemaV2Once sync.Once
	manifestSchemaV2     avro.Schema
	manifestSchemaV2Err  error

	manifestSchemaV3Once sync.Once
	manifestSchemaV3     avro.Schema
	manifestSchemaV3Err  error

	manifestSchemaV4Once sync.Once
	manifestSchemaV4     avro.Schema
	manifestSchemaV4Err  error
)

// ManifestSchema returns the current Avro schema for snapshot segment manifests.
func ManifestSchema() (avro.Schema, error) {
	return ManifestSchemaV4()
}

// ManifestSchemaV1 returns the Avro schema used by legacy snapshot manifests.
func ManifestSchemaV1() (avro.Schema, error) {
	manifestSchemaV1Once.Do(func() {
		manifestSchemaV1, manifestSchemaV1Err = avro.Parse(AvroSchemaV1())
	})
	return manifestSchemaV1, manifestSchemaV1Err
}

// ManifestSchemaV2 returns the Avro schema used by version-2 snapshot manifests.
func ManifestSchemaV2() (avro.Schema, error) {
	manifestSchemaV2Once.Do(func() {
		manifestSchemaV2, manifestSchemaV2Err = avro.Parse(AvroSchemaV2())
	})
	return manifestSchemaV2, manifestSchemaV2Err
}

// ManifestSchemaV3 returns the Avro schema used by current snapshot manifests.
func ManifestSchemaV3() (avro.Schema, error) {
	manifestSchemaV3Once.Do(func() {
		manifestSchemaV3, manifestSchemaV3Err = avro.Parse(AvroSchemaV3())
	})
	return manifestSchemaV3, manifestSchemaV3Err
}

// ManifestSchemaV4 returns the Avro schema used by current snapshot manifests.
func ManifestSchemaV4() (avro.Schema, error) {
	manifestSchemaV4Once.Do(func() {
		manifestSchemaV4, manifestSchemaV4Err = avro.Parse(AvroSchemaV4())
	})
	return manifestSchemaV4, manifestSchemaV4Err
}

// ManifestSchemaByVersion returns the Avro schema for a snapshot format version.
func ManifestSchemaByVersion(version int) (avro.Schema, error) {
	switch version {
	case 0, 1:
		return ManifestSchemaV1()
	case 2:
		return ManifestSchemaV2()
	case 3:
		return ManifestSchemaV3()
	case 4:
		return ManifestSchemaV4()
	default:
		return nil, fmt.Errorf("unsupported manifest schema version: %d", version)
	}
}

// ValidateFormatVersion checks if a snapshot metadata version can be read.
func ValidateFormatVersion(version int) error {
	if version == 0 {
		return nil
	}
	if version > SnapshotFormatVersion {
		return fmt.Errorf("snapshot format version %d is too new, current supported version: %d (please upgrade Milvus)",
			version, SnapshotFormatVersion)
	}
	return nil
}

// ParseSnapshotMetadata deserializes a snapshot metadata JSON file.
func ParseSnapshotMetadata(data []byte) (*datapb.SnapshotMetadata, error) {
	metadata := &datapb.SnapshotMetadata{}
	opts := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	if err := opts.Unmarshal(data, metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}
	return metadata, nil
}

// ParseSnapshotMetadataWithVersionCheck deserializes metadata and rejects unsupported versions.
func ParseSnapshotMetadataWithVersionCheck(data []byte) (*datapb.SnapshotMetadata, error) {
	metadata, err := ParseSnapshotMetadata(data)
	if err != nil {
		return nil, err
	}
	if err := ValidateFormatVersion(int(metadata.GetFormatVersion())); err != nil {
		return nil, fmt.Errorf("incompatible snapshot format: %w", err)
	}
	return metadata, nil
}

// ManifestEntry represents a single segment record in a snapshot manifest Avro file.
type ManifestEntry struct {
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
	JSONKeyIndexFiles []AvroJSONKeyIndexEntry `avro:"json_key_index_files"`
	StartPosition     *AvroMsgPosition        `avro:"start_position"`
	DmlPosition       *AvroMsgPosition        `avro:"dml_position"`
	StorageVersion    int64                   `avro:"storage_version"`
	IsSorted          bool                    `avro:"is_sorted"`
	CommitTimestamp   int64                   `avro:"commit_timestamp"`
}

// AvroFieldBinlog represents datapb.FieldBinlog in Avro-compatible format.
type AvroFieldBinlog struct {
	FieldID     int64        `avro:"field_id"`
	ChildFields []int64      `avro:"child_fields"`
	Format      string       `avro:"format"`
	Binlogs     []AvroBinlog `avro:"binlogs"`
}

// AvroBinlog represents datapb.Binlog in Avro-compatible format.
type AvroBinlog struct {
	EntriesNum    int64  `avro:"entries_num"`
	TimestampFrom int64  `avro:"timestamp_from"`
	TimestampTo   int64  `avro:"timestamp_to"`
	LogPath       string `avro:"log_path"`
	LogSize       int64  `avro:"log_size"`
	LogID         int64  `avro:"log_id"`
	MemorySize    int64  `avro:"memory_size"`
}

// AvroIndexFilePathInfo represents indexpb.IndexFilePathInfo in Avro-compatible format.
type AvroIndexFilePathInfo struct {
	SegmentID                 int64              `avro:"segment_id"`
	FieldID                   int64              `avro:"field_id"`
	IndexID                   int64              `avro:"index_id"`
	BuildID                   int64              `avro:"build_id"`
	IndexName                 string             `avro:"index_name"`
	IndexParams               []AvroKeyValuePair `avro:"index_params"`
	IndexFilePaths            []string           `avro:"index_file_paths"`
	SerializedSize            int64              `avro:"serialized_size"`
	IndexVersion              int64              `avro:"index_version"`
	NumRows                   int64              `avro:"num_rows"`
	CurrentIndexVersion       int32              `avro:"current_index_version"`
	CurrentScalarIndexVersion int32              `avro:"current_scalar_index_version"`
	MemSize                   int64              `avro:"mem_size"`
	IndexStorePathVersion     int32              `avro:"index_store_path_version"`
}

// AvroKeyValuePair represents commonpb.KeyValuePair in Avro-compatible format.
type AvroKeyValuePair struct {
	Key   string `avro:"key"`
	Value string `avro:"value"`
}

// AvroMsgPosition represents msgpb.MsgPosition in Avro-compatible format.
type AvroMsgPosition struct {
	ChannelName string `avro:"channel_name"`
	MsgID       []byte `avro:"msg_id"`
	MsgGroup    string `avro:"msg_group"`
	Timestamp   int64  `avro:"timestamp"`
}

// AvroTextIndexStats represents datapb.TextIndexStats in Avro-compatible format.
type AvroTextIndexStats struct {
	FieldID                   int64    `avro:"field_id"`
	Version                   int64    `avro:"version"`
	Files                     []string `avro:"files"`
	LogSize                   int64    `avro:"log_size"`
	MemorySize                int64    `avro:"memory_size"`
	BuildID                   int64    `avro:"build_id"`
	CurrentScalarIndexVersion int32    `avro:"current_scalar_index_version"`
}

// AvroJSONKeyStats represents datapb.JsonKeyStats in Avro-compatible format.
type AvroJSONKeyStats struct {
	FieldID                int64    `avro:"field_id"`
	Version                int64    `avro:"version"`
	Files                  []string `avro:"files"`
	LogSize                int64    `avro:"log_size"`
	MemorySize             int64    `avro:"memory_size"`
	BuildID                int64    `avro:"build_id"`
	JSONKeyStatsDataFormat int64    `avro:"json_key_stats_data_format"`
}

// AvroTextIndexEntry wraps AvroTextIndexStats with its field ID.
type AvroTextIndexEntry struct {
	FieldID int64               `avro:"field_id"`
	Stats   *AvroTextIndexStats `avro:"stats"`
}

// AvroJSONKeyIndexEntry wraps AvroJSONKeyStats with its field ID.
type AvroJSONKeyIndexEntry struct {
	FieldID int64             `avro:"field_id"`
	Stats   *AvroJSONKeyStats `avro:"stats"`
}

// MarshalSegmentManifest serializes a segment description as a snapshot manifest.
func MarshalSegmentManifest(segment *datapb.SegmentDescription) ([]byte, error) {
	avroSchema, err := ManifestSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest schema: %w", err)
	}
	data, err := avro.Marshal(avroSchema, SegmentToManifestEntry(segment))
	if err != nil {
		return nil, fmt.Errorf("failed to serialize entry to avro: %w", err)
	}
	return data, nil
}

// ParseSegmentManifest deserializes a snapshot segment manifest Avro file.
func ParseSegmentManifest(data []byte, formatVersion int) (*datapb.SegmentDescription, error) {
	avroSchema, err := ManifestSchemaByVersion(formatVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest schema for version %d: %w", formatVersion, err)
	}
	var record ManifestEntry
	if err := avro.Unmarshal(avroSchema, data, &record); err != nil {
		return nil, fmt.Errorf("failed to parse avro data: %w", err)
	}
	return ManifestEntryToSegment(record), nil
}

// SegmentToManifestEntry converts a protobuf SegmentDescription to Avro format.
func SegmentToManifestEntry(segment *datapb.SegmentDescription) ManifestEntry {
	var avroBinlogFiles []AvroFieldBinlog
	for _, binlog := range segment.GetBinlogs() {
		avroBinlogFiles = append(avroBinlogFiles, FieldBinlogToAvro(binlog))
	}

	var avroDeltalogFiles []AvroFieldBinlog
	for _, deltalog := range segment.GetDeltalogs() {
		avroDeltalogFiles = append(avroDeltalogFiles, FieldBinlogToAvro(deltalog))
	}

	var avroStatslogFiles []AvroFieldBinlog
	for _, statslog := range segment.GetStatslogs() {
		avroStatslogFiles = append(avroStatslogFiles, FieldBinlogToAvro(statslog))
	}

	var avroBm25StatslogFiles []AvroFieldBinlog
	for _, bm25Statslog := range segment.GetBm25Statslogs() {
		avroBm25StatslogFiles = append(avroBm25StatslogFiles, FieldBinlogToAvro(bm25Statslog))
	}

	var avroIndexFiles []AvroIndexFilePathInfo
	for _, indexFile := range segment.GetIndexFiles() {
		avroIndexFiles = append(avroIndexFiles, IndexFilePathInfoToAvro(indexFile))
	}

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
		TextIndexFiles:    TextIndexMapToAvro(segment.GetTextIndexFiles()),
		JSONKeyIndexFiles: JSONKeyIndexMapToAvro(segment.GetJsonKeyIndexFiles()),
		StartPosition:     MsgPositionToAvro(segment.GetStartPosition()),
		DmlPosition:       MsgPositionToAvro(segment.GetDmlPosition()),
		StorageVersion:    segment.GetStorageVersion(),
		IsSorted:          segment.GetIsSorted(),
		CommitTimestamp:   int64(segment.GetCommitTimestamp()),
	}
}

// ManifestEntryToSegment converts an Avro manifest record to a protobuf segment.
func ManifestEntryToSegment(record ManifestEntry) *datapb.SegmentDescription {
	segment := &datapb.SegmentDescription{
		SegmentId:       record.SegmentID,
		PartitionId:     record.PartitionID,
		SegmentLevel:    datapb.SegmentLevel(record.SegmentLevel),
		ChannelName:     record.ChannelName,
		NumOfRows:       record.NumOfRows,
		StartPosition:   AvroToMsgPosition(record.StartPosition),
		DmlPosition:     AvroToMsgPosition(record.DmlPosition),
		StorageVersion:  record.StorageVersion,
		IsSorted:        record.IsSorted,
		CommitTimestamp: uint64(record.CommitTimestamp),
	}

	for _, binlogFile := range record.BinlogFiles {
		segment.Binlogs = append(segment.Binlogs, AvroToFieldBinlog(binlogFile))
	}
	for _, deltalogFile := range record.DeltalogFiles {
		segment.Deltalogs = append(segment.Deltalogs, AvroToFieldBinlog(deltalogFile))
	}
	for _, statslogFile := range record.StatslogFiles {
		segment.Statslogs = append(segment.Statslogs, AvroToFieldBinlog(statslogFile))
	}
	for _, bm25StatslogFile := range record.Bm25StatslogFiles {
		segment.Bm25Statslogs = append(segment.Bm25Statslogs, AvroToFieldBinlog(bm25StatslogFile))
	}
	for _, indexFile := range record.IndexFiles {
		segment.IndexFiles = append(segment.IndexFiles, AvroToIndexFilePathInfo(indexFile))
	}
	segment.TextIndexFiles = AvroToTextIndexMap(record.TextIndexFiles)
	segment.JsonKeyIndexFiles = AvroToJSONKeyIndexMap(record.JSONKeyIndexFiles)
	return segment
}

// FieldBinlogToAvro converts protobuf FieldBinlog to Avro format.
func FieldBinlogToAvro(fb *datapb.FieldBinlog) AvroFieldBinlog {
	avroFieldBinlog := AvroFieldBinlog{
		FieldID:     fb.GetFieldID(),
		ChildFields: append([]int64(nil), fb.GetChildFields()...),
		Format:      fb.GetFormat(),
		Binlogs:     make([]AvroBinlog, len(fb.GetBinlogs())),
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

// AvroToFieldBinlog converts Avro FieldBinlog back to protobuf format.
func AvroToFieldBinlog(avroFB AvroFieldBinlog) *datapb.FieldBinlog {
	fieldBinlog := &datapb.FieldBinlog{
		FieldID:     avroFB.FieldID,
		ChildFields: append([]int64(nil), avroFB.ChildFields...),
		Format:      avroFB.Format,
		Binlogs:     make([]*datapb.Binlog, len(avroFB.Binlogs)),
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

// IndexFilePathInfoToAvro converts protobuf IndexFilePathInfo to Avro format.
func IndexFilePathInfoToAvro(info *indexpb.IndexFilePathInfo) AvroIndexFilePathInfo {
	avroInfo := AvroIndexFilePathInfo{
		SegmentID:                 info.GetSegmentID(),
		FieldID:                   info.GetFieldID(),
		IndexID:                   info.GetIndexID(),
		BuildID:                   info.GetBuildID(),
		IndexName:                 info.GetIndexName(),
		IndexFilePaths:            info.GetIndexFilePaths(),
		SerializedSize:            int64(info.GetSerializedSize()),
		IndexVersion:              info.GetIndexVersion(),
		NumRows:                   info.GetNumRows(),
		CurrentIndexVersion:       info.GetCurrentIndexVersion(),
		CurrentScalarIndexVersion: info.GetCurrentScalarIndexVersion(),
		MemSize:                   int64(info.GetMemSize()),
		IndexStorePathVersion:     int32(info.GetIndexStorePathVersion()),
		IndexParams:               make([]AvroKeyValuePair, len(info.GetIndexParams())),
	}
	for i, param := range info.GetIndexParams() {
		avroInfo.IndexParams[i] = AvroKeyValuePair{
			Key:   param.GetKey(),
			Value: param.GetValue(),
		}
	}
	return avroInfo
}

// AvroToIndexFilePathInfo converts Avro IndexFilePathInfo back to protobuf format.
func AvroToIndexFilePathInfo(avroInfo AvroIndexFilePathInfo) *indexpb.IndexFilePathInfo {
	info := &indexpb.IndexFilePathInfo{
		SegmentID:                 avroInfo.SegmentID,
		FieldID:                   avroInfo.FieldID,
		IndexID:                   avroInfo.IndexID,
		BuildID:                   avroInfo.BuildID,
		IndexName:                 avroInfo.IndexName,
		IndexFilePaths:            avroInfo.IndexFilePaths,
		SerializedSize:            uint64(avroInfo.SerializedSize),
		IndexVersion:              avroInfo.IndexVersion,
		NumRows:                   avroInfo.NumRows,
		CurrentIndexVersion:       avroInfo.CurrentIndexVersion,
		CurrentScalarIndexVersion: avroInfo.CurrentScalarIndexVersion,
		MemSize:                   uint64(avroInfo.MemSize),
		IndexStorePathVersion:     indexpb.IndexStorePathVersion(avroInfo.IndexStorePathVersion),
	}
	for _, param := range avroInfo.IndexParams {
		info.IndexParams = append(info.IndexParams, &commonpb.KeyValuePair{
			Key:   param.Key,
			Value: param.Value,
		})
	}
	return info
}

// MsgPositionToAvro converts protobuf MsgPosition to Avro format.
func MsgPositionToAvro(pos *msgpb.MsgPosition) *AvroMsgPosition {
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

// AvroToMsgPosition converts Avro MsgPosition back to protobuf format.
func AvroToMsgPosition(avroPos *AvroMsgPosition) *msgpb.MsgPosition {
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

// TextIndexStatsToAvro converts protobuf TextIndexStats to Avro format.
func TextIndexStatsToAvro(stats *datapb.TextIndexStats) *AvroTextIndexStats {
	if stats == nil {
		return nil
	}
	return &AvroTextIndexStats{
		FieldID:                   stats.GetFieldID(),
		Version:                   stats.GetVersion(),
		Files:                     stats.GetFiles(),
		LogSize:                   stats.GetLogSize(),
		MemorySize:                stats.GetMemorySize(),
		BuildID:                   stats.GetBuildID(),
		CurrentScalarIndexVersion: stats.GetCurrentScalarIndexVersion(),
	}
}

// AvroToTextIndexStats converts Avro TextIndexStats back to protobuf format.
func AvroToTextIndexStats(avroStats *AvroTextIndexStats) *datapb.TextIndexStats {
	if avroStats == nil {
		return nil
	}
	return &datapb.TextIndexStats{
		FieldID:                   avroStats.FieldID,
		Version:                   avroStats.Version,
		Files:                     avroStats.Files,
		LogSize:                   avroStats.LogSize,
		MemorySize:                avroStats.MemorySize,
		BuildID:                   avroStats.BuildID,
		CurrentScalarIndexVersion: avroStats.CurrentScalarIndexVersion,
	}
}

// TextIndexMapToAvro converts protobuf map[int64]*TextIndexStats to Avro entries.
func TextIndexMapToAvro(indexMap map[int64]*datapb.TextIndexStats) []AvroTextIndexEntry {
	var entries []AvroTextIndexEntry
	for fieldID, stats := range indexMap {
		entries = append(entries, AvroTextIndexEntry{
			FieldID: fieldID,
			Stats:   TextIndexStatsToAvro(stats),
		})
	}
	return entries
}

// AvroToTextIndexMap converts Avro text-index entries back to protobuf map format.
func AvroToTextIndexMap(entries []AvroTextIndexEntry) map[int64]*datapb.TextIndexStats {
	indexMap := make(map[int64]*datapb.TextIndexStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = AvroToTextIndexStats(entry.Stats)
	}
	return indexMap
}

// JSONKeyStatsToAvro converts protobuf JsonKeyStats to Avro format.
func JSONKeyStatsToAvro(stats *datapb.JsonKeyStats) *AvroJSONKeyStats {
	if stats == nil {
		return nil
	}
	return &AvroJSONKeyStats{
		FieldID:                stats.GetFieldID(),
		Version:                stats.GetVersion(),
		Files:                  stats.GetFiles(),
		LogSize:                stats.GetLogSize(),
		MemorySize:             stats.GetMemorySize(),
		BuildID:                stats.GetBuildID(),
		JSONKeyStatsDataFormat: stats.GetJsonKeyStatsDataFormat(),
	}
}

// AvroToJSONKeyStats converts Avro JsonKeyStats back to protobuf format.
func AvroToJSONKeyStats(avroStats *AvroJSONKeyStats) *datapb.JsonKeyStats {
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
		JsonKeyStatsDataFormat: avroStats.JSONKeyStatsDataFormat,
	}
}

// JSONKeyIndexMapToAvro converts protobuf map[int64]*JsonKeyStats to Avro entries.
func JSONKeyIndexMapToAvro(indexMap map[int64]*datapb.JsonKeyStats) []AvroJSONKeyIndexEntry {
	var entries []AvroJSONKeyIndexEntry
	for fieldID, stats := range indexMap {
		entries = append(entries, AvroJSONKeyIndexEntry{
			FieldID: fieldID,
			Stats:   JSONKeyStatsToAvro(stats),
		})
	}
	return entries
}

// AvroToJSONKeyIndexMap converts Avro JSON-key-index entries back to protobuf map format.
func AvroToJSONKeyIndexMap(entries []AvroJSONKeyIndexEntry) map[int64]*datapb.JsonKeyStats {
	indexMap := make(map[int64]*datapb.JsonKeyStats)
	for _, entry := range entries {
		indexMap[entry.FieldID] = AvroToJSONKeyStats(entry.Stats)
	}
	return indexMap
}

// ProperAvroSchema returns the V2 Avro schema definition for segment manifests.
func ProperAvroSchema() string {
	return AvroSchemaV2()
}

// AvroSchemaV2 returns the version-2 Avro schema for segment manifests.
func AvroSchemaV2() string {
	return `{
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
								{"name": "mem_size", "type": "long"},
								{"name": "current_scalar_index_version", "type": "int", "default": 0},
								{"name": "index_store_path_version", "type": "int", "default": 0}
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
							"name": "AvroJSONKeyIndexEntry",
							"fields": [
								{"name": "field_id", "type": "long"},
								{
									"name": "stats",
									"type": {
										"type": "record",
										"name": "AvroJSONKeyStats",
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
	}`
}

// AvroSchemaV1 returns the legacy Avro schema without index_store_path_version.
func AvroSchemaV1() string {
	return strings.Replace(AvroSchemaV2(),
		`,
								{"name": "index_store_path_version", "type": "int", "default": 0}`,
		"",
		1)
}

// AvroSchemaV3 returns the current schema with commit_timestamp.
func AvroSchemaV3() string {
	return strings.Replace(AvroSchemaV2(),
		`{"name": "is_sorted", "type": "boolean"},`,
		`{"name": "is_sorted", "type": "boolean"},
				{"name": "commit_timestamp", "type": "long", "default": 0},`,
		1)
}

// AvroSchemaV4 returns the schema with FieldBinlog child fields and format.
func AvroSchemaV4() string {
	return strings.Replace(AvroSchemaV3(),
		`"name": "AvroFieldBinlog",
							"fields": [
								{"name": "field_id", "type": "long"},
								{
									"name": "binlogs",`,
		`"name": "AvroFieldBinlog",
							"fields": [
								{"name": "field_id", "type": "long"},
								{"name": "child_fields", "type": {"type": "array", "items": "long"}, "default": []},
								{"name": "format", "type": "string", "default": ""},
								{
									"name": "binlogs",`,
		1)
}
