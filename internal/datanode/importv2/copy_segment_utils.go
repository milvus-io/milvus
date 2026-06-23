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

package importv2

import (
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CopySegmentFileOptions struct {
	Schema        *schemapb.CollectionSchema
	StorageConfig *indexpb.StorageConfig
}

// SegmentFiles organizes source files by type for copy operations.
// InsertBinlogs come from manifest (when storage_version >= StorageV3) or pb (otherwise).
// Other types are always from pb.
type SegmentFiles struct {
	// From manifest (when storage_version >= StorageV3) or pb (when < StorageV3)
	InsertBinlogs []string

	// LOB files at partition level (only for StorageV3+ with TEXT fields)
	LobFiles []string

	// Always from pb
	DeltaBinlogs      []string
	StatsBinlogs      []string
	Bm25Binlogs       []string
	VectorScalarIndex []string
	TextIndex         []string
	JSONKeyIndex      []string
	JSONStats         []string
}

// Copy Mode Implementation for Snapshot/Backup Import
//
// This file implements high-performance segment import by copying files directly
// instead of reading, parsing, and rewriting data. This is specifically designed
// for snapshot restore and backup import scenarios where data format is identical.
//
// IMPLEMENTATION APPROACH:
// 1. Pre-calculate all file path mappings (source -> target) in one pass
// 2. Copy files sequentially using ChunkManager.Copy()
// 3. Preserve all binlog metadata (EntriesNum, Timestamps, LogSize) from source
// 4. Build complete index metadata (vector/scalar, text, JSON) from source
// 5. Generate complete segment metadata with accurate row counts
//
// SUPPORTED FILE TYPES:
// - Binlogs: Insert (required), Delta, Stats, BM25
// - Indexes: Vector/Scalar indexes, Text indexes, JSON Key indexes
//
// WHY THIS APPROACH:
// - Direct file copying is 10-100x faster than data parsing/rewriting
// - Snapshot/backup scenarios guarantee data format compatibility
// - All metadata is preserved from source (binlogs, indexes, row counts, timestamps)
// - Simplified error handling - any copy failure aborts the entire operation
//
// SAFETY:
// - All file operations are validated and logged
// - Copy failures are properly detected and reported with full context
// - Fail-fast behavior prevents partial/inconsistent imports
//
// CopySegmentAndIndexFiles copies all segment files and index files sequentially.
//
// Process flow:
// 1. Collect all source files (from manifest or pb)
// 2. Generate src->dst path mappings
// 3. Execute file copy operations
// 3.5. For manifest segments (StorageV3+), add logical pb path mappings after physical
//      files are copied (needed for metadata generation which expects pb paths)
// 4. Build index metadata from source
// 5. Generate segment metadata with path mappings
// 6. Compress paths for RPC efficiency
// 7. Build result with all metadata
// 8. Transform manifest path if present

// transformManifestPath replaces source IDs in manifest path with target IDs.
//
// Manifest path is a JSON string: {"ver": 2, "base_path": "files/insert_log/coll/part/seg"}
//
// Process:
// 1. Unmarshal JSON to get base_path and version
// 2. Replace collection/partition/segment IDs in base_path
// 3. Marshal back to JSON
func transformManifestPath(
	manifestPath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (string, error) {
	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.Wrap(err, "failed to unmarshal manifest path")
	}

	targetBasePath, err := generateTargetPath(basePath, source, target)
	if err != nil {
		return "", merr.Wrap(err, "failed to generate target base path")
	}

	targetManifestPath := packed.MarshalManifestPath(targetBasePath, version)
	return targetManifestPath, nil
}

type cutoffPK struct {
	int64PK int64
	strPK   string
}

type cutoffDeleteEntry struct {
	pk storage.PrimaryKey
	ts uint64
}

func isDeltaOnlyCutoffSource(source *datapb.CopySegmentSource) bool {
	return source.GetCutoffTs() > 0 &&
		source.GetManifestPath() == "" &&
		len(source.GetInsertBinlogs()) == 0 &&
		hasBinlogPath(source.GetDeltaBinlogs())
}

type cutoffLogAction int

const (
	cutoffLogKeep cutoffLogAction = iota
	cutoffLogDrop
	cutoffLogRewrite
)

type restoreCutoffPlan struct {
	copySource    *datapb.CopySegmentSource
	metaSource    *datapb.CopySegmentSource
	mappings      map[string]string
	copiedFiles   []string
	updateNumRows bool
	importedRows  int64
}

func classifyBinlogForCutoff(binlog *datapb.Binlog, cutoffTs uint64) cutoffLogAction {
	if binlog.GetTimestampTo() > 0 && binlog.GetTimestampTo() <= cutoffTs {
		return cutoffLogKeep
	}
	if binlog.GetTimestampFrom() > 0 && binlog.GetTimestampFrom() > cutoffTs {
		return cutoffLogDrop
	}
	return cutoffLogRewrite
}

func classifyInsertBatchForCutoff(batch []*datapb.Binlog, cutoffTs uint64) cutoffLogAction {
	keep := true
	drop := true
	for _, binlog := range batch {
		if binlog.GetTimestampTo() == 0 || binlog.GetTimestampTo() > cutoffTs {
			keep = false
		}
		if binlog.GetTimestampFrom() == 0 || binlog.GetTimestampFrom() <= cutoffTs {
			drop = false
		}
	}
	if keep {
		return cutoffLogKeep
	}
	if drop {
		return cutoffLogDrop
	}
	return cutoffLogRewrite
}

func writeDeltaLog(
	ctx context.Context,
	cm storage.ChunkManager,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	logID int64,
	deltaPath string,
	deletes []cutoffDeleteEntry,
	version int64,
	storageConfig *indexpb.StorageConfig,
) (*datapb.FieldBinlog, error) {
	if version == storage.StorageV1 && cm == nil {
		return nil, merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for legacy deltalog writer")
	}

	pks := make([]storage.PrimaryKey, 0, len(deletes))
	tss := make([]storage.Timestamp, 0, len(deletes))
	for _, delete := range deletes {
		pks = append(pks, delete.pk)
		tss = append(tss, delete.ts)
	}

	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(pks, tss)
	if err != nil {
		return nil, merr.Wrap(err, "failed to build cutoff delete record")
	}
	defer record.Release()

	writer, err := storage.NewDeltalogWriter(
		ctx,
		target.GetCollectionId(),
		target.GetPartitionId(),
		target.GetSegmentId(),
		logID,
		pkType,
		deltaPath,
		storage.WithVersion(version),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(schema.GetProperties()),
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			if cm == nil {
				return merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for legacy deltalog writer")
			}
			for key, blob := range kvs {
				if err := cm.Write(ctx, key, blob); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create cutoff deltalog writer")
	}
	if err := writer.Write(record); err != nil {
		return nil, merr.Wrap(err, "failed to write cutoff deltalog")
	}
	if err := writer.Close(); err != nil {
		return nil, merr.Wrap(err, "failed to close cutoff deltalog")
	}

	return &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{{
			LogID:         logID,
			EntriesNum:    int64(len(deletes)),
			LogPath:       deltaPath,
			TimestampFrom: tsFrom,
			TimestampTo:   tsTo,
			MemorySize:    int64(writer.GetWrittenUncompressed()),
		}},
	}, nil
}

func prepareNonManifestRestoreCutoff(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
) (*restoreCutoffPlan, error) {
	if schema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("cutoff requires collection schema")
	}
	if storageConfig == nil {
		storageConfig = compaction.CreateStorageConfig()
	}
	pkField, err := findPrimaryKeyField(schema)
	if err != nil {
		return nil, err
	}

	copySource := proto.Clone(source).(*datapb.CopySegmentSource)
	metaSource := proto.Clone(source).(*datapb.CopySegmentSource)
	plan := &restoreCutoffPlan{
		copySource:    copySource,
		metaSource:    metaSource,
		mappings:      make(map[string]string),
		updateNumRows: true,
	}

	insertMutated := false
	if source.GetStorageVersion() == storage.StorageV2 {
		copyInsert, metaInsert, mappings, files, mutated, err := rewriteV2InsertBinlogsForCutoff(
			ctx, source, target, schema, storageConfig)
		if err != nil {
			return nil, err
		}
		copySource.InsertBinlogs = copyInsert
		metaSource.InsertBinlogs = metaInsert
		insertMutated = mutated
		for src, dst := range mappings {
			plan.mappings[src] = dst
		}
		plan.copiedFiles = append(plan.copiedFiles, files...)
	}

	copyDeltas, metaDeltas, mappings, files, retainedDeletes, err := rewriteDeltaFieldBinlogsForCutoff(
		ctx, cm, source, target, schema, pkField.GetDataType(), source.GetDeltaBinlogs(), storageConfig)
	if err != nil {
		return nil, err
	}
	copySource.DeltaBinlogs = copyDeltas
	metaSource.DeltaBinlogs = metaDeltas
	for src, dst := range mappings {
		plan.mappings[src] = dst
	}
	plan.copiedFiles = append(plan.copiedFiles, files...)

	if insertMutated {
		clearCopiedDerivedData(copySource)
		clearCopiedDerivedData(metaSource)
	}
	if isDeltaOnlyCutoffSource(source) {
		plan.importedRows = retainedDeletes
	}
	return plan, nil
}

func rewriteManifestSegmentForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
) (*datapb.CopySegmentResult, []string, error) {
	if schema == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("cutoff requires collection schema")
	}
	if storageConfig == nil {
		storageConfig = compaction.CreateStorageConfig()
	}
	pkField, err := findPrimaryKeyField(schema)
	if err != nil {
		return nil, nil, err
	}
	rewriteSchema := buildCutoffRewriteSchema(schema)

	targetManifestPath, err := transformManifestPath(source.GetManifestPath(), source, target)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to transform manifest path")
	}
	targetBasePath, _, err := packed.UnmarshalManifestPath(targetManifestPath)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to parse target manifest path")
	}

	reader, err := storage.NewManifestRecordReader(ctx, source.GetManifestPath(), rewriteSchema,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultReadBufferSize),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to create manifest reader for cutoff rewrite")
	}
	defer reader.Close()

	writerOptions := []storage.RwOption{
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultWriteBufferSize),
		storage.WithMultiPartUploadSize(packed.DefaultMultiPartUploadSize),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()),
	}
	textColumnConfigs := buildCutoffTextColumnConfigs(rewriteSchema, path.Dir(targetBasePath))
	if len(textColumnConfigs) > 0 {
		writerOptions = append(writerOptions, storage.WithTextColumnConfigs(textColumnConfigs))
	}

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		target.GetCollectionId(),
		target.GetPartitionId(),
		target.GetSegmentId(),
		rewriteSchema,
		allocator.NewLocalAllocator(1, math.MaxInt64),
		uint64(packed.DefaultWriteBufferSize),
		math.MaxInt64,
		writerOptions...,
	)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to create manifest writer for cutoff rewrite")
	}

	var predicateErr error
	rowCount, _, err := storage.Sort(
		uint64(packed.DefaultWriteBufferSize),
		rewriteSchema,
		[]storage.RecordReader{reader},
		writer,
		func(record storage.Record, _, row int) bool {
			if predicateErr != nil {
				return false
			}
			ts, err := timestampAt(record, row)
			if err != nil {
				predicateErr = err
				return false
			}
			return ts <= source.GetCutoffTs()
		},
		nil,
	)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to rewrite manifest insert data for cutoff")
	}
	if predicateErr != nil {
		return nil, nil, predicateErr
	}
	if err := writer.Close(); err != nil {
		return nil, nil, merr.Wrap(err, "failed to close manifest cutoff writer")
	}

	fieldBinlogs, statsLog, bm25StatsLogs, manifestPath, _ := writer.GetLogs()
	if manifestPath == "" {
		manifestPath = packed.MarshalManifestPath(targetBasePath, packed.ManifestEarliest)
	}
	binlogs := storage.SortFieldBinlogs(fieldBinlogs)
	copiedFiles := extractFromPb(binlogs)

	var statsFiles []string
	manifestPath, statsFiles, err = addWriterStatsToManifest(manifestPath, storageConfig, statsLog, bm25StatsLogs)
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to add cutoff writer stats to manifest")
	}
	copiedFiles = append(copiedFiles, statsFiles...)

	deltaEntries, deltaFiles, err := rewriteManifestDeltasForCutoff(
		ctx, source, target, schema, pkField.GetDataType(), storageConfig)
	if err != nil {
		return nil, copiedFiles, err
	}
	copiedFiles = append(copiedFiles, deltaFiles...)
	if len(deltaEntries) > 0 {
		if rowCount > 0 {
			manifestPath, err = packed.AddDeltaLogsToManifestOverwrite(manifestPath, storageConfig, deltaEntries)
		} else {
			manifestPath, err = packed.CommitManifestUpdates(
				targetBasePath,
				packed.ManifestEarliest,
				storageConfig,
				&packed.ManifestUpdates{DeltaLogs: deltaEntries},
			)
		}
		if err != nil {
			return nil, copiedFiles, merr.Wrap(err, "failed to add cutoff deltas to manifest")
		}
	}
	copiedFiles = append(copiedFiles, manifestPhysicalPath(manifestPath))
	if statsFiles, err := manifestStatsFiles(manifestPath, storageConfig); err == nil {
		copiedFiles = append(copiedFiles, statsFiles...)
	}

	if err := binlog.CompressBinLogs(binlogs, nil, nil, nil); err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to compress cutoff manifest binlog paths")
	}
	return &datapb.CopySegmentResult{
		SegmentId:     target.GetSegmentId(),
		ImportedRows:  int64(rowCount),
		Binlogs:       binlogs,
		ManifestPath:  manifestPath,
		UpdateNumRows: true,
	}, copiedFiles, nil
}

func addWriterStatsToManifest(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	statsLog *datapb.FieldBinlog,
	bm25StatsLogs map[storage.FieldID]*datapb.FieldBinlog,
) (string, []string, error) {
	statEntries := make([]packed.StatEntry, 0, len(bm25StatsLogs)+1)
	if statsLog != nil {
		statEntries = append(statEntries, packed.FieldBinlogStatEntry("bloom_filter", statsLog.GetFieldID(), statsLog))
	}
	bm25FieldIDs := make([]int64, 0, len(bm25StatsLogs))
	for fieldID := range bm25StatsLogs {
		bm25FieldIDs = append(bm25FieldIDs, fieldID)
	}
	sort.Slice(bm25FieldIDs, func(i, j int) bool {
		return bm25FieldIDs[i] < bm25FieldIDs[j]
	})
	for _, fieldID := range bm25FieldIDs {
		statEntries = append(statEntries, packed.FieldBinlogStatEntry("bm25", fieldID, bm25StatsLogs[fieldID]))
	}
	if len(statEntries) == 0 {
		return manifestPath, nil, nil
	}
	if manifestPath == "" {
		return "", nil, merr.WrapErrServiceInternalMsg("cutoff rewrite produced stats without manifest")
	}
	manifestPath, err := packed.AddStatsToManifest(manifestPath, storageConfig, statEntries)
	if err != nil {
		return "", nil, err
	}
	return manifestPath, statEntryFiles(statEntries), nil
}

func statEntryFiles(entries []packed.StatEntry) []string {
	files := make([]string, 0)
	for _, entry := range entries {
		files = append(files, entry.Files...)
	}
	return files
}

func rewriteManifestDeltasForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	storageConfig *indexpb.StorageConfig,
) ([]packed.DeltaLogEntry, []string, error) {
	paths, err := packed.GetDeltaLogPathsFromManifest(source.GetManifestPath(), storageConfig)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to read source manifest deltas")
	}
	paths = append(paths, extractFromPb(source.GetDeltaBinlogs())...)
	if len(paths) == 0 {
		return nil, nil, nil
	}

	seen := make(map[string]struct{}, len(paths))
	entries := make([]packed.DeltaLogEntry, 0, len(paths))
	copiedFiles := make([]string, 0, len(paths))
	for _, sourcePath := range paths {
		if _, ok := seen[sourcePath]; ok {
			continue
		}
		seen[sourcePath] = struct{}{}

		deletes, err := scanDeltaLogPathsForCutoff([]string{sourcePath}, pkType, source.GetCutoffTs(),
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(storageConfig),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()))
		if err != nil {
			return nil, copiedFiles, merr.Wrap(err, "failed to scan manifest delta for cutoff")
		}
		if len(deletes) == 0 {
			continue
		}
		logID, err := strconv.ParseInt(path.Base(sourcePath), 10, 64)
		if err != nil {
			return nil, copiedFiles, merr.WrapErrParameterInvalidMsg("failed to parse delta log id from path %s", sourcePath)
		}
		targetPath, err := generateTargetPath(sourcePath, source, target)
		if err != nil {
			return nil, copiedFiles, err
		}
		if _, err := writeDeltaLog(ctx, nil, target, schema, pkType, logID, targetPath, deletes, storage.StorageV2, storageConfig); err != nil {
			return nil, copiedFiles, err
		}
		entries = append(entries, packed.DeltaLogEntry{Path: targetPath, NumEntries: int64(len(deletes))})
		copiedFiles = append(copiedFiles, targetPath)
	}
	return entries, copiedFiles, nil
}

func buildCutoffTextColumnConfigs(schema *schemapb.CollectionSchema, partitionBasePath string) []packed.TextColumnConfig {
	configs := make([]packed.TextColumnConfig, 0)
	for _, field := range schema.GetFields() {
		if field.GetDataType() != schemapb.DataType_Text {
			continue
		}
		fieldID := field.GetFieldID()
		configs = append(configs, packed.TextColumnConfig{
			FieldID:             fieldID,
			LobBasePath:         path.Join(partitionBasePath, "lobs", strconv.FormatInt(fieldID, 10)),
			InlineThreshold:     paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64(),
			MaxLobFileBytes:     paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(),
			FlushThresholdBytes: paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
			RewriteMode:         true,
		})
	}
	return configs
}

func manifestStatsFiles(manifestPath string, storageConfig *indexpb.StorageConfig) ([]string, error) {
	stats, err := packed.GetManifestStats(manifestPath, storageConfig)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0)
	for _, stat := range stats {
		paths = append(paths, stat.Paths...)
	}
	return paths, nil
}

func clearCopiedDerivedData(source *datapb.CopySegmentSource) {
	source.StatsBinlogs = nil
	source.Bm25Binlogs = nil
	source.IndexFiles = nil
	source.TextIndexFiles = nil
	source.JsonKeyIndexFiles = nil
}

func appendBinlogToField(builders map[int64]*datapb.FieldBinlog, field *datapb.FieldBinlog, binlog *datapb.Binlog) {
	fieldID := field.GetFieldID()
	dst, ok := builders[fieldID]
	if !ok {
		dst = proto.Clone(field).(*datapb.FieldBinlog)
		dst.Binlogs = nil
		builders[fieldID] = dst
	}
	dst.Binlogs = append(dst.Binlogs, proto.Clone(binlog).(*datapb.Binlog))
}

func fieldBinlogBuildersToList(builders map[int64]*datapb.FieldBinlog) []*datapb.FieldBinlog {
	fieldIDs := make([]int64, 0, len(builders))
	for fieldID := range builders {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Slice(fieldIDs, func(i, j int) bool {
		return fieldIDs[i] < fieldIDs[j]
	})
	result := make([]*datapb.FieldBinlog, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		if len(builders[fieldID].GetBinlogs()) > 0 {
			result = append(result, builders[fieldID])
		}
	}
	return result
}

func rewriteV2InsertBinlogsForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, map[string]string, []string, bool, error) {
	mappings := make(map[string]string)
	if len(source.GetInsertBinlogs()) == 0 {
		return nil, nil, mappings, nil, false, nil
	}

	fields := make([]*datapb.FieldBinlog, 0, len(source.GetInsertBinlogs()))
	for _, field := range source.GetInsertBinlogs() {
		fields = append(fields, proto.Clone(field).(*datapb.FieldBinlog))
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].GetFieldID() < fields[j].GetFieldID()
	})

	batchCount := len(fields[0].GetBinlogs())
	for _, field := range fields[1:] {
		if len(field.GetBinlogs()) != batchCount {
			return nil, nil, nil, nil, false, merr.WrapErrServiceInternalMsg(
				"v2 cutoff requires aligned column-group binlogs: field %d has %d batches, expected %d",
				field.GetFieldID(), len(field.GetBinlogs()), batchCount)
		}
	}

	copyBuilders := make(map[int64]*datapb.FieldBinlog)
	metaBuilders := make(map[int64]*datapb.FieldBinlog)
	copiedFiles := make([]string, 0)
	mutated := false
	for batchIdx := 0; batchIdx < batchCount; batchIdx++ {
		batch := make([]*datapb.Binlog, 0, len(fields))
		batchFields := make([]*datapb.FieldBinlog, 0, len(fields))
		for _, field := range fields {
			batch = append(batch, field.GetBinlogs()[batchIdx])
			batchFields = append(batchFields, &datapb.FieldBinlog{
				FieldID:     field.GetFieldID(),
				ChildFields: append([]int64(nil), field.GetChildFields()...),
				Binlogs:     []*datapb.Binlog{proto.Clone(field.GetBinlogs()[batchIdx]).(*datapb.Binlog)},
				Format:      field.GetFormat(),
			})
		}

		switch classifyInsertBatchForCutoff(batch, source.GetCutoffTs()) {
		case cutoffLogKeep:
			for i, field := range fields {
				appendBinlogToField(copyBuilders, field, batch[i])
				appendBinlogToField(metaBuilders, field, batch[i])
			}
		case cutoffLogDrop:
			mutated = true
		case cutoffLogRewrite:
			mutated = true
			rewritten, batchMappings, batchFiles, err := rewriteV2InsertBatchForCutoff(
				ctx, source, target, schema, storageConfig, batchFields)
			if err != nil {
				return nil, nil, nil, nil, false, err
			}
			for _, rewrittenField := range rewritten {
				for _, binlog := range rewrittenField.GetBinlogs() {
					appendBinlogToField(metaBuilders, rewrittenField, binlog)
				}
			}
			for src, dst := range batchMappings {
				mappings[src] = dst
			}
			copiedFiles = append(copiedFiles, batchFiles...)
		}
	}

	return fieldBinlogBuildersToList(copyBuilders),
		fieldBinlogBuildersToList(metaBuilders),
		mappings,
		copiedFiles,
		mutated,
		nil
}

func rewriteV2InsertBatchForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	batchFields []*datapb.FieldBinlog,
) ([]*datapb.FieldBinlog, map[string]string, []string, error) {
	rewriteSchema := buildCutoffRewriteSchema(schema)
	columnGroups, logIDStart, err := columnGroupsFromFieldBinlogs(rewriteSchema, batchFields)
	if err != nil {
		return nil, nil, nil, err
	}

	reader, err := storage.NewBinlogRecordReader(ctx, batchFields, rewriteSchema,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	if err != nil {
		return nil, nil, nil, merr.Wrap(err, "failed to create v2 binlog reader for cutoff rewrite")
	}
	defer reader.Close()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		target.GetCollectionId(),
		target.GetPartitionId(),
		target.GetSegmentId(),
		rewriteSchema,
		allocator.NewLocalAllocator(logIDStart, math.MaxInt64),
		uint64(packed.DefaultWriteBufferSize),
		math.MaxInt64,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultWriteBufferSize),
		storage.WithMultiPartUploadSize(packed.DefaultMultiPartUploadSize),
		storage.WithColumnGroups(columnGroups),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()),
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			for key, blob := range kvs {
				if err := packed.WriteFile(storageConfig, key, blob); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	if err != nil {
		return nil, nil, nil, merr.Wrap(err, "failed to create v2 binlog writer for cutoff rewrite")
	}

	var predicateErr error
	rowCount, _, err := storage.Sort(
		uint64(packed.DefaultWriteBufferSize),
		rewriteSchema,
		[]storage.RecordReader{reader},
		writer,
		func(record storage.Record, _, row int) bool {
			if predicateErr != nil {
				return false
			}
			ts, err := timestampAt(record, row)
			if err != nil {
				predicateErr = err
				return false
			}
			return ts <= source.GetCutoffTs()
		},
		nil,
	)
	if err != nil {
		return nil, nil, nil, merr.Wrap(err, "failed to rewrite v2 insert binlog for cutoff")
	}
	if predicateErr != nil {
		return nil, nil, nil, predicateErr
	}
	if rowCount == 0 {
		return nil, nil, nil, nil
	}
	if err := writer.Close(); err != nil {
		return nil, nil, nil, merr.Wrap(err, "failed to close v2 cutoff insert writer")
	}

	sourceByField := make(map[int64]*datapb.Binlog, len(batchFields))
	for _, field := range batchFields {
		if len(field.GetBinlogs()) != 1 {
			return nil, nil, nil, merr.WrapErrServiceInternalMsg("cutoff rewrite expects one binlog per column group")
		}
		sourceByField[field.GetFieldID()] = field.GetBinlogs()[0]
	}

	fieldBinlogs, _, _, _, _ := writer.GetLogs()
	rewritten := storage.SortFieldBinlogs(fieldBinlogs)
	mappings := make(map[string]string, len(rewritten))
	copiedFiles := make([]string, 0, len(rewritten))
	for _, field := range rewritten {
		sourceBinlog := sourceByField[field.GetFieldID()]
		if sourceBinlog == nil || len(field.GetBinlogs()) != 1 {
			return nil, nil, nil, merr.WrapErrServiceInternalMsg("cutoff rewrite produced unexpected field binlog")
		}
		sourcePath := sourceBinlog.GetLogPath()
		targetPath := field.GetBinlogs()[0].GetLogPath()
		logID, err := binlogLogID(sourceBinlog)
		if err != nil {
			return nil, nil, nil, err
		}

		rewrittenField := proto.Clone(field).(*datapb.FieldBinlog)
		rewrittenField.Binlogs[0].LogPath = sourcePath
		rewrittenField.Binlogs[0].LogID = logID
		mappings[sourcePath] = targetPath
		copiedFiles = append(copiedFiles, targetPath)
		field.Binlogs = rewrittenField.Binlogs
	}
	return rewritten, mappings, copiedFiles, nil
}

func columnGroupsFromFieldBinlogs(
	schema *schemapb.CollectionSchema,
	fieldBinlogs []*datapb.FieldBinlog,
) ([]storagecommon.ColumnGroup, int64, error) {
	if len(fieldBinlogs) == 0 {
		return nil, 0, merr.WrapErrServiceInternalMsg("empty field binlogs for cutoff rewrite")
	}
	fields := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for _, field := range fieldBinlogs {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].GetFieldID() < fields[j].GetFieldID()
	})

	allFields := typeutil.GetAllFieldSchemas(schema)
	fieldColumn := make(map[int64]int, len(allFields))
	for idx, field := range allFields {
		fieldColumn[field.GetFieldID()] = idx
	}
	explicitFields := make(map[int64]struct{})
	for _, field := range fields {
		if len(field.GetChildFields()) > 0 {
			for _, fieldID := range field.GetChildFields() {
				explicitFields[fieldID] = struct{}{}
			}
			continue
		}
		if field.GetFieldID() != storagecommon.DefaultShortColumnGroupID {
			explicitFields[field.GetFieldID()] = struct{}{}
		}
	}

	logIDStart, err := binlogLogID(fields[0].GetBinlogs()[0])
	if err != nil {
		return nil, 0, err
	}
	columnGroups := make([]storagecommon.ColumnGroup, 0, len(fields))
	for idx, field := range fields {
		if len(field.GetBinlogs()) != 1 {
			return nil, 0, merr.WrapErrServiceInternalMsg("cutoff rewrite expects one binlog per column group")
		}
		logID, err := binlogLogID(field.GetBinlogs()[0])
		if err != nil {
			return nil, 0, err
		}
		if logID != logIDStart+int64(idx) {
			return nil, 0, merr.WrapErrParameterInvalidMsg(
				"cutoff rewrite requires contiguous v2 column-group log IDs, got %d want %d",
				logID, logIDStart+int64(idx))
		}

		groupFields := append([]int64(nil), field.GetChildFields()...)
		if len(groupFields) == 0 {
			if field.GetFieldID() == storagecommon.DefaultShortColumnGroupID {
				for _, schemaField := range allFields {
					if _, ok := explicitFields[schemaField.GetFieldID()]; !ok {
						groupFields = append(groupFields, schemaField.GetFieldID())
					}
				}
			} else {
				groupFields = []int64{field.GetFieldID()}
			}
		}
		columns := make([]int, 0, len(groupFields))
		for _, fieldID := range groupFields {
			column, ok := fieldColumn[fieldID]
			if !ok {
				return nil, 0, merr.WrapErrServiceInternalMsg("field %d not found in cutoff rewrite schema", fieldID)
			}
			columns = append(columns, column)
		}
		columnGroups = append(columnGroups, storagecommon.ColumnGroup{
			GroupID: field.GetFieldID(),
			Columns: columns,
			Fields:  groupFields,
			Format:  field.GetFormat(),
		})
	}
	return columnGroups, logIDStart, nil
}

func rewriteDeltaFieldBinlogsForCutoff(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	fieldBinlogs []*datapb.FieldBinlog,
	storageConfig *indexpb.StorageConfig,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, map[string]string, []string, int64, error) {
	mappings := make(map[string]string)
	copyBuilders := make(map[int64]*datapb.FieldBinlog)
	metaBuilders := make(map[int64]*datapb.FieldBinlog)
	copiedFiles := make([]string, 0)
	var retainedRows int64

	for _, field := range fieldBinlogs {
		for _, srcBinlog := range field.GetBinlogs() {
			switch classifyBinlogForCutoff(srcBinlog, source.GetCutoffTs()) {
			case cutoffLogKeep:
				appendBinlogToField(copyBuilders, field, srcBinlog)
				appendBinlogToField(metaBuilders, field, srcBinlog)
				retainedRows += srcBinlog.GetEntriesNum()
			case cutoffLogDrop:
				continue
			case cutoffLogRewrite:
				rewritten, targetPath, retained, err := rewriteDeltaBinlogForCutoff(
					ctx, cm, source, target, schema, pkType, field, srcBinlog, storageConfig)
				if err != nil {
					return nil, nil, nil, nil, 0, err
				}
				if rewritten == nil || retained == 0 {
					continue
				}
				retainedRows += retained
				sourcePath := srcBinlog.GetLogPath()
				mappings[sourcePath] = targetPath
				copiedFiles = append(copiedFiles, targetPath)
				appendBinlogToField(metaBuilders, field, rewritten.GetBinlogs()[0])
			}
		}
	}
	return fieldBinlogBuildersToList(copyBuilders),
		fieldBinlogBuildersToList(metaBuilders),
		mappings,
		copiedFiles,
		retainedRows,
		nil
}

func rewriteDeltaBinlogForCutoff(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	field *datapb.FieldBinlog,
	srcBinlog *datapb.Binlog,
	storageConfig *indexpb.StorageConfig,
) (*datapb.FieldBinlog, string, int64, error) {
	sourcePath := srcBinlog.GetLogPath()
	logID, err := binlogLogID(srcBinlog)
	if err != nil {
		return nil, "", 0, err
	}
	targetPath, err := generateTargetPath(sourcePath, source, target)
	if err != nil {
		return nil, "", 0, err
	}

	deletes, version, err := scanDeltaBinlogForCutoff(ctx, cm, source, schema, pkType, sourcePath, source.GetCutoffTs(), storageConfig)
	if err != nil {
		return nil, "", 0, err
	}
	if len(deletes) == 0 {
		return nil, targetPath, 0, nil
	}

	deltaLog, err := writeDeltaLog(ctx, cm, target, schema, pkType, logID, targetPath, deletes, version, storageConfig)
	if err != nil {
		return nil, "", 0, err
	}
	deltaLog.FieldID = field.GetFieldID()
	deltaLog.Binlogs[0].LogPath = sourcePath
	deltaLog.Binlogs[0].LogID = logID
	return deltaLog, targetPath, int64(len(deletes)), nil
}

func scanDeltaBinlogForCutoff(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	sourcePath string,
	cutoffTs uint64,
	storageConfig *indexpb.StorageConfig,
) ([]cutoffDeleteEntry, int64, error) {
	if source.GetManifestPath() != "" {
		deletes, err := scanDeltaLogPathsForCutoff([]string{sourcePath}, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(storageConfig),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()))
		return deletes, storage.StorageV2, err
	}
	if source.GetStorageVersion() == storage.StorageV2 {
		if cm == nil {
			return nil, 0, merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for v2 legacy deltalogs")
		}
		deletes, err := scanDeltaLogPathsForCutoff([]string{sourcePath}, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV1),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()),
			storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
				return cm.MultiRead(ctx, paths)
			}))
		if err == nil {
			return deletes, storage.StorageV1, nil
		}
		deletes, err = scanDeltaLogPathsForCutoff([]string{sourcePath}, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(storageConfig),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()))
		return deletes, storage.StorageV2, err
	}
	if cm == nil {
		return nil, 0, merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for legacy deltalogs")
	}
	deletes, err := scanDeltaLogPathsForCutoff([]string{sourcePath}, pkType, cutoffTs,
		storage.WithVersion(storage.StorageV1),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(schema.GetProperties()),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return cm.MultiRead(ctx, paths)
		}))
	return deletes, storage.StorageV1, err
}

func timestampAt(record storage.Record, row int) (uint64, error) {
	tsColumn, ok := record.Column(common.TimeStampField).(*array.Int64)
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg("timestamp column has unexpected type %T", record.Column(common.TimeStampField))
	}
	return uint64(tsColumn.Value(row)), nil
}

func binlogLogID(binlog *datapb.Binlog) (int64, error) {
	if binlog.GetLogID() != 0 {
		return binlog.GetLogID(), nil
	}
	logID, err := strconv.ParseInt(path.Base(binlog.GetLogPath()), 10, 64)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to parse log id from path %s", binlog.GetLogPath())
	}
	return logID, nil
}

func scanInsertBinlogsForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	schema *schemapb.CollectionSchema,
	pkField *schemapb.FieldSchema,
	cutoffTs uint64,
	storageConfig *indexpb.StorageConfig,
) (map[cutoffPK]struct{}, map[cutoffPK]cutoffDeleteEntry, error) {
	preCutoffPKs := make(map[cutoffPK]struct{})
	postCutoffPKs := make(map[cutoffPK]cutoffDeleteEntry)
	if len(source.GetInsertBinlogs()) == 0 {
		return preCutoffPKs, postCutoffPKs, nil
	}

	readSchema := buildV2CutoffReadSchema(schema, pkField)
	reader, err := storage.NewBinlogRecordReader(ctx, source.GetInsertBinlogs(), readSchema,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(schema.GetProperties()))
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to create v2 binlog reader for cutoff")
	}
	defer reader.Close()

	return scanCutoffPKs(reader, pkField, cutoffTs)
}

func buildV2CutoffReadSchema(schema *schemapb.CollectionSchema, pkField *schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:       schema.GetName(),
		Properties: schema.GetProperties(),
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     common.RowIDFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			},
			proto.Clone(pkField).(*schemapb.FieldSchema),
		},
	}
}

func buildCutoffRewriteSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	hasRowID := false
	hasTimestamp := false
	for _, field := range schema.GetFields() {
		switch field.GetFieldID() {
		case common.RowIDField:
			hasRowID = true
		case common.TimeStampField:
			hasTimestamp = true
		}
	}
	if hasRowID && hasTimestamp {
		return schema
	}

	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields())+2)
	if !hasRowID {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID:  common.RowIDField,
			Name:     common.RowIDFieldName,
			DataType: schemapb.DataType_Int64,
		})
	}
	if !hasTimestamp {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID:  common.TimeStampField,
			Name:     common.TimeStampFieldName,
			DataType: schemapb.DataType_Int64,
		})
	}
	fields = append(fields, schema.GetFields()...)
	rewritten := proto.Clone(schema).(*schemapb.CollectionSchema)
	rewritten.Fields = fields
	return rewritten
}

func scanInsertManifestForCutoff(
	ctx context.Context,
	source *datapb.CopySegmentSource,
	schema *schemapb.CollectionSchema,
	pkField *schemapb.FieldSchema,
	cutoffTs uint64,
	storageConfig *indexpb.StorageConfig,
) (map[cutoffPK]struct{}, map[cutoffPK]cutoffDeleteEntry, error) {
	minimalSchema := &schemapb.CollectionSchema{
		Name:       schema.GetName(),
		Properties: schema.GetProperties(),
		Fields: []*schemapb.FieldSchema{
			proto.Clone(pkField).(*schemapb.FieldSchema),
			{
				FieldID:  common.TimeStampField,
				Name:     "ts",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	reader, err := storage.NewManifestRecordReader(ctx, source.GetManifestPath(), minimalSchema,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultReadBufferSize),
		storage.WithCollectionID(source.GetCollectionId()))
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to create manifest reader for cutoff")
	}
	defer reader.Close()

	return scanCutoffPKs(reader, pkField, cutoffTs)
}

func scanCutoffPKs(
	reader storage.RecordReader,
	pkField *schemapb.FieldSchema,
	cutoffTs uint64,
) (map[cutoffPK]struct{}, map[cutoffPK]cutoffDeleteEntry, error) {
	preCutoffPKs := make(map[cutoffPK]struct{})
	postCutoffPKs := make(map[cutoffPK]cutoffDeleteEntry)
	for {
		record, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, merr.Wrap(err, "failed to read insert record for cutoff")
		}
		if err := visitPKTimestampRecord(record, pkField.GetFieldID(), pkField.GetDataType(), func(key cutoffPK, pk storage.PrimaryKey, ts uint64) error {
			if ts <= cutoffTs {
				preCutoffPKs[key] = struct{}{}
				return nil
			}
			existing, ok := postCutoffPKs[key]
			if !ok || ts > existing.ts {
				postCutoffPKs[key] = cutoffDeleteEntry{pk: pk, ts: ts}
			}
			return nil
		}); err != nil {
			record.Release()
			return nil, nil, err
		}
		record.Release()
	}
	return preCutoffPKs, postCutoffPKs, nil
}

func scanSourceDeltaLogsForCutoff(
	source *datapb.CopySegmentSource,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	cutoffTs uint64,
	storageConfig *indexpb.StorageConfig,
) ([]cutoffDeleteEntry, error) {
	paths, err := packed.GetDeltaLogPathsFromManifest(source.GetManifestPath(), storageConfig)
	if err != nil {
		return nil, merr.Wrap(err, "failed to read source manifest deltas")
	}
	if len(paths) == 0 {
		return nil, nil
	}

	return scanDeltaLogPathsForCutoff(paths, pkType, cutoffTs,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(schema.GetProperties()))
}

func scanSourceDeltaLogPathsForCutoff(
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	schema *schemapb.CollectionSchema,
	pkType schemapb.DataType,
	cutoffTs uint64,
	storageConfig *indexpb.StorageConfig,
) ([]cutoffDeleteEntry, error) {
	paths := extractFromPb(source.GetDeltaBinlogs())
	if source.GetManifestPath() != "" {
		manifestPaths, err := packed.GetDeltaLogPathsFromManifest(source.GetManifestPath(), storageConfig)
		if err != nil {
			return nil, merr.Wrap(err, "failed to read source manifest deltas")
		}
		paths = append(paths, manifestPaths...)
	}
	if len(paths) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(paths))
	uniquePaths := make([]string, 0, len(paths))
	for _, p := range paths {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		uniquePaths = append(uniquePaths, p)
	}
	if source.GetManifestPath() != "" {
		return scanDeltaLogPathsForCutoff(uniquePaths, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(storageConfig),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()))
	}
	if source.GetStorageVersion() == storage.StorageV2 {
		if cm == nil {
			return nil, merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for v2 legacy deltalogs")
		}
		deletes, err := scanDeltaLogPathsForCutoff(uniquePaths, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV1),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()),
			storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
				return cm.MultiRead(ctx, paths)
			}))
		if err == nil {
			return deletes, nil
		}
		return scanDeltaLogPathsForCutoff(uniquePaths, pkType, cutoffTs,
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(storageConfig),
			storage.WithCollectionID(source.GetCollectionId()),
			storage.WithCollectionProperties(schema.GetProperties()))
	}
	if cm == nil {
		return nil, merr.WrapErrParameterInvalidMsg("cutoff requires chunk manager for legacy deltalogs")
	}
	return scanDeltaLogPathsForCutoff(uniquePaths, pkType, cutoffTs,
		storage.WithVersion(storage.StorageV1),
		storage.WithCollectionID(source.GetCollectionId()),
		storage.WithCollectionProperties(schema.GetProperties()),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return cm.MultiRead(ctx, paths)
		}))
}

func scanDeltaLogPathsForCutoff(
	paths []string,
	pkType schemapb.DataType,
	cutoffTs uint64,
	options ...storage.RwOption,
) ([]cutoffDeleteEntry, error) {
	reader, err := storage.NewDeltalogReader(
		pkType,
		paths,
		options...,
	)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create source deltalog reader")
	}
	defer reader.Close()

	deletes := make([]cutoffDeleteEntry, 0)
	for {
		record, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, merr.Wrap(err, "failed to read source deltalog record")
		}
		if err := visitPKTimestampRecord(record, 0, pkType, func(_ cutoffPK, pk storage.PrimaryKey, ts uint64) error {
			if ts <= cutoffTs {
				deletes = append(deletes, cutoffDeleteEntry{pk: pk, ts: ts})
			}
			return nil
		}); err != nil {
			record.Release()
			return nil, err
		}
		record.Release()
	}
	return deletes, nil
}

func visitPKTimestampRecord(
	record storage.Record,
	pkFieldID int64,
	pkType schemapb.DataType,
	visit func(cutoffPK, storage.PrimaryKey, uint64) error,
) error {
	tsColumn, ok := record.Column(common.TimeStampField).(*array.Int64)
	if !ok {
		return merr.WrapErrServiceInternalMsg("timestamp column has unexpected type %T", record.Column(common.TimeStampField))
	}

	switch pkType {
	case schemapb.DataType_Int64:
		pkColumn, ok := record.Column(pkFieldID).(*array.Int64)
		if !ok {
			return merr.WrapErrServiceInternalMsg("int64 primary key column has unexpected type %T", record.Column(pkFieldID))
		}
		for i := 0; i < record.Len(); i++ {
			pkValue := pkColumn.Value(i)
			ts := uint64(tsColumn.Value(i))
			pk := storage.NewInt64PrimaryKey(pkValue)
			key := cutoffPK{int64PK: pkValue}
			if err := visit(key, pk, ts); err != nil {
				return err
			}
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		pkColumn, ok := record.Column(pkFieldID).(*array.String)
		if !ok {
			return merr.WrapErrServiceInternalMsg("varchar primary key column has unexpected type %T", record.Column(pkFieldID))
		}
		for i := 0; i < record.Len(); i++ {
			pkValue := pkColumn.Value(i)
			ts := uint64(tsColumn.Value(i))
			pk := storage.NewVarCharPrimaryKey(pkValue)
			key := cutoffPK{strPK: pkValue}
			if err := visit(key, pk, ts); err != nil {
				return err
			}
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported primary key type for restore cutoff: %s", pkType.String())
	}
	return nil
}

func findPrimaryKeyField(schema *schemapb.CollectionSchema) (*schemapb.FieldSchema, error) {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("primary key field not found in collection schema")
}

func manifestPhysicalPath(manifestPath string) string {
	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s/_metadata/manifest-%d.avro", basePath, version)
}

// listAllFiles recursively lists all files under the given path using WalkWithPrefix.
// Returns (nil, error) if the walk fails.
func listAllFiles(ctx context.Context, cm storage.ChunkManager, basePath string) ([]string, error) {
	var files []string
	err := cm.WalkWithPrefix(ctx, basePath, true, func(info *storage.ChunkObjectInfo) bool {
		files = append(files, info.FilePath)
		return true
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func isManifestControlFile(basePath, filePath string) bool {
	rel := strings.TrimPrefix(filePath, strings.TrimRight(basePath, "/")+"/")
	return strings.HasPrefix(rel, "_delta/") || strings.HasPrefix(rel, "_metadata/")
}

// copyFile copies a single file from src to dst using the chunk manager.
func copyFile(ctx context.Context, cm storage.ChunkManager, src, dst string) error {
	return cm.Copy(ctx, src, dst)
}

// extractFromPb extracts file paths from FieldBinlog list (insert/delta/stats/bm25).
func extractFromPb(fieldBinlogs []*datapb.FieldBinlog) []string {
	var paths []string
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if path := binlog.GetLogPath(); path != "" {
				paths = append(paths, path)
			}
		}
	}
	return paths
}

func hasBinlogPath(fieldBinlogs []*datapb.FieldBinlog) bool {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogPath() != "" {
				return true
			}
		}
	}
	return false
}

// extractIndexFiles extracts vector/scalar index file paths.
func extractIndexFiles(indexInfos []*indexpb.IndexFilePathInfo) []string {
	var paths []string
	for _, info := range indexInfos {
		paths = append(paths, info.GetIndexFilePaths()...)
	}
	return paths
}

func buildIndexPathVersionByFile(source *datapb.CopySegmentSource) map[string]indexpb.IndexStorePathVersion {
	versions := make(map[string]indexpb.IndexStorePathVersion)
	for _, indexInfo := range source.GetIndexFiles() {
		for _, filePath := range indexInfo.GetIndexFilePaths() {
			versions[filePath] = indexInfo.GetIndexStorePathVersion()
		}
	}
	return versions
}

// extractTextIndexFiles extracts text index file paths.
func extractTextIndexFiles(textIndexInfos map[int64]*datapb.TextIndexStats) []string {
	var paths []string
	for _, info := range textIndexInfos {
		paths = append(paths, info.GetFiles()...)
	}
	return paths
}

// extractJSONFiles extracts JSON index files, separated by data format version.
// Returns (jsonKeyFiles, jsonStatsFiles).
func extractJSONFiles(jsonIndexInfos map[int64]*datapb.JsonKeyStats) ([]string, []string) {
	var jsonKeyFiles []string
	var jsonStatsFiles []string

	for _, info := range jsonIndexInfos {
		dataFormat := info.GetJsonKeyStatsDataFormat()
		files := info.GetFiles()

		if dataFormat < 2 {
			// Legacy format (< v2) -> JSON Key Index
			jsonKeyFiles = append(jsonKeyFiles, files...)
		} else {
			// New format (>= v2) -> JSON Stats
			jsonStatsFiles = append(jsonStatsFiles, files...)
		}
	}

	return jsonKeyFiles, jsonStatsFiles
}

// collectSegmentFiles collects all files to copy, organized by type.
//
// For InsertBinlogs, the decision is based on storage_version:
//   - storage_version >= StorageV3 (3): MUST resolve from manifest_path.
//     manifest_path missing → error. Listing fails → error. Empty file list → OK (no binlogs).
//   - storage_version < StorageV3: use pb paths (traditional non-packed format).
//
// For other 7 types: always from pb (not yet in manifest).
func collectSegmentFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
) (*SegmentFiles, error) {
	files := &SegmentFiles{}

	if source.GetStorageVersion() >= storage.StorageV3 {
		// StorageV3+: binlog paths MUST come from manifest
		manifestPath := source.GetManifestPath()
		if manifestPath == "" {
			if !(source.GetCutoffTs() > 0 && len(source.GetInsertBinlogs()) == 0) {
				return nil, merr.WrapErrParameterInvalidMsg("storage_version=%d requires manifest_path but it is empty (segmentID=%d)",
					source.GetStorageVersion(), source.GetSegmentId())
			}
			mlog.Info(context.TODO(), "using delta-only cutoff source without manifest",
				mlog.Int64("segmentID", source.GetSegmentId()),
				mlog.Int64("storageVersion", source.GetStorageVersion()))
		} else {
			basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
			if err != nil {
				return nil, merr.Wrapf(err, "failed to unmarshal manifest path %q for segment %d", manifestPath, source.GetSegmentId())
			}

			allFiles, listErr := listAllFiles(ctx, cm, basePath)
			if listErr != nil {
				return nil, merr.Wrapf(listErr, "failed to list files from manifest base path %q for segment %d", basePath, source.GetSegmentId())
			}

			// Empty file list is OK for V3 - segment may have only deltas and no insert binlogs.
			// With restore cutoff enabled, the target manifest is rebuilt and its deltas
			// are rewritten, so source manifest metadata and source delta files must not
			// be copied into the target segment directory.
			if source.GetCutoffTs() > 0 {
				for _, file := range allFiles {
					if !isManifestControlFile(basePath, file) {
						files.InsertBinlogs = append(files.InsertBinlogs, file)
					}
				}
			} else {
				files.InsertBinlogs = allFiles
			}
			mlog.Info(context.TODO(), "collected InsertBinlogs from manifest",
				mlog.String("basePath", basePath),
				mlog.Int("fileCount", len(files.InsertBinlogs)),
				mlog.Int64("storageVersion", source.GetStorageVersion()))

			// Collect LOB files owned by THIS segment from the manifest.
			// LOB files live at partition level ({root}/insert_log/{coll}/{part}/lobs/),
			// but multiple segments share that directory. We must only copy the files
			// referenced by this segment's manifest to preserve the invariant that
			// each LOB file belongs to exactly one segment.
			storageConfig := compaction.CreateStorageConfig()
			lobFileInfos, lobErr := packed.GetManifestLobFiles(manifestPath, storageConfig)
			if lobErr != nil {
				mlog.Debug(context.TODO(), "no LOB files found in manifest (may not have TEXT fields)",
					mlog.String("manifestPath", manifestPath),
					mlog.Err(lobErr))
			} else if len(lobFileInfos) > 0 {
				// GetManifestLobFiles returns absolute paths (the manifest
				// deserializer calls ToAbsolute internally), so use them directly.
				files.LobFiles = lobFileInfosToPaths(lobFileInfos)
				mlog.Info(context.TODO(), "collected LOB files from segment manifest",
					mlog.String("manifestPath", manifestPath),
					mlog.Int("lobFileCount", len(files.LobFiles)))
			}
		}
	} else {
		// StorageV1/V2: use pb paths (traditional non-packed format)
		files.InsertBinlogs = extractFromPb(source.GetInsertBinlogs())
		mlog.Info(context.TODO(), "using InsertBinlogs from pb",
			mlog.Int("fileCount", len(files.InsertBinlogs)),
			mlog.Int64("storageVersion", source.GetStorageVersion()))
	}

	// Other types from pb. In cutoff mode callers pass an already-filtered
	// source, so it is safe to copy the remaining delta logs directly.
	files.DeltaBinlogs = extractFromPb(source.GetDeltaBinlogs())
	files.StatsBinlogs = extractFromPb(source.GetStatsBinlogs())
	files.Bm25Binlogs = extractFromPb(source.GetBm25Binlogs())
	files.VectorScalarIndex = extractIndexFiles(source.GetIndexFiles())

	// For V3, text/json stats files live under basePath/_stats/ and are already
	// included in InsertBinlogs via listAllFiles(). Skip pb extraction to avoid
	// using potentially stale or wrong-format paths from etcd metadata.
	if source.GetStorageVersion() < storage.StorageV3 {
		files.TextIndex = extractTextIndexFiles(source.GetTextIndexFiles())
		files.JSONKeyIndex, files.JSONStats = extractJSONFiles(source.GetJsonKeyIndexFiles())
	}

	return files, nil
}

// generateMappingsFromFiles generates file copy mappings from SegmentFiles.
// Each source file path is transformed to target path by replacing collection/partition/segment IDs.
func generateMappingsFromFiles(
	files *SegmentFiles,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (map[string]string, error) {
	mappings := make(map[string]string)
	indexPathVersions := buildIndexPathVersionByFile(source)

	// Helper to add mappings with error handling
	addMappings := func(srcPaths []string, fileType string) error {
		for _, srcPath := range srcPaths {
			var dstPath string
			var err error

			// Determine path generation logic based on file type
			switch fileType {
			case IndexTypeVectorScalarV0, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats:
				dstPath, err = generateTargetIndexPath(srcPath, source, target, fileType, indexPathVersions[srcPath])
			case FileTypeLOB:
				dstPath, err = generateTargetLOBPath(srcPath, source, target)
			default:
				dstPath, err = generateTargetPath(srcPath, source, target)
			}

			if err != nil {
				return merr.Wrapf(err, "failed to generate target path for %s file %s", fileType, srcPath)
			}
			mappings[srcPath] = dstPath
		}
		return nil
	}

	// Generate mappings for all file types
	if err := addMappings(files.InsertBinlogs, BinlogTypeInsert); err != nil {
		return nil, err
	}
	if err := addMappings(files.DeltaBinlogs, BinlogTypeDelta); err != nil {
		return nil, err
	}
	if err := addMappings(files.StatsBinlogs, BinlogTypeStats); err != nil {
		return nil, err
	}
	if err := addMappings(files.Bm25Binlogs, BinlogTypeBM25); err != nil {
		return nil, err
	}
	// Vector/scalar index copy uses the v0 type as the logical input; the
	// per-file IndexStorePathVersion switches storage matching to index_v1 when needed.
	if err := addMappings(files.VectorScalarIndex, IndexTypeVectorScalarV0); err != nil {
		return nil, err
	}
	if err := addMappings(files.TextIndex, IndexTypeText); err != nil {
		return nil, err
	}
	if err := addMappings(files.JSONKeyIndex, IndexTypeJSONKey); err != nil {
		return nil, err
	}
	if err := addMappings(files.JSONStats, IndexTypeJSONStats); err != nil {
		return nil, err
	}
	if err := addMappings(files.LobFiles, FileTypeLOB); err != nil {
		return nil, err
	}

	return mappings, nil
}

func CopySegmentAndIndexFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []mlog.Field,
	options ...CopySegmentFileOptions,
) (*datapb.CopySegmentResult, []string, error) {
	segmentID := source.GetSegmentId()
	deltaOnlyCutoffSource := isDeltaOnlyCutoffSource(source)
	useManifest := source.GetStorageVersion() >= storage.StorageV3 && !deltaOnlyCutoffSource
	fileOptions := CopySegmentFileOptions{StorageConfig: compaction.CreateStorageConfig()}
	if len(options) > 0 {
		fileOptions = options[0]
		if fileOptions.StorageConfig == nil {
			fileOptions.StorageConfig = compaction.CreateStorageConfig()
		}
	}

	mlog.Info(context.TODO(), "start copying segment and index files",
		mlog.Int64("sourceSegmentID", segmentID),
		mlog.Int64("storageVersion", source.GetStorageVersion()),
		mlog.Uint64("cutoffTs", source.GetCutoffTs()),
		mlog.Bool("useManifest", useManifest),
		mlog.Bool("isExternalCollection", source.GetIsExternalCollection()))

	if source.GetCutoffTs() > 0 &&
		source.GetStorageVersion() != storage.StorageV2 &&
		source.GetStorageVersion() < storage.StorageV3 &&
		!deltaOnlyCutoffSource {
		return nil, nil, merr.WrapErrParameterInvalidMsg("cutoff only supports StorageV2/StorageV3 segments, got storage_version=%d", source.GetStorageVersion())
	}

	if useManifest && source.GetCutoffTs() > 0 {
		return rewriteManifestSegmentForCutoff(ctx, source, target, fileOptions.Schema, fileOptions.StorageConfig)
	}

	copySource := source
	metaSource := source
	var cutoffPlan *restoreCutoffPlan
	if source.GetCutoffTs() > 0 && (source.GetStorageVersion() == storage.StorageV2 || deltaOnlyCutoffSource) {
		var err error
		cutoffPlan, err = prepareNonManifestRestoreCutoff(ctx, cm, source, target, fileOptions.Schema, fileOptions.StorageConfig)
		if err != nil {
			return nil, nil, merr.Wrap(err, "failed to prepare restore cutoff")
		}
		copySource = cutoffPlan.copySource
		metaSource = cutoffPlan.metaSource
	}

	// Step 1: Collect all files to copy
	files, err := collectSegmentFiles(ctx, cm, copySource)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to collect segment files")
	}

	// Step 2: Generate src->dst mappings for file copying
	mappings, err := generateMappingsFromFiles(files, copySource, target)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to generate file mappings")
	}

	// Step 3: Execute all copy operations
	copiedFiles := make([]string, 0, len(mappings))
	if cutoffPlan != nil {
		copiedFiles = append(copiedFiles, cutoffPlan.copiedFiles...)
	}
	for src, dst := range mappings {
		mlog.Debug(context.TODO(), "copying file",
			mlog.String("src", src),
			mlog.String("dst", dst))

		if err := copyFile(ctx, cm, src, dst); err != nil {
			fields := make([]mlog.Field, 0, len(logFields)+3)
			fields = append(fields, logFields...)
			fields = append(fields, mlog.String("src", src), mlog.String("dst", dst), mlog.Err(err))
			mlog.Warn(context.TODO(), "failed to copy file", fields...)
			return nil, copiedFiles, merr.Wrapf(err, "failed to copy file from %s to %s", src, dst)
		}
		copiedFiles = append(copiedFiles, dst)
	}

	mlog.Info(context.TODO(), "all files copied successfully",
		mlog.Int("fileCount", len(mappings)))

	if cutoffPlan != nil {
		for src, dst := range cutoffPlan.mappings {
			mappings[src] = dst
		}
	}

	// Step 3.5: When manifest is used (StorageV3+), InsertBinlogs were collected from manifest
	// (actual file paths under base_path including _data/ and _metadata/), but
	// generateSegmentInfoFromSource needs mappings for the protobuf logical paths too.
	// Add these "logical-only" mappings AFTER file copying so they don't trigger actual copy operations.
	if useManifest {
		pbInsertPaths := extractFromPb(source.GetInsertBinlogs())
		for _, srcPath := range pbInsertPaths {
			if _, exists := mappings[srcPath]; !exists {
				dstPath, pathErr := generateTargetPath(srcPath, source, target)
				if pathErr != nil {
					return nil, copiedFiles, merr.Wrapf(pathErr, "failed to generate target path for pb insert binlog %s", srcPath)
				}
				mappings[srcPath] = dstPath
			}
		}
		mlog.Info(context.TODO(), "added logical insert binlog mappings for manifest segment",
			mlog.Int("pbPathCount", len(pbInsertPaths)))
	}

	// Step 4: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(metaSource, target, mappings)
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to build index info")
	}

	// Step 5: Generate segment metadata with path mappings.
	segmentInfo, err := generateSegmentInfoFromSource(metaSource, target, mappings)
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to generate segment info")
	}

	// Step 6: Compress paths
	err = binlog.CompressBinLogs(segmentInfo.GetBinlogs(), segmentInfo.GetStatslogs(),
		segmentInfo.GetDeltalogs(), segmentInfo.GetBm25Logs())
	if err != nil {
		return nil, copiedFiles, merr.Wrap(err, "failed to compress binlog paths")
	}

	for _, indexInfo := range indexInfos {
		indexInfo.IndexFilePaths = shortenIndexFilePaths(indexInfo.IndexFilePaths)
	}

	jsonKeyIndexInfos = shortenJSONStatsPath(jsonKeyIndexInfos)

	mlog.Info(context.TODO(), "path compression completed",
		mlog.Int("binlogFields", len(segmentInfo.GetBinlogs())),
		mlog.Int("indexCount", len(indexInfos)),
		mlog.Int("jsonStatsCount", len(jsonKeyIndexInfos)))

	// Step 7: Build result
	result := &datapb.CopySegmentResult{
		SegmentId:         segmentInfo.GetSegmentID(),
		ImportedRows:      segmentInfo.GetImportedRows(),
		Binlogs:           segmentInfo.GetBinlogs(),
		Statslogs:         segmentInfo.GetStatslogs(),
		Deltalogs:         segmentInfo.GetDeltalogs(),
		Bm25Logs:          segmentInfo.GetBm25Logs(),
		IndexInfos:        indexInfos,
		TextIndexInfos:    textIndexInfos,
		JsonKeyIndexInfos: jsonKeyIndexInfos,
	}

	if cutoffPlan != nil && cutoffPlan.updateNumRows {
		result.UpdateNumRows = true
		if deltaOnlyCutoffSource {
			result.ImportedRows = cutoffPlan.importedRows
		}
	}

	// Step 8: Transform and propagate manifest_path for StorageV3+ segments
	if useManifest {
		targetManifestPath, err := transformManifestPath(source.GetManifestPath(), source, target)
		if err != nil {
			return nil, copiedFiles, merr.Wrap(err, "failed to transform manifest path")
		}
		result.ManifestPath = targetManifestPath
	}

	mlog.Info(context.TODO(), "copy segment and index files completed successfully",
		mlog.Int64("importedRows", result.ImportedRows))

	return result, copiedFiles, nil
}

// transformFieldBinlogs transforms source FieldBinlog list to destination by replacing paths
// using the pre-calculated mappings, while preserving all other metadata.
//
// This function is used to build the segment metadata that DataCoord needs for tracking
// the imported segment. All source binlog metadata is preserved except for the file paths,
// which are replaced using the mappings generated during the copy operation.
//
// Parameters:
//   - srcFieldBinlogs: Source field binlogs with original paths
//   - mappings: Pre-calculated map of source path -> target path
//   - countRows: If true, accumulate total row count from EntriesNum (for insert logs only)
//   - isExternalTable: If true, skip path mapping because external table insert
//     binlogs carry row metadata without physical log paths
//
// Returns:
//   - []*datapb.FieldBinlog: Transformed binlog list with target paths
//   - int64: Total row count (sum of EntriesNum from all binlogs if countRows=true, 0 otherwise)
//   - error: Non-nil if any source path has no mapping (fail-fast on missing mappings)
func transformFieldBinlogs(
	srcFieldBinlogs []*datapb.FieldBinlog,
	mappings map[string]string,
	countRows bool,
	isExternalTable bool,
) ([]*datapb.FieldBinlog, int64, error) {
	result := make([]*datapb.FieldBinlog, 0, len(srcFieldBinlogs))
	var totalRows int64
	if countRows {
		totalRows = countRowsFromFieldBinlogs(srcFieldBinlogs)
	}

	for _, srcFieldBinlog := range srcFieldBinlogs {
		dstFieldBinlog := proto.Clone(srcFieldBinlog).(*datapb.FieldBinlog)
		dstFieldBinlog.Binlogs = make([]*datapb.Binlog, 0, len(srcFieldBinlog.GetBinlogs()))

		for _, srcBinlog := range srcFieldBinlog.GetBinlogs() {
			dstBinlog := proto.Clone(srcBinlog).(*datapb.Binlog)

			if !isExternalTable {
				srcPath := srcBinlog.GetLogPath()
				if srcPath == "" {
					continue
				}
				dstPath, ok := mappings[srcPath]
				if !ok {
					return nil, 0, merr.WrapErrServiceInternalMsg("no mapping found for source path: %s", srcPath)
				}
				dstBinlog.LogPath = dstPath
			}

			dstFieldBinlog.Binlogs = append(dstFieldBinlog.Binlogs, dstBinlog)
		}

		if len(dstFieldBinlog.Binlogs) > 0 {
			result = append(result, dstFieldBinlog)
		}
	}

	return result, totalRows, nil
}

func countRowsFromFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) int64 {
	for _, fieldBinlog := range fieldBinlogs {
		var rows int64
		for _, binlog := range fieldBinlog.GetBinlogs() {
			rows += binlog.GetEntriesNum()
		}
		if rows > 0 {
			return rows
		}
	}
	return 0
}

// generateSegmentInfoFromSource generates ImportSegmentInfo from CopySegmentSource
// by transforming all binlog paths and preserving metadata.
//
// This function constructs the complete segment metadata that DataCoord uses to track
// the imported segment. It processes all four types of binlogs:
//   - Insert binlogs (required): Contains row data, row count is summed for ImportedRows
//   - Stats binlogs (optional): Contains statistics like min/max values
//   - Delta binlogs (optional): Contains delete operations
//   - BM25 binlogs (optional): Contains BM25 index data
//
// All source binlog metadata (EntriesNum, TimestampFrom, TimestampTo, LogSize) is preserved
// to maintain data integrity and enable proper query/compaction operations.
//
// Parameters:
//   - source: Source segment with original binlog paths and metadata
//   - target: Target IDs (collection/partition/segment) for segment identification
//   - mappings: Pre-calculated path mappings (source -> target)
//
// Returns:
//   - *datapb.ImportSegmentInfo: Complete segment metadata with target paths and row counts
//   - error: Error if any binlog transformation fails
func generateSegmentInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (*datapb.ImportSegmentInfo, error) {
	segmentInfo := &datapb.ImportSegmentInfo{
		SegmentID:    target.GetSegmentId(),
		ImportedRows: 0,
		Binlogs:      []*datapb.FieldBinlog{},
		Statslogs:    []*datapb.FieldBinlog{},
		Deltalogs:    []*datapb.FieldBinlog{},
		Bm25Logs:     []*datapb.FieldBinlog{},
	}

	// Process insert binlogs (count rows)
	binlogs, totalRows, err := transformFieldBinlogs(source.GetInsertBinlogs(), mappings, true, source.GetIsExternalCollection())
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform insert binlogs")
	}
	segmentInfo.Binlogs = binlogs
	segmentInfo.ImportedRows = totalRows

	// Process stats binlogs (no row counting)
	statslogs, _, err := transformFieldBinlogs(source.GetStatsBinlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform stats binlogs")
	}
	segmentInfo.Statslogs = statslogs

	// Process delta binlogs (no row counting)
	deltalogs, _, err := transformFieldBinlogs(source.GetDeltaBinlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform delta binlogs")
	}
	segmentInfo.Deltalogs = deltalogs

	// Process BM25 binlogs (no row counting)
	bm25logs, _, err := transformFieldBinlogs(source.GetBm25Binlogs(), mappings, false, false)
	if err != nil {
		return nil, merr.Wrap(err, "failed to transform BM25 binlogs")
	}
	segmentInfo.Bm25Logs = bm25logs

	return segmentInfo, nil
}

// generateTargetPath converts source file path to target path by replacing collection/partition/segment IDs
// Binlog path format: {rootPath}/{log_type}/{collectionID}/{partitionID}/{segmentID}/{fieldID}/{logID}
// Example: files/insert_log/111/222/333/444/555.log -> files/insert_log/aaa/bbb/ccc/444/555.log
func generateTargetPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	// Convert IDs to strings for replacement
	targetCollectionIDStr := strconv.FormatInt(target.GetCollectionId(), 10)
	targetPartitionIDStr := strconv.FormatInt(target.GetPartitionId(), 10)
	targetSegmentIDStr := strconv.FormatInt(target.GetSegmentId(), 10)

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Find the log type index (insert_log, delta_log, stats_log, bm25_stats)
	// Path structure: .../log_type/collectionID/partitionID/segmentID/...
	logTypeIndex := -1
	for i, part := range parts {
		if part == BinlogTypeInsert || part == BinlogTypeDelta || part == BinlogTypeStats || part == BinlogTypeBM25 {
			logTypeIndex = i
			break
		}
	}

	if logTypeIndex == -1 || logTypeIndex+3 >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid binlog path structure: %s (expected log_type at a valid position)", sourcePath)
	}

	// Replace IDs in order: collectionID, partitionID, segmentID
	// log_type is at index logTypeIndex
	// collectionID is at index logTypeIndex + 1
	// partitionID is at index logTypeIndex + 2
	// segmentID is at index logTypeIndex + 3
	parts[logTypeIndex+1] = targetCollectionIDStr
	parts[logTypeIndex+2] = targetPartitionIDStr
	parts[logTypeIndex+3] = targetSegmentIDStr

	return path.Join(parts...), nil
}

// generateTargetLOBPath replaces collection and partition IDs in a LOB file path.
// LOB path structure: {root}/insert_log/{coll}/{part}/lobs/{field}/_data/{file}.vx
// Unlike segment paths, LOB paths have no segment ID component.
func generateTargetLOBPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	parts := strings.Split(sourcePath, "/")

	logTypeIndex := -1
	for i, part := range parts {
		if part == BinlogTypeInsert {
			logTypeIndex = i
			break
		}
	}

	// Path: .../{insert_log}/{coll}/{part}/lobs/...
	// Need at least logTypeIndex + 2 (coll and part) after insert_log
	if logTypeIndex == -1 || logTypeIndex+2 >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid LOB path structure: %s", sourcePath)
	}

	parts[logTypeIndex+1] = strconv.FormatInt(target.GetCollectionId(), 10)
	parts[logTypeIndex+2] = strconv.FormatInt(target.GetPartitionId(), 10)

	return path.Join(parts...), nil
}

// buildIndexInfoFromSource builds complete index metadata from source information.
//
// This function extracts and transforms all index metadata (vector/scalar, text, JSON)
// from the source segment, converting file paths to target paths using the provided mappings.
//
// Parameters:
//   - source: Source segment with index file information
//   - target: Target IDs for the segment
//   - mappings: Pre-calculated source->target path mappings
//
// Returns:
//   - Vector/Scalar index metadata (buildID -> VectorScalarIndexInfo)
//   - Text index metadata (fieldID -> TextIndexStats)
//   - JSON Key index metadata (fieldID -> JsonKeyStats)
//   - error: Non-nil if any index file path has no mapping (fail-fast on missing mappings)
func buildIndexInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (
	map[int64]*datapb.VectorScalarIndexInfo,
	map[int64]*datapb.TextIndexStats,
	map[int64]*datapb.JsonKeyStats,
	error,
) {
	// Process vector/scalar indexes
	indexInfos := make(map[int64]*datapb.VectorScalarIndexInfo)
	for _, srcIndex := range source.GetIndexFiles() {
		// Transform index file paths using mappings
		targetPaths := make([]string, 0, len(srcIndex.GetIndexFilePaths()))
		for _, srcPath := range srcIndex.GetIndexFilePaths() {
			targetPath, ok := mappings[srcPath]
			if !ok {
				return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for index file: %s", srcPath)
			}
			targetPaths = append(targetPaths, targetPath)
		}

		// Use new buildID if available, otherwise fall back to source buildID
		buildID := srcIndex.GetBuildID()
		if newID, ok := target.GetNewBuildIds()[buildID]; ok {
			buildID = newID
		}

		indexInfos[buildID] = &datapb.VectorScalarIndexInfo{
			FieldId:                   srcIndex.GetFieldID(),
			IndexId:                   srcIndex.GetIndexID(),
			BuildId:                   buildID,
			Version:                   srcIndex.GetIndexVersion(),
			IndexFilePaths:            targetPaths,
			IndexSize:                 int64(srcIndex.GetSerializedSize()),
			CurrentIndexVersion:       srcIndex.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: srcIndex.GetCurrentScalarIndexVersion(),
			IndexName:                 srcIndex.GetIndexName(),
			IndexStorePathVersion:     srcIndex.GetIndexStorePathVersion(),
		}
	}

	// Process text indexes
	// For V3, text files are already copied via manifest basePath/_stats/;
	// pass metadata as placeholders (etcd paths may be stale or wrong format).
	// For V2, transform file paths using mappings.
	textIndexInfos := make(map[int64]*datapb.TextIndexStats)
	if source.GetStorageVersion() >= storage.StorageV3 {
		for fieldID, srcText := range source.GetTextIndexFiles() {
			dstText := proto.Clone(srcText).(*datapb.TextIndexStats)
			if newID, ok := target.GetNewBuildIds()[dstText.GetBuildID()]; ok {
				dstText.BuildID = newID
			}
			textIndexInfos[fieldID] = dstText
		}
	} else {
		for fieldID, srcText := range source.GetTextIndexFiles() {
			targetFiles := make([]string, 0, len(srcText.GetFiles()))
			for _, srcFile := range srcText.GetFiles() {
				targetFile, ok := mappings[srcFile]
				if !ok {
					return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for text index file: %s", srcFile)
				}
				targetFiles = append(targetFiles, targetFile)
			}

			dstText := proto.Clone(srcText).(*datapb.TextIndexStats)
			dstText.Files = targetFiles
			if newID, ok := target.GetNewBuildIds()[dstText.GetBuildID()]; ok {
				dstText.BuildID = newID
			}
			textIndexInfos[fieldID] = dstText
		}
	}

	// Process JSON Key indexes
	// For V3, json files are already copied via manifest basePath/_stats/;
	// pass metadata as placeholders. For V2, transform file paths using mappings.
	jsonKeyIndexInfos := make(map[int64]*datapb.JsonKeyStats)
	if source.GetStorageVersion() >= storage.StorageV3 {
		for fieldID, srcJSON := range source.GetJsonKeyIndexFiles() {
			dstJSON := proto.Clone(srcJSON).(*datapb.JsonKeyStats)
			if newID, ok := target.GetNewBuildIds()[dstJSON.GetBuildID()]; ok {
				dstJSON.BuildID = newID
			}
			jsonKeyIndexInfos[fieldID] = dstJSON
		}
	} else {
		for fieldID, srcJSON := range source.GetJsonKeyIndexFiles() {
			targetFiles := make([]string, 0, len(srcJSON.GetFiles()))
			for _, srcFile := range srcJSON.GetFiles() {
				targetFile, ok := mappings[srcFile]
				if !ok {
					return nil, nil, nil, merr.WrapErrServiceInternalMsg("no mapping found for JSON index file: %s", srcFile)
				}
				targetFiles = append(targetFiles, targetFile)
			}

			dstJSON := proto.Clone(srcJSON).(*datapb.JsonKeyStats)
			dstJSON.Files = targetFiles
			if newID, ok := target.GetNewBuildIds()[dstJSON.GetBuildID()]; ok {
				dstJSON.BuildID = newID
			}
			jsonKeyIndexInfos[fieldID] = dstJSON
		}
	}

	return indexInfos, textIndexInfos, jsonKeyIndexInfos, nil
}

// ============================================================================
// File Type Constants
// ============================================================================

// lobFileInfosToPaths extracts absolute file paths from LobFileInfo structs.
// GetManifestLobFiles returns paths that have already been resolved to absolute
// form by the C++ manifest deserializer (Manifest::ToAbsolutePaths), so we use
// them directly without any path concatenation.
func lobFileInfosToPaths(infos []packed.LobFileInfo) []string {
	paths := make([]string, 0, len(infos))
	for _, info := range infos {
		paths = append(paths, info.Path)
	}
	return paths
}

// File type constants used for path identification and generation.
// These constants match the directory names in Milvus storage paths.
const (
	BinlogTypeInsert        = "insert_log"
	BinlogTypeStats         = "stats_log"
	BinlogTypeDelta         = "delta_log"
	BinlogTypeBM25          = "bm25_stats"
	IndexTypeVectorScalarV0 = "index_files"
	IndexTypeVectorScalarV1 = "index_v1"
	IndexTypeText           = "text_log"
	IndexTypeJSONKey        = "json_key_index_log" // Legacy: JSON Key Inverted Index
	IndexTypeJSONStats      = "json_stats"         // New: JSON Stats with Shredding Design
	FileTypeLOB             = "lob"                // LOB files at partition level for TEXT fields
)

// generateTargetIndexPath is the unified function for generating target paths for all index types
// The indexType parameter specifies which type of index path to generate
//
// Supported index types (use constants):
//   - IndexTypeVectorScalarV0: Vector/Scalar v0 path format (legacy index_files prefix)
//     {rootPath}/index_files/{build_id}/{index_version}/{partition_id}/{segment_id}/file
//     Note: collectionID is NOT in the path, only partitionID and segmentID are replaced
//   - IndexTypeVectorScalarV1: Vector/Scalar v1 path format (index_v1 prefix)
//     {rootPath}/index_v1/{collection_id}/{partition_id}/{segment_id}/{build_id}/{index_version}/file
//   - IndexTypeText: Text Index path format
//     {rootPath}/text_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONKey: JSON Key Index path format (legacy)
//     {rootPath}/json_key_index_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONStats: JSON Stats path format (new, data_format >= 2)
//     {rootPath}/json_stats/{data_format_version}/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/(shared_key_index|shredding_data)/...
//
// Examples:
// generateTargetIndexPath(..., IndexTypeVectorScalarV0):
//
//	files/index_files/1001/1/222/333/scalar_index -> files/index_files/1001/1/bbb/ccc/scalar_index
//
// generateTargetIndexPath(..., IndexTypeText):
//
//	files/text_log/123/1/111/222/333/444/index_file -> files/text_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONKey):
//
//	files/json_key_index_log/123/1/111/222/333/444/index_file -> files/json_key_index_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONStats):
//
//	files/json_stats/2/123/1/111/222/333/444/shared_key_index/file -> files/json_stats/2/123/1/aaa/bbb/ccc/444/shared_key_index/file
func generateTargetIndexPath(
	sourcePath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	indexType string,
	pathVersion indexpb.IndexStorePathVersion,
) (string, error) {
	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Determine keyword and offsets based on index type
	var keywordIdx int
	var collectionOffset, partitionOffset, segmentOffset int

	keyword := indexType
	if indexType == IndexTypeVectorScalarV0 && metautil.IsCollectionRooted(pathVersion) {
		// The caller still passes the vector/scalar logical type, but v1 files
		// live under a different object-storage prefix.
		keyword = IndexTypeVectorScalarV1
	}

	// Find the keyword position in the path
	keywordIdx = -1
	for i, part := range parts {
		if part == keyword {
			keywordIdx = i
			break
		}
	}

	if keywordIdx == -1 {
		return "", merr.WrapErrServiceInternalMsg("keyword '%s' not found in path: %s", keyword, sourcePath)
	}

	// Set offsets based on index type
	// collectionOffset = -1 means collectionID is not present in the path
	var buildIDOffset int
	switch indexType {
	case IndexTypeVectorScalarV0:
		if metautil.IsCollectionRooted(pathVersion) {
			collectionOffset = 1
			partitionOffset = 2
			segmentOffset = 3
			buildIDOffset = 4
		} else {
			collectionOffset = -1
			partitionOffset = 3
			segmentOffset = 4
			buildIDOffset = 1
		}
	case IndexTypeText, IndexTypeJSONKey:
		// Text/JSON index: text_log|json_key_index_log/build/ver/coll/part/seg/field
		collectionOffset = 3
		partitionOffset = 4
		segmentOffset = 5
		buildIDOffset = 1
	case IndexTypeJSONStats:
		// JSON Stats: json_stats/data_format_ver/build/ver/coll/part/seg/field/(shared_key_index|shredding_data)/...
		collectionOffset = 4 // One more level than legacy (data_format_version)
		partitionOffset = 5
		segmentOffset = 6
		buildIDOffset = 2
	default:
		return "", merr.WrapErrParameterInvalidMsg("unsupported index type: %s (expected '%s', '%s', '%s', or '%s')",
			indexType, IndexTypeVectorScalarV0, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats)
	}

	// Validate path structure has enough components
	if keywordIdx+segmentOffset >= len(parts) {
		return "", merr.WrapErrParameterInvalidMsg("invalid %s path structure: %s (expected '%s' with at least %d components after it)",
			indexType, sourcePath, indexType, segmentOffset+1)
	}

	// Replace buildID if a mapping exists in target.NewBuildIds
	if keywordIdx+buildIDOffset < len(parts) {
		oldBuildIDStr := parts[keywordIdx+buildIDOffset]
		oldBuildID, parseErr := strconv.ParseInt(oldBuildIDStr, 10, 64)
		if parseErr == nil {
			if newBuildID, ok := target.GetNewBuildIds()[oldBuildID]; ok {
				parts[keywordIdx+buildIDOffset] = strconv.FormatInt(newBuildID, 10)
			}
		}
	}

	// Replace IDs at specified offsets
	// collectionOffset = -1 means collectionID is not present in the path (e.g., vector/scalar index)
	if collectionOffset >= 0 {
		parts[keywordIdx+collectionOffset] = strconv.FormatInt(target.GetCollectionId(), 10)
	}
	parts[keywordIdx+partitionOffset] = strconv.FormatInt(target.GetPartitionId(), 10)
	parts[keywordIdx+segmentOffset] = strconv.FormatInt(target.GetSegmentId(), 10)

	return path.Join(parts...), nil
}

// ============================================================================
// Path Compression Utilities
// ============================================================================
// These functions compress file paths before returning to DataCoord to reduce
// RPC response size and network transmission overhead.
// The implementations are copied from internal/datacoord/copy_segment_task.go
// to maintain consistency with DataCoord's compression logic.

const (
	jsonStatsSharedIndexPath   = "shared_key_index"
	jsonStatsShreddingDataPath = "shredding_data"
)

// shortenIndexFilePaths shortens vector/scalar index file paths to only keep the base filename.
//
// In normal index building flow, only the base filename (last path segment) is stored in IndexFileKeys.
// In copy segment flow, DataNode returns full paths after file copying.
// This function extracts the base filename to match the format expected by QueryNode loading.
//
// Path transformation:
//   - Input:  "files/index_files/444/555/666/100/1001/1002/scalar_index"
//   - Output: "scalar_index"
//
// Why only base filename:
// - DataCoord rebuilds full paths using BuildSegmentIndexFilePaths when needed
// - Storing full paths would cause duplicate path concatenation
// - Matches the convention from normal index building
//
// Parameters:
//   - fullPaths: List of full index file paths
//
// Returns:
//   - List of base filenames (last segment of each path)
func shortenIndexFilePaths(fullPaths []string) []string {
	result := make([]string, 0, len(fullPaths))
	for _, fullPath := range fullPaths {
		// Extract base filename (last segment after final '/')
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			result = append(result, parts[len(parts)-1])
		}
	}
	return result
}

// shortenJSONStatsPath shortens JSON stats file paths to only keep the last 2+ segments.
//
// In normal import flow, the C++ core returns already-shortened paths (e.g., "shared_key_index/file").
// In copy segment flow, DataNode returns full paths after file copying.
// This function normalizes the paths to match the format expected by query nodes.
//
// Path transformation:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//   - Output: "shared_key_index/inverted_index_0"
//
// Parameters:
//   - jsonStats: Map of field ID to JsonKeyStats with full paths
//
// Returns:
//   - Map of field ID to JsonKeyStats with shortened paths
func shortenJSONStatsPath(jsonStats map[int64]*datapb.JsonKeyStats) map[int64]*datapb.JsonKeyStats {
	result := make(map[int64]*datapb.JsonKeyStats)
	for fieldID, stats := range jsonStats {
		shortenedFiles := make([]string, 0, len(stats.GetFiles()))
		for _, file := range stats.GetFiles() {
			shortenedFiles = append(shortenedFiles, shortenSingleJSONStatsPath(file))
		}

		result[fieldID] = &datapb.JsonKeyStats{
			FieldID:                stats.GetFieldID(),
			Version:                stats.GetVersion(),
			BuildID:                stats.GetBuildID(),
			Files:                  shortenedFiles,
			JsonKeyStatsDataFormat: stats.GetJsonKeyStatsDataFormat(),
			MemorySize:             stats.GetMemorySize(),
			LogSize:                stats.GetLogSize(),
		}
	}
	return result
}

// shortenSingleJSONStatsPath shortens a single JSON stats file path.
//
// This function extracts the relative path from a full JSON stats file path by:
//  1. Finding "shared_key_index" or "shredding_data" keywords and extracting from that position
//  2. For files directly under fieldID directory (e.g., meta.json), extracting everything after
//     the 7 path components following "json_stats"
//
// Path format: {root}/json_stats/{dataFormat}/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}/...
//
// Path examples:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//     Output: "shared_key_index/inverted_index_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shredding_data/parquet_data_0"
//     Output: "shredding_data/parquet_data_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/meta.json"
//     Output: "meta.json"
//   - Input:  "shared_key_index/inverted_index_0" (already shortened)
//     Output: "shared_key_index/inverted_index_0" (idempotent)
//   - Input:  "meta.json" (already shortened)
//     Output: "meta.json" (idempotent)
//
// Parameters:
//   - fullPath: Full or partial JSON stats file path
//
// Returns:
//   - Shortened path relative to fieldID directory
func shortenSingleJSONStatsPath(fullPath string) string {
	// Find "shared_key_index" in path
	if idx := strings.Index(fullPath, jsonStatsSharedIndexPath); idx != -1 {
		return fullPath[idx:]
	}
	// Find "shredding_data" in path
	if idx := strings.Index(fullPath, jsonStatsShreddingDataPath); idx != -1 {
		return fullPath[idx:]
	}

	// Handle files directly under fieldID directory (e.g., meta.json)
	// Path format: .../json_stats/{dataFormat}/{build}/{ver}/{coll}/{part}/{seg}/{field}/filename
	// json_stats is followed by 7 components, the 8th onwards is the file path
	parts := strings.Split(fullPath, "/")
	for i, part := range parts {
		if part == common.JSONStatsPath && i+8 < len(parts) {
			return path.Join(parts[i+8:]...)
		}
	}

	// If already shortened or no json_stats found, return as-is
	return fullPath
}
