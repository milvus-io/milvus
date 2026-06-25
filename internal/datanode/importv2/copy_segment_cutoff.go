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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
	copySource   *datapb.CopySegmentSource
	metaSource   *datapb.CopySegmentSource
	mappings     map[string]string
	copiedFiles  []string
	importedRows int64
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
		copySource: copySource,
		metaSource: metaSource,
		mappings:   make(map[string]string),
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
		SegmentId:    target.GetSegmentId(),
		ImportedRows: int64(rowCount),
		Binlogs:      binlogs,
		ManifestPath: manifestPath,
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
		if err := visitPKTimestampRecord(record, 0, pkType, func(pk storage.PrimaryKey, ts uint64) error {
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
	visit func(storage.PrimaryKey, uint64) error,
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
			if err := visit(pk, ts); err != nil {
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
			if err := visit(pk, ts); err != nil {
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
func isManifestControlFile(basePath, filePath string) bool {
	rel := strings.TrimPrefix(filePath, strings.TrimRight(basePath, "/")+"/")
	return strings.HasPrefix(rel, "_delta/") || strings.HasPrefix(rel, "_metadata/")
}

// copyFile copies a single file from src to dst using the chunk manager.
