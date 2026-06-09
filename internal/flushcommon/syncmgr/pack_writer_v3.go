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

package syncmgr

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// packedBatchWriter is the common subset both storage.packedRecordBatchWriter
// and storage.packedTextBatchWriter expose to the V3 sync path. Close
// returns a packed.WriterOutput regardless of which concrete writer is
// in use; the V3 sync path treats both uniformly.
type packedBatchWriter interface {
	Write(r storage.Record) error
	Close() (packed.WriterOutput, error)
	GetWrittenUncompressed() uint64
	GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64
	GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64
	GetWrittenPaths(columnGroup typeutil.UniqueID) string
	GetWrittenRowNum() int64
}

type BulkPackWriterV3 struct {
	*BulkPackWriterV2

	manifestPath string

	// initialManifestPath captures the manifest path observed when Write is
	// invoked. Read-based merges (existing bloom filter / BM25 file lists)
	// reference this stable value instead of bw.manifestPath so they cannot
	// drift after Phase 2's commit updates manifestPath.
	initialManifestPath string

	// pendingMetaCacheActions accumulates RollStats / MergeBm25Stats actions
	// that writeStats / writeBM25Stasts would otherwise apply directly to the
	// metaCache. They are drained by SyncTask.Run only after a successful
	// Write, so a failed commit retry never observes partial metaCache state.
	pendingMetaCacheActions []metacache.SegmentAction

	// singlePKStats holds the per-batch PK statistic computed during
	// writeStats and passed explicitly into the serializer's *With variants
	// so merged-stats computation can include this batch without requiring
	// the metaCache to be updated yet.
	singlePKStats *storage.PrimaryKeyStats

	// statsBlobSize tracks THIS SYNC's newly-written bloom-filter / BM25 blob
	// bytes — the per-sync delta the StatisticsCollector accumulates, NOT the
	// cumulative footprint. The merged PK/BM25 blob is written only at flush and
	// per-sync batch blobs use unique keys, so the sum of these per-sync deltas
	// over the segment's life equals the manifest's final cumulative memory_size.
	// Reset at the top of Write and fed into the StatisticsCollector Digest.
	statsBlobSize int64
}

func NewBulkPackWriterV3(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, bufferSize, multiPartUploadSize int64,
	storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup, curManifestPath string, writeRetryOpts ...retry.Option,
) *BulkPackWriterV3 {
	bwV2 := NewBulkPackWriterV2(metaCache, schema, chunkManager, allocator, bufferSize,
		multiPartUploadSize, storageConfig, columnGroups, writeRetryOpts...)
	return &BulkPackWriterV3{
		BulkPackWriterV2: bwV2,
		manifestPath:     curManifestPath,
	}
}

// Write executes a SyncPack as do-then-commit:
//
//  1. Phase 1 (slow, runs once): write all data files — parquet/LOB via the
//     loon writer, deltalog via the deltalog writer, stat blobs via the
//     filesystem FFI — and assemble a single packed.ManifestUpdates payload
//     describing every change the segment needs to register.
//  2. Phase 2 (fast, retried on transient loon errors): call
//     packed.CommitManifestUpdates once. The loon transaction handle is
//     opened, all changes are staged, and the transaction is committed in
//     one shot, producing exactly one manifest version bump.
//
// metaCache mutations produced by Phase 1's stats helpers are accumulated
// in bw.pendingMetaCacheActions and only drained on success, so a failed
// retry cycle leaves the metaCache untouched.
func (bw *BulkPackWriterV3) Write(ctx context.Context, pack *SyncPack) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	segmentStats *datapb.Statistics,
	err error,
) {
	log := log.Ctx(ctx)
	bw.initialManifestPath = bw.manifestPath
	bw.statsBlobSize = 0

	// Drain deferred metaCache actions on successful Write only. If we
	// return an error we leave the metaCache untouched, so a failed sync
	// does not pollute bloom-filter / BM25 history.
	defer func() {
		if err != nil {
			return
		}
		if len(bw.pendingMetaCacheActions) == 0 {
			return
		}
		bw.metaCache.UpdateSegments(
			metacache.MergeSegmentAction(bw.pendingMetaCacheActions...),
			metacache.WithSegmentIDs(pack.segmentID),
		)
	}()

	basePath, baseVersion, parseErr := packed.UnmarshalManifestPath(bw.initialManifestPath)
	if parseErr != nil {
		err = parseErr
		return
	}

	// Phase 1: write files. Each helper returns its contribution to the
	// final ManifestUpdates instead of mutating shared state.
	var (
		insertFiles  packed.WriterOutput
		statEntries  []packed.StatEntry
		deltaEntries []packed.DeltaLogEntry
		bm25Entries  []packed.StatEntry
	)
	defer func() {
		if insertFiles != nil {
			insertFiles.Destroy()
		}
	}()

	if inserts, insertFiles, err = bw.writeInserts(ctx, pack, basePath); err != nil {
		log.Warn("failed to write insert data", zap.Error(err))
		return
	}
	if statEntries, err = bw.writeStats(ctx, pack, basePath); err != nil {
		log.Warn("failed to process stats blob", zap.Error(err))
		return
	}
	if deltas, deltaEntries, err = bw.writeDelta(ctx, pack, basePath); err != nil {
		log.Warn("failed to process delta blob", zap.Error(err))
		return
	}
	if bm25Entries, err = bw.writeBM25Stasts(ctx, pack, basePath); err != nil {
		log.Warn("failed to process bm25 stats blob", zap.Error(err))
		return
	}

	updates := &packed.ManifestUpdates{
		NewFiles:  insertFiles,
		DeltaLogs: deltaEntries,
		Stats:     append(statEntries, bm25Entries...),
	}

	// Phase 2: commit the assembled updates. CommitManifestUpdates short-
	// circuits to the unchanged manifest path when updates carry nothing.
	// The outer retry.Do handles transient FFI errors classified as
	// packed.ErrLoonTransient; loon's own optimistic retry covers
	// manifest-version conflicts within a single attempt.
	err = retry.Do(ctx, func() error {
		newPath, commitErr := packed.CommitManifestUpdates(basePath, baseVersion, bw.storageConfig, updates)
		if commitErr != nil {
			return classifyLoonErr(commitErr)
		}
		bw.manifestPath = newPath
		return nil
	}, bw.writeRetryOpts...)
	if err != nil {
		return
	}

	// Digest this sync's writes into the growing segment's cumulative
	// StatisticsCollector. The digest is DEFERRED like RollStats /
	// MergeBm25Stats so it commits atomically with the manifest on a
	// successful Write — the metaCache is only mutated by the success defer at
	// the top of Write. V3 feeds the tracked statsBlobSize instead of summing
	// a stats array (it returns no stats array).
	digested := len(inserts) > 0 || bw.statsBlobSize > 0 || len(deltas.GetBinlogs()) > 0
	if digested {
		bw.pendingMetaCacheActions = append(bw.pendingMetaCacheActions,
			metacache.MergeStatistics(inserts, deltas, bw.statsBlobSize, pack.batchRows, pack.tsFrom, pack.tsTo))
	}

	manifest = bw.manifestPath
	size = bw.sizeWritten

	// Publish the segment's cumulative Statistics. No scaling — the value
	// reflects exactly the syncs the collector has digested so far.
	//
	// The MergeStatistics digest above is deferred and has NOT been applied to
	// the metaCache yet (the success defer runs after this Write body returns).
	// To publish a value that includes THIS sync we clone the segment's
	// collector, fold this sync's writes into the clone, and publish the
	// clone — leaving the metaCache untouched until the deferred commit. This
	// mirrors how V3 includes the current batch in merged PK stats via
	// serializeMergedPkStatsWith before RollStats is applied.
	seg, ok := bw.metaCache.GetSegmentByID(pack.segmentID)
	if !ok {
		err = merr.WrapErrSegmentNotFound(pack.segmentID)
		return
	}
	clone := seg.Statistics().Clone()
	if digested {
		clone.Digest(inserts, deltas, bw.statsBlobSize, pack.batchRows, pack.tsFrom, pack.tsTo)
	}
	segmentStats = clone.Publish()
	return
}

// classifyLoonErr maps loon FFI failures to retryable errors and everything
// else to retry.Unrecoverable so the outer retry loop terminates immediately.
//
// NOTE: today milvus-storage does not expose structured error codes, so
// packed.ErrLoonTransient covers ALL loon errors, including non-recoverable
// IO failures. The bounded retry budget keeps the worst case finite. Once
// milvus-storage adds explicit error codes, narrow the retryable set here so
// only the concurrent-transaction case retries.
func classifyLoonErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, packed.ErrLoonTransient) {
		return err
	}
	return retry.Unrecoverable(err)
}

// writeInserts writes the insert data files for the SyncPack and returns
// the produced WriterOutput plus the per-column-group binlog metadata.
// The returned WriterOutput is owned by the caller and must be Destroy'd
// after the surrounding commit (success or failure).
func (bw *BulkPackWriterV3) writeInserts(ctx context.Context, pack *SyncPack, basePath string) (logs map[int64]*datapb.FieldBinlog, files packed.WriterOutput, err error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), nil, nil
	}

	rec, err := bw.serializeBinlog(ctx, pack)
	if err != nil {
		return nil, nil, err
	}
	defer rec.Release()

	tsFrom, tsTo := bw.getTsRange(rec)
	pluginContextPtr := bw.getPluginContext(pack.collectionID)
	writerFormat, schemaBasedFormats, err := bw.resolveInsertWriterFormats()
	if err != nil {
		return nil, nil, err
	}

	// LOB base path is at partition level: {basePath}/.. = {root}/insert_log/{coll}/{part}
	partitionBasePath := path.Dir(basePath)
	textColumnConfigs := buildTextColumnConfigs(bw.schema, partitionBasePath)
	var w packedBatchWriter
	if len(textColumnConfigs) > 0 {
		log.Ctx(ctx).Info("using TEXT-aware writer for import",
			zap.Int("textFieldCount", len(textColumnConfigs)),
			zap.String("basePath", basePath))
		w, err = storage.NewPackedTextBatchWriter("", basePath, bw.schema,
			bw.bufferSize, bw.multiPartUploadSize, bw.columnGroups, bw.storageConfig, textColumnConfigs, writerFormat, schemaBasedFormats)
	} else {
		w, err = storage.NewPackedRecordBatchWriter(basePath, bw.schema,
			bw.bufferSize, bw.multiPartUploadSize, bw.columnGroups, bw.storageConfig, pluginContextPtr, writerFormat, schemaBasedFormats)
	}
	if err != nil {
		return nil, nil, err
	}

	// Ensure the FFI writer's C resources are reclaimed even if Write fails
	// before we reach the explicit Close below. Once Close has been called
	// (success or failure) the writer's own defer has already released its
	// handle and properties, so closeAttempted gates against a redundant
	// second Close from this defer.
	closeAttempted := false
	defer func() {
		if !closeAttempted {
			if out, closeErr := w.Close(); closeErr == nil && out != nil {
				out.Destroy()
			}
		}
	}()

	if err = w.Write(rec); err != nil {
		log.Ctx(ctx).Warn("failed to write inserts",
			zap.Int64("collectionID", pack.collectionID),
			zap.Int64("segmentID", pack.segmentID),
			zap.Error(err))
		return nil, nil, err
	}

	closeAttempted = true
	if files, err = w.Close(); err != nil {
		return nil, nil, err
	}

	getFieldNullCounts := func(columnGroup storagecommon.ColumnGroup) map[int64]int64 {
		result := make(map[int64]int64, len(columnGroup.Fields))
		for _, fieldID := range columnGroup.Fields {
			if col := rec.Column(fieldID); col != nil {
				result[fieldID] = int64(col.NullN())
			}
		}
		return result
	}

	logs = make(map[int64]*datapb.FieldBinlog)
	for _, columnGroup := range bw.columnGroups {
		columnGroupID := columnGroup.GroupID
		memSize := int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID))
		nullCounts := getFieldNullCounts(columnGroup)
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Format:      columnGroup.Format,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:         int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
					MemorySize:      memSize,
					LogPath:         w.GetWrittenPaths(columnGroupID),
					EntriesNum:      w.GetWrittenRowNum(),
					TimestampFrom:   tsFrom,
					TimestampTo:     tsTo,
					FieldNullCounts: nullCounts,
				},
			},
		}
	}
	return logs, files, nil
}

func (bw *BulkPackWriterV3) resolveInsertWriterFormats() (string, []string, error) {
	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	if bw.initialManifestPath != "" {
		_, version, err := packed.UnmarshalManifestPath(bw.initialManifestPath)
		if err != nil {
			return "", nil, err
		}
		if version != packed.ManifestEarliest {
			for _, columnGroup := range bw.columnGroups {
				if columnGroup.Format == "" {
					return "", nil, fmt.Errorf("column group %d fields %v missing format for existing manifest %s",
						columnGroup.GroupID, columnGroup.Fields, bw.initialManifestPath)
				}
			}
		}
	}
	schemaBasedFormats := storagecommon.ColumnGroupFormats(bw.columnGroups, writerFormat)
	return writerFormat, schemaBasedFormats, nil
}

// writeDelta writes the deltalog file and returns the DeltaLogEntry list
// the caller will fold into ManifestUpdates plus a pathless delta summary
// FieldBinlog (EntriesNum + MemorySize) used by compaction-trigger
// decisions.
func (bw *BulkPackWriterV3) writeDelta(ctx context.Context, pack *SyncPack, basePath string) (*datapb.FieldBinlog, []packed.DeltaLogEntry, error) {
	if pack.deltaData == nil || pack.deltaData.RowCount == 0 {
		return nil, nil, nil
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(bw.schema)
	if err != nil {
		return nil, nil, fmt.Errorf("primary key field not found: %w", err)
	}

	logID, err := bw.allocator.AllocOne()
	if err != nil {
		return nil, nil, err
	}
	deltaPath := metautil.BuildDeltaLogPathV3(basePath, logID)

	writer, err := storage.NewDeltalogWriter(
		ctx, pack.collectionID, pack.partitionID, pack.segmentID, logID, pkField.DataType, deltaPath,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(bw.storageConfig),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create deltalog writer: %w", err)
	}

	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(pack.deltaData.Pks, pack.deltaData.Tss)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build delete record: %w", err)
	}
	defer record.Release()

	if err := writer.Write(record); err != nil {
		return nil, nil, fmt.Errorf("failed to write delta record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close delta writer: %w", err)
	}

	bw.sizeWritten += pack.deltaData.Size()
	deltaMemSize := int64(writer.GetWrittenUncompressed())

	summary := &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{{
			LogID:         logID,
			EntriesNum:    pack.deltaData.RowCount,
			MemorySize:    deltaMemSize,
			TimestampFrom: tsFrom,
			TimestampTo:   tsTo,
		}},
	}
	return summary, []packed.DeltaLogEntry{{Path: deltaPath, NumEntries: pack.deltaData.RowCount}}, nil
}

// writeStats writes bloom filter stat blobs under basePath/_stats and
// returns the resulting StatEntry list. The caller folds the entries into
// a ManifestUpdates that commits atomically with inserts / delta / bm25.
func (bw *BulkPackWriterV3) writeStats(ctx context.Context, pack *SyncPack, basePath string) ([]packed.StatEntry, error) {
	if len(pack.insertData) == 0 {
		return nil, nil
	}

	serializer, err := NewStorageSerializer(bw.metaCache, bw.schema)
	if err != nil {
		return nil, err
	}
	singlePKStats, batchStatsBlob, err := serializer.serializeStatslog(pack)
	if err != nil {
		return nil, err
	}
	bw.singlePKStats = singlePKStats

	// DEFERRED: do NOT call metaCache.UpdateSegments(RollStats(...)) here.
	// Stash the action; SyncTask.Run applies it after Write returns success
	// so that retries do not double-roll the bloom filter. The merged-stats
	// path below uses serializeMergedPkStatsWith to inject this batch
	// explicitly without depending on the metaCache being updated yet.
	bw.pendingMetaCacheActions = append(bw.pendingMetaCacheActions,
		metacache.RollStats(singlePKStats))

	pkFieldID := serializer.pkField.GetFieldID()

	var files []string
	var memorySize int64
	// newBlobBytes is only the blob bytes WRITTEN this sync (NOT the preserved
	// existing footprint). The collector accumulates these per-sync deltas; the
	// merged blob writes only at flush so Σ(newBlobBytes) == final memory_size.
	var newBlobBytes int64

	// Preserve existing bloom filter files from previous batches.
	// loon_transaction_update_stat uses replace semantics, so we must
	// merge previously written files into the new entry.
	statKey := fmt.Sprintf("bloom_filter.%d", pkFieldID)
	existingStats, err := packed.GetManifestStats(bw.initialManifestPath, bw.storageConfig)
	if err == nil {
		if existing, ok := existingStats[statKey]; ok && len(existing.Paths) > 0 {
			files = append(files, existing.Paths...)
			if memStr, ok := existing.Metadata["memory_size"]; ok {
				existingMem, _ := strconv.ParseInt(memStr, 10, 64)
				memorySize += existingMem
			}
		}
	}

	// Write batch stats blob via filesystem FFI.
	id, err := bw.allocator.AllocOne()
	if err != nil {
		return nil, err
	}
	relPath := fmt.Sprintf("_stats/bloom_filter.%d/%d", pkFieldID, id)
	fullPath := path.Join(basePath, relPath)
	if err := packed.WriteFile(bw.storageConfig, fullPath, batchStatsBlob.Value); err != nil {
		return nil, err
	}
	bw.sizeWritten += int64(len(batchStatsBlob.Value))
	memorySize += int64(len(batchStatsBlob.Value))
	newBlobBytes += int64(len(batchStatsBlob.Value))
	files = append(files, fullPath)

	// Write merged stats on flush
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 {
		// Use the *With variant to include this batch's PK stats explicitly,
		// since the corresponding RollStats has been deferred above.
		mergedStatsBlob, err := serializer.serializeMergedPkStatsWith(pack, singlePKStats)
		if err != nil {
			return nil, err
		}
		mergedRelPath := fmt.Sprintf("_stats/bloom_filter.%d/%d", pkFieldID, int64(storage.CompoundStatsType))
		mergedFullPath := path.Join(basePath, mergedRelPath)
		if err := packed.WriteFile(bw.storageConfig, mergedFullPath, mergedStatsBlob.Value); err != nil {
			return nil, err
		}
		bw.sizeWritten += int64(len(mergedStatsBlob.Value))
		memorySize += int64(len(mergedStatsBlob.Value))
		newBlobBytes += int64(len(mergedStatsBlob.Value))
		files = append(files, mergedFullPath)
	}

	// Feed the collector only THIS SYNC's newly-written blob bytes; memorySize
	// stays cumulative for the manifest StatEntry below.
	bw.statsBlobSize += newBlobBytes

	return []packed.StatEntry{{
		Key:      statKey,
		Files:    files,
		Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", memorySize)},
	}}, nil
}

// writeBM25Stasts writes BM25 stat blobs under basePath/_stats and returns
// the resulting StatEntry list. The caller folds the entries into a
// ManifestUpdates that commits atomically with inserts / delta / stats.
func (bw *BulkPackWriterV3) writeBM25Stasts(ctx context.Context, pack *SyncPack, basePath string) ([]packed.StatEntry, error) {
	if len(pack.bm25Stats) == 0 {
		return nil, nil
	}

	serializer, err := NewStorageSerializer(bw.metaCache, bw.schema)
	if err != nil {
		return nil, err
	}
	bm25Blobs, err := serializer.serializeBM25Stats(pack)
	if err != nil {
		return nil, err
	}

	// Track per-field files and memory sizes, then build stat entries at the end
	type fieldStats struct {
		files      []string
		memorySize int64
	}
	fieldMap := make(map[int64]*fieldStats)

	// newBlobBytes is only the BM25 blob bytes WRITTEN this sync across all
	// fields (NOT the preserved existing footprint). The collector accumulates
	// these per-sync deltas; the merged blob writes only at flush and per-sync
	// blobs use unique keys, so Σ(newBlobBytes) == final cumulative memory_size.
	var newBlobBytes int64

	// Preserve existing BM25 stat files from previous batches.
	existingStats, err := packed.GetManifestStats(bw.initialManifestPath, bw.storageConfig)
	if err == nil {
		for key, existing := range existingStats {
			prefix, fieldID, ok := packed.ParseStatKey(key)
			if !ok || prefix != "bm25" || len(existing.Paths) == 0 {
				continue
			}
			fs := &fieldStats{files: existing.Paths}
			if memStr, ok := existing.Metadata["memory_size"]; ok {
				fs.memorySize, _ = strconv.ParseInt(memStr, 10, 64)
			}
			fieldMap[fieldID] = fs
		}
	}

	for fieldID, blob := range bm25Blobs {
		id, err := bw.allocator.AllocOne()
		if err != nil {
			return nil, err
		}

		relPath := fmt.Sprintf("_stats/bm25.%d/%d", fieldID, id)
		fullPath := path.Join(basePath, relPath)
		if err := packed.WriteFile(bw.storageConfig, fullPath, blob.Value); err != nil {
			return nil, err
		}
		bw.sizeWritten += int64(len(blob.Value))

		fs := fieldMap[fieldID]
		if fs == nil {
			fs = &fieldStats{}
			fieldMap[fieldID] = fs
		}
		fs.files = append(fs.files, fullPath)
		fs.memorySize += int64(len(blob.Value))
		newBlobBytes += int64(len(blob.Value))
	}

	// DEFERRED: do NOT apply MergeBm25Stats to the metaCache here. Stash the
	// action; SyncTask.Run applies it after a successful Write so retries do
	// not double-merge the BM25 stats. The merged-stats path below uses
	// serializeMergedBM25StatsWith to inject this batch explicitly.
	bw.pendingMetaCacheActions = append(bw.pendingMetaCacheActions,
		metacache.MergeBm25Stats(pack.bm25Stats))

	// Write merged BM25 stats on flush
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 && hasBM25Function(bw.schema) {
		// Use the *With variant to include this batch's bm25 stats explicitly,
		// since the corresponding MergeBm25Stats has been deferred above.
		mergedBM25Blob, err := serializer.serializeMergedBM25StatsWith(pack, pack.bm25Stats)
		if err != nil {
			return nil, err
		}
		for fieldID, blob := range mergedBM25Blob {
			mergedRelPath := fmt.Sprintf("_stats/bm25.%d/%d", fieldID, int64(storage.CompoundStatsType))
			mergedFullPath := path.Join(basePath, mergedRelPath)
			if err := packed.WriteFile(bw.storageConfig, mergedFullPath, blob.Value); err != nil {
				return nil, err
			}
			bw.sizeWritten += int64(len(blob.Value))

			fs := fieldMap[fieldID]
			if fs == nil {
				fs = &fieldStats{}
				fieldMap[fieldID] = fs
			}
			fs.files = append(fs.files, mergedFullPath)
			fs.memorySize += int64(len(blob.Value))
			newBlobBytes += int64(len(blob.Value))
		}
	}

	var entries []packed.StatEntry
	for fieldID, fs := range fieldMap {
		entries = append(entries, packed.StatEntry{
			Key:      fmt.Sprintf("bm25.%d", fieldID),
			Files:    fs.files,
			Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", fs.memorySize)},
		})
	}

	// Feed the collector only THIS SYNC's newly-written BM25 blob bytes (summed
	// across all fields); fs.memorySize stays cumulative for the StatEntry above.
	bw.statsBlobSize += newBlobBytes

	return entries, nil
}

// buildTextColumnConfigs builds TextColumnConfig for all TEXT fields in the schema.
// partitionBasePath is the partition-level path: {root}/insert_log/{coll}/{part}
// Per-column LOB path: {partitionBasePath}/lobs/{field_id}
func buildTextColumnConfigs(schema *schemapb.CollectionSchema, partitionBasePath string) []packed.TextColumnConfig {
	var configs []packed.TextColumnConfig
	for _, field := range schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			fieldID := field.GetFieldID()
			configs = append(configs, packed.TextColumnConfig{
				FieldID:             fieldID,
				LobBasePath:         path.Join(partitionBasePath, "lobs", strconv.FormatInt(fieldID, 10)),
				InlineThreshold:     paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64(),
				MaxLobFileBytes:     paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(),
				FlushThresholdBytes: paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
			})
		}
	}
	return configs
}
