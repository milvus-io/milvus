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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BulkPackWriterV3 struct {
	*BulkPackWriterV2

	manifestPath string

	// initialManifestPath captures the manifest path observed when Write was
	// first invoked. resetForRetry restores manifestPath to this value so each
	// retry attempt restarts from the same base manifest version. Touching this
	// invariant breaks the retry correctness argument — see
	// ccmd/pack_writer_v3_retry_plan.md.
	initialManifestPath string

	// pendingMetaCacheActions accumulates RollStats / MergeBm25Stats actions
	// that writeStats / writeBM25Stasts would otherwise apply directly to the
	// metaCache. They are deferred and drained by SyncTask.Run only after a
	// successful Write, so that retried attempts do not double-apply.
	pendingMetaCacheActions []metacache.SegmentAction

	// singlePKStats holds the per-batch PK statistic computed once per Write
	// call. It is reused across retry attempts and passed explicitly into the
	// serializer's *With variants so merged-stats computation can include this
	// batch without requiring the metaCache to be updated yet.
	singlePKStats *storage.PrimaryKeyStats
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

// Write performs the four manifest-mutating steps for a SyncPack inside a
// retry loop. The retry handles loon transaction conflicts (currently
// surfaced as packed.ErrLoonTransient — see internal/storagev2/packed
// /ffi_common.go for the reason this is coarse-grained today).
//
// Each attempt restarts from bw.initialManifestPath via resetForRetry so the
// manifest read base does not drift across retries. metaCache mutations
// produced by writeStats / writeBM25Stasts are accumulated in
// bw.pendingMetaCacheActions and applied via a deferred drainer that runs
// only when Write returns nil — this is what makes retries idempotent on
// metaCache without exposing the pending list to callers.
func (bw *BulkPackWriterV3) Write(ctx context.Context, pack *SyncPack) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	err error,
) {
	log := log.Ctx(ctx)
	bw.initialManifestPath = bw.manifestPath

	// Drain deferred metaCache actions on successful Write only. If the retry
	// loop terminates with an error we leave the metaCache untouched, so a
	// failed sync does not pollute bloom-filter / BM25 history.
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

	var deltaSummary *datapb.FieldBinlog
	err = retry.Do(ctx, func() error {
		bw.resetForRetry()

		var innerErr error
		if inserts, manifest, innerErr = bw.writeInserts(ctx, pack); innerErr != nil {
			log.Warn("failed to write insert data", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// Update manifestPath after writeInserts
		bw.manifestPath = manifest

		// writeStats for V3 adds bloom filter stats to manifest
		if stats, innerErr = bw.writeStats(ctx, pack); innerErr != nil {
			log.Warn("failed to process stats blob", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// writeDelta for V3 updates manifest and returns delta summary
		if manifest, deltaSummary, innerErr = bw.writeDelta(ctx, pack); innerErr != nil {
			log.Warn("failed to process delta blob", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// writeBM25Stasts for V3 adds BM25 stats to manifest
		if bm25Stats, innerErr = bw.writeBM25Stasts(ctx, pack); innerErr != nil {
			log.Warn("failed to process bm25 stats blob", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		return nil
	}, bw.writeRetryOpts...)
	if err != nil {
		return
	}

	// For V3, stats are in manifest; delta summary is returned for compaction trigger
	deltas = deltaSummary
	manifest = bw.manifestPath
	size = bw.sizeWritten
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

// resetForRetry restores per-attempt state. It MUST restore manifestPath to
// initialManifestPath — see the field doc on initialManifestPath for why.
func (bw *BulkPackWriterV3) resetForRetry() {
	bw.manifestPath = bw.initialManifestPath
	bw.sizeWritten = 0
	bw.pendingMetaCacheActions = bw.pendingMetaCacheActions[:0]
	bw.singlePKStats = nil
	// Files written under _stats/ and _delta/ by previous failed attempts are
	// intentionally NOT cleaned up here: they are unreferenced by any
	// committed manifest and will be reclaimed by loon GC. See
	// ccmd/pack_writer_v3_retry_plan.md decision (2).
}

func (bw *BulkPackWriterV3) writeInserts(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, string, error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), bw.manifestPath, nil
	}

	rec, err := bw.serializeBinlog(ctx, pack)
	if err != nil {
		return nil, "", err
	}
	defer rec.Release()

	tsFrom, tsTo := bw.getTsRange(rec)
	pluginContextPtr := bw.getPluginContext(pack.collectionID)

	// NOTE: this used to wrap the call below in its own retry.Do; the outer
	// BulkPackWriterV3.Write retry loop now covers it, so we drop the inner
	// retry to avoid Attempts^2 amplification.
	logs, manifestPath, err := bw.writeInsertsIntoStorage(ctx, pluginContextPtr, rec, tsFrom, tsTo)
	if err != nil {
		log.Ctx(ctx).Warn("failed to write inserts into storage",
			zap.Int64("collectionID", pack.collectionID),
			zap.Int64("segmentID", pack.segmentID),
			zap.Error(err))
		return nil, "", err
	}
	return logs, manifestPath, nil
}

func (bw *BulkPackWriterV3) writeInsertsIntoStorage(ctx context.Context,
	pluginContextPtr *indexcgopb.StoragePluginContext,
	rec storage.Record,
	tsFrom typeutil.Timestamp,
	tsTo typeutil.Timestamp,
) (map[int64]*datapb.FieldBinlog, string, error) {
	log := log.Ctx(ctx)
	logs := make(map[int64]*datapb.FieldBinlog)
	columnGroups := bw.columnGroups

	var err error
	doWrite := func(w storage.RecordWriter) error {
		if err = w.Write(rec); err != nil {
			if closeErr := w.Close(); closeErr != nil {
				log.Error("failed to close writer after write failed", zap.Error(closeErr))
			}
			return err
		}
		// close first the get stats & output
		return w.Close()
	}

	var manifestPath string
	getFieldNullCounts := func(columnGroup storagecommon.ColumnGroup) map[int64]int64 {
		result := make(map[int64]int64, len(columnGroup.Fields))
		for _, fieldID := range columnGroup.Fields {
			if col := rec.Column(fieldID); col != nil {
				result[fieldID] = int64(col.NullN())
			}
		}
		return result
	}

	basePath, version, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return nil, "", err
	}
	w, err := storage.NewPackedRecordManifestWriter(basePath, version, bw.schema, bw.bufferSize, bw.multiPartUploadSize, columnGroups, bw.storageConfig, pluginContextPtr)
	if err != nil {
		return nil, "", err
	}
	if err = doWrite(w); err != nil {
		return nil, "", err
	}
	for _, columnGroup := range columnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:         int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
					MemorySize:      int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
					LogPath:         w.GetWrittenPaths(columnGroupID),
					EntriesNum:      w.GetWrittenRowNum(),
					TimestampFrom:   tsFrom,
					TimestampTo:     tsTo,
					FieldNullCounts: getFieldNullCounts(columnGroup),
				},
			},
		}
	}
	manifestPath = w.GetWrittenManifest()
	return logs, manifestPath, nil
}

// writeDelta writes deltalog to storage and updates the manifest.
// Returns the updated manifest path and a pathless delta summary FieldBinlog
// containing only EntriesNum and MemorySize for compaction trigger decisions.
func (bw *BulkPackWriterV3) writeDelta(ctx context.Context, pack *SyncPack) (string, *datapb.FieldBinlog, error) {
	if pack.deltaData == nil || pack.deltaData.RowCount == 0 {
		return bw.manifestPath, nil, nil
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(bw.schema)
	if err != nil {
		return "", nil, fmt.Errorf("primary key field not found: %w", err)
	}

	// Allocate log ID for deltalog
	logID, err := bw.allocator.AllocOne()
	if err != nil {
		return "", nil, err
	}

	// Build deltalog path under basePath/_delta/
	basePath, _, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse manifest path: %w", err)
	}
	deltaPath := metautil.BuildDeltaLogPathV3(basePath, logID)

	// Create deltalog writer with V2 storage
	writer, err := storage.NewDeltalogWriter(
		ctx, pack.collectionID, pack.partitionID, pack.segmentID, logID, pkField.DataType, deltaPath,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(bw.storageConfig),
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create deltalog writer: %w", err)
	}

	// Build Arrow record from delete data using existing utility
	record, _, _, err := storage.BuildDeleteRecord(pack.deltaData.Pks, pack.deltaData.Tss)
	if err != nil {
		return "", nil, fmt.Errorf("failed to build delete record: %w", err)
	}
	defer record.Release()

	// Write and close
	if err := writer.Write(record); err != nil {
		return "", nil, fmt.Errorf("failed to write delta record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", nil, fmt.Errorf("failed to close delta writer: %w", err)
	}

	// Update manifest with the new deltalog
	newManifest, err := packed.AddDeltaLogsToManifest(
		bw.manifestPath,
		bw.storageConfig,
		[]packed.DeltaLogEntry{{Path: deltaPath, NumEntries: pack.deltaData.RowCount}},
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to add deltalog to manifest: %w", err)
	}

	bw.manifestPath = newManifest
	bw.sizeWritten += pack.deltaData.Size()

	// Return delta summary for compaction trigger decisions (no path, only stats)
	summary := &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{{
			LogID:      logID,
			EntriesNum: pack.deltaData.RowCount,
			MemorySize: int64(writer.GetWrittenUncompressed()),
		}},
	}
	return newManifest, summary, nil
}

// writeStats overrides the base class to write bloom filter stats into the
// manifest instead of separate binlog files. The stat files are written
// under _stats/ relative to the manifest base path, then registered in
// the manifest via a transaction.
func (bw *BulkPackWriterV3) writeStats(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), nil
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
	basePath, _, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return nil, err
	}

	var files []string
	var memorySize int64

	// Preserve existing bloom filter files from previous batches.
	// loon_transaction_update_stat uses replace semantics, so we must
	// merge previously written files into the new entry.
	statKey := fmt.Sprintf("bloom_filter.%d", pkFieldID)
	existingStats, err := packed.GetManifestStats(bw.manifestPath, bw.storageConfig)
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
		files = append(files, mergedFullPath)
	}

	// Register stats in manifest
	newManifest, err := packed.AddStatsToManifest(bw.manifestPath, bw.storageConfig, []packed.StatEntry{
		{
			Key:      statKey,
			Files:    files,
			Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", memorySize)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add stats to manifest: %w", err)
	}
	bw.manifestPath = newManifest

	// Return empty map - stats are in manifest
	return make(map[int64]*datapb.FieldBinlog), nil
}

// writeBM25Stasts overrides the base class to write BM25 stats into the
// manifest instead of separate binlog files.
func (bw *BulkPackWriterV3) writeBM25Stasts(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, error) {
	if len(pack.bm25Stats) == 0 {
		return make(map[int64]*datapb.FieldBinlog), nil
	}

	serializer, err := NewStorageSerializer(bw.metaCache, bw.schema)
	if err != nil {
		return nil, err
	}
	bm25Blobs, err := serializer.serializeBM25Stats(pack)
	if err != nil {
		return nil, err
	}

	basePath, _, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return nil, err
	}

	// Track per-field files and memory sizes, then build stat entries at the end
	type fieldStats struct {
		files      []string
		memorySize int64
	}
	fieldMap := make(map[int64]*fieldStats)

	// Preserve existing BM25 stat files from previous batches.
	existingStats, err := packed.GetManifestStats(bw.manifestPath, bw.storageConfig)
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
		}
	}

	// Build stat entries with memory_size metadata
	var statEntries []packed.StatEntry
	for fieldID, fs := range fieldMap {
		statEntries = append(statEntries, packed.StatEntry{
			Key:      fmt.Sprintf("bm25.%d", fieldID),
			Files:    fs.files,
			Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", fs.memorySize)},
		})
	}

	if len(statEntries) > 0 {
		newManifest, err := packed.AddStatsToManifest(bw.manifestPath, bw.storageConfig, statEntries)
		if err != nil {
			return nil, fmt.Errorf("failed to add BM25 stats to manifest: %w", err)
		}
		bw.manifestPath = newManifest
	}

	// Return empty map - stats are in manifest
	return make(map[int64]*datapb.FieldBinlog), nil
}
