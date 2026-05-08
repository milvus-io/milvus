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
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
}

func NewBulkPackWriterV3(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, pack *SyncPack, bufferSize, multiPartUploadSize int64,
	storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup, curManifestPath string, writeRetryOpts ...retry.Option,
) *BulkPackWriterV3 {
	bwV2 := NewBulkPackWriterV2(metaCache, schema, chunkManager, allocator, pack, bufferSize,
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
func (bw *BulkPackWriterV3) Write(ctx context.Context) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	err error,
) {
	log := log.Ctx(ctx)
	pack := bw.pack
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
		if inserts, manifest, innerErr = bw.writeInserts(ctx); innerErr != nil {
			log.Warn("failed to write insert data", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// Update manifestPath after writeInserts
		bw.manifestPath = manifest

		// writeStats for V3 adds bloom filter stats to manifest
		if stats, innerErr = bw.writeStats(ctx); innerErr != nil {
			log.Warn("failed to process stats blob", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// writeDelta for V3 updates manifest and returns delta summary
		if manifest, deltaSummary, innerErr = bw.writeDelta(ctx); innerErr != nil {
			log.Warn("failed to process delta blob", zap.Error(innerErr))
			return classifyLoonErr(innerErr)
		}
		// writeBM25Stasts for V3 adds BM25 stats to manifest
		if bm25Stats, innerErr = bw.writeBM25Stasts(ctx); innerErr != nil {
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
	// Files written under _stats/ and _delta/ by previous failed attempts are
	// intentionally NOT cleaned up here: they are unreferenced by any
	// committed manifest and will be reclaimed by loon GC. See
	// ccmd/pack_writer_v3_retry_plan.md decision (2).
}

func (bw *BulkPackWriterV3) writeInserts(ctx context.Context) (map[int64]*datapb.FieldBinlog, string, error) {
	pack := bw.pack
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), bw.manifestPath, nil
	}

	rec, err := bw.serializeBinlog(ctx)
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
	writer := &ManifestSegmentWriter{
		PackedSegmentWriter: &PackedSegmentWriter{
			SegmentWriter:       bw.segmentWriter.WithTimeRange(tsFrom, tsTo),
			StorageConfig:       bw.storageConfig,
			ColumnGroups:        bw.columnGroups,
			BufferSize:          bw.bufferSize,
			MultiPartUploadSize: bw.multiPartUploadSize,
			PluginContext:       pluginContextPtr,
		},
		ManifestPath: bw.manifestPath,
	}
	return writer.WriteManifestInsert(ctx, rec)
}

// writeDelta writes deltalog to storage and updates the manifest.
// Returns the updated manifest path and a pathless delta summary FieldBinlog
// containing only EntriesNum and MemorySize for compaction trigger decisions.
func (bw *BulkPackWriterV3) writeDelta(ctx context.Context) (string, *datapb.FieldBinlog, error) {
	pack := bw.pack
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

	deltaLog, _, err := bw.segmentWriter.WriteDelta(ctx, DeltaWriteParams{
		LogID:         logID,
		PKType:        pkField.DataType,
		Path:          deltaPath,
		DeleteData:    pack.deltaData,
		Version:       storage.StorageV2,
		StorageConfig: bw.storageConfig,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to write delta record: %w", err)
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
	summary := deltaSummary(logID, pack.deltaData.RowCount, deltaLog.GetMemorySize())
	return newManifest, summary, nil
}

// writeStats overrides the base class to write bloom filter stats into the
// manifest instead of separate binlog files. The stat files are written
// under _stats/ relative to the manifest base path, then registered in
// the manifest via a transaction.
func (bw *BulkPackWriterV3) writeStats(ctx context.Context) (map[int64]*datapb.FieldBinlog, error) {
	pack := bw.pack
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

	// DEFERRED: do NOT call metaCache.UpdateSegments(RollStats(...)) here.
	// Stash the action; SyncTask.Run applies it after Write returns success
	// so that retries do not double-roll the bloom filter. The merged-stats
	// path below uses serializeMergedPkStatsWith to inject this batch
	// explicitly without depending on the metaCache being updated yet.
	bw.pendingMetaCacheActions = append(bw.pendingMetaCacheActions,
		metacache.RollStats(singlePKStats))

	pkFieldID := serializer.pkField.GetFieldID()

	var mergedStatsBlob *storage.Blob
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 {
		// Use the *With variant to include this batch's PK stats explicitly,
		// since the corresponding RollStats has been deferred above.
		mergedStatsBlob, err = serializer.serializeMergedPkStatsWith(pack, singlePKStats)
		if err != nil {
			return nil, err
		}
	}

	writer := &ManifestSegmentWriter{
		PackedSegmentWriter: &PackedSegmentWriter{
			SegmentWriter: bw.segmentWriter,
			StorageConfig: bw.storageConfig,
		},
		ManifestPath: bw.manifestPath,
	}
	newManifest, size, err := writer.WritePKStatsBlobs(ctx, ManifestPKStatsInput{
		FieldID:    pkFieldID,
		BatchBlob:  batchStatsBlob,
		MergedBlob: mergedStatsBlob,
	})
	if err != nil {
		return nil, err
	}
	bw.manifestPath = newManifest
	bw.sizeWritten += size

	// Return empty map - stats are in manifest
	return make(map[int64]*datapb.FieldBinlog), nil
}

// writeBM25Stasts overrides the base class to write BM25 stats into the
// manifest instead of separate binlog files.
func (bw *BulkPackWriterV3) writeBM25Stasts(ctx context.Context) (map[int64]*datapb.FieldBinlog, error) {
	pack := bw.pack
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

	// DEFERRED: do NOT apply MergeBm25Stats to the metaCache here. Stash the
	// action; SyncTask.Run applies it after a successful Write so retries do
	// not double-merge the BM25 stats. The merged-stats path below uses
	// serializeMergedBM25StatsWith to inject this batch explicitly.
	bw.pendingMetaCacheActions = append(bw.pendingMetaCacheActions,
		metacache.MergeBm25Stats(pack.bm25Stats))

	var mergedBM25Blob map[int64]*storage.Blob
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 && hasBM25Function(bw.schema) {
		// Use the *With variant to include this batch's bm25 stats explicitly,
		// since the corresponding MergeBm25Stats has been deferred above.
		mergedBM25Blob, err = serializer.serializeMergedBM25StatsWith(pack, pack.bm25Stats)
		if err != nil {
			return nil, err
		}
	}

	writer := &ManifestSegmentWriter{
		PackedSegmentWriter: &PackedSegmentWriter{
			SegmentWriter: bw.segmentWriter,
			StorageConfig: bw.storageConfig,
		},
		ManifestPath: bw.manifestPath,
	}
	newManifest, size, err := writer.WriteBM25StatsBlobs(ctx, ManifestBM25StatsInput{
		BatchBlobs:  bm25Blobs,
		MergedBlobs: mergedBM25Blob,
	})
	if err != nil {
		return nil, err
	}
	bw.manifestPath = newManifest
	bw.sizeWritten += size

	// Return empty map - stats are in manifest
	return make(map[int64]*datapb.FieldBinlog), nil
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
