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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

// perBatchStatPaths drops the compound (merged) stat blob from a manifest stat
// entry's paths, keeping only the per-batch blobs. The flush merge rebuilds the
// compound from the per-batch blobs, so a compound written by an EARLIER flush
// of the same Flushing segment (a segment in SegmentState_Flushing emits a flush
// pack on every sync) must be excluded: DeserializeBloomFilterStats short-circuits
// on a compound path and would drop the per-batch blobs written since that flush
// (PK undercount), and the BM25 merge would additively re-count it (double-count).
func perBatchStatPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if _, logidx := path.Split(p); logidx == storage.CompoundStatsType.LogIdx() {
			continue
		}
		out = append(out, p)
	}
	return out
}

// loadPriorPkStats reads the per-batch bloom-filter blobs already persisted for
// this segment and deserializes them into PrimaryKeyStats. paths come from the
// StatsResolver over the pre-commit manifest, so they cover every prior sync's
// batch blob and are chunkManager-readable as-is. Returns nil when empty.
func (bw *BulkPackWriterV3) loadPriorPkStats(ctx context.Context, paths []string) ([]*storage.PrimaryKeyStats, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	values, err := bw.chunkManager.MultiRead(ctx, paths)
	if err != nil {
		return nil, err
	}
	blobs := make([]*storage.Blob, len(values))
	for i := range values {
		blobs[i] = &storage.Blob{Value: values[i]}
	}
	return storage.DeserializeBloomFilterStats(paths, blobs)
}

// loadPriorBM25Stats reads the per-batch BM25 blobs already persisted for this
// segment (grouped by field) and merges them into one combined SegmentBM25Stats.
func (bw *BulkPackWriterV3) loadPriorBM25Stats(ctx context.Context, fieldPaths map[int64][]string) (*metacache.SegmentBM25Stats, error) {
	combined := metacache.NewEmptySegmentBM25Stats()
	for fieldID, paths := range fieldPaths {
		if len(paths) == 0 {
			continue
		}
		values, err := bw.chunkManager.MultiRead(ctx, paths)
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			stats, err := storage.NewBM25StatsWithBytes(v)
			if err != nil {
				return nil, err
			}
			combined.Merge(map[int64]*storage.BM25Stats{fieldID: stats})
		}
	}
	return combined, nil
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
// The metaCache is not mutated by Write. The only metaCache state this sync
// produces is bw.preparedStats — this sync's cumulative stats clone — which
// SyncTask.Run installs after the DataCoord ack. The bloom-filter / BM25 merged
// blobs are built at flush from the per-batch blobs already persisted in the
// manifest (see writeStats / writeBM25Stasts), so no per-sync roll into the
// metaCache is needed.
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
	bw.initialManifestPath = bw.manifestPath
	bw.statsBlobSize = 0

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
		mlog.Warn(ctx, "failed to write insert data", mlog.Err(err))
		return
	}
	if statEntries, err = bw.writeStats(ctx, pack, basePath); err != nil {
		mlog.Warn(ctx, "failed to process stats blob", mlog.Err(err))
		return
	}
	if deltas, deltaEntries, err = bw.writeDelta(ctx, pack, basePath); err != nil {
		mlog.Warn(ctx, "failed to process delta blob", mlog.Err(err))
		return
	}
	if bm25Entries, err = bw.writeBM25Stasts(ctx, pack, basePath); err != nil {
		mlog.Warn(ctx, "failed to process bm25 stats blob", mlog.Err(err))
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

	digested := len(inserts) > 0 || bw.statsBlobSize > 0 || len(deltas.GetBinlogs()) > 0

	manifest = bw.manifestPath
	size = bw.sizeWritten

	// V3 feeds the tracked statsBlobSize instead of summing a stats array (it
	// returns no stats array); finalizeStats produces the cumulative Statistics.
	segmentStats, err = bw.finalizeStats(pack, digested, inserts, deltas, bw.statsBlobSize)
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
		mlog.Info(ctx, "using TEXT-aware writer for import",
			mlog.Int("textFieldCount", len(textColumnConfigs)),
			mlog.String("basePath", basePath))
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
		mlog.Warn(ctx, "failed to write inserts",
			mlog.FieldCollectionID(pack.collectionID),
			mlog.FieldSegmentID(pack.segmentID),
			mlog.Err(err))
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
					return "", nil, merr.WrapErrDataIntegrityMsg("column group %d fields %v missing format for existing manifest %s",
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
		return nil, nil, merr.Wrap(err, "primary key field not found")
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
		return nil, nil, merr.Wrap(err, "failed to create deltalog writer")
	}

	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(pack.deltaData.Pks, pack.deltaData.Tss)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to build delete record")
	}
	defer record.Release()

	if err := writer.Write(record); err != nil {
		return nil, nil, merr.Wrap(err, "failed to write delta record")
	}
	if err := writer.Close(); err != nil {
		return nil, nil, merr.Wrap(err, "failed to close delta writer")
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
	var priorBloomPaths []string
	existingStats, err := packed.GetManifestStats(bw.initialManifestPath, bw.storageConfig)
	if err == nil {
		if existing, ok := existingStats[statKey]; ok && len(existing.Paths) > 0 {
			// These are the prior syncs' per-batch bloom paths (already absolute,
			// chunkManager-readable). Reused by the flush merge below so we don't
			// re-read the manifest via a StatsResolver. Exclude any compound blob
			// from an earlier flush so the merge rebuilds it from per-batch blobs.
			priorBloomPaths = perBatchStatPaths(existing.Paths)
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

	// Write merged stats on flush. Build the merged blob from the per-batch
	// bloom blobs already persisted in the pre-commit manifest (priorBloomPaths,
	// read once above) plus this flush batch's singlePKStats — no metaCache
	// history is consulted, so nothing needs to have been rolled in.
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 {
		priorStats, err := bw.loadPriorPkStats(ctx, priorBloomPaths)
		if err != nil {
			return nil, err
		}
		segment, ok := bw.metaCache.GetSegmentByID(pack.segmentID)
		if !ok {
			return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
		}
		mergedStatsBlob, err := serializer.serializeMergedPkStatsList(append(priorStats, singlePKStats), segment.NumOfRows())
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

	// Preserve existing BM25 stat files from previous batches. priorBM25Paths
	// captures them per field (already absolute, chunkManager-readable) for the
	// flush merge below, so we don't re-read the manifest via a StatsResolver.
	priorBM25Paths := make(map[int64][]string)
	existingStats, err := packed.GetManifestStats(bw.initialManifestPath, bw.storageConfig)
	if err == nil {
		for key, existing := range existingStats {
			prefix, fieldID, ok := packed.ParseStatKey(key)
			if !ok || prefix != "bm25" || len(existing.Paths) == 0 {
				continue
			}
			priorBM25Paths[fieldID] = perBatchStatPaths(existing.Paths)
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

	// Write merged BM25 stats on flush. Build the merged blob from the
	// per-batch BM25 blobs already persisted in the pre-commit manifest
	// (priorBM25Paths, read once above) plus this flush batch — no metaCache
	// history is consulted.
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 && hasBM25Function(bw.schema) {
		combined, err := bw.loadPriorBM25Stats(ctx, priorBM25Paths)
		if err != nil {
			return nil, err
		}
		combined.Merge(pack.bm25Stats)
		mergedBM25Blob, err := serializer.serializeMergedBM25StatsFrom(combined)
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
