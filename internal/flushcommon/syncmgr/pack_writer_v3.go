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

	if inserts, manifest, err = bw.writeInserts(ctx, pack); err != nil {
		log.Error("failed to write insert data", zap.Error(err))
		return
	}
	// Update manifestPath after writeInserts
	bw.manifestPath = manifest

	// writeStats for V3 adds bloom filter stats to manifest
	if stats, err = bw.writeStats(ctx, pack); err != nil {
		log.Error("failed to process stats blob", zap.Error(err))
		return
	}
	// writeDelta for V3 updates manifest and returns nil FieldBinlog
	if manifest, err = bw.writeDelta(ctx, pack); err != nil {
		log.Error("failed to process delta blob", zap.Error(err))
		return
	}
	// writeBM25Stasts for V3 adds BM25 stats to manifest
	if bm25Stats, err = bw.writeBM25Stasts(ctx, pack); err != nil {
		log.Error("failed to process bm25 stats blob", zap.Error(err))
		return
	}

	// For V3, deltas and stats are in manifest
	deltas = nil
	manifest = bw.manifestPath
	size = bw.sizeWritten
	return
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

	var logs map[int64]*datapb.FieldBinlog
	var manifestPath string

	if err := retry.Do(ctx, func() error {
		var err error
		logs, manifestPath, err = bw.writeInsertsIntoStorage(ctx, pluginContextPtr, rec, tsFrom, tsTo)
		if err != nil {
			log.Warn("failed to write inserts into storage",
				zap.Int64("collectionID", pack.collectionID),
				zap.Int64("segmentID", pack.segmentID),
				zap.Error(err))
			return err
		}
		return nil
	}, bw.writeRetryOpts...); err != nil {
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
// For V3, deltalogs are stored in the manifest, not returned as FieldBinlog.
func (bw *BulkPackWriterV3) writeDelta(ctx context.Context, pack *SyncPack) (string, error) {
	if pack.deltaData == nil || pack.deltaData.RowCount == 0 {
		return bw.manifestPath, nil
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(bw.schema)
	if err != nil {
		return "", fmt.Errorf("primary key field not found: %w", err)
	}

	// Allocate log ID for deltalog
	logID, err := bw.allocator.AllocOne()
	if err != nil {
		return "", err
	}

	// Build deltalog path under basePath/_delta/
	basePath, _, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest path: %w", err)
	}
	deltaPath := metautil.BuildDeltaLogPathV3(basePath, logID)

	// Create deltalog writer with V2 storage
	writer, err := storage.NewDeltalogWriter(
		ctx, pack.collectionID, pack.partitionID, pack.segmentID, logID, pkField.DataType, deltaPath,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(bw.storageConfig),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create deltalog writer: %w", err)
	}

	// Build Arrow record from delete data using existing utility
	record, _, _, err := storage.BuildDeleteRecord(pack.deltaData.Pks, pack.deltaData.Tss)
	if err != nil {
		return "", fmt.Errorf("failed to build delete record: %w", err)
	}
	defer record.Release()

	// Write and close
	if err := writer.Write(record); err != nil {
		return "", fmt.Errorf("failed to write delta record: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close delta writer: %w", err)
	}

	// Update manifest with the new deltalog
	newManifest, err := packed.AddDeltaLogsToManifest(
		bw.manifestPath,
		bw.storageConfig,
		[]packed.DeltaLogEntry{{Path: deltaPath, NumEntries: pack.deltaData.RowCount}},
	)
	if err != nil {
		return "", fmt.Errorf("failed to add deltalog to manifest: %w", err)
	}

	bw.manifestPath = newManifest
	bw.sizeWritten += pack.deltaData.Size()

	return newManifest, nil
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

	// Update metacache (same as base class)
	actions := []metacache.SegmentAction{metacache.RollStats(singlePKStats)}
	bw.metaCache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(pack.segmentID))

	pkFieldID := serializer.pkField.GetFieldID()
	basePath, _, err := packed.UnmarshalManifestPath(bw.manifestPath)
	if err != nil {
		return nil, err
	}

	var files []string
	var memorySize int64

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
		mergedStatsBlob, err := serializer.serializeMergedPkStats(pack)
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
			Key:      fmt.Sprintf("bloom_filter.%d", pkFieldID),
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

	// Update metacache (same as base class)
	actions := []metacache.SegmentAction{metacache.MergeBm25Stats(pack.bm25Stats)}
	bw.metaCache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(pack.segmentID))

	// Write merged BM25 stats on flush
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 && hasBM25Function(bw.schema) {
		mergedBM25Blob, err := serializer.serializeMergedBM25Stats(pack)
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
