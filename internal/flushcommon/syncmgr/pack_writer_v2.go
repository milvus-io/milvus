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
	"encoding/base64"
	"math"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BulkPackWriterV2 struct {
	*BulkPackWriter
	bufferSize          int64
	multiPartUploadSize int64

	storageConfig *indexpb.StorageConfig
	columnGroups  []storagecommon.ColumnGroup
	preparedStats *metacache.SegmentStats
}

// PreparedStats returns this sync's cumulative SegmentStats clone for SyncTask
// to install on the metaCache after the DataCoord ack. Shared by V2 and the
// embedding V3 writer.
func (bw *BulkPackWriterV2) PreparedStats() *metacache.SegmentStats {
	return bw.preparedStats
}

// finalizeStats builds this sync's cumulative Statistics by cloning the live
// collector and folding this sync's writes into the clone — the metaCache stays
// untouched, mutated only by SyncTask.Run (via the installed preparedStats)
// after the DataCoord ack. The clone is stashed as preparedStats and its
// Publish() returned to DataCoord, so the two are one object. Shared by V2 and
// the embedding V3 writer; only the statsBlobSize source differs (V2 sums the
// returned stats/bm25 arrays, V3 tracks bw.statsBlobSize). digested is false for
// an empty sync, where the publish returns the segment's prior cumulative value.
func (bw *BulkPackWriterV2) finalizeStats(
	pack *SyncPack,
	digested bool,
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	statsBlobSize int64,
) (*datapb.Statistics, error) {
	seg, ok := bw.metaCache.GetSegmentByID(pack.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
	}
	clone := seg.Statistics().Clone()
	if digested {
		clone.Digest(inserts, deltas, statsBlobSize, pack.batchRows, pack.tsFrom, pack.tsTo)
	}
	bw.preparedStats = clone
	return clone.Publish(), nil
}

func NewBulkPackWriterV2(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, bufferSize, multiPartUploadSize int64,
	storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup, writeRetryOpts ...retry.Option,
) *BulkPackWriterV2 {
	return &BulkPackWriterV2{
		BulkPackWriter: &BulkPackWriter{
			metaCache:      metaCache,
			schema:         schema,
			chunkManager:   chunkManager,
			allocator:      allocator,
			writeRetryOpts: writeRetryOpts,
		},
		bufferSize:          bufferSize,
		multiPartUploadSize: multiPartUploadSize,
		storageConfig:       storageConfig,
		columnGroups:        columnGroups,
	}
}

func (bw *BulkPackWriterV2) Write(ctx context.Context, pack *SyncPack) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	segmentStats *datapb.Statistics,
	err error,
) {
	if inserts, manifest, err = bw.writeInserts(ctx, pack); err != nil {
		mlog.Error(ctx, "failed to write insert data", mlog.Err(err))
		return
	}
	if stats, err = bw.writeStats(ctx, pack); err != nil {
		mlog.Error(ctx, "failed to process stats blob", mlog.Err(err))
		return
	}
	if deltas, err = bw.writeDelta(ctx, pack); err != nil {
		mlog.Error(ctx, "failed to process delta blob", mlog.Err(err))
		return
	}
	if bm25Stats, err = bw.writeBM25Stasts(ctx, pack); err != nil {
		mlog.Error(ctx, "failed to process bm25 stats blob", mlog.Err(err))
		return
	}

	size = bw.sizeWritten

	// V2 returns the stats / bm25Stats arrays, so statsBlobSize is summed from
	// them here, then finalizeStats produces the cumulative Statistics.
	digested := len(inserts) > 0 || len(stats) > 0 || len(bm25Stats) > 0 || len(deltas.GetBinlogs()) > 0
	var statsBlobSize int64
	if digested {
		for _, fb := range stats {
			for _, l := range fb.GetBinlogs() {
				statsBlobSize += l.GetMemorySize()
			}
		}
		for _, fb := range bm25Stats {
			for _, l := range fb.GetBinlogs() {
				statsBlobSize += l.GetMemorySize()
			}
		}
	}

	segmentStats, err = bw.finalizeStats(pack, digested, inserts, deltas, statsBlobSize)
	return
}

// getRootPath returns the rootPath current task shall use.
// when storageConfig is set, use the rootPath in it.
// otherwise, use chunkManager.RootPath() instead.
func (bw *BulkPackWriterV2) getRootPath() string {
	if bw.storageConfig != nil {
		return bw.storageConfig.RootPath
	}
	return bw.chunkManager.RootPath()
}

func (bw *BulkPackWriterV2) getBucketName() string {
	if bw.storageConfig != nil {
		return bw.storageConfig.BucketName
	}
	return paramtable.Get().MinioCfg.BucketName.GetValue()
}

func (bw *BulkPackWriterV2) getPluginContext(collectionID int64) *indexcgopb.StoragePluginContext {
	if !hookutil.IsClusterEncryptionEnabled() {
		return nil
	}
	ez := hookutil.GetEzByCollProperties(bw.schema.GetProperties(), collectionID)
	if ez == nil {
		return nil
	}
	unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
	if len(unsafe) == 0 {
		return nil
	}

	return &indexcgopb.StoragePluginContext{
		EncryptionZoneId: ez.EzID,
		CollectionId:     ez.CollectionID,
		EncryptionKey:    base64.StdEncoding.EncodeToString(unsafe),
	}
}

func (bw *BulkPackWriterV2) getTsRange(rec storage.Record) (tsFrom, tsTo uint64) {
	tsArray := rec.Column(common.TimeStampField).(*array.Int64)
	rows := rec.Len()
	tsFrom = math.MaxUint64
	tsTo = 0
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < tsFrom {
			tsFrom = ts
		}
		if ts > tsTo {
			tsTo = ts
		}
	}
	return tsFrom, tsTo
}

func (bw *BulkPackWriterV2) writeInserts(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, string, error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), "", nil
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
		logs, manifestPath, err = bw.writeInsertsIntoStorage(ctx, pluginContextPtr, pack, rec, tsFrom, tsTo)
		if err != nil {
			mlog.Warn(ctx, "failed to write inserts into storage",
				mlog.FieldCollectionID(pack.collectionID),
				mlog.FieldSegmentID(pack.segmentID),
				mlog.Err(err))
			return err
		}
		return nil
	}, bw.writeRetryOpts...); err != nil {
		return nil, "", err
	}
	return logs, manifestPath, nil
}

func (bw *BulkPackWriterV2) writeInsertsIntoStorage(_ context.Context,
	pluginContextPtr *indexcgopb.StoragePluginContext,
	pack *SyncPack,
	rec storage.Record,
	tsFrom typeutil.Timestamp,
	tsTo typeutil.Timestamp,
) (map[int64]*datapb.FieldBinlog, string, error) {
	logs := make(map[int64]*datapb.FieldBinlog)
	columnGroups := bw.columnGroups
	bucketName := bw.getBucketName()

	var err error
	doWrite := func(w storage.RecordWriter) error {
		if err = w.Write(rec); err != nil {
			if closeErr := w.Close(); closeErr != nil {
				mlog.Error(context.TODO(), "failed to close writer after write failed", mlog.Err(closeErr))
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

	paths := make([]string, 0)
	for _, columnGroup := range columnGroups {
		id, err := bw.allocator.AllocOne()
		if err != nil {
			return nil, "", err
		}
		path := metautil.BuildInsertLogPath(bw.getRootPath(), pack.collectionID, pack.partitionID, pack.segmentID, columnGroup.GroupID, id)
		paths = append(paths, path)
	}
	w, err := storage.NewPackedRecordWriter(bucketName, paths, bw.schema, bw.bufferSize, bw.multiPartUploadSize, columnGroups, bw.storageConfig, pluginContextPtr)
	if err != nil {
		return nil, "", err
	}
	if err = doWrite(w); err != nil {
		return nil, "", err
	}
	// workaround to store row num
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

	return logs, manifestPath, nil
}

func (bw *BulkPackWriterV2) serializeBinlog(_ context.Context, pack *SyncPack) (storage.Record, error) {
	if len(pack.insertData) == 0 {
		return nil, nil
	}
	arrowSchema, err := storage.ConvertToArrowSchema(bw.schema, true)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	for _, chunk := range pack.insertData {
		if err := storage.BuildRecord(builder, chunk, bw.schema); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	allFields := typeutil.GetAllFieldSchemas(bw.schema)
	field2Col := make(map[storage.FieldID]int, len(allFields))
	for c, field := range allFields {
		field2Col[field.FieldID] = c
	}
	return storage.NewSimpleArrowRecord(rec, field2Col), nil
}
