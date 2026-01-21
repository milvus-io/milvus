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
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BulkPackWriterV3 struct {
	*BulkPackWriterV2
	schema              *schemapb.CollectionSchema
	bufferSize          int64
	multiPartUploadSize int64

	storageConfig *indexpb.StorageConfig
	columnGroups  []storagecommon.ColumnGroup
	manifestPath  string
}

func NewBulkPackWriterV3(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, bufferSize, multiPartUploadSize int64,
	storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup, curManifestPath string, writeRetryOpts ...retry.Option,
) *BulkPackWriterV3 {
	bwV2 := NewBulkPackWriterV2(metaCache, schema, chunkManager, allocator, bufferSize,
		multiPartUploadSize, storageConfig, columnGroups, writeRetryOpts...)
	return &BulkPackWriterV3{
		BulkPackWriterV2:    bwV2,
		schema:              schema,
		bufferSize:          bufferSize,
		multiPartUploadSize: multiPartUploadSize,
		storageConfig:       storageConfig,
		columnGroups:        columnGroups,
		manifestPath:        curManifestPath,
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
	if stats, err = bw.writeStats(ctx, pack); err != nil {
		log.Error("failed to process stats blob", zap.Error(err))
		return
	}
	if deltas, err = bw.writeDelta(ctx, pack); err != nil {
		log.Error("failed to process delta blob", zap.Error(err))
		return
	}
	if bm25Stats, err = bw.writeBM25Stasts(ctx, pack); err != nil {
		log.Error("failed to process bm25 stats blob", zap.Error(err))
		return
	}

	size = bw.sizeWritten
	return
}

func (bw *BulkPackWriterV3) writeInserts(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, string, error) {
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

	basePath, version, err := packed.UnmarshalManfestPath(bw.manifestPath)
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
