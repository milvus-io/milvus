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
	"path"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BulkPackWriterV2 struct {
	*BulkPackWriter
	schema              *schemapb.CollectionSchema
	bufferSize          int64
	multiPartUploadSize int64

	storageConfig *indexpb.StorageConfig
	columnGroups  []storagecommon.ColumnGroup
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
		schema:              schema,
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
	err error,
) {
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
	return paramtable.Get().ServiceParam.MinioCfg.BucketName.GetValue()
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

	tsArray := rec.Column(common.TimeStampField).(*array.Int64)
	rows := rec.Len()
	var tsFrom uint64 = math.MaxUint64
	var tsTo uint64 = 0
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < tsFrom {
			tsFrom = ts
		}
		if ts > tsTo {
			tsTo = ts
		}
	}
	var pluginContextPtr *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncyptionEnabled() {
		ez := hookutil.GetEzByCollProperties(bw.schema.GetProperties(), pack.collectionID)
		if ez != nil {
			unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
			if len(unsafe) > 0 {
				pluginContext := indexcgopb.StoragePluginContext{
					EncryptionZoneId: ez.EzID,
					CollectionId:     ez.CollectionID,
					EncryptionKey:    base64.StdEncoding.EncodeToString(unsafe),
				}
				pluginContextPtr = &pluginContext
			}
		}
	}
	var logs map[int64]*datapb.FieldBinlog
	var manifestPath string

	if err := retry.Do(ctx, func() error {
		var err error
		logs, manifestPath, err = bw.writeInsertsIntoStorage(ctx, pluginContextPtr, pack, rec, tsFrom, tsTo)
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
				log.Error("failed to close writer after write failed", zap.Error(closeErr))
			}
			return err
		}
		// close first the get stats & output
		return w.Close()
	}

	var manifestPath string
	if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		k := metautil.JoinIDPath(pack.collectionID, pack.partitionID, pack.segmentID)
		basePath := path.Join(bw.getRootPath(), common.SegmentInsertLogPath, k)
		w, err := storage.NewPackedRecordManifestWriter(bucketName, basePath, bw.schema, bw.bufferSize, bw.multiPartUploadSize, columnGroups, bw.storageConfig, pluginContextPtr)
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
						LogSize:       int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
						MemorySize:    int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
						LogPath:       w.GetWrittenPaths(columnGroupID),
						EntriesNum:    w.GetWrittenRowNum(),
						TimestampFrom: tsFrom,
						TimestampTo:   tsTo,
					},
				},
			}
		}
		manifestPath = w.GetWrittenManifest()
	} else {
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
						LogSize:       int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
						MemorySize:    int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
						LogPath:       w.GetWrittenPaths(columnGroupID),
						EntriesNum:    w.GetWrittenRowNum(),
						TimestampFrom: tsFrom,
						TimestampTo:   tsTo,
					},
				},
			}
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
