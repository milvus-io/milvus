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
	"math"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
}

func NewBulkPackWriterV2(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, bufferSize, multiPartUploadSize int64, writeRetryOpts ...retry.Option,
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
	}
}

func (bw *BulkPackWriterV2) Write(ctx context.Context, pack *SyncPack) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	err = bw.prefetchIDs(pack)
	if err != nil {
		log.Warn("failed allocate ids for sync task", zap.Error(err))
		return
	}

	if inserts, err = bw.writeInserts(ctx, pack); err != nil {
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

func (bw *BulkPackWriterV2) writeInserts(ctx context.Context, pack *SyncPack) (map[int64]*datapb.FieldBinlog, error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), nil
	}
	columnGroups := storagecommon.SplitBySchema(bw.schema.GetFields())

	rec, err := bw.serializeBinlog(ctx, pack)
	if err != nil {
		return nil, err
	}

	logs := make(map[int64]*datapb.FieldBinlog)
	paths := make([]string, 0)
	for _, columnGroup := range columnGroups {
		path := metautil.BuildInsertLogPath(bw.chunkManager.RootPath(), pack.collectionID, pack.partitionID, pack.segmentID, columnGroup.GroupID, bw.nextID())
		paths = append(paths, path)
	}
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

	bucketName := paramtable.Get().ServiceParam.MinioCfg.BucketName.GetValue()

	w, err := storage.NewPackedRecordWriter(bucketName, paths, bw.schema, bw.bufferSize, bw.multiPartUploadSize, columnGroups, nil)
	if err != nil {
		return nil, err
	}
	if err = w.Write(rec); err != nil {
		return nil, err
	}
	for _, columnGroup := range columnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID: columnGroupID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:       int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
					MemorySize:    int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
					LogPath:       w.GetWrittenPaths(columnGroupID),
					EntriesNum:    w.GetWrittenRowNum(),
					TimestampFrom: tsFrom,
					TimestampTo:   tsTo,
				},
			},
		}
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return logs, nil
}

func (bw *BulkPackWriterV2) serializeBinlog(ctx context.Context, pack *SyncPack) (storage.Record, error) {
	if len(pack.insertData) == 0 {
		return nil, nil
	}
	arrowSchema, err := storage.ConvertToArrowSchema(bw.schema.Fields)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	for _, chunk := range pack.insertData {
		if err := storage.BuildRecord(builder, chunk, bw.schema.GetFields()); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	field2Col := make(map[storage.FieldID]int, len(bw.schema.GetFields()))

	for c, field := range bw.schema.GetFields() {
		field2Col[field.FieldID] = c
	}
	return storage.NewSimpleArrowRecord(rec, field2Col), nil
}
