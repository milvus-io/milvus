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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BulkPackWriterV2 struct {
	*BulkPackWriter
	bufferSize          int64
	multiPartUploadSize int64

	storageConfig *indexpb.StorageConfig
	columnGroups  []storagecommon.ColumnGroup
}

func NewBulkPackWriterV2(metaCache metacache.MetaCache, schema *schemapb.CollectionSchema, chunkManager storage.ChunkManager,
	allocator allocator.Interface, pack *SyncPack, bufferSize, multiPartUploadSize int64,
	storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup, writeRetryOpts ...retry.Option,
) *BulkPackWriterV2 {
	return &BulkPackWriterV2{
		BulkPackWriter:      NewBulkPackWriter(metaCache, schema, chunkManager, allocator, pack, writeRetryOpts...),
		bufferSize:          bufferSize,
		multiPartUploadSize: multiPartUploadSize,
		storageConfig:       storageConfig,
		columnGroups:        columnGroups,
	}
}

func (bw *BulkPackWriterV2) Write(ctx context.Context) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	err error,
) {
	if inserts, manifest, err = bw.writeInserts(ctx); err != nil {
		log.Error("failed to write insert data", zap.Error(err))
		return
	}
	if stats, err = bw.writeStats(ctx); err != nil {
		log.Error("failed to process stats blob", zap.Error(err))
		return
	}
	if deltas, err = bw.writeDelta(ctx); err != nil {
		log.Error("failed to process delta blob", zap.Error(err))
		return
	}
	if bm25Stats, err = bw.writeBM25Stasts(ctx); err != nil {
		log.Error("failed to process bm25 stats blob", zap.Error(err))
		return
	}

	size = bw.sizeWritten

	return
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

func (bw *BulkPackWriterV2) writeInserts(ctx context.Context) (map[int64]*datapb.FieldBinlog, string, error) {
	pack := bw.pack
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), "", nil
	}

	rec, err := bw.serializeBinlog(ctx)
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

func (bw *BulkPackWriterV2) writeInsertsIntoStorage(ctx context.Context,
	pluginContextPtr *indexcgopb.StoragePluginContext,
	rec storage.Record,
	tsFrom typeutil.Timestamp,
	tsTo typeutil.Timestamp,
) (map[int64]*datapb.FieldBinlog, string, error) {
	return (&PackedSegmentWriter{
		SegmentWriter:       bw.segmentWriter.WithTimeRange(tsFrom, tsTo),
		StorageConfig:       bw.storageConfig,
		ColumnGroups:        bw.columnGroups,
		BufferSize:          bw.bufferSize,
		MultiPartUploadSize: bw.multiPartUploadSize,
		PluginContext:       pluginContextPtr,
	}).WritePackedInsert(ctx, rec)
}

func (bw *BulkPackWriterV2) serializeBinlog(_ context.Context) (storage.Record, error) {
	pack := bw.pack
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
