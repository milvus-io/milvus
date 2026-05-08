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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BulkPackWriter struct {
	metaCache      metacache.MetaCache
	schema         *schemapb.CollectionSchema
	chunkManager   storage.ChunkManager
	allocator      allocator.Interface
	pack           *SyncPack
	segmentWriter  *SegmentWriter
	writeRetryOpts []retry.Option

	sizeWritten int64
}

func NewBulkPackWriter(metaCache metacache.MetaCache,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager,
	allocator allocator.Interface,
	pack *SyncPack,
	writeRetryOpts ...retry.Option,
) *BulkPackWriter {
	return &BulkPackWriter{
		metaCache:    metaCache,
		schema:       schema,
		chunkManager: chunkManager,
		allocator:    allocator,
		pack:         pack,
		segmentWriter: &SegmentWriter{SegmentWriteContext: SegmentWriteContext{
			CollectionID: pack.collectionID,
			PartitionID:  pack.partitionID,
			SegmentID:    pack.segmentID,
			TsFrom:       pack.tsFrom,
			TsTo:         pack.tsTo,
			Schema:       schema,
			ChunkManager: chunkManager,
			Allocator:    allocator,
			RetryOptions: writeRetryOpts,
		}},
		writeRetryOpts: writeRetryOpts,
	}
}

func (bw *BulkPackWriter) Write(ctx context.Context) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	if inserts, err = bw.writeInserts(ctx); err != nil {
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

func (bw *BulkPackWriter) writeInserts(ctx context.Context) (map[int64]*datapb.FieldBinlog, error) {
	pack := bw.pack
	if len(pack.insertData) == 0 {
		return make(map[int64]*datapb.FieldBinlog), nil
	}

	serializer, err := NewStorageSerializer(bw.metaCache, bw.schema)
	if err != nil {
		return nil, err
	}

	binlogBlobs, err := serializer.serializeBinlog(ctx, pack)
	if err != nil {
		return nil, err
	}

	logs, size, err := bw.segmentWriter.WriteV1InsertBlobs(ctx, binlogBlobs)
	if err != nil {
		return nil, err
	}
	bw.sizeWritten += size
	return logs, nil
}

func (bw *BulkPackWriter) writeStats(ctx context.Context) (map[int64]*datapb.FieldBinlog, error) {
	pack := bw.pack
	if len(pack.insertData) == 0 {
		// TODO: we should not skip here, if the flush operation don't carry any insert data,
		// the merge stats operation will be skipped, which is a bad case.
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

	actions := []metacache.SegmentAction{metacache.RollStats(singlePKStats)}
	bw.metaCache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(pack.segmentID))

	pkFieldID := serializer.pkField.GetFieldID()
	var mergedStatsBlob *storage.Blob
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 {
		mergedStatsBlob, err = serializer.serializeMergedPkStats(pack)
		if err != nil {
			return nil, err
		}
	}

	logs, size, err := bw.segmentWriter.WriteV1StatsBlobs(ctx, V1StatsBlobsInput{
		FieldID:    pkFieldID,
		BatchBlob:  batchStatsBlob,
		MergedBlob: mergedStatsBlob,
	})
	if err != nil {
		return nil, err
	}
	bw.sizeWritten += size
	return logs, nil
}

func (bw *BulkPackWriter) writeBM25Stasts(ctx context.Context) (map[int64]*datapb.FieldBinlog, error) {
	pack := bw.pack
	if len(pack.bm25Stats) == 0 {
		// TODO: we should not skip here, if the flush operation don't carry any insert data,
		// the merge stats operation will be skipped, which is a bad case.
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

	var mergedBM25Blob map[int64]*storage.Blob
	if pack.isFlush && pack.level != datapb.SegmentLevel_L0 && hasBM25Function(bw.schema) {
		mergedBM25Blob, err = serializer.serializeMergedBM25Stats(pack)
		if err != nil {
			return nil, err
		}
	}
	logs, size, err := bw.segmentWriter.WriteV1BM25Blobs(ctx, V1BM25BlobsInput{
		BatchBlobs:  bm25Blobs,
		MergedBlobs: mergedBM25Blob,
	})
	if err != nil {
		return nil, err
	}
	actions := []metacache.SegmentAction{metacache.MergeBm25Stats(pack.bm25Stats)}
	bw.metaCache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(pack.segmentID))
	bw.sizeWritten += size
	return logs, nil
}

func (bw *BulkPackWriter) writeDelta(ctx context.Context) (*datapb.FieldBinlog, error) {
	pack := bw.pack
	if pack.deltaData == nil || pack.deltaData.RowCount == 0 {
		return &datapb.FieldBinlog{}, nil
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(bw.schema)
	if err != nil {
		return nil, fmt.Errorf("primary key field not found: %w", err)
	}

	logID, err := bw.allocator.AllocOne()
	if err != nil {
		return nil, err
	}

	k := metautil.JoinIDPath(pack.collectionID, pack.partitionID, pack.segmentID, logID)
	deltaPath := path.Join(bw.chunkManager.RootPath(), common.SegmentDeltaLogPath, k)

	deltalog, size, err := bw.segmentWriter.WriteDelta(ctx, DeltaWriteParams{
		LogID:      logID,
		PKType:     pkField.DataType,
		Path:       deltaPath,
		DeleteData: pack.deltaData,
		Version:    storage.StorageV1,
		Uploader: func(ctx context.Context, kvs map[string][]byte) error {
			for k, blob := range kvs {
				return bw.chunkManager.Write(ctx, k, blob)
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	bw.sizeWritten += size

	return &datapb.FieldBinlog{
		FieldID: pkField.GetFieldID(),
		Binlogs: []*datapb.Binlog{deltalog},
	}, nil
}
