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
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type storageV1Serializer struct {
	schema  *schemapb.CollectionSchema
	pkField *schemapb.FieldSchema

	inCodec *storage.InsertCodec

	metacache metacache.MetaCache
}

func NewStorageSerializer(metacache metacache.MetaCache, schema *schemapb.CollectionSchema) (*storageV1Serializer, error) {
	pkField := lo.FindOrElse(schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return nil, merr.WrapErrServiceInternal("cannot find pk field")
	}
	meta := &etcdpb.CollectionMeta{
		ID:     metacache.Collection(),
		Schema: schema,
	}
	inCodec := storage.NewInsertCodecWithSchema(meta)
	return &storageV1Serializer{
		schema:  schema,
		pkField: pkField,

		inCodec:   inCodec,
		metacache: metacache,
	}, nil
}

func (s *storageV1Serializer) serializeBinlog(ctx context.Context, pack *SyncPack) (map[int64]*storage.Blob, error) {
	if len(pack.insertData) == 0 {
		return make(map[int64]*storage.Blob), nil
	}
	log := log.Ctx(ctx)
	blobs, err := s.inCodec.Serialize(pack.partitionID, pack.segmentID, pack.insertData...)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]*storage.Blob)
	for _, blob := range blobs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("serialize buffer failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, err
		}

		result[fieldID] = blob
	}
	return result, nil
}

func (s *storageV1Serializer) serializeBM25Stats(pack *SyncPack) (map[int64]*storage.Blob, error) {
	if len(pack.bm25Stats) == 0 {
		return make(map[int64]*storage.Blob), nil
	}
	blobs := make(map[int64]*storage.Blob)
	for fieldID, stats := range pack.bm25Stats {
		bytes, err := stats.Serialize()
		if err != nil {
			return nil, err
		}

		blobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     stats.NumRow(),
		}
	}
	return blobs, nil
}

func (s *storageV1Serializer) serializeStatslog(pack *SyncPack) (*storage.PrimaryKeyStats, *storage.Blob, error) {
	if len(pack.insertData) == 0 {
		return nil, nil, nil
	}
	var rowNum int64
	var pkFieldData []storage.FieldData
	for _, chunk := range pack.insertData {
		chunkPKData := chunk.Data[s.pkField.GetFieldID()]
		pkFieldData = append(pkFieldData, chunkPKData)
		rowNum += int64(chunkPKData.RowNum())
	}

	stats, err := storage.NewPrimaryKeyStats(s.pkField.GetFieldID(), int64(s.pkField.GetDataType()), rowNum)
	if err != nil {
		return nil, nil, err
	}
	for _, chunkPkData := range pkFieldData {
		stats.UpdateByMsgs(chunkPkData)
	}

	blob, err := s.inCodec.SerializePkStats(stats, pack.batchRows)
	if err != nil {
		return nil, nil, err
	}
	return stats, blob, nil
}

func (s *storageV1Serializer) serializeMergedPkStats(pack *SyncPack) (*storage.Blob, error) {
	segment, ok := s.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	// Allow to flush empty segment to make streaming service easier to implement rollback transaction.
	stats := lo.Map(segment.GetHistory(), func(pks *storage.PkStatistics, _ int) *storage.PrimaryKeyStats {
		return &storage.PrimaryKeyStats{
			FieldID: s.pkField.GetFieldID(),
			MaxPk:   pks.MaxPK,
			MinPk:   pks.MinPK,
			BFType:  pks.PkFilter.Type(),
			BF:      pks.PkFilter,
			PkType:  int64(s.pkField.GetDataType()),
		}
	})
	if len(stats) == 0 {
		return nil, nil
	}
	return s.inCodec.SerializePkStatsList(stats, segment.NumOfRows())
}

func (s *storageV1Serializer) serializeMergedBM25Stats(pack *SyncPack) (map[int64]*storage.Blob, error) {
	segment, ok := s.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	stats := segment.GetBM25Stats()
	// Allow to flush empty segment to make streaming service easier to implement rollback transaction.
	if stats == nil {
		return nil, nil
	}

	fieldBytes, numRow, err := stats.Serialize()
	if err != nil {
		return nil, err
	}

	blobs := make(map[int64]*storage.Blob)
	for fieldID, bytes := range fieldBytes {
		blobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     numRow[fieldID],
		}
	}
	return blobs, nil
}

// serializeMergedPkStatsWith is like serializeMergedPkStats but includes an
// explicitly provided current-batch PrimaryKeyStats in the merged result,
// without requiring the metaCache to have been updated via RollStats.
//
// This is used by BulkPackWriterV3 so that the metaCache.RollStats action can
// be deferred until after a successful Write, while still emitting a correct
// merged stats blob on flush. See ccmd/pack_writer_v3_retry_plan.md.
func (s *storageV1Serializer) serializeMergedPkStatsWith(
	pack *SyncPack, extra *storage.PrimaryKeyStats,
) (*storage.Blob, error) {
	segment, ok := s.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	stats := lo.Map(segment.GetHistory(), func(pks *storage.PkStatistics, _ int) *storage.PrimaryKeyStats {
		return &storage.PrimaryKeyStats{
			FieldID: s.pkField.GetFieldID(),
			MaxPk:   pks.MaxPK,
			MinPk:   pks.MinPK,
			BFType:  pks.PkFilter.Type(),
			BF:      pks.PkFilter,
			PkType:  int64(s.pkField.GetDataType()),
		}
	})
	if extra != nil {
		stats = append(stats, extra)
	}
	if len(stats) == 0 {
		return nil, nil
	}
	// segment.NumOfRows() already includes flushed + syncing + buffered rows,
	// independent of whether RollStats has been applied for this batch.
	return s.inCodec.SerializePkStatsList(stats, segment.NumOfRows())
}

// serializeMergedBM25StatsWith is like serializeMergedBM25Stats but includes
// an explicitly provided current-batch bm25 stats map in the merged result,
// without mutating the metaCache copy. See serializeMergedPkStatsWith for
// rationale.
func (s *storageV1Serializer) serializeMergedBM25StatsWith(
	pack *SyncPack, extra map[int64]*storage.BM25Stats,
) (map[int64]*storage.Blob, error) {
	segment, ok := s.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	historyStats := segment.GetBM25Stats()
	var combined *metacache.SegmentBM25Stats
	if historyStats == nil {
		combined = metacache.NewEmptySegmentBM25Stats()
	} else {
		// Clone so we do not mutate the metaCache copy when merging extras.
		combined = historyStats.Clone()
	}
	if len(extra) > 0 {
		combined.Merge(extra)
	}

	fieldBytes, numRow, err := combined.Serialize()
	if err != nil {
		return nil, err
	}
	if len(fieldBytes) == 0 {
		return nil, nil
	}

	blobs := make(map[int64]*storage.Blob)
	for fieldID, bytes := range fieldBytes {
		blobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     numRow[fieldID],
		}
	}
	return blobs, nil
}

func hasBM25Function(schema *schemapb.CollectionSchema) bool {
	for _, function := range schema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return true
		}
	}
	return false
}
