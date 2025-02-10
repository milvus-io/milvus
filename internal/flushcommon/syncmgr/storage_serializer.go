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
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type storageV1Serializer struct {
	schema  *schemapb.CollectionSchema
	pkField *schemapb.FieldSchema

	inCodec *storage.InsertCodec

	metacache metacache.MetaCache
}

func NewStorageSerializer(metacache metacache.MetaCache) (*storageV1Serializer, error) {
	schema := metacache.Schema()
	pkField := lo.FindOrElse(schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return nil, merr.WrapErrServiceInternal("cannot find pk field")
	}
	meta := &etcdpb.CollectionMeta{
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

func (s *storageV1Serializer) serializeBinlog(ctx context.Context, pack *SyncDataPack) (map[int64]*storage.Blob, error) {
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

func (s *storageV1Serializer) serializeBM25Stats(pack *SyncDataPack) (map[int64]*storage.Blob, error) {
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

func (s *storageV1Serializer) serializeStatslog(pack *SyncDataPack) (*storage.PrimaryKeyStats, *storage.Blob, error) {
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

func (s *storageV1Serializer) serializeMergedPkStats(pack *SyncDataPack) (*storage.Blob, error) {
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

func (s *storageV1Serializer) serializeMergedBM25Stats(pack *SyncDataPack) (map[int64]*storage.Blob, error) {
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

func (s *storageV1Serializer) serializeDeltalog(pack *SyncDataPack) (*storage.Blob, error) {
	if len(pack.deltaData.Pks) == 0 {
		return &storage.Blob{}, nil
	}

	writer, finalizer, err := storage.CreateDeltalogWriter(pack.collectionID, pack.partitionID, pack.segmentID, pack.deltaData.Pks[0].Type(), 1024)
	if err != nil {
		return nil, err
	}

	if len(pack.deltaData.Pks) != len(pack.deltaData.Tss) {
		return nil, fmt.Errorf("pk and ts should have same length in delta log, but get %d and %d", len(pack.deltaData.Pks), len(pack.deltaData.Tss))
	}

	for i := 0; i < len(pack.deltaData.Pks); i++ {
		deleteLog := storage.NewDeleteLog(pack.deltaData.Pks[i], pack.deltaData.Tss[i])
		err = writer.Write(deleteLog)
		if err != nil {
			return nil, err
		}
	}
	writer.Close()
	return finalizer()
}

func hasBM25Function(schema *schemapb.CollectionSchema) bool {
	for _, function := range schema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return true
		}
	}
	return false
}
