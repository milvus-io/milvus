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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type storageV1Serializer struct {
	collectionID int64
	schema       *schemapb.CollectionSchema
	pkField      *schemapb.FieldSchema

	inCodec  *storage.InsertCodec
	delCodec *storage.DeleteCodec

	metacache metacache.MetaCache
}

func NewStorageSerializer(collectionID int64, schema *schemapb.CollectionSchema, metacache metacache.MetaCache) (*storageV1Serializer, error) {
	pkField := lo.FindOrElse(schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return nil, merr.WrapErrServiceInternal("cannot find pk field")
	}
	meta := &etcdpb.CollectionMeta{
		Schema: schema,
		ID:     collectionID,
	}
	inCodec := storage.NewInsertCodecWithSchema(meta)
	return &storageV1Serializer{
		collectionID: collectionID,
		schema:       schema,
		pkField:      pkField,

		inCodec:   inCodec,
		delCodec:  storage.NewDeleteCodec(),
		metacache: metacache,
	}, nil
}

func (s *storageV1Serializer) EncodeBuffer(ctx context.Context, pack *SyncPack) (Task, error) {
	task := &SyncTask{}

	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", pack.segmentID),
		zap.Int64("collectionID", pack.collectionID),
	)

	if pack.insertData != nil {
		binlogBlobs, err := s.serializeBinlog(ctx, pack)
		if err != nil {
			log.Warn("failed to serialize binlog", zap.Error(err))
			return nil, err
		}
		task.binlogBlobs = binlogBlobs

		singlePKStats, batchStatsBlob, err := s.serializeStatslog(pack)
		if err != nil {
			log.Warn("failed to serialized statslog", zap.Error(err))
			return nil, err
		}

		task.batchStatsBlob = batchStatsBlob
		s.metacache.UpdateSegments(metacache.RollStats(singlePKStats), metacache.WithSegmentIDs(pack.segmentID))
	}

	if pack.isFlush {
		mergedStatsBlob, err := s.serializeMergedPkStats(pack)
		if err != nil {
			log.Warn("failed to serialize merged stats log", zap.Error(err))
			return nil, err
		}

		task.mergedStatsBlob = mergedStatsBlob
		task.WithFlush()
	}

	if pack.deltaData != nil {
		deltaBlob, err := s.serializeDeltalog(pack)
		if err != nil {
			log.Warn("failed to serialize delta log", zap.Error(err))
			return nil, err
		}
		task.deltaBlob = deltaBlob
	}
	if pack.isDrop {
		task.WithDrop()
	}

	return task, nil
}

func (s *storageV1Serializer) serializeBinlog(ctx context.Context, pack *SyncPack) (map[int64]*storage.Blob, error) {
	blobs, err := s.inCodec.Serialize(pack.collectionID, pack.partitionID, pack.insertData)
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

func (s *storageV1Serializer) serializeStatslog(pack *SyncPack) (*storage.PrimaryKeyStats, *storage.Blob, error) {
	pkFieldData := pack.insertData.Data[s.pkField.GetFieldID()]
	rowNum := int64(pkFieldData.RowNum())

	stats, err := storage.NewPrimaryKeyStats(s.pkField.GetFieldID(), int64(s.pkField.GetDataType()), rowNum)
	if err != nil {
		return nil, nil, err
	}
	stats.UpdateByMsgs(pkFieldData)

	blob, err := s.inCodec.SerializePkStats(stats, pack.batchSize)
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

	return s.inCodec.SerializePkStatsList(lo.Map(segment.GetHistory(), func(pks *storage.PkStatistics, _ int) *storage.PrimaryKeyStats {
		return &storage.PrimaryKeyStats{
			FieldID: s.pkField.GetFieldID(),
			MaxPk:   pks.MaxPK,
			MinPk:   pks.MinPK,
			BF:      pks.PkFilter,
			PkType:  int64(s.pkField.GetDataType()),
		}
	}), segment.NumOfRows())
}

func (s *storageV1Serializer) serializeDeltalog(pack *SyncPack) (*storage.Blob, error) {
	return s.delCodec.Serialize(pack.collectionID, pack.partitionID, pack.segmentID, pack.deltaData)
}
