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
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type storageV1Serializer struct {
	collectionID int64
	schema       *schemapb.CollectionSchema
	pkField      *schemapb.FieldSchema

	inCodec  *storage.InsertCodec
	delCodec *storage.DeleteCodec

	metacache  metacache.MetaCache
	metaWriter MetaWriter
}

func NewStorageSerializer(metacache metacache.MetaCache, metaWriter MetaWriter) (*storageV1Serializer, error) {
	collectionID := metacache.Collection()
	schema := metacache.Schema()
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

		inCodec:    inCodec,
		delCodec:   storage.NewDeleteCodec(),
		metacache:  metacache,
		metaWriter: metaWriter,
	}, nil
}

func (s *storageV1Serializer) EncodeBuffer(ctx context.Context, pack *SyncPack) (Task, error) {
	task := NewSyncTask()
	tr := timerecord.NewTimeRecorder("storage_serializer")

	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", pack.segmentID),
		zap.Int64("collectionID", pack.collectionID),
		zap.String("channel", pack.channelName),
	)

	if len(pack.insertData) > 0 {
		memSize := make(map[int64]int64)
		for _, chunk := range pack.insertData {
			for fieldID, fieldData := range chunk.Data {
				memSize[fieldID] += int64(fieldData.GetMemorySize())
			}
		}
		task.binlogMemsize = memSize

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
		if pack.level != datapb.SegmentLevel_L0 {
			mergedStatsBlob, err := s.serializeMergedPkStats(pack)
			if err != nil {
				log.Warn("failed to serialize merged stats log", zap.Error(err))
				return nil, err
			}
			task.mergedStatsBlob = mergedStatsBlob
		}

		task.WithFlush()
	}

	if pack.deltaData != nil {
		deltaBlob, err := s.serializeDeltalog(pack)
		if err != nil {
			log.Warn("failed to serialize delta log", zap.Error(err))
			return nil, err
		}
		task.deltaBlob = deltaBlob
		task.deltaRowCount = pack.deltaData.RowCount
	}
	if pack.isDrop {
		task.WithDrop()
	}

	s.setTaskMeta(task, pack)

	metrics.DataNodeEncodeBufferLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), pack.level.String()).Observe(float64(tr.RecordSpan().Milliseconds()))
	return task, nil
}

func (s *storageV1Serializer) setTaskMeta(task *SyncTask, pack *SyncPack) {
	task.WithCollectionID(pack.collectionID).
		WithPartitionID(pack.partitionID).
		WithChannelName(pack.channelName).
		WithSegmentID(pack.segmentID).
		WithBatchSize(pack.batchSize).
		WithSchema(s.metacache.Schema()).
		WithStartPosition(pack.startPosition).
		WithCheckpoint(pack.checkpoint).
		WithLevel(pack.level).
		WithDataSource(pack.dataSource).
		WithTimeRange(pack.tsFrom, pack.tsTo).
		WithMetaCache(s.metacache).
		WithMetaWriter(s.metaWriter).
		WithFailureCallback(func(err error) {
			// TODO could change to unsub channel in the future
			panic(err)
		})
}

func (s *storageV1Serializer) serializeBinlog(ctx context.Context, pack *SyncPack) (map[int64]*storage.Blob, error) {
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

func (s *storageV1Serializer) serializeStatslog(pack *SyncPack) (*storage.PrimaryKeyStats, *storage.Blob, error) {
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
			BFType:  pks.PkFilter.Type(),
			BF:      pks.PkFilter,
			PkType:  int64(s.pkField.GetDataType()),
		}
	}), segment.NumOfRows())
}

func (s *storageV1Serializer) serializeDeltalog(pack *SyncPack) (*storage.Blob, error) {
	return s.delCodec.Serialize(pack.collectionID, pack.partitionID, pack.segmentID, pack.deltaData)
}
