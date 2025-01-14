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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus-storage/go/storage/schema"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	iTypeutil "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type storageV2Serializer struct {
	*storageV1Serializer

	arrowSchema    *arrow.Schema
	storageV2Cache *metacache.StorageV2Cache
	inCodec        *storage.InsertCodec
	metacache      metacache.MetaCache
}

func NewStorageV2Serializer(
	storageV2Cache *metacache.StorageV2Cache,
	metacache metacache.MetaCache,
	metaWriter MetaWriter,
) (*storageV2Serializer, error) {
	v1Serializer, err := NewStorageSerializer(metacache, metaWriter)
	if err != nil {
		return nil, err
	}

	return &storageV2Serializer{
		storageV1Serializer: v1Serializer,
		storageV2Cache:      storageV2Cache,
		arrowSchema:         storageV2Cache.ArrowSchema(),
		metacache:           metacache,
	}, nil
}

func (s *storageV2Serializer) EncodeBuffer(ctx context.Context, pack *SyncPack) (Task, error) {
	task := NewSyncTaskV2()
	tr := timerecord.NewTimeRecorder("storage_serializer_v2")
	metricSegLevel := pack.level.String()

	space, err := s.storageV2Cache.GetOrCreateSpace(pack.segmentID, SpaceCreatorFunc(pack.segmentID, s.schema, s.arrowSchema))
	if err != nil {
		log.Warn("failed to get or create space", zap.Error(err))
		return nil, err
	}

	task.space = space
	if len(pack.insertData) > 0 {
		insertReader, err := s.serializeInsertData(pack)
		if err != nil {
			log.Warn("failed to serialize insert data with storagev2", zap.Error(err))
			return nil, err
		}

		task.reader = insertReader

		singlePKStats, batchStatsBlob, err := s.serializeStatslog(pack)
		if err != nil {
			log.Warn("failed to serialized statslog", zap.Error(err))
			return nil, err
		}

		task.statsBlob = batchStatsBlob
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
		deltaReader, err := s.serializeDeltaData(pack)
		if err != nil {
			log.Warn("failed to serialize delta data", zap.Error(err))
			return nil, err
		}
		task.deleteReader = deltaReader
	}

	if pack.isDrop {
		task.WithDrop()
	}

	s.setTaskMeta(task, pack)
	metrics.DataNodeEncodeBufferLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metricSegLevel).Observe(float64(tr.RecordSpan().Milliseconds()))
	return task, nil
}

func (s *storageV2Serializer) setTaskMeta(task *SyncTaskV2, pack *SyncPack) {
	task.WithCollectionID(pack.collectionID).
		WithPartitionID(pack.partitionID).
		WithChannelName(pack.channelName).
		WithSegmentID(pack.segmentID).
		WithBatchSize(pack.batchSize).
		WithSchema(s.metacache.Schema()).
		WithStartPosition(pack.startPosition).
		WithCheckpoint(pack.checkpoint).
		WithLevel(pack.level).
		WithTimeRange(pack.tsFrom, pack.tsTo).
		WithMetaCache(s.metacache).
		WithMetaWriter(s.metaWriter).
		WithFailureCallback(func(err error) {
			// TODO could change to unsub channel in the future
			panic(err)
		})
}

func (s *storageV2Serializer) serializeInsertData(pack *SyncPack) (array.RecordReader, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, s.arrowSchema)
	defer builder.Release()

	for _, chunk := range pack.insertData {
		if err := iTypeutil.BuildRecord(builder, chunk, s.schema.GetFields()); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	defer rec.Release()

	itr, err := array.NewRecordReader(s.arrowSchema, []arrow.Record{rec})
	if err != nil {
		return nil, err
	}
	itr.Retain()

	return itr, nil
}

func (s *storageV2Serializer) serializeDeltaData(pack *SyncPack) (array.RecordReader, error) {
	fields := make([]*schemapb.FieldSchema, 0, 2)
	tsField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	fields = append(fields, s.pkField, tsField)

	deltaArrowSchema, err := iTypeutil.ConvertToArrowSchema(fields)
	if err != nil {
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, deltaArrowSchema)
	defer builder.Release()

	switch s.pkField.GetDataType() {
	case schemapb.DataType_Int64:
		pb := builder.Field(0).(*array.Int64Builder)
		for _, pk := range pack.deltaData.Pks {
			pb.Append(pk.GetValue().(int64))
		}
	case schemapb.DataType_VarChar:
		pb := builder.Field(0).(*array.StringBuilder)
		for _, pk := range pack.deltaData.Pks {
			pb.Append(pk.GetValue().(string))
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unexpected pk type %v", s.pkField.GetDataType())
	}

	for _, ts := range pack.deltaData.Tss {
		builder.Field(1).(*array.Int64Builder).Append(int64(ts))
	}

	rec := builder.NewRecord()
	defer rec.Release()

	reader, err := array.NewRecordReader(deltaArrowSchema, []arrow.Record{rec})
	if err != nil {
		return nil, err
	}
	reader.Retain()

	return reader, nil
}

func SpaceCreatorFunc(segmentID int64, collSchema *schemapb.CollectionSchema, arrowSchema *arrow.Schema) func() (*milvus_storage.Space, error) {
	return func() (*milvus_storage.Space, error) {
		url, err := iTypeutil.GetStorageURI(params.Params.CommonCfg.StorageScheme.GetValue(), params.Params.CommonCfg.StoragePathPrefix.GetValue(), segmentID)
		if err != nil {
			return nil, err
		}

		pkSchema, err := typeutil.GetPrimaryFieldSchema(collSchema)
		if err != nil {
			return nil, err
		}
		vecSchema, err := typeutil.GetVectorFieldSchema(collSchema)
		if err != nil {
			return nil, err
		}
		space, err := milvus_storage.Open(
			url,
			options.NewSpaceOptionBuilder().
				SetSchema(schema.NewSchema(
					arrowSchema,
					&schema.SchemaOptions{
						PrimaryColumn: pkSchema.Name,
						VectorColumn:  vecSchema.Name,
						VersionColumn: common.TimeStampFieldName,
					},
				)).
				Build(),
		)
		return space, err
	}
}
