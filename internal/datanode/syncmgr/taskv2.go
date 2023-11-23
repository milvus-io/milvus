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
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncTaskV2 struct {
	*SyncTask
	arrowSchema    *arrow.Schema
	reader         array.RecordReader
	statsBlob      *storage.Blob
	deleteReader   array.RecordReader
	storageVersion int64
	space          *milvus_storage.Space

	failureCallback func(err error)
}

func (t *SyncTaskV2) getLogger() *log.MLogger {
	return log.Ctx(context.Background()).With(
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.String("channel", t.channelName),
	)
}

func (t *SyncTaskV2) handleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}
}

func (t *SyncTaskV2) Run() error {
	log := t.getLogger()
	var err error

	infos := t.metacache.GetSegmentsBy(metacache.WithSegmentIDs(t.segmentID))
	if len(infos) == 0 {
		log.Warn("failed to sync data, segment not found in metacache")
		t.handleError(err)
		return merr.WrapErrSegmentNotFound(t.segmentID)
	}

	segment := infos[0]
	if segment.CompactTo() > 0 {
		log.Info("syncing segment compacted, update segment id", zap.Int64("compactTo", segment.CompactTo()))
		// update sync task segment id
		// it's ok to use compactTo segmentID here, since there shall be no insert for compacted segment
		t.segmentID = segment.CompactTo()
	}

	if err = t.serializeInsertData(); err != nil {
		t.handleError(err)
		return err
	}

	if err = t.serializeStatsData(); err != nil {
		t.handleError(err)
		return err
	}

	if err = t.serializeDeleteData(); err != nil {
		t.handleError(err)
		return err
	}

	if err = t.writeSpace(); err != nil {
		t.handleError(err)
		return err
	}

	if err = t.writeMeta(); err != nil {
		t.handleError(err)
		return err
	}

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchSize)}
	switch {
	case t.isDrop:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Dropped))
	case t.isFlush:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}

	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))

	return nil
}

func (t *SyncTaskV2) serializeInsertData() error {
	if t.insertData == nil {
		return nil
	}

	b := array.NewRecordBuilder(memory.DefaultAllocator, t.arrowSchema)
	defer b.Release()

	if err := buildRecord(b, t.insertData, t.schema.Fields); err != nil {
		return err
	}

	rec := b.NewRecord()
	defer rec.Release()

	itr, err := array.NewRecordReader(t.arrowSchema, []arrow.Record{rec})
	if err != nil {
		return err
	}
	itr.Retain()
	t.reader = itr
	return nil
}

func (t *SyncTaskV2) serializeStatsData() error {
	if t.insertData == nil {
		return nil
	}

	pkField := lo.FindOrElse(t.schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return merr.WrapErrServiceInternal("cannot find pk field")
	}
	fieldID := pkField.GetFieldID()

	stats, rowNum := t.convertInsertData2PkStats(fieldID, pkField.GetDataType())

	// not flush and not insert data
	if !t.isFlush && stats == nil {
		return nil
	}
	if t.isFlush {
		return t.serializeMergedPkStats(fieldID, pkField.GetDataType(), stats, rowNum)
	}

	return t.serializeSinglePkStats(fieldID, stats, rowNum)
}

func (t *SyncTaskV2) serializeMergedPkStats(fieldID int64, pkType schemapb.DataType, stats *storage.PrimaryKeyStats, rowNum int64) error {
	segments := t.metacache.GetSegmentsBy(metacache.WithSegmentIDs(t.segmentID))
	var statsList []*storage.PrimaryKeyStats
	var oldRowNum int64
	for _, segment := range segments {
		oldRowNum += segment.NumOfRows()
		statsList = append(statsList, lo.Map(segment.GetHistory(), func(pks *storage.PkStatistics, _ int) *storage.PrimaryKeyStats {
			return &storage.PrimaryKeyStats{
				FieldID: fieldID,
				MaxPk:   pks.MaxPK,
				MinPk:   pks.MinPK,
				BF:      pks.PkFilter,
				PkType:  int64(pkType),
			}
		})...)
	}
	if stats != nil {
		statsList = append(statsList, stats)
	}

	blob, err := t.getInCodec().SerializePkStatsList(statsList, oldRowNum+rowNum)
	if err != nil {
		return err
	}
	blob.Key = strconv.Itoa(int(storage.CompoundStatsType))
	t.statsBlob = blob
	return nil
}

func (t *SyncTaskV2) serializeSinglePkStats(fieldID int64, stats *storage.PrimaryKeyStats, rowNum int64) error {
	blob, err := t.getInCodec().SerializePkStats(stats, rowNum)
	if err != nil {
		return err
	}

	logidx, err := t.allocator.AllocOne()
	if err != nil {
		return err
	}

	blob.Key = strconv.Itoa(int(logidx))
	t.statsBlob = blob
	return nil
}

func (t *SyncTaskV2) serializeDeleteData() error {
	if t.deleteData == nil {
		return nil
	}

	fields := make([]*schemapb.FieldSchema, 0)
	pkField := lo.FindOrElse(t.schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return merr.WrapErrServiceInternal("cannot find pk field")
	}
	fields = append(fields, pkField)
	tsField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	fields = append(fields, tsField)

	schema, err := metacache.ConvertToArrowSchema(fields)
	if err != nil {
		return err
	}

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	switch pkField.DataType {
	case schemapb.DataType_Int64:
		pb := b.Field(0).(*array.Int64Builder)
		for _, pk := range t.deleteData.Pks {
			pb.Append(pk.GetValue().(int64))
		}
	case schemapb.DataType_VarChar:
		pb := b.Field(0).(*array.StringBuilder)
		for _, pk := range t.deleteData.Pks {
			pb.Append(pk.GetValue().(string))
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unexpected pk type %v", pkField.DataType)
	}

	for _, ts := range t.deleteData.Tss {
		b.Field(1).(*array.Int64Builder).Append(int64(ts))
	}

	rec := b.NewRecord()
	defer rec.Release()

	reader, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return err
	}

	t.deleteReader = reader
	return nil
}

func (t *SyncTaskV2) writeSpace() error {
	defer func() {
		if t.reader != nil {
			t.reader.Release()
		}
		if t.deleteReader != nil {
			t.deleteReader.Release()
		}
	}()

	// url := fmt.Sprintf("s3://%s:%s@%s/%d?endpoint_override=%s",
	// 	params.Params.MinioCfg.AccessKeyID.GetValue(),
	// 	params.Params.MinioCfg.SecretAccessKey.GetValue(),
	// 	params.Params.MinioCfg.BucketName.GetValue(),
	// 	t.segmentID,
	// 	params.Params.MinioCfg.Address.GetValue())

	// pkSchema, err := typeutil.GetPrimaryFieldSchema(t.schema)
	// if err != nil {
	// 	return err
	// }
	// vecSchema, err := typeutil.GetVectorFieldSchema(t.schema)
	// if err != nil {
	// 	return err
	// }
	// space, err := milvus_storage.Open(
	// 	url,
	// 	options.NewSpaceOptionBuilder().
	// 		SetSchema(schema.NewSchema(
	// 			t.arrowSchema,
	// 			&schema.SchemaOptions{
	// 				PrimaryColumn: pkSchema.Name,
	// 				VectorColumn:  vecSchema.Name,
	// 				VersionColumn: common.TimeStampFieldName,
	// 			},
	// 		)).
	// 		Build(),
	// )
	// if err != nil {
	// 	return err
	// }
	txn := t.space.NewTransaction()
	if t.reader != nil {
		txn.Write(t.reader, &options.DefaultWriteOptions)
	}
	if t.deleteReader != nil {
		txn.Delete(t.deleteReader)
	}
	if t.statsBlob != nil {
		txn.WriteBlob(t.statsBlob.Value, t.statsBlob.Key, false)
	}

	return txn.Commit()
}

func (t *SyncTaskV2) writeMeta() error {
	return t.metaWriter.UpdateSyncV2(t)
}

func buildRecord(b *array.RecordBuilder, data *storage.InsertData, fields []*schemapb.FieldSchema) error {
	if data == nil {
		log.Info("no buffer data to flush")
		return nil
	}
	for i, field := range fields {
		fBuilder := b.Field(i)
		switch field.DataType {
		case schemapb.DataType_Bool:
			fBuilder.(*array.BooleanBuilder).AppendValues(data.Data[field.FieldID].(*storage.BoolFieldData).Data, nil)
		case schemapb.DataType_Int8:
			fBuilder.(*array.Int8Builder).AppendValues(data.Data[field.FieldID].(*storage.Int8FieldData).Data, nil)
		case schemapb.DataType_Int16:
			fBuilder.(*array.Int16Builder).AppendValues(data.Data[field.FieldID].(*storage.Int16FieldData).Data, nil)
		case schemapb.DataType_Int32:
			fBuilder.(*array.Int32Builder).AppendValues(data.Data[field.FieldID].(*storage.Int32FieldData).Data, nil)
		case schemapb.DataType_Int64:
			fBuilder.(*array.Int64Builder).AppendValues(data.Data[field.FieldID].(*storage.Int64FieldData).Data, nil)
		case schemapb.DataType_Float:
			fBuilder.(*array.Float32Builder).AppendValues(data.Data[field.FieldID].(*storage.FloatFieldData).Data, nil)
		case schemapb.DataType_Double:
			fBuilder.(*array.Float64Builder).AppendValues(data.Data[field.FieldID].(*storage.DoubleFieldData).Data, nil)
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			fBuilder.(*array.StringBuilder).AppendValues(data.Data[field.FieldID].(*storage.StringFieldData).Data, nil)
		case schemapb.DataType_Array:
			appendListValues(fBuilder.(*array.ListBuilder), data.Data[field.FieldID].(*storage.ArrayFieldData))
		case schemapb.DataType_JSON:
			fBuilder.(*array.BinaryBuilder).AppendValues(data.Data[field.FieldID].(*storage.JSONFieldData).Data, nil)
		case schemapb.DataType_BinaryVector:
			vecData := data.Data[field.FieldID].(*storage.BinaryVectorFieldData)
			for i := 0; i < len(vecData.Data); i += vecData.Dim / 8 {
				fBuilder.(*array.FixedSizeBinaryBuilder).Append(vecData.Data[i : i+vecData.Dim/8])
			}
		case schemapb.DataType_FloatVector:
			vecData := data.Data[field.FieldID].(*storage.FloatVectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim * 4
			length := len(data) / dim

			builder.Reserve(length)
			bytesData := make([]byte, byteLength)
			for i := 0; i < length; i++ {
				vec := data[i*dim : (i+1)*dim]
				for j := range vec {
					bytes := math.Float32bits(vec[j])
					common.Endian.PutUint32(bytesData[j*4:], bytes)
				}
				builder.Append(bytesData)
			}
		case schemapb.DataType_Float16Vector:
			vecData := data.Data[field.FieldID].(*storage.Float16VectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim * 2
			length := len(data) / byteLength

			builder.Reserve(length)
			for i := 0; i < length; i++ {
				builder.Append(data[i*byteLength : (i+1)*byteLength])
			}

		default:
			return merr.WrapErrParameterInvalidMsg("unknown type %v", field.DataType.String())
		}
	}

	return nil
}

func appendListValues(builder *array.ListBuilder, data *storage.ArrayFieldData) error {
	vb := builder.ValueBuilder()
	switch data.ElementType {
	case schemapb.DataType_Bool:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.BooleanBuilder).AppendValues(data.GetBoolData().Data, nil)
		}
	case schemapb.DataType_Int8:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Int8Builder).AppendValues(castIntArray[int8](data.GetIntData().Data), nil)
		}
	case schemapb.DataType_Int16:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Int16Builder).AppendValues(castIntArray[int16](data.GetIntData().Data), nil)
		}
	case schemapb.DataType_Int32:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Int32Builder).AppendValues(data.GetIntData().Data, nil)
		}
	case schemapb.DataType_Int64:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Int64Builder).AppendValues(data.GetLongData().Data, nil)
		}
	case schemapb.DataType_Float:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Float32Builder).AppendValues(data.GetFloatData().Data, nil)
		}
	case schemapb.DataType_Double:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.Float64Builder).AppendValues(data.GetDoubleData().Data, nil)
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		for _, data := range data.Data {
			builder.Append(true)
			vb.(*array.StringBuilder).AppendValues(data.GetStringData().Data, nil)
		}

	default:
		return merr.WrapErrParameterInvalidMsg("unknown type %v", data.ElementType.String())
	}
	return nil
}

func castIntArray[T int8 | int16](nums []int32) []T {
	ret := make([]T, 0, len(nums))
	for _, n := range nums {
		ret = append(ret, T(n))
	}
	return ret
}

func NewSyncTaskV2() *SyncTaskV2 {
	return &SyncTaskV2{
		SyncTask: NewSyncTask(),
	}
}

func (t *SyncTaskV2) WithChunkManager(cm storage.ChunkManager) *SyncTaskV2 {
	t.chunkManager = cm
	return t
}

func (t *SyncTaskV2) WithAllocator(allocator allocator.Interface) *SyncTaskV2 {
	t.allocator = allocator
	return t
}

func (t *SyncTaskV2) WithInsertData(insertData *storage.InsertData) *SyncTaskV2 {
	t.insertData = insertData
	return t
}

func (t *SyncTaskV2) WithDeleteData(deleteData *storage.DeleteData) *SyncTaskV2 {
	t.deleteData = deleteData
	return t
}

func (t *SyncTaskV2) WithStartPosition(start *msgpb.MsgPosition) *SyncTaskV2 {
	t.startPosition = start
	return t
}

func (t *SyncTaskV2) WithCheckpoint(cp *msgpb.MsgPosition) *SyncTaskV2 {
	t.checkpoint = cp
	return t
}

func (t *SyncTaskV2) WithCollectionID(collID int64) *SyncTaskV2 {
	t.collectionID = collID
	return t
}

func (t *SyncTaskV2) WithPartitionID(partID int64) *SyncTaskV2 {
	t.partitionID = partID
	return t
}

func (t *SyncTaskV2) WithSegmentID(segID int64) *SyncTaskV2 {
	t.segmentID = segID
	return t
}

func (t *SyncTaskV2) WithChannelName(chanName string) *SyncTaskV2 {
	t.channelName = chanName
	return t
}

func (t *SyncTaskV2) WithSchema(schema *schemapb.CollectionSchema) *SyncTaskV2 {
	t.schema = schema
	return t
}

func (t *SyncTaskV2) WithTimeRange(from, to typeutil.Timestamp) *SyncTaskV2 {
	t.tsFrom, t.tsTo = from, to
	return t
}

func (t *SyncTaskV2) WithFlush() *SyncTaskV2 {
	t.isFlush = true
	return t
}

func (t *SyncTaskV2) WithDrop() *SyncTaskV2 {
	t.isDrop = true
	return t
}

func (t *SyncTaskV2) WithMetaCache(metacache metacache.MetaCache) *SyncTaskV2 {
	t.metacache = metacache
	return t
}

func (t *SyncTaskV2) WithMetaWriter(metaWriter MetaWriter) *SyncTaskV2 {
	t.metaWriter = metaWriter
	return t
}

func (t *SyncTaskV2) WithWriteRetryOptions(opts ...retry.Option) *SyncTaskV2 {
	t.writeRetryOpts = opts
	return t
}

func (t *SyncTaskV2) WithFailureCallback(callback func(error)) *SyncTaskV2 {
	t.failureCallback = callback
	return t
}

func (t *SyncTaskV2) WithBatchSize(batchSize int64) *SyncTaskV2 {
	t.batchSize = batchSize
	return t
}

func (t *SyncTaskV2) WithSpace(space *milvus_storage.Space) *SyncTaskV2 {
	t.space = space
	return t
}

func (t *SyncTaskV2) WithArrowSchema(arrowSchema *arrow.Schema) *SyncTaskV2 {
	t.arrowSchema = arrowSchema
	return t
}

func (t *SyncTaskV2) WithLevel(level datapb.SegmentLevel) *SyncTaskV2 {
	t.level = level
	return t
}
