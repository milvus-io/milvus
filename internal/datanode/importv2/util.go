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

package importv2

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func WrapTaskNotFoundError(taskID int64) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("cannot find import task with id %d", taskID))
}

func NewSyncTask(ctx context.Context,
	allocator allocator.Interface,
	metaCaches map[string]metacache.MetaCache,
	ts uint64,
	segmentID, partitionID, collectionID int64, vchannel string,
	insertData *storage.InsertData,
	deleteData *storage.DeleteData,
	bm25Stats map[int64]*storage.BM25Stats,
) (syncmgr.Task, error) {
	metaCache := metaCaches[vchannel]
	if _, ok := metaCache.GetSegmentByID(segmentID); !ok {
		metaCache.AddSegment(&datapb.SegmentInfo{
			ID:            segmentID,
			State:         commonpb.SegmentState_Importing,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: vchannel,
		}, func(info *datapb.SegmentInfo) pkoracle.PkStat {
			bfs := pkoracle.NewBloomFilterSet()
			return bfs
		}, metacache.NewBM25StatsFactory)
	}

	var serializer syncmgr.Serializer
	var err error
	serializer, err = syncmgr.NewStorageSerializer(
		allocator,
		metaCache,
		nil,
	)
	if err != nil {
		return nil, err
	}

	segmentLevel := datapb.SegmentLevel_L1
	if insertData == nil && deleteData != nil {
		segmentLevel = datapb.SegmentLevel_L0
	}

	syncPack := &syncmgr.SyncPack{}
	syncPack.WithInsertData([]*storage.InsertData{insertData}).
		WithDeleteData(deleteData).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithLevel(segmentLevel).
		WithDataSource(metrics.BulkinsertDataSourceLabel).
		WithBatchRows(int64(insertData.GetRowNum()))
	if bm25Stats != nil {
		syncPack.WithBM25Stats(bm25Stats)
	}

	return serializer.EncodeBuffer(ctx, syncPack)
}

func NewImportSegmentInfo(syncTask syncmgr.Task, metaCaches map[string]metacache.MetaCache) (*datapb.ImportSegmentInfo, error) {
	segmentID := syncTask.SegmentID()
	insertBinlogs, statsBinlog, deltaLog, bm25Log := syncTask.(*syncmgr.SyncTask).Binlogs()
	metaCache := metaCaches[syncTask.ChannelName()]
	segment, ok := metaCache.GetSegmentByID(segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(segmentID, "import failed")
	}
	var deltaLogs []*datapb.FieldBinlog
	if len(deltaLog.GetBinlogs()) > 0 {
		deltaLogs = []*datapb.FieldBinlog{deltaLog}
	}
	return &datapb.ImportSegmentInfo{
		SegmentID:    segmentID,
		ImportedRows: segment.FlushedRows(),
		Binlogs:      lo.Values(insertBinlogs),
		Statslogs:    lo.Values(statsBinlog),
		Bm25Logs:     lo.Values(bm25Log),
		Deltalogs:    deltaLogs,
	}, nil
}

func PickSegment(segments []*datapb.ImportRequestSegment, vchannel string, partitionID int64) (int64, error) {
	candidates := lo.Filter(segments, func(info *datapb.ImportRequestSegment, _ int) bool {
		return info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})

	if len(candidates) == 0 {
		return 0, fmt.Errorf("no candidate segments found for channel %s and partition %d",
			vchannel, partitionID)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return candidates[r.Intn(len(candidates))].GetSegmentID(), nil
}

func CheckRowsEqual(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(data.Data) == 0 {
		return nil
	}
	idToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	rows, field := GetInsertDataRowCount(data, schema)
	for fieldID, d := range data.Data {
		if d.RowNum() == 0 && (CanBeZeroRowField(idToField[fieldID])) {
			continue
		}
		if d.RowNum() != rows {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, field '%s' with '%d' rows",
					idToField[field].GetName(), rows, idToField[fieldID].GetName(), d.RowNum()))
		}
	}
	return nil
}

func AppendSystemFieldsData(task *ImportTask, data *storage.InsertData, rowNum int) error {
	pkField, err := typeutil.GetPrimaryFieldSchema(task.GetSchema())
	if err != nil {
		return err
	}
	ids := make([]int64, rowNum)
	start, _, err := task.allocator.Alloc(uint32(rowNum))
	if err != nil {
		return err
	}
	for i := 0; i < rowNum; i++ {
		ids[i] = start + int64(i)
	}
	pkData, ok := data.Data[pkField.GetFieldID()]
	allowInsertAutoID, _ := common.IsAllowInsertAutoID(task.req.Schema.GetProperties()...)
	if pkField.GetAutoID() && (!ok || pkData == nil || pkData.RowNum() == 0 || !allowInsertAutoID) {
		switch pkField.GetDataType() {
		case schemapb.DataType_Int64:
			data.Data[pkField.GetFieldID()] = &storage.Int64FieldData{Data: ids}
		case schemapb.DataType_VarChar:
			strIDs := lo.Map(ids, func(id int64, _ int) string {
				return strconv.FormatInt(id, 10)
			})
			data.Data[pkField.GetFieldID()] = &storage.StringFieldData{Data: strIDs}
		}
	}
	if _, ok := data.Data[common.RowIDField]; !ok { // for binlog import, keep original rowID and ts
		data.Data[common.RowIDField] = &storage.Int64FieldData{Data: ids}
	}
	if _, ok := data.Data[common.TimeStampField]; !ok {
		tss := make([]int64, rowNum)
		ts := int64(task.req.GetTs())
		for i := 0; i < rowNum; i++ {
			tss[i] = ts
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
	}
	return nil
}

type nullDefaultAppender[T any] struct{}

func (h *nullDefaultAppender[T]) AppendDefault(fieldData storage.FieldData, defaultVal T, rowNum int) error {
	values := make([]T, rowNum)
	if fieldData.GetNullable() {
		validData := make([]bool, rowNum)
		for i := 0; i < rowNum; i++ {
			validData[i] = true    // all true
			values[i] = defaultVal // fill with default value
		}
		return fieldData.AppendRows(values, validData)
	} else {
		for i := 0; i < rowNum; i++ {
			values[i] = defaultVal // fill with default value
		}
		return fieldData.AppendDataRows(values)
	}
	return nil
}

func (h *nullDefaultAppender[T]) AppendNull(fieldData storage.FieldData, rowNum int) error {
	if fieldData.GetNullable() {
		values := make([]T, rowNum)
		validData := make([]bool, rowNum)
		for i := 0; i < rowNum; i++ {
			validData[i] = false
		}
		return fieldData.AppendRows(values, validData)
	}
	return nil
}

func IsFillableField(field *schemapb.FieldSchema) bool {
	nullable := field.GetNullable()
	defaultVal := field.GetDefaultValue()
	return nullable || defaultVal != nil
}

func AppendNullableDefaultFieldsData(schema *schemapb.CollectionSchema, data *storage.InsertData, rowNum int) error {
	for _, field := range schema.GetFields() {
		if !IsFillableField(field) {
			continue
		}
		if tempData, ok := data.Data[field.GetFieldID()]; ok {
			if tempData.RowNum() > 0 {
				continue // values have been read from data file
			}
		}

		// add a new column and fill with null or default
		dataType := field.GetDataType()
		fieldData, err := storage.NewFieldData(dataType, field, rowNum)
		if err != nil {
			return err
		}
		data.Data[field.GetFieldID()] = fieldData

		nullable := field.GetNullable()
		defaultVal := field.GetDefaultValue()

		// bool/int8/int16/int32/int64/float/double/varchar/json/array can be null value
		// bool/int8/int16/int32/int64/float/double/varchar can be default value
		switch dataType {
		case schemapb.DataType_Bool:
			appender := &nullDefaultAppender[bool]{}
			if defaultVal != nil {
				v := defaultVal.GetBoolData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Int8:
			appender := &nullDefaultAppender[int8]{}
			if defaultVal != nil {
				v := defaultVal.GetIntData()
				err = appender.AppendDefault(fieldData, int8(v), rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Int16:
			appender := &nullDefaultAppender[int16]{}
			if defaultVal != nil {
				v := defaultVal.GetIntData()
				err = appender.AppendDefault(fieldData, int16(v), rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Int32:
			appender := &nullDefaultAppender[int32]{}
			if defaultVal != nil {
				v := defaultVal.GetIntData()
				err = appender.AppendDefault(fieldData, int32(v), rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Int64:
			appender := &nullDefaultAppender[int64]{}
			if defaultVal != nil {
				v := defaultVal.GetLongData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Float:
			appender := &nullDefaultAppender[float32]{}
			if defaultVal != nil {
				v := defaultVal.GetFloatData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Double:
			appender := &nullDefaultAppender[float64]{}
			if defaultVal != nil {
				v := defaultVal.GetDoubleData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_VarChar:
			appender := &nullDefaultAppender[string]{}
			if defaultVal != nil {
				v := defaultVal.GetStringData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_JSON:
			if nullable {
				appender := &nullDefaultAppender[[]byte]{}
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_Array:
			if nullable {
				appender := &nullDefaultAppender[*schemapb.ScalarField]{}
				err = appender.AppendNull(fieldData, rowNum)
			}
		default:
			return fmt.Errorf("Unexpected data type: %d, cannot be filled with default value", dataType)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func RunEmbeddingFunction(task *ImportTask, data *storage.InsertData) error {
	fns := task.GetSchema().GetFunctions()
	for _, fn := range fns {
		runner, err := function.NewFunctionRunner(task.GetSchema(), fn)
		if err != nil {
			return err
		}
		defer runner.Close()

		inputFieldIDs := lo.Map(runner.GetInputFields(), func(field *schemapb.FieldSchema, _ int) int64 { return field.GetFieldID() })
		inputDatas := make([]any, 0, len(inputFieldIDs))
		for _, inputFieldID := range inputFieldIDs {
			inputDatas = append(inputDatas, data.Data[inputFieldID].GetDataRows())
		}
		outputFieldData, err := runner.BatchRun(inputDatas...)
		if err != nil {
			return err
		}
		for i, outputFieldID := range fn.OutputFieldIds {
			outputField := typeutil.GetField(task.GetSchema(), outputFieldID)
			// TODO: added support for vector output field only, scalar output field in function is not supported yet
			switch outputField.GetDataType() {
			case schemapb.DataType_FloatVector:
				data.Data[outputFieldID] = outputFieldData[i].(*storage.FloatVectorFieldData)
			case schemapb.DataType_BFloat16Vector:
				data.Data[outputFieldID] = outputFieldData[i].(*storage.BFloat16VectorFieldData)
			case schemapb.DataType_Float16Vector:
				data.Data[outputFieldID] = outputFieldData[i].(*storage.Float16VectorFieldData)
			case schemapb.DataType_BinaryVector:
				data.Data[outputFieldID] = outputFieldData[i].(*storage.BinaryVectorFieldData)
			case schemapb.DataType_SparseFloatVector:
				sparseArray := outputFieldData[i].(*schemapb.SparseFloatArray)
				data.Data[outputFieldID] = &storage.SparseFloatVectorFieldData{
					SparseFloatArray: schemapb.SparseFloatArray{
						Dim:      sparseArray.GetDim(),
						Contents: sparseArray.GetContents(),
					},
				}
			default:
				return fmt.Errorf("unsupported output data type for embedding function: %s", outputField.GetDataType().String())
			}
		}
	}
	return nil
}

func CanBeZeroRowField(field *schemapb.FieldSchema) bool {
	if field.GetIsPrimaryKey() && field.GetAutoID() {
		return true // auto-generated primary key, the row count must be 0
	}
	if field.GetIsDynamic() {
		return true // dyanmic field, row count could be 0
	}
	if IsFillableField(field) {
		return true // nullable/default_value field can be automatically filled if the file doesn't contain this column
	}
	return false
}

func GetInsertDataRowCount(data *storage.InsertData, schema *schemapb.CollectionSchema) (int, int64) {
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for fieldID, fd := range data.Data {
		if fd.RowNum() == 0 && CanBeZeroRowField(fields[fieldID]) {
			continue
		}

		// each collection must contains at least one vector field, there must be one field that row number is not 0
		if fd.RowNum() != 0 {
			return fd.RowNum(), fieldID
		}
	}
	return 0, 0
}

func LogStats(manager TaskManager) {
	logFunc := func(tasks []Task, taskType TaskType) {
		byState := lo.GroupBy(tasks, func(t Task) datapb.ImportTaskStateV2 {
			return t.GetState()
		})
		log.Info("import task stats", zap.String("type", taskType.String()),
			zap.Int("pending", len(byState[datapb.ImportTaskStateV2_Pending])),
			zap.Int("inProgress", len(byState[datapb.ImportTaskStateV2_InProgress])),
			zap.Int("completed", len(byState[datapb.ImportTaskStateV2_Completed])),
			zap.Int("failed", len(byState[datapb.ImportTaskStateV2_Failed])))
	}
	tasks := manager.GetBy(WithType(PreImportTaskType))
	logFunc(tasks, PreImportTaskType)
	tasks = manager.GetBy(WithType(ImportTaskType))
	logFunc(tasks, ImportTaskType)
}

func UnsetAutoID(schema *schemapb.CollectionSchema) {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			field.AutoID = false
			return
		}
	}
}

func NewMetaCache(req *datapb.ImportRequest) map[string]metacache.MetaCache {
	metaCaches := make(map[string]metacache.MetaCache)
	schema := typeutil.AppendSystemFields(req.GetSchema())
	for _, channel := range req.GetVchannels() {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: schema,
		}
		metaCache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		}, metacache.NoneBm25StatsFactory)
		metaCaches[channel] = metaCache
	}
	return metaCaches
}
