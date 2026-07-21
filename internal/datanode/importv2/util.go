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
	"path"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func WrapTaskNotFoundError(taskID int64) error {
	return merr.WrapErrImportSysFailedMsg("cannot find import task with id %d", taskID)
}

func NewSyncTask(ctx context.Context,
	allocator allocator.Interface,
	metaCaches map[string]metacache.MetaCache,
	ts uint64,
	segmentID, partitionID, collectionID int64, vchannel string,
	insertData *storage.InsertData,
	deleteData *storage.DeleteData,
	bm25Stats map[int64]*storage.BM25Stats,
	storageVersion int64,
	useLoonFFI bool,
	storageConfig *indexpb.StorageConfig,
) (syncmgr.Task, error) {
	metaCache := metaCaches[vchannel]
	if _, ok := metaCache.GetSegmentByID(segmentID); !ok {
		segment := &datapb.SegmentInfo{
			ID:             segmentID,
			State:          commonpb.SegmentState_Importing,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  vchannel,
			StorageVersion: storageVersion,
		}
		// init first manifest path
		if useLoonFFI {
			k := metautil.JoinIDPath(collectionID, partitionID, segmentID)
			basePath := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, k)
			// ManifestEarliest for first write
			segment.ManifestPath = packed.MarshalManifestPath(basePath, packed.ManifestEarliest)
		}
		metaCache.AddSegment(segment, func(info *datapb.SegmentInfo) pkoracle.PkStat {
			bfs := pkoracle.NewBloomFilterSet()
			return bfs
		}, metacache.NewBM25StatsFactory)
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

	writeRetryAttempts := paramtable.Get().DataNodeCfg.ImportMaxWriteRetryAttempts.GetAsUint()
	retryOpts := []retry.Option{
		retry.Attempts(writeRetryAttempts), // default retry always
		retry.MaxSleepTime(10 * time.Second),
	}
	task := syncmgr.NewSyncTask().
		WithAllocator(allocator).
		WithMetaCache(metaCache).
		WithSchema(metaCache.GetSchema(0)). // TODO specify import schema if needed
		WithSyncPack(syncPack).
		WithStorageConfig(storageConfig).
		WithWriteRetryOptions(retryOpts...)
	return task, nil
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
		ManifestPath: segment.ManifestPath(),
	}, nil
}

func PickSegment(segments []*datapb.ImportRequestSegment, vchannel string, partitionID int64) (int64, error) {
	candidates := lo.Filter(segments, func(info *datapb.ImportRequestSegment, _ int) bool {
		return info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})

	if len(candidates) == 0 {
		return 0, merr.WrapErrServiceInternalMsg("no candidate segments found for channel %s and partition %d",
			vchannel, partitionID)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return candidates[r.Intn(len(candidates))].GetSegmentID(), nil
}

func CheckRowsEqual(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(data.Data) == 0 {
		return nil
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	idToField := lo.KeyBy(allFields, func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	rows, baseFieldID := GetInsertDataRowCount(data, schema)
	for fieldID, d := range data.Data {
		tempField := idToField[fieldID]
		if d.RowNum() == 0 && (CanBeZeroRowField(tempField)) {
			continue
		}
		if d.RowNum() != rows {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, field '%s' with '%d' rows",
					idToField[baseFieldID].GetName(), rows, tempField.GetName(), d.RowNum()))
		}
	}
	return nil
}

// CheckStructArrayConsistency verifies that within each StructArrayField all
// sub-field columns are row-wise consistent: for every row, the sub-fields
// must agree on null-ness, and, when the row is valid, on the number of
// struct elements (array length for scalar Array sub-fields, vector count
// for ArrayOfVector sub-fields). The proxy enforces this invariant on the
// insert path (checkAndFlattenStructFieldData), and JSON/CSV/Parquet/NumPy
// importers produce consistent columns by construction, but binlog import
// deserializes each sub-field's binlogs independently — divergent sub-field
// data would otherwise be persisted and poison the segment (query-time
// assertion failures or silently wrong element-level filter results).
// Cost is O(rows * subFields), negligible compared with reading the data.
func CheckStructArrayConsistency(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	type subColumn struct {
		name      string
		data      storage.FieldData
		validData []bool
	}
	for _, structField := range schema.GetStructArrayFields() {
		subFields := structField.GetFields()
		columns := make([]subColumn, 0, len(subFields))
		var firstAbsent string
		for _, subField := range subFields {
			fieldData, ok := data.Data[subField.GetFieldID()]
			if !ok || fieldData == nil {
				if firstAbsent == "" {
					firstAbsent = subField.GetName()
				}
				continue
			}

			var validData []bool
			if fieldData.GetNullable() {
				switch fd := fieldData.(type) {
				case *storage.ArrayFieldData:
					validData = fd.ValidData
				case *storage.VectorArrayFieldData:
					validData = fd.ValidData
				default:
					return merr.WrapErrImportSysFailedMsg(
						"unexpected nullable column type '%s' for sub-field '%s' of struct field '%s'",
						fieldData.GetDataType().String(), subField.GetName(), structField.GetName())
				}
				if len(validData) != fieldData.RowNum() {
					return merr.WrapErrImportSysFailedMsg(
						"nullable sub-field '%s' of struct field '%s' has invalid ValidData length %d, expected %d",
						subField.GetName(), structField.GetName(), len(validData), fieldData.RowNum())
				}
			}

			if fieldData.RowNum() == 0 {
				if firstAbsent == "" {
					firstAbsent = subField.GetName()
				}
				continue
			}
			columns = append(columns, subColumn{name: subField.GetName(), data: fieldData, validData: validData})
		}
		// A struct must be supplied whole: either all sub-fields present or all
		// absent. A partial set is malformed input — the absent sub-fields get
		// backfilled as all-NULL (AppendNullableDefaultFieldsData) while the
		// present ones carry real elements, producing per-row element-count
		// mismatches that poison the segment. (checkAndFlattenStructFieldData
		// enforces the same on the proxy insert path.)
		if len(columns) == 0 {
			continue
		}
		if len(columns) < len(subFields) {
			return merr.WrapErrImportFailedMsg(
				"struct field '%s' has a partial sub-field set: sub-field '%s' is present but sub-field '%s' is missing; provide all sub-fields or none",
				structField.GetName(), columns[0].name, firstAbsent)
		}
		ref := columns[0]
		rows := ref.data.RowNum()
		for _, col := range columns[1:] {
			if col.data.RowNum() != rows {
				return merr.WrapErrImportFailedMsg(
					"struct field '%s' has misaligned sub-fields, sub-field '%s' with '%d' rows, sub-field '%s' with '%d' rows",
					structField.GetName(), ref.name, rows, col.name, col.data.RowNum())
			}
		}

		for i := 0; i < rows; i++ {
			refValid := !ref.data.GetNullable() || ref.validData[i]
			refCount := -1
			if refValid {
				var err error
				refCount, err = structSubFieldRowElementCount(ref.data, i, structField.GetName(), ref.name)
				if err != nil {
					return err
				}
			}
			for _, col := range columns[1:] {
				valid := !col.data.GetNullable() || col.validData[i]
				if valid != refValid {
					return merr.WrapErrImportFailedMsg(
						"struct field '%s' has inconsistent sub-field null-ness at row %d, sub-field '%s' valid=%t, sub-field '%s' valid=%t",
						structField.GetName(), i, ref.name, refValid, col.name, valid)
				}
				if !valid {
					continue
				}
				count, err := structSubFieldRowElementCount(col.data, i, structField.GetName(), col.name)
				if err != nil {
					return err
				}
				if count != refCount {
					return merr.WrapErrImportFailedMsg(
						"struct field '%s' has inconsistent element count at row %d, sub-field '%s' with %d elements, sub-field '%s' with %d elements",
						structField.GetName(), i, ref.name, refCount, col.name, count)
				}
			}
		}
	}
	return nil
}

// structSubFieldRowElementCount returns the number of struct elements in row i
// of a struct sub-field column: the array length for scalar Array sub-fields,
// or the number of vectors for ArrayOfVector sub-fields.
func structSubFieldRowElementCount(fieldData storage.FieldData, i int, structName, subFieldName string) (int, error) {
	switch fd := fieldData.(type) {
	case *storage.ArrayFieldData:
		count, ok := scalarFieldElementCount(fd.Data[i])
		if !ok {
			return 0, merr.WrapErrImportFailedMsg(
				"invalid scalar array data at row %d of sub-field '%s' of struct field '%s': missing or unsupported scalar payload",
				i, subFieldName, structName)
		}
		return count, nil
	case *storage.VectorArrayFieldData:
		row := fd.Data[i]
		var payloadLen, width int
		dim := int(fd.Dim)
		switch fd.ElementType {
		case schemapb.DataType_FloatVector:
			payloadLen = len(row.GetFloatVector().GetData())
			width = dim
		case schemapb.DataType_BinaryVector:
			payloadLen = len(row.GetBinaryVector())
			width = (dim + 7) / 8
		case schemapb.DataType_Float16Vector:
			payloadLen = len(row.GetFloat16Vector())
			width = dim * 2
		case schemapb.DataType_BFloat16Vector:
			payloadLen = len(row.GetBfloat16Vector())
			width = dim * 2
		case schemapb.DataType_Int8Vector:
			payloadLen = len(row.GetInt8Vector())
			width = dim
		default:
			return 0, merr.WrapErrImportSysFailedMsg(
				"unsupported vector element type '%s' for sub-field '%s' of struct field '%s'",
				fd.ElementType.String(), subFieldName, structName)
		}
		if width <= 0 || payloadLen%width != 0 {
			return 0, merr.WrapErrImportFailedMsg(
				"corrupted vector array data at row %d of sub-field '%s' of struct field '%s', payload length %d is not a multiple of vector width %d",
				i, subFieldName, structName, payloadLen, width)
		}
		return payloadLen / width, nil
	default:
		return 0, merr.WrapErrImportSysFailedMsg(
			"unexpected column type '%s' for sub-field '%s' of struct field '%s'",
			fieldData.GetDataType().String(), subFieldName, structName)
	}
}

// scalarFieldElementCount returns the number of elements held by one Array row
// and whether its scalar payload type is recognized. A typed empty payload is
// valid and returns (0, true); a nil or unset payload returns (0, false).
func scalarFieldElementCount(sf *schemapb.ScalarField) (int, bool) {
	switch d := sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return len(d.BoolData.GetData()), true
	case *schemapb.ScalarField_IntData:
		return len(d.IntData.GetData()), true
	case *schemapb.ScalarField_LongData:
		return len(d.LongData.GetData()), true
	case *schemapb.ScalarField_FloatData:
		return len(d.FloatData.GetData()), true
	case *schemapb.ScalarField_DoubleData:
		return len(d.DoubleData.GetData()), true
	case *schemapb.ScalarField_StringData:
		return len(d.StringData.GetData()), true
	case *schemapb.ScalarField_BytesData:
		return len(d.BytesData.GetData()), true
	case *schemapb.ScalarField_ArrayData:
		return len(d.ArrayData.GetData()), true
	case *schemapb.ScalarField_JsonData:
		return len(d.JsonData.GetData()), true
	case *schemapb.ScalarField_TimestamptzData:
		return len(d.TimestamptzData.GetData()), true
	default:
		return 0, false
	}
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
	allFields := typeutil.GetAllFieldSchemas(schema)
	for _, field := range allFields {
		if !IsFillableField(field) {
			continue
		}

		tempData, ok := data.Data[field.GetFieldID()]
		if ok && tempData != nil {
			// values have been read from data file, row number must be equal to other fields
			// checked by CheckRowsEqual() in preImportTask , double-check here
			if tempData.RowNum() == rowNum {
				continue
			}
			if tempData.RowNum() > 0 && tempData.RowNum() != rowNum {
				return merr.WrapErrImportFailed(
					fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, other fields with '%d' rows",
						field.GetName(), tempData.RowNum(), rowNum))
			}
		}

		// if the FieldData is not found, or it is nil, add a new column and fill with null or default
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
				err = appender.AppendDefault(fieldData, v, rowNum)
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
		case schemapb.DataType_Timestamptz:
			appender := &nullDefaultAppender[int64]{}
			if defaultVal != nil {
				v := defaultVal.GetTimestamptzData()
				err = appender.AppendDefault(fieldData, v, rowNum)
			} else if nullable {
				err = appender.AppendNull(fieldData, rowNum)
			}
		case schemapb.DataType_VarChar, schemapb.DataType_Text:
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
		case schemapb.DataType_FloatVector,
			schemapb.DataType_Float16Vector,
			schemapb.DataType_BFloat16Vector,
			schemapb.DataType_BinaryVector,
			schemapb.DataType_SparseFloatVector,
			schemapb.DataType_Int8Vector,
			schemapb.DataType_ArrayOfVector:
			if nullable {
				for i := 0; i < rowNum; i++ {
					if err = fieldData.AppendRow(nil); err != nil {
						return err
					}
				}
			}
		default:
			return merr.WrapErrServiceInternalMsg("unexpected data type: %d, cannot be filled with default value", dataType)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func FillDynamicData(schema *schemapb.CollectionSchema, data *storage.InsertData, rowNum int) error {
	if !schema.GetEnableDynamicField() {
		return nil
	}
	dynamicField := typeutil.GetDynamicField(schema)
	if dynamicField == nil {
		return merr.WrapErrImportSysFailed("collection schema is illegal, enable_dynamic_field is true but the dynamic field doesn't exist")
	}

	tempData, ok := data.Data[dynamicField.GetFieldID()]
	if ok && tempData != nil {
		// values have been read from data file, row number must be equal to other fields
		// checked by CheckRowsEqual() in preImportTask , double-check here
		if tempData.RowNum() == rowNum {
			return nil
		}
		if tempData.RowNum() > 0 && tempData.RowNum() != rowNum {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, other fields with '%d' rows",
					dynamicField.GetName(), tempData.RowNum(), rowNum))
		}
	}

	// if the FieldData is not found, or it is nil, add a new column and fill with empty json
	fieldData, err := storage.NewFieldData(dynamicField.GetDataType(), dynamicField, rowNum)
	if err != nil {
		return err
	}
	jsonFD := fieldData.(*storage.JSONFieldData)
	bs := []byte("{}")
	for i := 0; i < rowNum; i++ {
		jsonFD.Data = append(jsonFD.Data, bs)
	}
	data.Data[dynamicField.GetFieldID()] = fieldData
	return nil
}

func RunEmbeddingFunction(task *ImportTask, data *storage.InsertData) error {
	mlog.Info(context.TODO(), "start to run embedding function")
	schema := task.GetSchema()
	allowNonBM25Outputs := common.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.GetProperties())
	if err := embedding.RunAll(context.Background(), schema, data, embedding.RunOptions{
		ClusterID:           task.req.GetClusterID(),
		DBName:              schema.GetDbName(),
		AllowNonBM25Outputs: allowNonBM25Outputs,
	}); err != nil {
		return errors.Wrap(merr.ErrInvalidInsertData, err.Error())
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
	if field.GetIsFunctionOutput() {
		return true // function output field, row count could be 0
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
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			fields[subField.GetFieldID()] = subField
		}
	}

	for fieldID, fd := range data.Data {
		if fd == nil {
			// normally is impossible, just to avoid potential crash here
			continue
		}
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
		mlog.Info(context.TODO(), "import task stats", mlog.String("type", taskType.String()),
			mlog.Int("pending", len(byState[datapb.ImportTaskStateV2_Pending])),
			mlog.Int("inProgress", len(byState[datapb.ImportTaskStateV2_InProgress])),
			mlog.Int("completed", len(byState[datapb.ImportTaskStateV2_Completed])),
			mlog.Int("failed", len(byState[datapb.ImportTaskStateV2_Failed])))
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
