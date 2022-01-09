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

package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	InsertTaskName                  = "InsertTask"
	CreateCollectionTaskName        = "CreateCollectionTask"
	DropCollectionTaskName          = "DropCollectionTask"
	SearchTaskName                  = "SearchTask"
	RetrieveTaskName                = "RetrieveTask"
	QueryTaskName                   = "QueryTask"
	AnnsFieldKey                    = "anns_field"
	TopKKey                         = "topk"
	MetricTypeKey                   = "metric_type"
	SearchParamsKey                 = "params"
	RoundDecimalKey                 = "round_decimal"
	HasCollectionTaskName           = "HasCollectionTask"
	DescribeCollectionTaskName      = "DescribeCollectionTask"
	GetCollectionStatisticsTaskName = "GetCollectionStatisticsTask"
	GetPartitionStatisticsTaskName  = "GetPartitionStatisticsTask"
	ShowCollectionTaskName          = "ShowCollectionTask"
	CreatePartitionTaskName         = "CreatePartitionTask"
	DropPartitionTaskName           = "DropPartitionTask"
	HasPartitionTaskName            = "HasPartitionTask"
	ShowPartitionTaskName           = "ShowPartitionTask"
	CreateIndexTaskName             = "CreateIndexTask"
	DescribeIndexTaskName           = "DescribeIndexTask"
	DropIndexTaskName               = "DropIndexTask"
	GetIndexStateTaskName           = "GetIndexStateTask"
	GetIndexBuildProgressTaskName   = "GetIndexBuildProgressTask"
	FlushTaskName                   = "FlushTask"
	LoadCollectionTaskName          = "LoadCollectionTask"
	ReleaseCollectionTaskName       = "ReleaseCollectionTask"
	LoadPartitionTaskName           = "LoadPartitionsTask"
	ReleasePartitionTaskName        = "ReleasePartitionsTask"
	deleteTaskName                  = "DeleteTask"
	CreateAliasTaskName             = "CreateAliasTask"
	DropAliasTaskName               = "DropAliasTask"
	AlterAliasTaskName              = "AlterAliasTask"

	// minFloat32 minimum float.
	minFloat32 = -1 * float32(math.MaxFloat32)
)

type task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type dmlTask interface {
	task
	getChannels() ([]vChan, error)
	getPChanStats() (map[pChan]pChanStatistics, error)
}

type BaseInsertTask = msgstream.InsertMsg

type insertTask struct {
	BaseInsertTask
	req *milvuspb.InsertRequest
	Condition
	ctx context.Context

	result         *milvuspb.MutationResult
	rowIDAllocator *allocator.IDAllocator
	segIDAssigner  *segIDAssigner
	chMgr          channelsMgr
	chTicker       channelsTimeTicker
	vChannels      []vChan
	pChannels      []pChan
	schema         *schemapb.CollectionSchema
}

// TraceCtx returns insertTask context
func (it *insertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *insertTask) ID() UniqueID {
	return it.Base.MsgID
}

func (it *insertTask) SetID(uid UniqueID) {
	it.Base.MsgID = uid
}

func (it *insertTask) Name() string {
	return InsertTaskName
}

func (it *insertTask) Type() commonpb.MsgType {
	return it.Base.MsgType
}

func (it *insertTask) BeginTs() Timestamp {
	return it.BeginTimestamp
}

func (it *insertTask) SetTs(ts Timestamp) {
	it.BeginTimestamp = ts
	it.EndTimestamp = ts
}

func (it *insertTask) EndTs() Timestamp {
	return it.EndTimestamp
}

func (it *insertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels, err := it.getChannels()
	if err != nil {
		return ret, err
	}

	beginTs := it.BeginTs()
	endTs := it.EndTs()

	for _, channel := range channels {
		ret[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	return ret, nil
}

func (it *insertTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.CollectionName)
	if err != nil {
		return nil, err
	}
	var channels []pChan
	channels, err = it.chMgr.getChannels(collID)
	if err != nil {
		err = it.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		channels, err = it.chMgr.getChannels(collID)
	}
	return channels, err
}

func (it *insertTask) OnEnqueue() error {
	it.BaseInsertTask.InsertRequest.Base = &commonpb.MsgBase{}
	return nil
}

func getNumRowsOfScalarField(datas interface{}) uint32 {
	realTypeDatas := reflect.ValueOf(datas)
	return uint32(realTypeDatas.Len())
}

func getNumRowsOfFloatVectorField(fDatas []float32, dim int64) (uint32, error) {
	if dim <= 0 {
		return 0, errDimLessThanOrEqualToZero(int(dim))
	}
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float data should divide the dim(%d)", l, dim)
	}
	return uint32(int(int64(l) / dim)), nil
}

func getNumRowsOfBinaryVectorField(bDatas []byte, dim int64) (uint32, error) {
	if dim <= 0 {
		return 0, errDimLessThanOrEqualToZero(int(dim))
	}
	if dim%8 != 0 {
		return 0, errDimShouldDivide8(int(dim))
	}
	l := len(bDatas)
	if (8*int64(l))%dim != 0 {
		return 0, fmt.Errorf("the num(%d) of all bits should divide the dim(%d)", 8*l, dim)
	}
	return uint32(int((8 * int64(l)) / dim)), nil
}

func (it *insertTask) checkLengthOfFieldsData() error {
	neededFieldsNum := 0
	for _, field := range it.schema.Fields {
		if !field.AutoID {
			neededFieldsNum++
		}
	}

	if len(it.req.FieldsData) < neededFieldsNum {
		return errFieldsLessThanNeeded(len(it.req.FieldsData), neededFieldsNum)
	}

	return nil
}

func (it *insertTask) checkRowNums() error {
	if it.req.NumRows <= 0 {
		return errNumRowsLessThanOrEqualToZero(it.req.NumRows)
	}

	if err := it.checkLengthOfFieldsData(); err != nil {
		return err
	}

	rowNums := it.req.NumRows

	for i, field := range it.req.FieldsData {
		switch field.Field.(type) {
		case *schemapb.FieldData_Scalars:
			scalarField := field.GetScalars()
			switch scalarField.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				fieldNumRows := getNumRowsOfScalarField(scalarField.GetBoolData().Data)
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.ScalarField_IntData:
				fieldNumRows := getNumRowsOfScalarField(scalarField.GetIntData().Data)
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.ScalarField_LongData:
				fieldNumRows := getNumRowsOfScalarField(scalarField.GetLongData().Data)
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.ScalarField_FloatData:
				fieldNumRows := getNumRowsOfScalarField(scalarField.GetFloatData().Data)
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.ScalarField_DoubleData:
				fieldNumRows := getNumRowsOfScalarField(scalarField.GetDoubleData().Data)
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.ScalarField_BytesData:
				return errUnsupportedDType("bytes")
			case *schemapb.ScalarField_StringData:
				return errUnsupportedDType("string")
			case nil:
				continue
			default:
				continue
			}
		case *schemapb.FieldData_Vectors:
			vectorField := field.GetVectors()
			switch vectorField.Data.(type) {
			case *schemapb.VectorField_FloatVector:
				dim := vectorField.GetDim()
				fieldNumRows, err := getNumRowsOfFloatVectorField(vectorField.GetFloatVector().Data, dim)
				if err != nil {
					return err
				}
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case *schemapb.VectorField_BinaryVector:
				dim := vectorField.GetDim()
				fieldNumRows, err := getNumRowsOfBinaryVectorField(vectorField.GetBinaryVector(), dim)
				if err != nil {
					return err
				}
				if fieldNumRows != rowNums {
					return errNumRowsOfFieldDataMismatchPassed(i, fieldNumRows, rowNums)
				}
			case nil:
				continue
			default:
				continue
			}
		}
	}

	return nil
}

// TODO(dragondriver): ignore the order of fields in request, use the order of CollectionSchema to reorganize data
func (it *insertTask) transferColumnBasedRequestToRowBasedData() error {
	dTypes := make([]schemapb.DataType, 0, len(it.req.FieldsData))
	datas := make([][]interface{}, 0, len(it.req.FieldsData))
	rowNum := 0

	appendScalarField := func(getDataFunc func() interface{}) error {
		fieldDatas := reflect.ValueOf(getDataFunc())
		if rowNum != 0 && rowNum != fieldDatas.Len() {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = fieldDatas.Len()
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		for i := 0; i < rowNum; i++ {
			datas[idx] = append(datas[idx], fieldDatas.Index(i).Interface())
		}

		return nil
	}

	appendFloatVectorField := func(fDatas []float32, dim int64) error {
		l := len(fDatas)
		if int64(l)%dim != 0 {
			return errors.New("invalid vectors")
		}
		r := int64(l) / dim
		if rowNum != 0 && rowNum != int(r) {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = int(r)
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		vector := make([]float32, 0, dim)
		for i := 0; i < l; i++ {
			vector = append(vector, fDatas[i])
			if int64(i+1)%dim == 0 {
				datas[idx] = append(datas[idx], vector)
				vector = make([]float32, 0, dim)
			}
		}

		return nil
	}

	appendBinaryVectorField := func(bDatas []byte, dim int64) error {
		l := len(bDatas)
		if dim%8 != 0 {
			return errors.New("invalid dim")
		}
		if (8*int64(l))%dim != 0 {
			return errors.New("invalid vectors")
		}
		r := (8 * int64(l)) / dim
		if rowNum != 0 && rowNum != int(r) {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = int(r)
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		vector := make([]byte, 0, dim)
		for i := 0; i < l; i++ {
			vector = append(vector, bDatas[i])
			if (8*int64(i+1))%dim == 0 {
				datas[idx] = append(datas[idx], vector)
				vector = make([]byte, 0, dim)
			}
		}

		return nil
	}

	for _, field := range it.req.FieldsData {
		switch field.Field.(type) {
		case *schemapb.FieldData_Scalars:
			scalarField := field.GetScalars()
			switch scalarField.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetBoolData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_IntData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetIntData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_LongData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetLongData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_FloatData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetFloatData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_DoubleData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetDoubleData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_BytesData:
				return errors.New("bytes field is not supported now")
			case *schemapb.ScalarField_StringData:
				return errors.New("string field is not supported now")
			case nil:
				continue
			default:
				continue
			}
		case *schemapb.FieldData_Vectors:
			vectorField := field.GetVectors()
			switch vectorField.Data.(type) {
			case *schemapb.VectorField_FloatVector:
				floatVectorFieldData := vectorField.GetFloatVector().Data
				dim := vectorField.GetDim()
				err := appendFloatVectorField(floatVectorFieldData, dim)
				if err != nil {
					return err
				}
			case *schemapb.VectorField_BinaryVector:
				binaryVectorFieldData := vectorField.GetBinaryVector()
				dim := vectorField.GetDim()
				err := appendBinaryVectorField(binaryVectorFieldData, dim)
				if err != nil {
					return err
				}
			case nil:
				continue
			default:
				continue
			}
		case nil:
			continue
		default:
			continue
		}

		dTypes = append(dTypes, field.Type)
	}

	it.RowData = make([]*commonpb.Blob, 0, rowNum)
	l := len(dTypes)
	// TODO(dragondriver): big endian or little endian?
	endian := common.Endian
	printed := false
	for i := 0; i < rowNum; i++ {
		blob := &commonpb.Blob{
			Value: make([]byte, 0),
		}

		for j := 0; j < l; j++ {
			var buffer bytes.Buffer
			switch dTypes[j] {
			case schemapb.DataType_Bool:
				d := datas[j][i].(bool)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int8:
				d := int8(datas[j][i].(int32))
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int16:
				d := int16(datas[j][i].(int32))
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int32:
				d := datas[j][i].(int32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int64:
				d := datas[j][i].(int64)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Float:
				d := datas[j][i].(float32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Double:
				d := datas[j][i].(float64)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_FloatVector:
				d := datas[j][i].([]float32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_BinaryVector:
				d := datas[j][i].([]byte)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			default:
				log.Warn("unsupported data type")
			}
		}
		if !printed {
			log.Debug("Proxy, transform", zap.Any("ID", it.ID()), zap.Any("BlobLen", len(blob.Value)), zap.Any("dTypes", dTypes))
			printed = true
		}
		it.RowData = append(it.RowData, blob)
	}

	return nil
}

func (it *insertTask) checkFieldAutoIDAndHashPK() error {
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if it.req.NumRows <= 0 {
		return errNumRowsLessThanOrEqualToZero(it.req.NumRows)
	}

	if err := it.checkLengthOfFieldsData(); err != nil {
		return err
	}

	rowNums := it.req.NumRows

	primaryFieldName := ""
	autoIDFieldName := ""
	autoIDLoc := -1
	primaryLoc := -1
	fields := it.schema.Fields

	for loc, field := range fields {
		if field.AutoID {
			autoIDLoc = loc
			autoIDFieldName = field.Name
		}
		if field.IsPrimaryKey {
			primaryLoc = loc
			primaryFieldName = field.Name
		}
	}

	if primaryLoc < 0 {
		return fmt.Errorf("primary field is not found")
	}

	if autoIDLoc >= 0 && autoIDLoc != primaryLoc {
		return fmt.Errorf("currently auto id field is only supported on primary field")
	}

	var primaryField *schemapb.FieldData
	var primaryData []int64
	for _, field := range it.req.FieldsData {
		if field.FieldName == autoIDFieldName {
			return fmt.Errorf("autoID field (%v) does not require data", autoIDFieldName)
		}
		if field.FieldName == primaryFieldName {
			primaryField = field
		}
	}

	if primaryField != nil {
		if primaryField.Type != schemapb.DataType_Int64 {
			return fmt.Errorf("currently only support DataType Int64 as PrimaryField and Enable autoID")
		}
		switch primaryField.Field.(type) {
		case *schemapb.FieldData_Scalars:
			scalarField := primaryField.GetScalars()
			switch scalarField.Data.(type) {
			case *schemapb.ScalarField_LongData:
				primaryData = scalarField.GetLongData().Data
			default:
				return fmt.Errorf("currently only support DataType Int64 as PrimaryField and Enable autoID")
			}
		default:
			return fmt.Errorf("currently only support DataType Int64 as PrimaryField and Enable autoID")
		}
		it.result.IDs.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: primaryData,
			},
		}
	}

	var rowIDBegin UniqueID
	var rowIDEnd UniqueID

	rowIDBegin, rowIDEnd, _ = it.rowIDAllocator.Alloc(rowNums)

	it.BaseInsertTask.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.BaseInsertTask.RowIDs[offset] = i
	}

	if autoIDLoc >= 0 {
		fieldData := schemapb.FieldData{
			FieldName: primaryFieldName,
			FieldId:   -1,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: it.BaseInsertTask.RowIDs,
						},
					},
				},
			},
		}

		// TODO(dragondriver): when we can ignore the order of input fields, use append directly
		// it.req.FieldsData = append(it.req.FieldsData, &fieldData)
		it.req.FieldsData = append(it.req.FieldsData, &schemapb.FieldData{})
		copy(it.req.FieldsData[autoIDLoc+1:], it.req.FieldsData[autoIDLoc:])
		it.req.FieldsData[autoIDLoc] = &fieldData

		it.result.IDs.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: it.BaseInsertTask.RowIDs,
			},
		}
		it.HashPK(it.BaseInsertTask.RowIDs)
	} else {
		it.HashPK(primaryData)
	}

	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	return nil
}

func (it *insertTask) HashPK(pks []int64) {
	if len(it.HashValues) != 0 {
		log.Warn("the hashvalues passed through client is not supported now, and will be overwritten")
	}
	it.HashValues = make([]uint32, 0, len(pks))
	for _, pk := range pks {
		hash, _ := typeutil.Hash32Int64(pk)
		it.HashValues = append(it.HashValues, hash)
	}
}

func (it *insertTask) PreExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(it.ctx, "Proxy-Insert-PreExecute")
	defer sp.Finish()
	it.Base.MsgType = commonpb.MsgType_Insert
	it.Base.SourceID = Params.ProxyCfg.ProxyID

	it.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	collectionName := it.BaseInsertTask.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		return err
	}

	partitionTag := it.BaseInsertTask.PartitionName
	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
	log.Debug("Proxy Insert PreExecute", zap.Any("collSchema", collSchema))
	if err != nil {
		return err
	}
	it.schema = collSchema

	err = it.checkRowNums()
	if err != nil {
		return err
	}

	err = it.checkFieldAutoIDAndHashPK()
	if err != nil {
		return err
	}

	err = it.transferColumnBasedRequestToRowBasedData()
	if err != nil {
		return err
	}

	rowNum := len(it.RowData)
	it.Timestamps = make([]uint64, rowNum)
	for index := range it.Timestamps {
		it.Timestamps[index] = it.BeginTimestamp
	}

	return nil
}

func (it *insertTask) _assignSegmentID(stream msgstream.MsgStream, pack *msgstream.MsgPack) (*msgstream.MsgPack, error) {
	newPack := &msgstream.MsgPack{
		BeginTs:        pack.BeginTs,
		EndTs:          pack.EndTs,
		StartPositions: pack.StartPositions,
		EndPositions:   pack.EndPositions,
		Msgs:           nil,
	}
	tsMsgs := pack.Msgs
	hashKeys := stream.ComputeProduceChannelIndexes(tsMsgs)
	reqID := it.Base.MsgID
	channelCountMap := make(map[int32]uint32)    //   channelID to count
	channelMaxTSMap := make(map[int32]Timestamp) //  channelID to max Timestamp
	channelNames, err := it.chMgr.getVChannels(it.GetCollectionID())
	if err != nil {
		return nil, err
	}
	log.Debug("_assignSemgentID, produceChannels:", zap.Any("Channels", channelNames))

	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_Insert {
			return nil, fmt.Errorf("msg's must be Insert")
		}
		insertRequest, ok := request.(*msgstream.InsertMsg)
		if !ok {
			return nil, fmt.Errorf("msg's must be Insert")
		}

		keys := hashKeys[i]
		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, fmt.Errorf("the length of hashValue, timestamps, rowIDs, RowData are not equal")
		}
		for idx, channelID := range keys {
			channelCountMap[channelID]++
			if _, ok := channelMaxTSMap[channelID]; !ok {
				channelMaxTSMap[channelID] = typeutil.ZeroTimestamp
			}
			ts := insertRequest.Timestamps[idx]
			if channelMaxTSMap[channelID] < ts {
				channelMaxTSMap[channelID] = ts
			}
		}
	}

	reqSegCountMap := make(map[int32]map[UniqueID]uint32)
	for channelID, count := range channelCountMap {
		ts, ok := channelMaxTSMap[channelID]
		if !ok {
			ts = typeutil.ZeroTimestamp
			log.Debug("Warning: did not get max Timestamp!")
		}
		channelName := channelNames[channelID]
		if channelName == "" {
			return nil, fmt.Errorf("proxy, repack_func, can not found channelName")
		}
		mapInfo, err := it.segIDAssigner.GetSegmentID(it.CollectionID, it.PartitionID, channelName, count, ts)
		if err != nil {
			log.Debug("insertTask.go", zap.Any("MapInfo", mapInfo),
				zap.Error(err))
			return nil, err
		}
		reqSegCountMap[channelID] = make(map[UniqueID]uint32)
		reqSegCountMap[channelID] = mapInfo
		log.Debug("Proxy", zap.Int64("repackFunc, reqSegCountMap, reqID", reqID), zap.Any("mapinfo", mapInfo))
	}

	reqSegAccumulateCountMap := make(map[int32][]uint32)
	reqSegIDMap := make(map[int32][]UniqueID)
	reqSegAllocateCounter := make(map[int32]uint32)

	for channelID, segInfo := range reqSegCountMap {
		reqSegAllocateCounter[channelID] = 0
		keys := make([]UniqueID, len(segInfo))
		i := 0
		for key := range segInfo {
			keys[i] = key
			i++
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		accumulate := uint32(0)
		for _, key := range keys {
			accumulate += segInfo[key]
			if _, ok := reqSegAccumulateCountMap[channelID]; !ok {
				reqSegAccumulateCountMap[channelID] = make([]uint32, 0)
			}
			reqSegAccumulateCountMap[channelID] = append(
				reqSegAccumulateCountMap[channelID],
				accumulate,
			)
			if _, ok := reqSegIDMap[channelID]; !ok {
				reqSegIDMap[channelID] = make([]UniqueID, 0)
			}
			reqSegIDMap[channelID] = append(
				reqSegIDMap[channelID],
				key,
			)
		}
	}

	var getSegmentID = func(channelID int32) UniqueID {
		reqSegAllocateCounter[channelID]++
		cur := reqSegAllocateCounter[channelID]
		accumulateSlice := reqSegAccumulateCountMap[channelID]
		segIDSlice := reqSegIDMap[channelID]
		for index, count := range accumulateSlice {
			if cur <= count {
				return segIDSlice[index]
			}
		}
		log.Warn("Can't Found SegmentID", zap.Any("reqSegAllocateCounter", reqSegAllocateCounter))
		return 0
	}

	threshold := Params.PulsarCfg.MaxMessageSize
	// not accurate
	/* #nosec G103 */
	getFixedSizeOfInsertMsg := func(msg *msgstream.InsertMsg) int {
		size := 0

		size += int(unsafe.Sizeof(*msg.Base))
		size += int(unsafe.Sizeof(msg.DbName))
		size += int(unsafe.Sizeof(msg.CollectionName))
		size += int(unsafe.Sizeof(msg.PartitionName))
		size += int(unsafe.Sizeof(msg.DbID))
		size += int(unsafe.Sizeof(msg.CollectionID))
		size += int(unsafe.Sizeof(msg.PartitionID))
		size += int(unsafe.Sizeof(msg.SegmentID))
		size += int(unsafe.Sizeof(msg.ShardName))
		size += int(unsafe.Sizeof(msg.Timestamps))
		size += int(unsafe.Sizeof(msg.RowIDs))
		return size
	}

	result := make(map[int32]msgstream.TsMsg)
	curMsgSizeMap := make(map[int32]int)

	for i, request := range tsMsgs {
		insertRequest := request.(*msgstream.InsertMsg)
		keys := hashKeys[i]
		collectionName := insertRequest.CollectionName
		collectionID := insertRequest.CollectionID
		partitionID := insertRequest.PartitionID
		partitionName := insertRequest.PartitionName
		proxyID := insertRequest.Base.SourceID
		for index, key := range keys {
			ts := insertRequest.Timestamps[index]
			rowID := insertRequest.RowIDs[index]
			row := insertRequest.RowData[index]
			segmentID := getSegmentID(key)
			if segmentID == 0 {
				return nil, fmt.Errorf("get SegmentID failed, segmentID is zero")
			}
			_, ok := result[key]
			if !ok {
				sliceRequest := internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						MsgID:     reqID,
						Timestamp: ts,
						SourceID:  proxyID,
					},
					CollectionID:   collectionID,
					PartitionID:    partitionID,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					SegmentID:      segmentID,
					ShardName:      channelNames[key],
				}
				insertMsg := &msgstream.InsertMsg{
					BaseMsg: msgstream.BaseMsg{
						Ctx: request.TraceCtx(),
					},
					InsertRequest: sliceRequest,
				}
				result[key] = insertMsg
				curMsgSizeMap[key] = getFixedSizeOfInsertMsg(insertMsg)
			}
			curMsg := result[key].(*msgstream.InsertMsg)
			curMsgSize := curMsgSizeMap[key]
			curMsg.HashValues = append(curMsg.HashValues, insertRequest.HashValues[index])
			curMsg.Timestamps = append(curMsg.Timestamps, ts)
			curMsg.RowIDs = append(curMsg.RowIDs, rowID)
			curMsg.RowData = append(curMsg.RowData, row)
			/* #nosec G103 */
			curMsgSize += 4 + 8 + int(unsafe.Sizeof(row.Value))
			curMsgSize += len(row.Value)

			if curMsgSize >= threshold {
				newPack.Msgs = append(newPack.Msgs, curMsg)
				delete(result, key)
				curMsgSize = 0
			}

			curMsgSizeMap[key] = curMsgSize
		}
	}
	for _, msg := range result {
		if msg != nil {
			newPack.Msgs = append(newPack.Msgs, msg)
		}
	}

	return newPack, nil
}

func (it *insertTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(it.ctx, "Proxy-Insert-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert %d", it.ID()))
	defer tr.Elapse("done")

	collectionName := it.BaseInsertTask.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		return err
	}
	it.CollectionID = collID
	var partitionID UniqueID
	if len(it.PartitionName) > 0 {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, it.PartitionName)
		if err != nil {
			return err
		}
	} else {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, Params.ProxyCfg.DefaultPartitionName)
		if err != nil {
			return err
		}
	}
	it.PartitionID = partitionID
	tr.Record("get collection id & partition id from cache")

	var tsMsg msgstream.TsMsg = &it.BaseInsertTask
	it.BaseMsg.Ctx = ctx
	msgPack := msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
		Msgs:    make([]msgstream.TsMsg, 1),
	}

	msgPack.Msgs[0] = tsMsg

	stream, err := it.chMgr.getDMLStream(collID)
	if err != nil {
		err = it.chMgr.createDMLMsgStream(collID)
		if err != nil {
			it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			it.result.Status.Reason = err.Error()
			return err
		}
		stream, err = it.chMgr.getDMLStream(collID)
		if err != nil {
			it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			it.result.Status.Reason = err.Error()
			return err
		}
	}
	tr.Record("get used message stream")

	// Assign SegmentID
	var pack *msgstream.MsgPack
	pack, err = it._assignSegmentID(stream, &msgPack)
	if err != nil {
		return err
	}
	tr.Record("assign segment id")

	err = stream.Produce(pack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	tr.Record("send insert request to message stream")

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}

type createCollectionTask struct {
	Condition
	*milvuspb.CreateCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	schema    *schemapb.CollectionSchema
}

func (cct *createCollectionTask) TraceCtx() context.Context {
	return cct.ctx
}

func (cct *createCollectionTask) ID() UniqueID {
	return cct.Base.MsgID
}

func (cct *createCollectionTask) SetID(uid UniqueID) {
	cct.Base.MsgID = uid
}

func (cct *createCollectionTask) Name() string {
	return CreateCollectionTaskName
}

func (cct *createCollectionTask) Type() commonpb.MsgType {
	return cct.Base.MsgType
}

func (cct *createCollectionTask) BeginTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) EndTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) SetTs(ts Timestamp) {
	cct.Base.Timestamp = ts
}

func (cct *createCollectionTask) OnEnqueue() error {
	cct.Base = &commonpb.MsgBase{}
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (cct *createCollectionTask) PreExecute(ctx context.Context) error {
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.ProxyID

	cct.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(cct.Schema, cct.schema)
	if err != nil {
		return err
	}
	cct.schema.AutoID = false
	cct.CreateCollectionRequest.Schema, err = proto.Marshal(cct.schema)
	if err != nil {
		return err
	}

	if cct.ShardsNum > Params.ProxyCfg.MaxShardNum {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum)
	}

	if int64(len(cct.schema.Fields)) > Params.ProxyCfg.MaxFieldNum {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum)
	}

	// validate collection name
	if err := validateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	if err := validateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	if err := validatePrimaryKey(cct.schema); err != nil {
		return err
	}

	if err := ValidateFieldAutoID(cct.schema); err != nil {
		return err
	}

	// validate field name
	for _, field := range cct.schema.Fields {
		if err := validateFieldName(field.Name); err != nil {
			return err
		}
		if field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector {
			exist := false
			var dim int64
			for _, param := range field.TypeParams {
				if param.Key == "dim" {
					exist = true
					tmp, err := strconv.ParseInt(param.Value, 10, 64)
					if err != nil {
						return err
					}
					dim = tmp
					break
				}
			}
			if !exist {
				return errors.New("dimension is not defined in field type params, check type param `dim` for vector field")
			}
			if field.DataType == schemapb.DataType_FloatVector {
				if err := validateDimension(dim, false); err != nil {
					return err
				}
			} else {
				if err := validateDimension(dim, true); err != nil {
					return err
				}
			}
		}
	}

	if err := validateMultipleVectorFields(cct.schema); err != nil {
		return err
	}

	return nil
}

func (cct *createCollectionTask) Execute(ctx context.Context) error {
	var err error
	cct.result, err = cct.rootCoord.CreateCollection(ctx, cct.CreateCollectionRequest)
	return err
}

func (cct *createCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
}

func (dct *dropCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *dropCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *dropCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *dropCollectionTask) Name() string {
	return DropCollectionTaskName
}

func (dct *dropCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *dropCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *dropCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *dropCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DropCollection
	dct.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *dropCollectionTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, dct.CollectionName)
	if err != nil {
		return err
	}

	dct.result, err = dct.rootCoord.DropCollection(ctx, dct.DropCollectionRequest)
	if err != nil {
		return err
	}

	_ = dct.chMgr.removeDMLStream(collID)
	_ = dct.chMgr.removeDQLStream(collID)

	return nil
}

func (dct *dropCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

// Support wildcard in output fields:
//   "*" - all scalar fields
//   "%" - all vector fields
// For example, A and B are scalar fields, C and D are vector fields, duplicated fields will automatically be removed.
//   output_fields=["*"] 	 ==> [A,B]
//   output_fields=["%"] 	 ==> [C,D]
//   output_fields=["*","%"] ==> [A,B,C,D]
//   output_fields=["*",A] 	 ==> [A,B]
//   output_fields=["*",C]   ==> [A,B,C]
func translateOutputFields(outputFields []string, schema *schemapb.CollectionSchema, addPrimary bool) ([]string, error) {
	var primaryFieldName string
	scalarFieldNameMap := make(map[string]bool)
	vectorFieldNameMap := make(map[string]bool)
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)

	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vectorFieldNameMap[field.Name] = true
		} else {
			scalarFieldNameMap[field.Name] = true
		}
	}

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == "*" {
			for fieldName := range scalarFieldNameMap {
				resultFieldNameMap[fieldName] = true
			}
		} else if outputFieldName == "%" {
			for fieldName := range vectorFieldNameMap {
				resultFieldNameMap[fieldName] = true
			}
		} else {
			resultFieldNameMap[outputFieldName] = true
		}
	}

	if addPrimary {
		resultFieldNameMap[primaryFieldName] = true
	}

	for fieldName := range resultFieldNameMap {
		resultFieldNames = append(resultFieldNames, fieldName)
	}
	return resultFieldNames, nil
}

type searchTask struct {
	Condition
	*internalpb.SearchRequest
	ctx       context.Context
	resultBuf chan []*internalpb.SearchResults
	result    *milvuspb.SearchResults
	query     *milvuspb.SearchRequest
	chMgr     channelsMgr
	qc        types.QueryCoord
}

func (st *searchTask) TraceCtx() context.Context {
	return st.ctx
}

func (st *searchTask) ID() UniqueID {
	return st.Base.MsgID
}

func (st *searchTask) SetID(uid UniqueID) {
	st.Base.MsgID = uid
}

func (st *searchTask) Name() string {
	return SearchTaskName
}

func (st *searchTask) Type() commonpb.MsgType {
	return st.Base.MsgType
}

func (st *searchTask) BeginTs() Timestamp {
	return st.Base.Timestamp
}

func (st *searchTask) EndTs() Timestamp {
	return st.Base.Timestamp
}

func (st *searchTask) SetTs(ts Timestamp) {
	st.Base.Timestamp = ts
}

func (st *searchTask) OnEnqueue() error {
	st.Base = &commonpb.MsgBase{}
	st.Base.MsgType = commonpb.MsgType_Search
	st.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (st *searchTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []pChan
	channels, err = st.chMgr.getChannels(collID)
	if err != nil {
		err := st.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return st.chMgr.getChannels(collID)
	}

	return channels, nil
}

func (st *searchTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []vChan
	channels, err = st.chMgr.getVChannels(collID)
	if err != nil {
		err := st.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return st.chMgr.getVChannels(collID)
	}

	return channels, nil
}

func (st *searchTask) PreExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-PreExecute")
	defer sp.Finish()
	st.Base.MsgType = commonpb.MsgType_Search
	st.Base.SourceID = Params.ProxyCfg.ProxyID

	collectionName := st.query.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	if err := validateCollectionName(st.query.CollectionName); err != nil {
		return err
	}

	for _, tag := range st.query.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return err
		}
	}

	// check if collection was already loaded into query node
	showResp, err := st.qc.ShowCollections(st.ctx, &querypb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     st.Base.MsgID,
			Timestamp: st.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID: 0, // TODO(dragondriver)
	})
	if err != nil {
		return err
	}
	if showResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(showResp.Status.Reason)
	}
	log.Debug("QueryCoord show collections",
		zap.Any("collID", collID),
		zap.Any("collections", showResp.CollectionIDs),
	)
	collectionLoaded := false

	for _, collectionID := range showResp.CollectionIDs {
		if collectionID == collID {
			collectionLoaded = true
			break
		}
	}
	if !collectionLoaded {
		return fmt.Errorf("collection %v was not loaded into memory", collectionName)
	}

	// TODO(dragondriver): necessary to check if partition was loaded into query node?

	st.Base.MsgType = commonpb.MsgType_Search

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, collectionName)

	outputFields, err := translateOutputFields(st.query.OutputFields, schema, false)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", outputFields))
	st.query.OutputFields = outputFields

	if st.query.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, st.query.SearchParams)
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, st.query.SearchParams)
		if err != nil {
			return errors.New(TopKKey + " not found in search_params")
		}
		topK, err := strconv.Atoi(topKStr)
		if err != nil {
			return errors.New(TopKKey + " " + topKStr + " is not invalid")
		}

		metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, st.query.SearchParams)
		if err != nil {
			return errors.New(MetricTypeKey + " not found in search_params")
		}

		searchParams, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, st.query.SearchParams)
		if err != nil {
			return errors.New(SearchParamsKey + " not found in search_params")
		}
		roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, st.query.SearchParams)
		if err != nil {
			roundDecimalStr = "-1"
		}
		roundDecimal, err := strconv.Atoi(roundDecimalStr)
		if err != nil {
			return errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
		}

		if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
			return errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
		}

		queryInfo := &planpb.QueryInfo{
			Topk:         int64(topK),
			MetricType:   metricType,
			SearchParams: searchParams,
			RoundDecimal: int64(roundDecimal),
		}

		log.Debug("create query plan",
			//zap.Any("schema", schema),
			zap.String("dsl", st.query.Dsl),
			zap.String("anns field", annsField),
			zap.Any("query info", queryInfo))

		plan, err := createQueryPlan(schema, st.query.Dsl, annsField, queryInfo)
		if err != nil {
			log.Debug("failed to create query plan",
				zap.Error(err),
				//zap.Any("schema", schema),
				zap.String("dsl", st.query.Dsl),
				zap.String("anns field", annsField),
				zap.Any("query info", queryInfo))

			return fmt.Errorf("failed to create query plan: %v", err)
		}
		for _, name := range st.query.OutputFields {
			hitField := false
			for _, field := range schema.Fields {
				if field.Name == name {
					if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
						return errors.New("search doesn't support vector field as output_fields")
					}

					st.SearchRequest.OutputFieldsId = append(st.SearchRequest.OutputFieldsId, field.FieldID)
					plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
					hitField = true
					break
				}
			}
			if !hitField {
				errMsg := "Field " + name + " not exist"
				return errors.New(errMsg)
			}
		}

		st.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		st.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		log.Debug("Proxy::searchTask::PreExecute", zap.Any("plan.OutputFieldIds", plan.OutputFieldIds),
			zap.Any("plan", plan.String()))
	}
	travelTimestamp := st.query.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = st.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(st.BeginTs(), travelTimestamp) / 1000
		if durationSeconds > Params.ProxyCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
	}
	guaranteeTimestamp := st.query.GuaranteeTimestamp
	if guaranteeTimestamp == 0 {
		guaranteeTimestamp = st.BeginTs()
	}
	st.SearchRequest.TravelTimestamp = travelTimestamp
	st.SearchRequest.GuaranteeTimestamp = guaranteeTimestamp
	deadline, ok := st.TraceCtx().Deadline()
	if ok {
		st.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	st.SearchRequest.ResultChannelID = Params.ProxyCfg.SearchResultChannelNames[0]
	st.SearchRequest.DbID = 0 // todo
	st.SearchRequest.CollectionID = collID
	st.SearchRequest.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return err
	}

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range st.query.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					st.PartitionIDs = append(st.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	st.SearchRequest.Dsl = st.query.Dsl
	st.SearchRequest.PlaceholderGroup = st.query.PlaceholderGroup

	return nil
}

func (st *searchTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", st.ID()))
	defer tr.Elapse("done")

	var tsMsg msgstream.TsMsg = &msgstream.SearchMsg{
		SearchRequest: *st.SearchRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyCfg.ProxyID)},
			BeginTimestamp: st.Base.Timestamp,
			EndTimestamp:   st.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: st.Base.Timestamp,
		EndTs:   st.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg

	collectionName := st.query.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	stream, err := st.chMgr.getDQLStream(collID)
	if err != nil {
		err = st.chMgr.createDQLStream(collID)
		if err != nil {
			st.result = &milvuspb.SearchResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}
			return err
		}
		stream, err = st.chMgr.getDQLStream(collID)
		if err != nil {
			st.result = &milvuspb.SearchResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}
			return err
		}
	}
	tr.Record("get used message stream")

	err = stream.Produce(&msgPack)
	if err != nil {
		log.Debug("proxy", zap.String("send search request failed", err.Error()))
	}
	log.Debug("proxy sent one searchMsg",
		zap.Int64("collectionID", st.CollectionID),
		zap.Int64("msgID", tsMsg.ID()),
		zap.Int("length of search msg", len(msgPack.Msgs)),
		zap.Uint64("timeoutTs", st.SearchRequest.TimeoutTimestamp))
	tr.Record("send search msg to message stream")

	return err
}

func decodeSearchResults(searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
	tr := timerecord.NewTimeRecorder("decodeSearchResults")
	log.Debug("decodeSearchResults", zap.Any("lenOfSearchResults", len(searchResults)))

	results := make([]*schemapb.SearchResultData, 0)
	// necessary to parallel this?
	for i, partialSearchResult := range searchResults {
		log.Debug("decodeSearchResults", zap.Any("i", i), zap.Any("len(SlicedBob)", len(partialSearchResult.SlicedBlob)))
		if partialSearchResult.SlicedBlob == nil {
			continue
		}

		var partialResultData schemapb.SearchResultData
		err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
		log.Debug("decodeSearchResults, Unmarshal partitalSearchResult.SliceBlob", zap.Error(err))
		if err != nil {
			return nil, err
		}

		results = append(results, &partialResultData)
	}
	log.Debug("decodeSearchResults", zap.Any("lenOfResults", len(results)))
	tr.Elapse("done")

	return results, nil
}

func checkSearchResultData(data *schemapb.SearchResultData, nq int64, topk int64) error {
	if data.NumQueries != nq {
		return fmt.Errorf("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
	}
	if data.TopK != topk {
		return fmt.Errorf("search result's topk(%d) mis-match with %d", data.TopK, topk)
	}
	if len(data.Ids.GetIntId().Data) != (int)(nq*topk) {
		return fmt.Errorf("search result's id length %d invalid", len(data.Ids.GetIntId().Data))
	}
	if len(data.Scores) != (int)(nq*topk) {
		return fmt.Errorf("search result's score length %d invalid", len(data.Scores))
	}
	return nil
}

func selectSearchResultData(dataArray []*schemapb.SearchResultData, offsets []int64, topk int64, qi int64) int {
	sel := -1
	maxDistance := minFloat32
	for i, offset := range offsets { // query num, the number of ways to merge
		if offset >= topk {
			continue
		}
		idx := qi*topk + offset
		id := dataArray[i].Ids.GetIntId().Data[idx]
		if id != -1 {
			distance := dataArray[i].Scores[idx]
			if distance > maxDistance {
				sel = i
				maxDistance = distance
			}
		}
	}
	return sel
}

//func printSearchResultData(data *schemapb.SearchResultData, header string) {
//	size := len(data.Ids.GetIntId().Data)
//	if size != len(data.Scores) {
//		log.Error("SearchResultData length mis-match")
//	}
//	log.Debug("==== SearchResultData ====",
//		zap.String("header", header), zap.Int64("nq", data.NumQueries), zap.Int64("topk", data.TopK))
//	for i := 0; i < size; i++ {
//		log.Debug("", zap.Int("i", i), zap.Int64("id", data.Ids.GetIntId().Data[i]), zap.Float32("score", data.Scores[i]))
//	}
//}

func reduceSearchResultData(searchResultData []*schemapb.SearchResultData, nq int64, topk int64, metricType string) (*milvuspb.SearchResults, error) {

	tr := timerecord.NewTimeRecorder("reduceSearchResultData")
	defer func() {
		tr.Elapse("done")
	}()

	log.Debug("reduceSearchResultData", zap.Int("len(searchResultData)", len(searchResultData)),
		zap.Int64("nq", nq), zap.Int64("topk", topk), zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: 0,
		},
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
			Scores:     make([]float32, 0),
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: make([]int64, 0),
					},
				},
			},
			Topks: make([]int64, 0),
		},
	}

	for i, sData := range searchResultData {
		log.Debug("reduceSearchResultData",
			zap.Int("i", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Any("len(FieldsData)", len(sData.FieldsData)))
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			return ret, err
		}
		//printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
	}

	var skipDupCnt int64
	var realTopK int64 = -1
	for i := int64(0); i < nq; i++ {
		offsets := make([]int64, len(searchResultData))

		var idSet = make(map[int64]struct{})
		var j int64
		for j = 0; j < topk; {
			sel := selectSearchResultData(searchResultData, offsets, topk, i)
			if sel == -1 {
				break
			}
			idx := i*topk + offsets[sel]

			id := searchResultData[sel].Ids.GetIntId().Data[idx]
			score := searchResultData[sel].Scores[idx]
			// ignore invalid search result
			if id == -1 {
				continue
			}

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.Results.FieldsData, searchResultData[sel].FieldsData, idx)
				ret.Results.Ids.GetIntId().Data = append(ret.Results.Ids.GetIntId().Data, id)
				ret.Results.Scores = append(ret.Results.Scores, score)
				idSet[id] = struct{}{}
				j++
			} else {
				// skip entity with same id
				skipDupCnt++
			}
			offsets[sel]++
		}
		if realTopK != -1 && realTopK != j {
			log.Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
			// return nil, errors.New("the length (topk) between all result of query is different")
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)
	}
	log.Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))
	ret.Results.TopK = realTopK

	if !distance.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}

	return ret, nil
}

//func printSearchResult(partialSearchResult *internalpb.SearchResults) {
//	for i := 0; i < len(partialSearchResult.Hits); i++ {
//		testHits := milvuspb.Hits{}
//		err := proto.Unmarshal(partialSearchResult.Hits[i], &testHits)
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(testHits.IDs)
//		fmt.Println(testHits.Scores)
//	}
//}

func (st *searchTask) PostExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-PostExecute")
	defer sp.Finish()
	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()
	for {
		select {
		case <-st.TraceCtx().Done():
			log.Debug("Proxy searchTask PostExecute Loop exit caused by ctx.Done", zap.Int64("taskID", st.ID()))
			return fmt.Errorf("searchTask:wait to finish failed, timeout: %d", st.ID())
		case searchResults := <-st.resultBuf:
			// fmt.Println("searchResults: ", searchResults)
			filterSearchResults := make([]*internalpb.SearchResults, 0)
			var filterReason string
			for _, partialSearchResult := range searchResults {
				if partialSearchResult.Status.ErrorCode == commonpb.ErrorCode_Success {
					filterSearchResults = append(filterSearchResults, partialSearchResult)
					// For debugging, please don't delete.
					// printSearchResult(partialSearchResult)
				} else {
					filterReason += partialSearchResult.Status.Reason + "\n"
				}
			}

			log.Debug("Proxy Search PostExecute stage1",
				zap.Any("len(filterSearchResults)", len(filterSearchResults)))
			tr.Record("Proxy Search PostExecute stage1 done")
			if len(filterSearchResults) <= 0 {
				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    filterReason,
					},
				}
				return fmt.Errorf("no Available QueryNode result, filter reason %s: id %d", filterReason, st.ID())
			}

			validSearchResults, err := decodeSearchResults(filterSearchResults)
			if err != nil {
				return err
			}

			log.Debug("Proxy Search PostExecute stage2", zap.Any("len(validSearchResults)", len(validSearchResults)))
			if len(validSearchResults) <= 0 {
				filterReason += "empty search result\n"
				log.Debug("Proxy Search PostExecute stage2 failed", zap.Any("filterReason", filterReason))

				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
						Reason:    filterReason,
					},
					Results: &schemapb.SearchResultData{
						NumQueries: searchResults[0].NumQueries,
						Topks:      make([]int64, searchResults[0].NumQueries),
					},
				}
				return nil
			}

			st.result, err = reduceSearchResultData(validSearchResults, searchResults[0].NumQueries, searchResults[0].TopK, searchResults[0].MetricType)
			if err != nil {
				return err
			}

			schema, err := globalMetaCache.GetCollectionSchema(ctx, st.query.CollectionName)
			if err != nil {
				return err
			}
			if len(st.query.OutputFields) != 0 && len(st.result.Results.FieldsData) != 0 {
				for k, fieldName := range st.query.OutputFields {
					for _, field := range schema.Fields {
						if st.result.Results.FieldsData[k] != nil && field.Name == fieldName {
							st.result.Results.FieldsData[k].FieldName = field.Name
							st.result.Results.FieldsData[k].FieldId = field.FieldID
							st.result.Results.FieldsData[k].Type = field.DataType
						}
					}
				}
			}
			return nil
		}
	}
}

type queryTask struct {
	Condition
	*internalpb.RetrieveRequest
	ctx       context.Context
	resultBuf chan []*internalpb.RetrieveResults
	result    *milvuspb.QueryResults
	query     *milvuspb.QueryRequest
	chMgr     channelsMgr
	qc        types.QueryCoord
	ids       *schemapb.IDs
}

func (qt *queryTask) TraceCtx() context.Context {
	return qt.ctx
}

func (qt *queryTask) ID() UniqueID {
	return qt.Base.MsgID
}

func (qt *queryTask) SetID(uid UniqueID) {
	qt.Base.MsgID = uid
}

func (qt *queryTask) Name() string {
	return RetrieveTaskName
}

func (qt *queryTask) Type() commonpb.MsgType {
	return qt.Base.MsgType
}

func (qt *queryTask) BeginTs() Timestamp {
	return qt.Base.Timestamp
}

func (qt *queryTask) EndTs() Timestamp {
	return qt.Base.Timestamp
}

func (qt *queryTask) SetTs(ts Timestamp) {
	qt.Base.Timestamp = ts
}

func (qt *queryTask) OnEnqueue() error {
	qt.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}

func (qt *queryTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(qt.ctx, qt.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []pChan
	channels, err = qt.chMgr.getChannels(collID)
	if err != nil {
		err := qt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return qt.chMgr.getChannels(collID)
	}

	return channels, nil
}

func (qt *queryTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(qt.ctx, qt.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []vChan
	channels, err = qt.chMgr.getVChannels(collID)
	if err != nil {
		err := qt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return qt.chMgr.getVChannels(collID)
	}

	return channels, nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids []int64) string {
	idsStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids)), ", "), "[]")
	return fieldName + " in [ " + idsStr + " ]"
}

func (qt *queryTask) PreExecute(ctx context.Context) error {
	qt.Base.MsgType = commonpb.MsgType_Retrieve
	qt.Base.SourceID = Params.ProxyCfg.ProxyID

	collectionName := qt.query.CollectionName

	if err := validateCollectionName(qt.query.CollectionName); err != nil {
		log.Debug("Invalid collection name.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	log.Info("Validate collection name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	log.Info("Get collection id by name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	for _, tag := range qt.query.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Debug("Invalid partition name.", zap.Any("partitionName", tag),
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return err
		}
	}
	log.Info("Validate partition names.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	// check if collection was already loaded into query node
	showResp, err := qt.qc.ShowCollections(qt.ctx, &querypb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     qt.Base.MsgID,
			Timestamp: qt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID: 0, // TODO(dragondriver)
	})
	if err != nil {
		return err
	}
	if showResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(showResp.Status.Reason)
	}
	log.Debug("QueryCoord show collections",
		zap.Any("collections", showResp.CollectionIDs),
		zap.Any("collID", collectionID))

	collectionLoaded := false
	for _, collID := range showResp.CollectionIDs {
		if collectionID == collID {
			collectionLoaded = true
			break
		}
	}
	if !collectionLoaded {
		return fmt.Errorf("collection %v was not loaded into memory", collectionName)
	}

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, qt.query.CollectionName)

	if qt.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		qt.query.Expr = IDs2Expr(pkField, qt.ids.GetIntId().Data)
	}

	if qt.query.Expr == "" {
		errMsg := "Query expression is empty"
		return fmt.Errorf(errMsg)
	}

	plan, err := createExprPlan(schema, qt.query.Expr)
	if err != nil {
		return err
	}
	qt.query.OutputFields, err = translateOutputFields(qt.query.OutputFields, schema, true)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", qt.query.OutputFields))
	if len(qt.query.OutputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= 100 && field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
				qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
			}
		}
	} else {
		addPrimaryKey := false
		for _, reqField := range qt.query.OutputFields {
			findField := false
			for _, field := range schema.Fields {
				if reqField == field.Name {
					if field.IsPrimaryKey {
						addPrimaryKey = true
					}
					findField = true
					qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
					plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
				} else {
					if field.IsPrimaryKey && !addPrimaryKey {
						qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
						plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
						addPrimaryKey = true
					}
				}
			}
			if !findField {
				errMsg := "Field " + reqField + " not exist"
				return errors.New(errMsg)
			}
		}
	}
	log.Debug("translate output fields to field ids", zap.Any("OutputFieldsID", qt.OutputFieldsId))

	qt.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}
	travelTimestamp := qt.query.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = qt.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(qt.BeginTs(), travelTimestamp) / 1000
		if durationSeconds > Params.ProxyCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
	}
	guaranteeTimestamp := qt.query.GuaranteeTimestamp
	if guaranteeTimestamp == 0 {
		guaranteeTimestamp = qt.BeginTs()
	}
	qt.TravelTimestamp = travelTimestamp
	qt.GuaranteeTimestamp = guaranteeTimestamp
	deadline, ok := qt.TraceCtx().Deadline()
	if ok {
		qt.RetrieveRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	qt.ResultChannelID = Params.ProxyCfg.RetrieveResultChannelNames[0]
	qt.DbID = 0 // todo(yukun)

	qt.CollectionID = collectionID
	qt.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get partitions in collection.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	log.Info("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range qt.query.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Debug("Failed to compile partition name regex expression.", zap.Any("partitionName", partitionName),
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					qt.PartitionIDs = append(qt.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			// FIXME(wxyu): undefined behavior
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	log.Info("Query PreExecute done.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}

func (qt *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", qt.ID()))
	defer tr.Elapse("done")

	var tsMsg msgstream.TsMsg = &msgstream.RetrieveMsg{
		RetrieveRequest: *qt.RetrieveRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyCfg.ProxyID)},
			BeginTimestamp: qt.Base.Timestamp,
			EndTimestamp:   qt.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: qt.Base.Timestamp,
		EndTs:   qt.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg

	stream, err := qt.chMgr.getDQLStream(qt.CollectionID)
	if err != nil {
		err = qt.chMgr.createDQLStream(qt.CollectionID)
		if err != nil {
			qt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			qt.result.Status.Reason = err.Error()
			return err
		}
		stream, err = qt.chMgr.getDQLStream(qt.CollectionID)
		if err != nil {
			qt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			qt.result.Status.Reason = err.Error()
			return err
		}
	}
	tr.Record("get used message stream")

	err = stream.Produce(&msgPack)
	if err != nil {
		log.Debug("Failed to send retrieve request.",
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	}
	log.Debug("proxy sent one retrieveMsg",
		zap.Int64("collectionID", qt.CollectionID),
		zap.Int64("msgID", tsMsg.ID()),
		zap.Int("length of search msg", len(msgPack.Msgs)),
		zap.Uint64("timeoutTs", qt.RetrieveRequest.TimeoutTimestamp))
	tr.Record("send retrieve request to message stream")

	log.Info("Query Execute done.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return err
}

func mergeRetrieveResults(retrieveResults []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	var ret *milvuspb.QueryResults
	var skipDupCnt int64
	var idSet = make(map[int64]struct{})

	// merge results and remove duplicates
	for _, rr := range retrieveResults {
		// skip empty result, it will break merge result
		if rr == nil || rr.Ids == nil || rr.Ids.GetIntId() == nil || len(rr.Ids.GetIntId().Data) == 0 {
			continue
		}

		if ret == nil {
			ret = &milvuspb.QueryResults{
				FieldsData: make([]*schemapb.FieldData, len(rr.FieldsData)),
			}
		}

		if len(ret.FieldsData) != len(rr.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in proxy RetrieveResults, expect %d get %d", len(ret.FieldsData), len(rr.FieldsData))
		}

		for i, id := range rr.Ids.GetIntId().GetData() {
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.FieldsData, rr.FieldsData, int64(i))
				idSet[id] = struct{}{}
			} else {
				// primary keys duplicate
				skipDupCnt++
			}
		}
	}
	log.Debug("skip duplicated query result", zap.Int64("count", skipDupCnt))

	if ret == nil {
		ret = &milvuspb.QueryResults{
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return ret, nil
}

func (qt *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()
	select {
	case <-qt.TraceCtx().Done():
		log.Debug("proxy", zap.Int64("Query: wait to finish failed, timeout!, taskID:", qt.ID()))
		return fmt.Errorf("queryTask:wait to finish failed, timeout : %d", qt.ID())
	case retrieveResults := <-qt.resultBuf:
		filterRetrieveResults := make([]*internalpb.RetrieveResults, 0)
		var reason string
		for _, partialRetrieveResult := range retrieveResults {
			if partialRetrieveResult.Status.ErrorCode == commonpb.ErrorCode_Success {
				filterRetrieveResults = append(filterRetrieveResults, partialRetrieveResult)
			} else {
				reason += partialRetrieveResult.Status.Reason + "\n"
			}
		}

		if len(filterRetrieveResults) == 0 {
			qt.result = &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    reason,
				},
			}
			log.Debug("Query failed on all querynodes.",
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return errors.New(reason)
		}

		var err error
		qt.result, err = mergeRetrieveResults(filterRetrieveResults)
		if err != nil {
			return err
		}

		if len(qt.result.FieldsData) > 0 {
			qt.result.Status = &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}
		} else {
			log.Info("Query result is nil", zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			qt.result.Status = &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_EmptyCollection,
				Reason:    reason,
			}
			return nil
		}

		schema, err := globalMetaCache.GetCollectionSchema(ctx, qt.query.CollectionName)
		if err != nil {
			return err
		}
		for i := 0; i < len(qt.result.FieldsData); i++ {
			for _, field := range schema.Fields {
				if field.FieldID == qt.OutputFieldsId[i] {
					qt.result.FieldsData[i].FieldName = field.Name
					qt.result.FieldsData[i].FieldId = field.FieldID
					qt.result.FieldsData[i].Type = field.DataType
				}
			}
		}
	}

	log.Info("Query PostExecute done", zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}

type hasCollectionTask struct {
	Condition
	*milvuspb.HasCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hct *hasCollectionTask) TraceCtx() context.Context {
	return hct.ctx
}

func (hct *hasCollectionTask) ID() UniqueID {
	return hct.Base.MsgID
}

func (hct *hasCollectionTask) SetID(uid UniqueID) {
	hct.Base.MsgID = uid
}

func (hct *hasCollectionTask) Name() string {
	return HasCollectionTaskName
}

func (hct *hasCollectionTask) Type() commonpb.MsgType {
	return hct.Base.MsgType
}

func (hct *hasCollectionTask) BeginTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) EndTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) SetTs(ts Timestamp) {
	hct.Base.Timestamp = ts
}

func (hct *hasCollectionTask) OnEnqueue() error {
	hct.Base = &commonpb.MsgBase{}
	return nil
}

func (hct *hasCollectionTask) PreExecute(ctx context.Context) error {
	hct.Base.MsgType = commonpb.MsgType_HasCollection
	hct.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(hct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (hct *hasCollectionTask) Execute(ctx context.Context) error {
	var err error
	hct.result, err = hct.rootCoord.HasCollection(ctx, hct.HasCollectionRequest)
	if err != nil {
		return err
	}
	if hct.result == nil {
		return errors.New("has collection resp is nil")
	}
	if hct.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hct.result.Status.Reason)
	}
	return nil
}

func (hct *hasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeCollectionTask struct {
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.DescribeCollectionResponse
}

func (dct *describeCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *describeCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *describeCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *describeCollectionTask) Name() string {
	return DescribeCollectionTaskName
}

func (dct *describeCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *describeCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *describeCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *describeCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DescribeCollection
	dct.Base.SourceID = Params.ProxyCfg.ProxyID

	if dct.CollectionID != 0 && len(dct.CollectionName) == 0 {
		return nil
	}

	return validateCollectionName(dct.CollectionName)
}

func (dct *describeCollectionTask) Execute(ctx context.Context) error {
	var err error
	dct.result = &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
	}

	result, err := dct.rootCoord.DescribeCollection(ctx, dct.DescribeCollectionRequest)

	if err != nil {
		return err
	}

	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		dct.result.Status = result.Status
	} else {
		dct.result.Schema.Name = result.Schema.Name
		dct.result.Schema.Description = result.Schema.Description
		dct.result.Schema.AutoID = result.Schema.AutoID
		dct.result.CollectionID = result.CollectionID
		dct.result.VirtualChannelNames = result.VirtualChannelNames
		dct.result.PhysicalChannelNames = result.PhysicalChannelNames
		dct.result.CreatedTimestamp = result.CreatedTimestamp
		dct.result.CreatedUtcTimestamp = result.CreatedUtcTimestamp
		dct.result.ShardsNum = result.ShardsNum
		dct.result.ConsistencyLevel = result.ConsistencyLevel
		for _, field := range result.Schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID {
				dct.result.Schema.Fields = append(dct.result.Schema.Fields, &schemapb.FieldSchema{
					FieldID:      field.FieldID,
					Name:         field.Name,
					IsPrimaryKey: field.IsPrimaryKey,
					AutoID:       field.AutoID,
					Description:  field.Description,
					DataType:     field.DataType,
					TypeParams:   field.TypeParams,
					IndexParams:  field.IndexParams,
				})
			}
		}
	}
	return nil
}

func (dct *describeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type getCollectionStatisticsTask struct {
	Condition
	*milvuspb.GetCollectionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.GetCollectionStatisticsResponse
}

func (g *getCollectionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getCollectionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getCollectionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getCollectionStatisticsTask) Name() string {
	return GetCollectionStatisticsTaskName
}

func (g *getCollectionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getCollectionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getCollectionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *getCollectionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetCollectionStatistics
	g.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (g *getCollectionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	req := &datapb.GetCollectionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetCollectionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
	}

	result, _ := g.dataCoord.GetCollectionStatistics(ctx, req)
	if result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetCollectionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *getCollectionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type getPartitionStatisticsTask struct {
	Condition
	*milvuspb.GetPartitionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.GetPartitionStatisticsResponse
}

func (g *getPartitionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getPartitionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getPartitionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getPartitionStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *getPartitionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getPartitionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getPartitionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *getPartitionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (g *getPartitionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	partitionID, err := globalMetaCache.GetPartitionID(ctx, g.CollectionName, g.PartitionName)
	if err != nil {
		return err
	}
	req := &datapb.GetPartitionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetPartitionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
		PartitionID:  partitionID,
	}

	result, _ := g.dataCoord.GetPartitionStatistics(ctx, req)
	if result == nil {
		return errors.New("get partition statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *getPartitionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type showCollectionsTask struct {
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowCollectionsResponse
}

func (sct *showCollectionsTask) TraceCtx() context.Context {
	return sct.ctx
}

func (sct *showCollectionsTask) ID() UniqueID {
	return sct.Base.MsgID
}

func (sct *showCollectionsTask) SetID(uid UniqueID) {
	sct.Base.MsgID = uid
}

func (sct *showCollectionsTask) Name() string {
	return ShowCollectionTaskName
}

func (sct *showCollectionsTask) Type() commonpb.MsgType {
	return sct.Base.MsgType
}

func (sct *showCollectionsTask) BeginTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) EndTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) SetTs(ts Timestamp) {
	sct.Base.Timestamp = ts
}

func (sct *showCollectionsTask) OnEnqueue() error {
	sct.Base = &commonpb.MsgBase{}
	return nil
}

func (sct *showCollectionsTask) PreExecute(ctx context.Context) error {
	sct.Base.MsgType = commonpb.MsgType_ShowCollections
	sct.Base.SourceID = Params.ProxyCfg.ProxyID
	if sct.GetType() == milvuspb.ShowType_InMemory {
		for _, collectionName := range sct.CollectionNames {
			if err := validateCollectionName(collectionName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sct *showCollectionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := sct.rootCoord.ShowCollections(ctx, sct.ShowCollectionsRequest)

	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show collections")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if sct.GetType() == milvuspb.ShowType_InMemory {
		IDs2Names := make(map[UniqueID]string)
		for offset, collectionName := range respFromRootCoord.CollectionNames {
			collectionID := respFromRootCoord.CollectionIds[offset]
			IDs2Names[collectionID] = collectionName
		}
		collectionIDs := make([]UniqueID, 0)
		for _, collectionName := range sct.CollectionNames {
			collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			collectionIDs = append(collectionIDs, collectionID)
			IDs2Names[collectionID] = collectionName
		}

		resp, err := sct.queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     sct.Base.MsgID,
				Timestamp: sct.Base.Timestamp,
				SourceID:  sct.Base.SourceID,
			},
			//DbID: sct.ShowCollectionsRequest.DbName,
			CollectionIDs: collectionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show collections")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		sct.result = &milvuspb.ShowCollectionsResponse{
			Status:               resp.Status,
			CollectionNames:      make([]string, 0, len(resp.CollectionIDs)),
			CollectionIds:        make([]int64, 0, len(resp.CollectionIDs)),
			CreatedTimestamps:    make([]uint64, 0, len(resp.CollectionIDs)),
			CreatedUtcTimestamps: make([]uint64, 0, len(resp.CollectionIDs)),
			InMemoryPercentages:  make([]int64, 0, len(resp.CollectionIDs)),
		}

		for offset, id := range resp.CollectionIDs {
			collectionName, ok := IDs2Names[id]
			if !ok {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return errors.New("failed to show collections")
			}
			collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			sct.result.CollectionIds = append(sct.result.CollectionIds, id)
			sct.result.CollectionNames = append(sct.result.CollectionNames, collectionName)
			sct.result.CreatedTimestamps = append(sct.result.CreatedTimestamps, collectionInfo.createdTimestamp)
			sct.result.CreatedUtcTimestamps = append(sct.result.CreatedUtcTimestamps, collectionInfo.createdUtcTimestamp)
			sct.result.InMemoryPercentages = append(sct.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
		}
	} else {
		sct.result = respFromRootCoord
	}

	return nil
}

func (sct *showCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type createPartitionTask struct {
	Condition
	*milvuspb.CreatePartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (cpt *createPartitionTask) TraceCtx() context.Context {
	return cpt.ctx
}

func (cpt *createPartitionTask) ID() UniqueID {
	return cpt.Base.MsgID
}

func (cpt *createPartitionTask) SetID(uid UniqueID) {
	cpt.Base.MsgID = uid
}

func (cpt *createPartitionTask) Name() string {
	return CreatePartitionTaskName
}

func (cpt *createPartitionTask) Type() commonpb.MsgType {
	return cpt.Base.MsgType
}

func (cpt *createPartitionTask) BeginTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) EndTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) SetTs(ts Timestamp) {
	cpt.Base.Timestamp = ts
}

func (cpt *createPartitionTask) OnEnqueue() error {
	cpt.Base = &commonpb.MsgBase{}
	return nil
}

func (cpt *createPartitionTask) PreExecute(ctx context.Context) error {
	cpt.Base.MsgType = commonpb.MsgType_CreatePartition
	cpt.Base.SourceID = Params.ProxyCfg.ProxyID

	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *createPartitionTask) Execute(ctx context.Context) (err error) {
	cpt.result, err = cpt.rootCoord.CreatePartition(ctx, cpt.CreatePartitionRequest)
	if cpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cpt.result.Reason)
	}
	return err
}

func (cpt *createPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropPartitionTask struct {
	Condition
	*milvuspb.DropPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (dpt *dropPartitionTask) TraceCtx() context.Context {
	return dpt.ctx
}

func (dpt *dropPartitionTask) ID() UniqueID {
	return dpt.Base.MsgID
}

func (dpt *dropPartitionTask) SetID(uid UniqueID) {
	dpt.Base.MsgID = uid
}

func (dpt *dropPartitionTask) Name() string {
	return DropPartitionTaskName
}

func (dpt *dropPartitionTask) Type() commonpb.MsgType {
	return dpt.Base.MsgType
}

func (dpt *dropPartitionTask) BeginTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) EndTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) SetTs(ts Timestamp) {
	dpt.Base.Timestamp = ts
}

func (dpt *dropPartitionTask) OnEnqueue() error {
	dpt.Base = &commonpb.MsgBase{}
	return nil
}

func (dpt *dropPartitionTask) PreExecute(ctx context.Context) error {
	dpt.Base.MsgType = commonpb.MsgType_DropPartition
	dpt.Base.SourceID = Params.ProxyCfg.ProxyID

	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (dpt *dropPartitionTask) Execute(ctx context.Context) (err error) {
	dpt.result, err = dpt.rootCoord.DropPartition(ctx, dpt.DropPartitionRequest)
	if dpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dpt.result.Reason)
	}
	return err
}

func (dpt *dropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasPartitionTask struct {
	Condition
	*milvuspb.HasPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hpt *hasPartitionTask) TraceCtx() context.Context {
	return hpt.ctx
}

func (hpt *hasPartitionTask) ID() UniqueID {
	return hpt.Base.MsgID
}

func (hpt *hasPartitionTask) SetID(uid UniqueID) {
	hpt.Base.MsgID = uid
}

func (hpt *hasPartitionTask) Name() string {
	return HasPartitionTaskName
}

func (hpt *hasPartitionTask) Type() commonpb.MsgType {
	return hpt.Base.MsgType
}

func (hpt *hasPartitionTask) BeginTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) EndTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) SetTs(ts Timestamp) {
	hpt.Base.Timestamp = ts
}

func (hpt *hasPartitionTask) OnEnqueue() error {
	hpt.Base = &commonpb.MsgBase{}
	return nil
}

func (hpt *hasPartitionTask) PreExecute(ctx context.Context) error {
	hpt.Base.MsgType = commonpb.MsgType_HasPartition
	hpt.Base.SourceID = Params.ProxyCfg.ProxyID

	collName, partitionTag := hpt.CollectionName, hpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (hpt *hasPartitionTask) Execute(ctx context.Context) (err error) {
	hpt.result, err = hpt.rootCoord.HasPartition(ctx, hpt.HasPartitionRequest)
	if hpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if hpt.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hpt.result.Status.Reason)
	}
	return err
}

func (hpt *hasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showPartitionsTask struct {
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowPartitionsResponse
}

func (spt *showPartitionsTask) TraceCtx() context.Context {
	return spt.ctx
}

func (spt *showPartitionsTask) ID() UniqueID {
	return spt.Base.MsgID
}

func (spt *showPartitionsTask) SetID(uid UniqueID) {
	spt.Base.MsgID = uid
}

func (spt *showPartitionsTask) Name() string {
	return ShowPartitionTaskName
}

func (spt *showPartitionsTask) Type() commonpb.MsgType {
	return spt.Base.MsgType
}

func (spt *showPartitionsTask) BeginTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) EndTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) SetTs(ts Timestamp) {
	spt.Base.Timestamp = ts
}

func (spt *showPartitionsTask) OnEnqueue() error {
	spt.Base = &commonpb.MsgBase{}
	return nil
}

func (spt *showPartitionsTask) PreExecute(ctx context.Context) error {
	spt.Base.MsgType = commonpb.MsgType_ShowPartitions
	spt.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(spt.CollectionName); err != nil {
		return err
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		for _, partitionName := range spt.PartitionNames {
			if err := validatePartitionTag(partitionName, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (spt *showPartitionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := spt.rootCoord.ShowPartitions(ctx, spt.ShowPartitionsRequest)
	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show partitions")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		collectionName := spt.CollectionName
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		if err != nil {
			log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
				zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
			return err
		}
		IDs2Names := make(map[UniqueID]string)
		for offset, partitionName := range respFromRootCoord.PartitionNames {
			partitionID := respFromRootCoord.PartitionIDs[offset]
			IDs2Names[partitionID] = partitionName
		}
		partitionIDs := make([]UniqueID, 0)
		for _, partitionName := range spt.PartitionNames {
			partitionID, err := globalMetaCache.GetPartitionID(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			partitionIDs = append(partitionIDs, partitionID)
			IDs2Names[partitionID] = partitionName
		}
		resp, err := spt.queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     spt.Base.MsgID,
				Timestamp: spt.Base.Timestamp,
				SourceID:  spt.Base.SourceID,
			},
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show partitions")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		spt.result = &milvuspb.ShowPartitionsResponse{
			Status:               resp.Status,
			PartitionNames:       make([]string, 0, len(resp.PartitionIDs)),
			PartitionIDs:         make([]int64, 0, len(resp.PartitionIDs)),
			CreatedTimestamps:    make([]uint64, 0, len(resp.PartitionIDs)),
			CreatedUtcTimestamps: make([]uint64, 0, len(resp.PartitionIDs)),
			InMemoryPercentages:  make([]int64, 0, len(resp.PartitionIDs)),
		}

		for offset, id := range resp.PartitionIDs {
			partitionName, ok := IDs2Names[id]
			if !ok {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return errors.New("failed to show partitions")
			}
			partitionInfo, err := globalMetaCache.GetPartitionInfo(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			spt.result.PartitionIDs = append(spt.result.PartitionIDs, id)
			spt.result.PartitionNames = append(spt.result.PartitionNames, partitionName)
			spt.result.CreatedTimestamps = append(spt.result.CreatedTimestamps, partitionInfo.createdTimestamp)
			spt.result.CreatedUtcTimestamps = append(spt.result.CreatedUtcTimestamps, partitionInfo.createdUtcTimestamp)
			spt.result.InMemoryPercentages = append(spt.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
		}
	} else {
		spt.result = respFromRootCoord
	}

	return nil
}

func (spt *showPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type createIndexTask struct {
	Condition
	*milvuspb.CreateIndexRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (cit *createIndexTask) TraceCtx() context.Context {
	return cit.ctx
}

func (cit *createIndexTask) ID() UniqueID {
	return cit.Base.MsgID
}

func (cit *createIndexTask) SetID(uid UniqueID) {
	cit.Base.MsgID = uid
}

func (cit *createIndexTask) Name() string {
	return CreateIndexTaskName
}

func (cit *createIndexTask) Type() commonpb.MsgType {
	return cit.Base.MsgType
}

func (cit *createIndexTask) BeginTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *createIndexTask) EndTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *createIndexTask) SetTs(ts Timestamp) {
	cit.Base.Timestamp = ts
}

func (cit *createIndexTask) OnEnqueue() error {
	cit.Base = &commonpb.MsgBase{}
	return nil
}

func (cit *createIndexTask) PreExecute(ctx context.Context) error {
	cit.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.Base.SourceID = Params.ProxyCfg.ProxyID

	collName, fieldName := cit.CollectionName, cit.FieldName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validateFieldName(fieldName); err != nil {
		return err
	}

	// check index param, not accurate, only some static rules
	indexParams := make(map[string]string)
	for _, kv := range cit.CreateIndexRequest.ExtraParams {
		if kv.Key == "params" { // TODO(dragondriver): change `params` to const variable
			params, err := funcutil.ParseIndexParamsMap(kv.Value)
			if err != nil {
				log.Warn("Failed to parse index params",
					zap.String("params", kv.Value),
					zap.Error(err))
				continue
			}
			for k, v := range params {
				indexParams[k] = v
			}
		} else {
			indexParams[kv.Key] = kv.Value
		}
	}

	indexType, exist := indexParams["index_type"] // TODO(dragondriver): change `index_type` to const variable
	if !exist {
		indexType = indexparamcheck.IndexFaissIvfPQ // IVF_PQ is the default index type
	}

	adapter, err := indexparamcheck.GetConfAdapterMgrInstance().GetAdapter(indexType)
	if err != nil {
		log.Warn("Failed to get conf adapter", zap.String("index_type", indexType))
		return fmt.Errorf("invalid index type: %s", indexType)
	}

	ok := adapter.CheckTrain(indexParams)
	if !ok {
		log.Warn("Create index with invalid params", zap.Any("index_params", indexParams))
		return fmt.Errorf("invalid index params: %v", cit.CreateIndexRequest.ExtraParams)
	}

	return nil
}

func (cit *createIndexTask) Execute(ctx context.Context) error {
	var err error
	cit.result, err = cit.rootCoord.CreateIndex(ctx, cit.CreateIndexRequest)
	if cit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cit.result.Reason)
	}
	return err
}

func (cit *createIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeIndexTask struct {
	Condition
	*milvuspb.DescribeIndexRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.DescribeIndexResponse
}

func (dit *describeIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *describeIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *describeIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *describeIndexTask) Name() string {
	return DescribeIndexTaskName
}

func (dit *describeIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *describeIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *describeIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *describeIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *describeIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *describeIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DescribeIndex
	dit.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(dit.CollectionName); err != nil {
		return err
	}

	// only support default index name for now. @2021.02.18
	if dit.IndexName == "" {
		dit.IndexName = Params.ProxyCfg.DefaultIndexName
	}

	return nil
}

func (dit *describeIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.rootCoord.DescribeIndex(ctx, dit.DescribeIndexRequest)
	if dit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dit.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Status.Reason)
	}
	return err
}

func (dit *describeIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropIndexTask struct {
	Condition
	ctx context.Context
	*milvuspb.DropIndexRequest
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (dit *dropIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *dropIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *dropIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *dropIndexTask) Name() string {
	return DropIndexTaskName
}

func (dit *dropIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *dropIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *dropIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *dropIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *dropIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *dropIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DropIndex
	dit.Base.SourceID = Params.ProxyCfg.ProxyID

	collName, fieldName := dit.CollectionName, dit.FieldName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validateFieldName(fieldName); err != nil {
		return err
	}

	if dit.IndexName == "" {
		dit.IndexName = Params.ProxyCfg.DefaultIndexName
	}

	return nil
}

func (dit *dropIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.rootCoord.DropIndex(ctx, dit.DropIndexRequest)
	if dit.result == nil {
		return errors.New("drop index resp is nil")
	}
	if dit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Reason)
	}
	return err
}

func (dit *dropIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type getIndexBuildProgressTask struct {
	Condition
	*milvuspb.GetIndexBuildProgressRequest
	ctx        context.Context
	indexCoord types.IndexCoord
	rootCoord  types.RootCoord
	dataCoord  types.DataCoord
	result     *milvuspb.GetIndexBuildProgressResponse
}

func (gibpt *getIndexBuildProgressTask) TraceCtx() context.Context {
	return gibpt.ctx
}

func (gibpt *getIndexBuildProgressTask) ID() UniqueID {
	return gibpt.Base.MsgID
}

func (gibpt *getIndexBuildProgressTask) SetID(uid UniqueID) {
	gibpt.Base.MsgID = uid
}

func (gibpt *getIndexBuildProgressTask) Name() string {
	return GetIndexBuildProgressTaskName
}

func (gibpt *getIndexBuildProgressTask) Type() commonpb.MsgType {
	return gibpt.Base.MsgType
}

func (gibpt *getIndexBuildProgressTask) BeginTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *getIndexBuildProgressTask) EndTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *getIndexBuildProgressTask) SetTs(ts Timestamp) {
	gibpt.Base.Timestamp = ts
}

func (gibpt *getIndexBuildProgressTask) OnEnqueue() error {
	gibpt.Base = &commonpb.MsgBase{}
	return nil
}

func (gibpt *getIndexBuildProgressTask) PreExecute(ctx context.Context) error {
	gibpt.Base.MsgType = commonpb.MsgType_GetIndexBuildProgress
	gibpt.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(gibpt.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) Execute(ctx context.Context) error {
	collectionName := gibpt.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         gibpt.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	partitions, err := gibpt.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return err
	}

	if gibpt.IndexName == "" {
		gibpt.IndexName = Params.ProxyCfg.DefaultIndexName
	}

	describeIndexReq := milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         gibpt.DbName,
		CollectionName: gibpt.CollectionName,
		//		IndexName:      gibpt.IndexName,
	}

	indexDescriptionResp, err2 := gibpt.rootCoord.DescribeIndex(ctx, &describeIndexReq)
	if err2 != nil {
		return err2
	}

	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gibpt.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return fmt.Errorf("no index is created")
	}

	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gibpt.rootCoord.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	buildIndexMap := make(map[int64]int64)
	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := gibpt.rootCoord.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			return err
		}
		if segmentDesc.IndexID == matchIndexID {
			if segmentDesc.EnableIndex {
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, segmentDesc.BuildID)
				buildIndexMap[segmentID] = segmentDesc.BuildID
			}
		}
	}

	states, err := gibpt.indexCoord.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
			Status: states.Status,
		}
	}

	buildFinishMap := make(map[int64]bool)
	for _, state := range states.States {
		if state.State == commonpb.IndexState_Finished {
			buildFinishMap[state.IndexBuildID] = true
		}
	}

	infoResp, err := gibpt.dataCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		SegmentIDs: allSegmentIDs,
	})
	if err != nil {
		return err
	}

	total := int64(0)
	indexed := int64(0)

	for _, info := range infoResp.Infos {
		total += info.NumOfRows
		if buildFinishMap[buildIndexMap[info.ID]] {
			indexed += info.NumOfRows
		}
	}

	gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		TotalRows:   total,
		IndexedRows: indexed,
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) PostExecute(ctx context.Context) error {
	return nil
}

type getIndexStateTask struct {
	Condition
	*milvuspb.GetIndexStateRequest
	ctx        context.Context
	indexCoord types.IndexCoord
	rootCoord  types.RootCoord
	result     *milvuspb.GetIndexStateResponse
}

func (gist *getIndexStateTask) TraceCtx() context.Context {
	return gist.ctx
}

func (gist *getIndexStateTask) ID() UniqueID {
	return gist.Base.MsgID
}

func (gist *getIndexStateTask) SetID(uid UniqueID) {
	gist.Base.MsgID = uid
}

func (gist *getIndexStateTask) Name() string {
	return GetIndexStateTaskName
}

func (gist *getIndexStateTask) Type() commonpb.MsgType {
	return gist.Base.MsgType
}

func (gist *getIndexStateTask) BeginTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *getIndexStateTask) EndTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *getIndexStateTask) SetTs(ts Timestamp) {
	gist.Base.Timestamp = ts
}

func (gist *getIndexStateTask) OnEnqueue() error {
	gist.Base = &commonpb.MsgBase{}
	return nil
}

func (gist *getIndexStateTask) PreExecute(ctx context.Context) error {
	gist.Base.MsgType = commonpb.MsgType_GetIndexState
	gist.Base.SourceID = Params.ProxyCfg.ProxyID

	if err := validateCollectionName(gist.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gist *getIndexStateTask) Execute(ctx context.Context) error {
	collectionName := gist.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         gist.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	partitions, err := gist.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return err
	}

	if gist.IndexName == "" {
		gist.IndexName = Params.ProxyCfg.DefaultIndexName
	}

	describeIndexReq := milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         gist.DbName,
		CollectionName: gist.CollectionName,
		IndexName:      gist.IndexName,
	}

	indexDescriptionResp, err2 := gist.rootCoord.DescribeIndex(ctx, &describeIndexReq)
	if err2 != nil {
		return err2
	}

	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gist.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return fmt.Errorf("no index is created")
	}

	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gist.Base.MsgID,
				Timestamp: gist.Base.Timestamp,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gist.rootCoord.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     gist.Base.MsgID,
				Timestamp: gist.Base.Timestamp,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := gist.rootCoord.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			return err
		}
		if segmentDesc.IndexID == matchIndexID {
			if segmentDesc.EnableIndex {
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, segmentDesc.BuildID)
			}
		}
	}

	gist.result = &milvuspb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		State:      commonpb.IndexState_Finished,
		FailReason: "",
	}

	log.Debug("Proxy GetIndexState", zap.Int("IndexBuildIDs", len(getIndexStatesRequest.IndexBuildIDs)), zap.Error(err))

	if len(getIndexStatesRequest.IndexBuildIDs) == 0 {
		return nil
	}
	states, err := gist.indexCoord.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gist.result = &milvuspb.GetIndexStateResponse{
			Status: states.Status,
			State:  commonpb.IndexState_Failed,
		}
		return nil
	}

	for _, state := range states.States {
		if state.State != commonpb.IndexState_Finished {
			gist.result = &milvuspb.GetIndexStateResponse{
				Status:     states.Status,
				State:      state.State,
				FailReason: state.Reason,
			}
			return nil
		}
	}

	return nil
}

func (gist *getIndexStateTask) PostExecute(ctx context.Context) error {
	return nil
}

type flushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.FlushResponse
}

func (ft *flushTask) TraceCtx() context.Context {
	return ft.ctx
}

func (ft *flushTask) ID() UniqueID {
	return ft.Base.MsgID
}

func (ft *flushTask) SetID(uid UniqueID) {
	ft.Base.MsgID = uid
}

func (ft *flushTask) Name() string {
	return FlushTaskName
}

func (ft *flushTask) Type() commonpb.MsgType {
	return ft.Base.MsgType
}

func (ft *flushTask) BeginTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) EndTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) SetTs(ts Timestamp) {
	ft.Base.Timestamp = ts
}

func (ft *flushTask) OnEnqueue() error {
	ft.Base = &commonpb.MsgBase{}
	return nil
}

func (ft *flushTask) PreExecute(ctx context.Context) error {
	ft.Base.MsgType = commonpb.MsgType_Flush
	ft.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (ft *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	for _, collName := range ft.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     ft.Base.MsgID,
				Timestamp: ft.Base.Timestamp,
				SourceID:  ft.Base.SourceID,
			},
			DbID:         0,
			CollectionID: collID,
		}
		resp, err := ft.dataCoord.Flush(ctx, flushReq)
		if err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
	}
	ft.result = &milvuspb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:     "",
		CollSegIDs: coll2Segments,
	}
	return nil
}

func (ft *flushTask) PostExecute(ctx context.Context) error {
	return nil
}

type loadCollectionTask struct {
	Condition
	*milvuspb.LoadCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (lct *loadCollectionTask) TraceCtx() context.Context {
	return lct.ctx
}

func (lct *loadCollectionTask) ID() UniqueID {
	return lct.Base.MsgID
}

func (lct *loadCollectionTask) SetID(uid UniqueID) {
	lct.Base.MsgID = uid
}

func (lct *loadCollectionTask) Name() string {
	return LoadCollectionTaskName
}

func (lct *loadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) BeginTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) EndTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) SetTs(ts Timestamp) {
	lct.Base.Timestamp = ts
}

func (lct *loadCollectionTask) OnEnqueue() error {
	lct.Base = &commonpb.MsgBase{}
	return nil
}

func (lct *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PreExecute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	lct.Base.MsgType = commonpb.MsgType_LoadCollection
	lct.Base.SourceID = Params.ProxyCfg.ProxyID

	collName := lct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lct *loadCollectionTask) Execute(ctx context.Context) (err error) {
	log.Debug("loadCollectionTask Execute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)
	if err != nil {
		return err
	}
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lct.CollectionName)
	if err != nil {
		return err
	}

	request := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     lct.Base.MsgID,
			Timestamp: lct.Base.Timestamp,
			SourceID:  lct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		Schema:       collSchema,
	}
	log.Debug("send LoadCollectionRequest to query coordinator", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", request.Base.MsgID), zap.Int64("collectionID", request.CollectionID),
		zap.Any("schema", request.Schema))
	lct.result, err = lct.queryCoord.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query coordinator LoadCollection: %s", err)
	}
	return nil
}

func (lct *loadCollectionTask) PostExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PostExecute", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", lct.Base.MsgID))
	return nil
}

type releaseCollectionTask struct {
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
	chMgr      channelsMgr
}

func (rct *releaseCollectionTask) TraceCtx() context.Context {
	return rct.ctx
}

func (rct *releaseCollectionTask) ID() UniqueID {
	return rct.Base.MsgID
}

func (rct *releaseCollectionTask) SetID(uid UniqueID) {
	rct.Base.MsgID = uid
}

func (rct *releaseCollectionTask) Name() string {
	return ReleaseCollectionTaskName
}

func (rct *releaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) BeginTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) EndTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) SetTs(ts Timestamp) {
	rct.Base.Timestamp = ts
}

func (rct *releaseCollectionTask) OnEnqueue() error {
	rct.Base = &commonpb.MsgBase{}
	return nil
}

func (rct *releaseCollectionTask) PreExecute(ctx context.Context) error {
	rct.Base.MsgType = commonpb.MsgType_ReleaseCollection
	rct.Base.SourceID = Params.ProxyCfg.ProxyID

	collName := rct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rct *releaseCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, rct.CollectionName)
	if err != nil {
		return err
	}
	request := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleaseCollection,
			MsgID:     rct.Base.MsgID,
			Timestamp: rct.Base.Timestamp,
			SourceID:  rct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
	}

	rct.result, err = rct.queryCoord.ReleaseCollection(ctx, request)

	_ = rct.chMgr.removeDQLStream(collID)

	return err
}

func (rct *releaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type loadPartitionsTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (lpt *loadPartitionsTask) TraceCtx() context.Context {
	return lpt.ctx
}

func (lpt *loadPartitionsTask) ID() UniqueID {
	return lpt.Base.MsgID
}

func (lpt *loadPartitionsTask) SetID(uid UniqueID) {
	lpt.Base.MsgID = uid
}

func (lpt *loadPartitionsTask) Name() string {
	return LoadPartitionTaskName
}

func (lpt *loadPartitionsTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionsTask) BeginTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) EndTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) SetTs(ts Timestamp) {
	lpt.Base.Timestamp = ts
}

func (lpt *loadPartitionsTask) OnEnqueue() error {
	lpt.Base = &commonpb.MsgBase{}
	return nil
}

func (lpt *loadPartitionsTask) PreExecute(ctx context.Context) error {
	lpt.Base.MsgType = commonpb.MsgType_LoadPartitions
	lpt.Base.SourceID = Params.ProxyCfg.ProxyID

	collName := lpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lpt *loadPartitionsTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range lpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, lpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadPartitions,
			MsgID:     lpt.Base.MsgID,
			Timestamp: lpt.Base.Timestamp,
			SourceID:  lpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
		Schema:       collSchema,
	}
	lpt.result, err = lpt.queryCoord.LoadPartitions(ctx, request)
	return err
}

func (lpt *loadPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type releasePartitionsTask struct {
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (rpt *releasePartitionsTask) TraceCtx() context.Context {
	return rpt.ctx
}

func (rpt *releasePartitionsTask) ID() UniqueID {
	return rpt.Base.MsgID
}

func (rpt *releasePartitionsTask) SetID(uid UniqueID) {
	rpt.Base.MsgID = uid
}

func (rpt *releasePartitionsTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionsTask) Name() string {
	return ReleasePartitionTaskName
}

func (rpt *releasePartitionsTask) BeginTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) EndTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) SetTs(ts Timestamp) {
	rpt.Base.Timestamp = ts
}

func (rpt *releasePartitionsTask) OnEnqueue() error {
	rpt.Base = &commonpb.MsgBase{}
	return nil
}

func (rpt *releasePartitionsTask) PreExecute(ctx context.Context) error {
	rpt.Base.MsgType = commonpb.MsgType_ReleasePartitions
	rpt.Base.SourceID = Params.ProxyCfg.ProxyID

	collName := rpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rpt *releasePartitionsTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, rpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range rpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, rpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleasePartitions,
			MsgID:     rpt.Base.MsgID,
			Timestamp: rpt.Base.Timestamp,
			SourceID:  rpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	rpt.result, err = rpt.queryCoord.ReleasePartitions(ctx, request)
	return err
}

func (rpt *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type BaseDeleteTask = msgstream.DeleteMsg

type deleteTask struct {
	Condition
	BaseDeleteTask
	ctx       context.Context
	req       *milvuspb.DeleteRequest
	result    *milvuspb.MutationResult
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
	vChannels []vChan
	pChannels []pChan
}

func (dt *deleteTask) TraceCtx() context.Context {
	return dt.ctx
}

func (dt *deleteTask) ID() UniqueID {
	return dt.Base.MsgID
}

func (dt *deleteTask) SetID(uid UniqueID) {
	dt.Base.MsgID = uid
}

func (dt *deleteTask) Type() commonpb.MsgType {
	return dt.Base.MsgType
}

func (dt *deleteTask) Name() string {
	return deleteTaskName
}

func (dt *deleteTask) BeginTs() Timestamp {
	return dt.Base.Timestamp
}

func (dt *deleteTask) EndTs() Timestamp {
	return dt.Base.Timestamp
}

func (dt *deleteTask) SetTs(ts Timestamp) {
	dt.Base.Timestamp = ts
}

func (dt *deleteTask) OnEnqueue() error {
	dt.DeleteRequest.Base = &commonpb.MsgBase{}
	return nil
}

func (dt *deleteTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels, err := dt.getChannels()
	if err != nil {
		return ret, err
	}

	beginTs := dt.BeginTs()
	endTs := dt.EndTs()

	for _, channel := range channels {
		ret[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	return ret, nil
}

func (dt *deleteTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(dt.ctx, dt.CollectionName)
	if err != nil {
		return nil, err
	}
	var channels []pChan
	channels, err = dt.chMgr.getChannels(collID)
	if err != nil {
		err = dt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		channels, err = dt.chMgr.getChannels(collID)
	}
	return channels, err
}

func getPrimaryKeysFromExpr(schema *schemapb.CollectionSchema, expr string) (res []int64, err error) {
	if len(expr) == 0 {
		log.Warn("empty expr")
		return
	}

	plan, err := createExprPlan(schema, expr)
	if err != nil {
		return res, fmt.Errorf("failed to create expr plan, expr = %s", expr)
	}

	// delete request only support expr "id in [a, b]"
	termExpr, ok := plan.Node.(*planpb.PlanNode_Predicates).Predicates.Expr.(*planpb.Expr_TermExpr)
	if !ok {
		return res, fmt.Errorf("invalid plan node type")
	}

	for _, v := range termExpr.TermExpr.Values {
		res = append(res, v.GetInt64Val())
	}

	return res, nil
}

func (dt *deleteTask) PreExecute(ctx context.Context) error {
	dt.Base.MsgType = commonpb.MsgType_Delete
	dt.Base.SourceID = Params.ProxyCfg.ProxyID

	dt.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: dt.BeginTs(),
	}

	collName := dt.req.CollectionName
	if err := validateCollectionName(collName); err != nil {
		log.Error("Invalid collection name", zap.String("collectionName", collName))
		return err
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, collName)
	if err != nil {
		log.Debug("Failed to get collection id", zap.String("collectionName", collName))
		return err
	}
	dt.DeleteRequest.CollectionID = collID

	// If partitionName is not empty, partitionID will be set.
	if len(dt.req.PartitionName) > 0 {
		partName := dt.req.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Error("Invalid partition name", zap.String("partitionName", partName))
			return err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, collName, partName)
		if err != nil {
			log.Debug("Failed to get partition id", zap.String("collectionName", collName), zap.String("partitionName", partName))
			return err
		}
		dt.DeleteRequest.PartitionID = partID
	} else {
		dt.DeleteRequest.PartitionID = common.InvalidPartitionID
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dt.req.CollectionName)
	if err != nil {
		log.Error("Failed to get collection schema", zap.String("collectionName", dt.req.CollectionName))
		return err
	}

	primaryKeys, err := getPrimaryKeysFromExpr(schema, dt.req.Expr)
	if err != nil {
		log.Error("Failed to get primary keys from expr", zap.Error(err))
		return err
	}
	log.Debug("get primary keys from expr", zap.Int("len of primary keys", len(primaryKeys)))
	dt.DeleteRequest.PrimaryKeys = primaryKeys

	// set result
	dt.result.IDs.IdField = &schemapb.IDs_IntId{
		IntId: &schemapb.LongArray{
			Data: primaryKeys,
		},
	}
	dt.result.DeleteCnt = int64(len(primaryKeys))

	dt.HashPK(primaryKeys)

	rowNum := len(primaryKeys)
	dt.Timestamps = make([]uint64, rowNum)
	for index := range dt.Timestamps {
		dt.Timestamps[index] = dt.BeginTs()
	}

	return nil
}

func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	sp, ctx := trace.StartSpanFromContextWithOperationName(dt.ctx, "Proxy-Delete-Execute")
	defer sp.Finish()

	var tsMsg msgstream.TsMsg = &dt.BaseDeleteTask
	msgPack := msgstream.MsgPack{
		BeginTs: dt.BeginTs(),
		EndTs:   dt.EndTs(),
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg

	collID := dt.DeleteRequest.CollectionID
	stream, err := dt.chMgr.getDMLStream(collID)
	if err != nil {
		err = dt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			dt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			dt.result.Status.Reason = err.Error()
			return err
		}
		stream, err = dt.chMgr.getDMLStream(collID)
		if err != nil {
			dt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			dt.result.Status.Reason = err.Error()
			return err
		}
	}
	result := make(map[int32]msgstream.TsMsg)
	hashKeys := stream.ComputeProduceChannelIndexes(msgPack.Msgs)
	// For each msg, assign PK to different message buckets by hash value of PK.
	for i, request := range msgPack.Msgs {
		deleteRequest := request.(*msgstream.DeleteMsg)
		keys := hashKeys[i]
		collectionName := deleteRequest.CollectionName
		collectionID := deleteRequest.CollectionID
		partitionID := deleteRequest.PartitionID
		partitionName := deleteRequest.PartitionName
		proxyID := deleteRequest.Base.SourceID
		for index, key := range keys {
			ts := deleteRequest.Timestamps[index]
			pks := deleteRequest.PrimaryKeys[index]
			_, ok := result[key]
			if !ok {
				sliceRequest := internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     dt.Base.MsgID,
						Timestamp: ts,
						SourceID:  proxyID,
					},
					CollectionID:   collectionID,
					PartitionID:    partitionID,
					CollectionName: collectionName,
					PartitionName:  partitionName,
				}
				deleteMsg := &msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						Ctx: ctx,
					},
					DeleteRequest: sliceRequest,
				}
				result[key] = deleteMsg
			}
			curMsg := result[key].(*msgstream.DeleteMsg)
			curMsg.HashValues = append(curMsg.HashValues, deleteRequest.HashValues[index])
			curMsg.Timestamps = append(curMsg.Timestamps, ts)
			curMsg.PrimaryKeys = append(curMsg.PrimaryKeys, pks)
		}
	}

	newPack := &msgstream.MsgPack{
		BeginTs:        msgPack.BeginTs,
		EndTs:          msgPack.EndTs,
		StartPositions: msgPack.StartPositions,
		EndPositions:   msgPack.EndPositions,
		Msgs:           make([]msgstream.TsMsg, 0),
	}

	for _, msg := range result {
		if msg != nil {
			newPack.Msgs = append(newPack.Msgs, msg)
		}
	}

	err = stream.Produce(newPack)
	if err != nil {
		dt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		dt.result.Status.Reason = err.Error()
		return err
	}
	return nil
}

func (dt *deleteTask) PostExecute(ctx context.Context) error {
	return nil
}

func (dt *deleteTask) HashPK(pks []int64) {
	if len(dt.HashValues) != 0 {
		log.Warn("the hashvalues passed through client is not supported now, and will be overwritten")
	}
	dt.HashValues = make([]uint32, 0, len(pks))
	for _, pk := range pks {
		hash, _ := typeutil.Hash32Int64(pk)
		dt.HashValues = append(dt.HashValues, hash)
	}
}

// CreateAliasTask contains task information of CreateAlias
type CreateAliasTask struct {
	Condition
	*milvuspb.CreateAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

// TraceCtx returns the trace context of the task.
func (c *CreateAliasTask) TraceCtx() context.Context {
	return c.ctx
}

// ID return the id of the task
func (c *CreateAliasTask) ID() UniqueID {
	return c.Base.MsgID
}

// SetID sets the id of the task
func (c *CreateAliasTask) SetID(uid UniqueID) {
	c.Base.MsgID = uid
}

// Name returns the name of the task
func (c *CreateAliasTask) Name() string {
	return CreateAliasTaskName
}

// Type returns the type of the task
func (c *CreateAliasTask) Type() commonpb.MsgType {
	return c.Base.MsgType
}

// BeginTs returns the ts
func (c *CreateAliasTask) BeginTs() Timestamp {
	return c.Base.Timestamp
}

// EndTs returns the ts
func (c *CreateAliasTask) EndTs() Timestamp {
	return c.Base.Timestamp
}

// SetTs sets the ts
func (c *CreateAliasTask) SetTs(ts Timestamp) {
	c.Base.Timestamp = ts
}

// OnEnqueue defines the behavior task enqueued
func (c *CreateAliasTask) OnEnqueue() error {
	c.Base = &commonpb.MsgBase{}
	return nil
}

// PreExecute defines the action before task execution
func (c *CreateAliasTask) PreExecute(ctx context.Context) error {
	c.Base.MsgType = commonpb.MsgType_CreateAlias
	c.Base.SourceID = Params.ProxyCfg.ProxyID

	collAlias := c.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := c.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}
	return nil
}

// Execute defines the actual execution of create alias
func (c *CreateAliasTask) Execute(ctx context.Context) error {
	var err error
	c.result, err = c.rootCoord.CreateAlias(ctx, c.CreateAliasRequest)
	return err
}

// PostExecute defines the post execution, do nothing for create alias
func (c *CreateAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DropAliasTask is the task to drop alias
type DropAliasTask struct {
	Condition
	*milvuspb.DropAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (d *DropAliasTask) TraceCtx() context.Context {
	return d.ctx
}

func (d *DropAliasTask) ID() UniqueID {
	return d.Base.MsgID
}

func (d *DropAliasTask) SetID(uid UniqueID) {
	d.Base.MsgID = uid
}

func (d *DropAliasTask) Name() string {
	return DropAliasTaskName
}

func (d *DropAliasTask) Type() commonpb.MsgType {
	return d.Base.MsgType
}

func (d *DropAliasTask) BeginTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) EndTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) SetTs(ts Timestamp) {
	d.Base.Timestamp = ts
}

func (d *DropAliasTask) OnEnqueue() error {
	d.Base = &commonpb.MsgBase{}
	return nil
}

func (d *DropAliasTask) PreExecute(ctx context.Context) error {
	d.Base.MsgType = commonpb.MsgType_DropAlias
	d.Base.SourceID = Params.ProxyCfg.ProxyID
	collAlias := d.Alias
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}
	return nil
}

func (d *DropAliasTask) Execute(ctx context.Context) error {
	var err error
	d.result, err = d.rootCoord.DropAlias(ctx, d.DropAliasRequest)
	return err
}

func (d *DropAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// AlterAliasTask is the task to alter alias
type AlterAliasTask struct {
	Condition
	*milvuspb.AlterAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (a *AlterAliasTask) TraceCtx() context.Context {
	return a.ctx
}

func (a *AlterAliasTask) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterAliasTask) SetID(uid UniqueID) {
	a.Base.MsgID = uid
}

func (a *AlterAliasTask) Name() string {
	return AlterAliasTaskName
}

func (a *AlterAliasTask) Type() commonpb.MsgType {
	return a.Base.MsgType
}

func (a *AlterAliasTask) BeginTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) EndTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) SetTs(ts Timestamp) {
	a.Base.Timestamp = ts
}

func (a *AlterAliasTask) OnEnqueue() error {
	a.Base = &commonpb.MsgBase{}
	return nil
}

func (a *AlterAliasTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_AlterAlias
	a.Base.SourceID = Params.ProxyCfg.ProxyID

	collAlias := a.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := a.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (a *AlterAliasTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.AlterAlias(ctx, a.AlterAliasRequest)
	return err
}

func (a *AlterAliasTask) PostExecute(ctx context.Context) error {
	return nil
}
