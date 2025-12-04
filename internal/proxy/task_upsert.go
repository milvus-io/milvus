// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type upsertTask struct {
	baseTask
	Condition

	upsertMsg *msgstream.UpsertMsg
	req       *milvuspb.UpsertRequest
	baseMsg   msgstream.BaseMsg

	ctx context.Context

	timestamps       []uint64
	rowIDs           []int64
	result           *milvuspb.MutationResult
	idAllocator      *allocator.IDAllocator
	collectionID     UniqueID
	chMgr            channelsMgr
	chTicker         channelsTimeTicker
	vChannels        []vChan
	pChannels        []pChan
	schema           *schemaInfo
	partitionKeyMode bool
	partitionKeys    *schemapb.FieldData
	// automatic generate pk as new pk wehen autoID == true
	// delete task need use the oldIDs
	oldIDs          *schemapb.IDs
	schemaTimestamp uint64

	// write after read, generate write part by queryPreExecute
	node types.ProxyComponent

	deletePKs       *schemapb.IDs
	insertFieldData []*schemapb.FieldData

	storageCost segcore.StorageCost
}

// TraceCtx returns upsertTask context
func (it *upsertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *upsertTask) ID() UniqueID {
	return it.req.Base.MsgID
}

func (it *upsertTask) SetID(uid UniqueID) {
	it.req.Base.MsgID = uid
}

func (it *upsertTask) Name() string {
	return UpsertTaskName
}

func (it *upsertTask) Type() commonpb.MsgType {
	return it.req.Base.MsgType
}

func (it *upsertTask) BeginTs() Timestamp {
	return it.baseMsg.BeginTimestamp
}

func (it *upsertTask) SetTs(ts Timestamp) {
	it.baseMsg.BeginTimestamp = ts
	it.baseMsg.EndTimestamp = ts
}

func (it *upsertTask) EndTs() Timestamp {
	return it.baseMsg.EndTimestamp
}

func (it *upsertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels := it.getChannels()

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

func (it *upsertTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.req.GetDbName(), it.req.CollectionName)
	if err != nil {
		return err
	}
	channels, err := it.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	it.pChannels = channels
	return nil
}

func (it *upsertTask) getChannels() []pChan {
	return it.pChannels
}

func (it *upsertTask) OnEnqueue() error {
	if it.req.Base == nil {
		it.req.Base = commonpbutil.NewMsgBase()
	}
	it.req.Base.MsgType = commonpb.MsgType_Upsert
	it.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func retrieveByPKs(ctx context.Context, t *upsertTask, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	log := log.Ctx(ctx).With(zap.String("collectionName", t.req.GetCollectionName()))
	var err error
	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Retrieve,
			Timestamp: t.BeginTs(),
		},
		DbName:                t.req.GetDbName(),
		CollectionName:        t.req.GetCollectionName(),
		ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
		NotReturnAllMeta:      false,
		OutputFields:          []string{"*"},
		UseDefaultConsistency: false,
		GuaranteeTimestamp:    t.BeginTs(),
		Namespace:             t.req.Namespace,
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(t.schema.CollectionSchema)
	if err != nil {
		return nil, segcore.StorageCost{}, err
	}

	var partitionIDs []int64
	if t.partitionKeyMode {
		// multi entities with same pk and diff partition keys may be hashed to multi physical partitions
		// if deleteMsg.partitionID = common.InvalidPartition,
		// all segments with this pk under the collection will have the delete record
		partitionIDs = []int64{common.AllPartitionsID}
		queryReq.PartitionNames = []string{}
	} else {
		// partition name could be defaultPartitionName or name specified by sdk
		partName := t.upsertMsg.DeleteMsg.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Warn("Invalid partition name", zap.String("partitionName", partName), zap.Error(err))
			return nil, segcore.StorageCost{}, err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, t.req.GetDbName(), t.req.GetCollectionName(), partName)
		if err != nil {
			log.Warn("Failed to get partition id", zap.String("partitionName", partName), zap.Error(err))
			return nil, segcore.StorageCost{}, err
		}
		partitionIDs = []int64{partID}
		queryReq.PartitionNames = []string{partName}
	}

	plan := planparserv2.CreateRequeryPlan(pkField, ids)
	plan.Namespace = t.req.Namespace
	qt := &queryTask{
		ctx:       t.ctx,
		Condition: NewTaskCondition(t.ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:            paramtable.GetNodeID(),
			PartitionIDs:     partitionIDs,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		},
		request:  queryReq,
		plan:     plan,
		mixCoord: t.node.(*Proxy).mixCoord,
		lb:       t.node.(*Proxy).lbPolicy,
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-retrieveByPKs")
	defer func() {
		sp.End()
	}()
	queryResult, storageCost, err := t.node.(*Proxy).query(ctx, qt, sp)
	if err := merr.CheckRPCCall(queryResult.GetStatus(), err); err != nil {
		return nil, storageCost, err
	}
	return queryResult, storageCost, err
}

func (it *upsertTask) queryPreExecute(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("collectionName", it.req.CollectionName))

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(it.schema.CollectionSchema)
	if err != nil {
		log.Warn("get primary field schema failed", zap.Error(err))
		return err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(it.req.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		log.Error("get primary field data failed", zap.Error(err))
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("must assign pk when upsert, primary field: %v", primaryFieldSchema.Name))
	}

	upsertIDs, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.Error(err))
		return err
	}

	upsertIDSize := typeutil.GetSizeOfIDs(upsertIDs)
	if upsertIDSize == 0 {
		it.deletePKs = &schemapb.IDs{}
		it.insertFieldData = it.req.GetFieldsData()
		log.Info("old records not found, just do insert")
		return nil
	}

	tr := timerecord.NewTimeRecorder("Proxy-Upsert-retrieveByPKs")
	// retrieve by primary key to get original field data
	resp, storageCost, err := retrieveByPKs(ctx, it, upsertIDs, []string{"*"})
	if err != nil {
		log.Info("retrieve by primary key failed", zap.Error(err))
		return err
	}
	it.storageCost = storageCost
	if len(resp.GetFieldsData()) == 0 {
		return merr.WrapErrParameterInvalidMsg("retrieve by primary key failed, no data found")
	}

	existFieldData := resp.GetFieldsData()
	pkFieldData, err := typeutil.GetPrimaryFieldData(existFieldData, primaryFieldSchema)
	if err != nil {
		log.Error("get primary field data failed", zap.Error(err))
		return err
	}
	existIDs, err := parsePrimaryFieldData2IDs(pkFieldData)
	if err != nil {
		log.Info("parse primary field data to ids failed", zap.Error(err))
		return err
	}
	log.Info("retrieveByPKs cost",
		zap.Int("resultNum", typeutil.GetSizeOfIDs(existIDs)),
		zap.Int64("latency", tr.ElapseSpan().Milliseconds()))

	// set field id for user passed field data, prepare for merge logic
	upsertFieldData := it.upsertMsg.InsertMsg.GetFieldsData()
	if len(upsertFieldData) == 0 {
		return merr.WrapErrParameterInvalidMsg("upsert field data is empty")
	}
	for _, fieldData := range upsertFieldData {
		fieldName := fieldData.GetFieldName()
		if fieldData.GetIsDynamic() {
			fieldName = "$meta"
		}
		fieldSchema, err := it.schema.schemaHelper.GetFieldFromName(fieldName)
		if err != nil {
			log.Info("get field schema failed", zap.Error(err))
			return err
		}
		fieldData.FieldId = fieldSchema.GetFieldID()
		fieldData.FieldName = fieldName

		// compatible with different nullable data format from sdk
		if len(fieldData.GetValidData()) != 0 {
			err := FillWithNullValue(fieldData, fieldSchema, int(it.upsertMsg.InsertMsg.NRows()))
			if err != nil {
				log.Info("unify null field data format failed", zap.Error(err))
				return err
			}
		}
	}

	// Two nullable data formats are supported:
	//
	//	COMPRESSED FORMAT (SDK format, before validateUtil.fillWithValue processing):
	//		Logical data: [1, null, 2]
	//		Storage: Data=[1, 2] + ValidData=[true, false, true]
	//		- Data array contains only non-null values (compressed)
	//		- ValidData array tracks null positions for all rows
	//
	//	FULL FORMAT (Milvus internal format, after validateUtil.fillWithValue processing):
	//		Logical data: [1, null, 2]
	//		Storage: Data=[1, 0, 2] + ValidData=[true, false, true]
	//		- Data array contains values for all rows (nulls filled with zero/default)
	//		- ValidData array still tracks null positions
	//
	// Note: we will unify the nullable format to FULL FORMAT before executing the merge logic
	insertIdxInUpsert := make([]int, 0)
	updateIdxInUpsert := make([]int, 0)
	// 1. split upsert data into insert and update by query result
	idsChecker, err := typeutil.NewIDsChecker(existIDs)
	if err != nil {
		log.Info("create primary key checker failed", zap.Error(err))
		return err
	}
	for upsertIdx := 0; upsertIdx < upsertIDSize; upsertIdx++ {
		exist, err := idsChecker.Contains(upsertIDs, upsertIdx)
		if err != nil {
			log.Info("check primary key exist in query result failed", zap.Error(err))
			return err
		}
		if exist {
			updateIdxInUpsert = append(updateIdxInUpsert, upsertIdx)
		} else {
			insertIdxInUpsert = append(insertIdxInUpsert, upsertIdx)
		}
	}

	// 2. merge field data on update semantic
	it.deletePKs = &schemapb.IDs{}
	it.insertFieldData = typeutil.PrepareResultFieldData(existFieldData, int64(upsertIDSize))

	if len(updateIdxInUpsert) > 0 {
		// Note: For fields containing default values, default values need to be set according to valid data during insertion,
		// but query results fields do not set valid data when returning default value fields,
		// therefore valid data needs to be manually set to true
		for _, fieldData := range existFieldData {
			fieldSchema, err := it.schema.schemaHelper.GetFieldFromName(fieldData.GetFieldName())
			if err != nil {
				log.Info("get field schema failed", zap.Error(err))
				return err
			}

			if fieldSchema.GetDefaultValue() != nil {
				fieldData.ValidData = make([]bool, upsertIDSize)
				for i := range fieldData.ValidData {
					fieldData.ValidData[i] = true
				}
			}
		}

		// Build mapping from existing primary keys to their positions in query result
		// This ensures we can correctly locate data even if query results are not in the same order as request
		existIDsLen := typeutil.GetSizeOfIDs(existIDs)
		existPKToIndex := make(map[interface{}]int, existIDsLen)
		for j := 0; j < existIDsLen; j++ {
			pk := typeutil.GetPK(existIDs, int64(j))
			existPKToIndex[pk] = j
		}

		baseIdx := 0
		for _, idx := range updateIdxInUpsert {
			typeutil.AppendIDs(it.deletePKs, upsertIDs, idx)
			oldPK := typeutil.GetPK(upsertIDs, int64(idx))
			existIndex, ok := existPKToIndex[oldPK]
			if !ok {
				return merr.WrapErrParameterInvalidMsg("primary key not found in exist data mapping")
			}
			typeutil.AppendFieldData(it.insertFieldData, existFieldData, int64(existIndex))
			err := typeutil.UpdateFieldData(it.insertFieldData, upsertFieldData, int64(baseIdx), int64(idx))
			baseIdx += 1
			if err != nil {
				log.Info("update field data failed", zap.Error(err))
				return err
			}
		}
	}

	// 3. merge field data on insert semantic
	if len(insertIdxInUpsert) > 0 {
		// if necessary field is not exist in upsert request, return error
		lackOfFieldErr := LackOfFieldsDataBySchema(it.schema.CollectionSchema, it.upsertMsg.InsertMsg.GetFieldsData(), false, true)
		if lackOfFieldErr != nil {
			log.Info("check fields data by schema failed", zap.Error(lackOfFieldErr))
			return lackOfFieldErr
		}

		// if the nullable field has not passed in upsert request, which means the len(upsertFieldData) < len(it.insertFieldData)
		// we need to generate the nullable field data before append as insert
		insertWithNullField := make([]*schemapb.FieldData, 0)
		upsertFieldMap := lo.SliceToMap(it.upsertMsg.InsertMsg.GetFieldsData(), func(field *schemapb.FieldData) (string, *schemapb.FieldData) {
			return field.GetFieldName(), field
		})
		for _, fieldSchema := range it.schema.CollectionSchema.Fields {
			if fieldData, ok := upsertFieldMap[fieldSchema.Name]; !ok {
				if fieldSchema.GetNullable() || fieldSchema.GetDefaultValue() != nil {
					fieldData, err := GenNullableFieldData(fieldSchema, upsertIDSize)
					if err != nil {
						log.Info("generate nullable field data failed", zap.Error(err))
						return err
					}
					insertWithNullField = append(insertWithNullField, fieldData)
				}
			} else {
				insertWithNullField = append(insertWithNullField, fieldData)
			}
		}
		for _, idx := range insertIdxInUpsert {
			typeutil.AppendFieldData(it.insertFieldData, insertWithNullField, int64(idx))
		}
	}

	for _, fieldData := range it.insertFieldData {
		if len(fieldData.GetValidData()) > 0 {
			err := ToCompressedFormatNullable(fieldData)
			if err != nil {
				log.Info("convert to compressed format nullable failed", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// ToCompressedFormatNullable converts the field data from full format nullable to compressed format nullable
func ToCompressedFormatNullable(field *schemapb.FieldData) error {
	if getValidNumber(field.GetValidData()) == len(field.GetValidData()) {
		return nil
	}
	switch field.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch sd := field.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.BoolData.Data = make([]bool, 0)
			} else {
				ret := make([]bool, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.BoolData.Data[i])
					}
				}
				sd.BoolData.Data = ret
			}

		case *schemapb.ScalarField_IntData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.IntData.Data = make([]int32, 0)
			} else {
				ret := make([]int32, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.IntData.Data[i])
					}
				}
				sd.IntData.Data = ret
			}

		case *schemapb.ScalarField_LongData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.LongData.Data = make([]int64, 0)
			} else {
				ret := make([]int64, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.LongData.Data[i])
					}
				}
				sd.LongData.Data = ret
			}

		case *schemapb.ScalarField_FloatData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.FloatData.Data = make([]float32, 0)
			} else {
				ret := make([]float32, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.FloatData.Data[i])
					}
				}
				sd.FloatData.Data = ret
			}

		case *schemapb.ScalarField_DoubleData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.DoubleData.Data = make([]float64, 0)
			} else {
				ret := make([]float64, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.DoubleData.Data[i])
					}
				}
				sd.DoubleData.Data = ret
			}

		case *schemapb.ScalarField_StringData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.StringData.Data = make([]string, 0)
			} else {
				ret := make([]string, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.StringData.Data[i])
					}
				}
				sd.StringData.Data = ret
			}

		case *schemapb.ScalarField_JsonData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.JsonData.Data = make([][]byte, 0)
			} else {
				ret := make([][]byte, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.JsonData.Data[i])
					}
				}
				sd.JsonData.Data = ret
			}

		case *schemapb.ScalarField_ArrayData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.ArrayData.Data = make([]*schemapb.ScalarField, 0)
			} else {
				ret := make([]*schemapb.ScalarField, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.ArrayData.Data[i])
					}
				}
				sd.ArrayData.Data = ret
			}
		case *schemapb.ScalarField_TimestamptzData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.TimestamptzData.Data = make([]int64, 0)
			} else {
				ret := make([]int64, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.TimestamptzData.Data[i])
					}
				}
				sd.TimestamptzData.Data = ret
			}

		case *schemapb.ScalarField_GeometryWktData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.GeometryWktData.Data = make([]string, 0)
			} else {
				ret := make([]string, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.GeometryWktData.Data[i])
					}
				}
				sd.GeometryWktData.Data = ret
			}

		case *schemapb.ScalarField_GeometryData:
			validRowNum := getValidNumber(field.GetValidData())
			if validRowNum == 0 {
				sd.GeometryData.Data = make([][]byte, 0)
			} else {
				ret := make([][]byte, 0, validRowNum)
				for i, valid := range field.GetValidData() {
					if valid {
						ret = append(ret, sd.GeometryData.Data[i])
					}
				}
				sd.GeometryData.Data = ret
			}

		default:
			return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("undefined data type:%s", field.Type.String()))
		}

	default:
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("undefined data type:%s", field.Type.String()))
	}

	return nil
}

// GenNullableFieldData generates nullable field data in FULL FORMAT
func GenNullableFieldData(field *schemapb.FieldSchema, upsertIDSize int) (*schemapb.FieldData, error) {
	switch field.DataType {
	case schemapb.DataType_Bool:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: make([]bool, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Int32:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: make([]int32, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Int64:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: make([]int64, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Float:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: make([]float32, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Double:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: make([]float64, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_VarChar:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: make([]string, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_JSON:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: make([][]byte, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Array:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data: make([]*schemapb.ScalarField, upsertIDSize),
						},
					},
				},
			},
		}, nil

	case schemapb.DataType_Timestamptz:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimestamptzData{
						TimestamptzData: &schemapb.TimestamptzArray{
							Data: make([]int64, upsertIDSize),
						},
					},
				},
			},
		}, nil

	// the intput data of geometry field is in wkt format
	case schemapb.DataType_Geometry:
		return &schemapb.FieldData{
			FieldId:   field.FieldID,
			FieldName: field.Name,
			Type:      field.DataType,
			IsDynamic: field.IsDynamic,
			ValidData: make([]bool, upsertIDSize),
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: make([]string, upsertIDSize),
						},
					},
				},
			},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("undefined scalar data type:%s", field.DataType.String()))
	}
}

func (it *upsertTask) insertPreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-insertPreExecute")
	defer sp.End()
	collectionName := it.upsertMsg.InsertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Ctx(ctx).Error("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	bm25Fields := typeutil.NewSet[string](GetBM25FunctionOutputFields(it.schema.CollectionSchema)...)
	if it.req.PartialUpdate {
		// remove the old bm25 fields
		ret := make([]*schemapb.FieldData, 0)
		for _, fieldData := range it.upsertMsg.InsertMsg.GetFieldsData() {
			if bm25Fields.Contain(fieldData.GetFieldName()) {
				continue
			}
			ret = append(ret, fieldData)
		}
		it.upsertMsg.InsertMsg.FieldsData = ret
	}

	rowNums := uint32(it.upsertMsg.InsertMsg.NRows())
	// set upsertTask.insertRequest.rowIDs
	tr := timerecord.NewTimeRecorder("applyPK")
	clusterID := Params.CommonCfg.ClusterID.GetAsUint64()
	rowIDBegin, rowIDEnd, _ := common.AllocAutoID(it.idAllocator.Alloc, rowNums, clusterID)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))

	it.upsertMsg.InsertMsg.RowIDs = make([]UniqueID, rowNums)
	it.rowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.upsertMsg.InsertMsg.RowIDs[offset] = i
		it.rowIDs[offset] = i
	}
	// set upsertTask.insertRequest.timeStamps
	rowNum := it.upsertMsg.InsertMsg.NRows()
	it.upsertMsg.InsertMsg.Timestamps = make([]uint64, rowNum)
	it.timestamps = make([]uint64, rowNum)
	for index := range it.timestamps {
		it.upsertMsg.InsertMsg.Timestamps[index] = it.BeginTs()
		it.timestamps[index] = it.BeginTs()
	}
	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	if it.schema.EnableDynamicField {
		err := checkDynamicFieldData(it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
		if err != nil {
			return err
		}
	}

	if Params.CommonCfg.EnableNamespace.GetAsBool() {
		err := addNamespaceData(it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
		if err != nil {
			return err
		}
	}

	if err := checkAndFlattenStructFieldData(it.schema.CollectionSchema, it.upsertMsg.InsertMsg); err != nil {
		return err
	}

	allFields := typeutil.GetAllFieldSchemas(it.schema.CollectionSchema)

	// use the passed pk as new pk when autoID == false
	// automatic generate pk as new pk wehen autoID == true
	var err error
	it.result.IDs, it.oldIDs, err = checkUpsertPrimaryFieldData(allFields, it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
	log := log.Ctx(ctx).With(zap.String("collectionName", it.upsertMsg.InsertMsg.CollectionName))
	if err != nil {
		log.Warn("check primary field data and hash primary key failed when upsert",
			zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrParameterInvalid)
	}

	// check varchar/text with analyzer was utf-8 format
	err = checkInputUtf8Compatiable(allFields, it.upsertMsg.InsertMsg)
	if err != nil {
		log.Warn("check varchar/text format failed", zap.Error(err))
		return err
	}

	// Validate and set field ID to insert field data
	err = validateFieldDataColumns(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema)
	if err != nil {
		log.Warn("validate field data columns failed when upsert", zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrParameterInvalid)
	}
	err = fillFieldPropertiesOnly(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema)
	if err != nil {
		log.Warn("fill field properties failed when upsert", zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrParameterInvalid)
	}

	if it.partitionKeyMode {
		fieldSchema, _ := typeutil.GetPartitionKeyFieldSchema(it.schema.CollectionSchema)
		it.partitionKeys, err = getPartitionKeyFieldData(fieldSchema, it.upsertMsg.InsertMsg)
		if err != nil {
			log.Warn("get partition keys from insert request failed",
				zap.String("collectionName", collectionName),
				zap.Error(err))
			return err
		}
	} else {
		partitionTag := it.upsertMsg.InsertMsg.PartitionName
		if err = validatePartitionTag(partitionTag, true); err != nil {
			log.Warn("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck(), withOverflowCheck(), withMaxLenCheck()).
		Validate(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema.schemaHelper, it.upsertMsg.InsertMsg.NRows()); err != nil {
		return err
	}

	log.Debug("Proxy Upsert insertPreExecute done")

	return nil
}

func (it *upsertTask) deletePreExecute(ctx context.Context) error {
	collName := it.upsertMsg.DeleteMsg.CollectionName
	log := log.Ctx(ctx).With(
		zap.String("collectionName", collName))

	if it.upsertMsg.DeleteMsg.PrimaryKeys == nil {
		// if primary keys are not set by queryPreExecute, use oldIDs to delete all given records
		it.upsertMsg.DeleteMsg.PrimaryKeys = it.oldIDs
	}
	if typeutil.GetSizeOfIDs(it.upsertMsg.DeleteMsg.PrimaryKeys) == 0 {
		log.Info("deletePKs is empty, skip deleteExecute")
		return nil
	}

	if err := validateCollectionName(collName); err != nil {
		log.Info("Invalid collectionName", zap.Error(err))
		return err
	}

	if it.partitionKeyMode {
		// multi entities with same pk and diff partition keys may be hashed to multi physical partitions
		// if deleteMsg.partitionID = common.InvalidPartition,
		// all segments with this pk under the collection will have the delete record
		it.upsertMsg.DeleteMsg.PartitionID = common.AllPartitionsID
	} else {
		// partition name could be defaultPartitionName or name specified by sdk
		partName := it.upsertMsg.DeleteMsg.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Warn("Invalid partition name", zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, it.req.GetDbName(), collName, partName)
		if err != nil {
			log.Warn("Failed to get partition id", zap.String("collectionName", collName), zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		it.upsertMsg.DeleteMsg.PartitionID = partID
	}

	it.upsertMsg.DeleteMsg.Timestamps = make([]uint64, it.upsertMsg.DeleteMsg.NumRows)
	for index := range it.upsertMsg.DeleteMsg.Timestamps {
		it.upsertMsg.DeleteMsg.Timestamps[index] = it.BeginTs()
	}
	log.Debug("Proxy Upsert deletePreExecute done")
	return nil
}

func (it *upsertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-PreExecute")
	defer sp.End()

	collectionName := it.req.CollectionName
	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName))

	it.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	replicateID, err := GetReplicateID(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get replicate info failed", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	if replicateID != "" {
		return merr.WrapErrCollectionReplicateMode("upsert")
	}

	// check collection exists
	collID, err := globalMetaCache.GetCollectionID(context.Background(), it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("fail to get collection id", zap.Error(err))
		return err
	}
	it.collectionID = collID

	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, it.req.GetDbName(), collectionName, collID)
	if err != nil {
		log.Warn("fail to get collection info", zap.Error(err))
		return err
	}
	if it.schemaTimestamp != 0 {
		if it.schemaTimestamp != colInfo.updateTimestamp {
			err := merr.WrapErrCollectionSchemaMisMatch(collectionName)
			log.Info("collection schema mismatch", zap.String("collectionName", collectionName),
				zap.Uint64("requestSchemaTs", it.schemaTimestamp),
				zap.Uint64("collectionSchemaTs", colInfo.updateTimestamp),
				zap.Error(err))
			return err
		}
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("Failed to get collection schema",
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return err
	}
	it.schema = schema

	err = common.CheckNamespace(schema.CollectionSchema, it.req.Namespace)
	if err != nil {
		return err
	}

	it.partitionKeyMode, err = isPartitionKeyMode(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed",
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return err
	}
	if it.partitionKeyMode {
		if len(it.req.GetPartitionName()) > 0 {
			return errors.New("not support manually specifying the partition names if partition key mode is used")
		}
	} else {
		// set default partition name if not use partition key
		// insert to _default partition
		partitionTag := it.req.GetPartitionName()
		if len(partitionTag) <= 0 {
			pinfo, err := globalMetaCache.GetPartitionInfo(ctx, it.req.GetDbName(), collectionName, "")
			if err != nil {
				log.Warn("get partition info failed", zap.String("collectionName", collectionName), zap.Error(err))
				return err
			}
			it.req.PartitionName = pinfo.name
		}
	}

	// deduplicate upsert data to handle duplicate primary keys in the same batch
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema.CollectionSchema)
	if err != nil {
		log.Warn("fail to get primary field schema", zap.Error(err))
		return err
	}
	duplicate, err := CheckDuplicatePkExist(primaryFieldSchema, it.req.GetFieldsData())
	if err != nil {
		log.Warn("fail to check duplicate primary keys", zap.Error(err))
		return err
	}
	if duplicate {
		return merr.WrapErrParameterInvalidMsg("duplicate primary keys are not allowed in the same batch")
	}

	it.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: it.req.CollectionName,
				CollectionID:   it.collectionID,
				PartitionName:  it.req.PartitionName,
				FieldsData:     it.req.FieldsData,
				NumRows:        uint64(it.req.NumRows),
				Version:        msgpb.InsertDataVersion_ColumnBased,
				DbName:         it.req.DbName,
				Namespace:      it.req.Namespace,
			},
		},
		DeleteMsg: &msgstream.DeleteMsg{
			DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				DbName:         it.req.DbName,
				CollectionName: it.req.CollectionName,
				CollectionID:   it.collectionID,
				NumRows:        int64(it.req.NumRows),
				PartitionName:  it.req.PartitionName,
			},
		},
	}

	// check if num_rows is valid
	if it.req.NumRows <= 0 {
		return merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(it.req.NumRows), "num_rows should be greater than 0")
	}

	if err := genFunctionFields(ctx, it.upsertMsg.InsertMsg, it.schema, it.req.GetPartialUpdate()); err != nil {
		return err
	}

	if it.req.GetPartialUpdate() {
		err = it.queryPreExecute(ctx)
		if err != nil {
			log.Warn("Fail to queryPreExecute", zap.Error(err))
			return err
		}
		// reconstruct upsert msg after queryPreExecute
		it.upsertMsg.InsertMsg.FieldsData = it.insertFieldData
		it.upsertMsg.DeleteMsg.PrimaryKeys = it.deletePKs
		it.upsertMsg.DeleteMsg.NumRows = int64(typeutil.GetSizeOfIDs(it.deletePKs))
	}

	err = it.insertPreExecute(ctx)
	if err != nil {
		log.Warn("Fail to insertPreExecute", zap.Error(err))
		return err
	}

	err = it.deletePreExecute(ctx)
	if err != nil {
		log.Warn("Fail to deletePreExecute", zap.Error(err))
		return err
	}

	it.result.DeleteCnt = it.upsertMsg.DeleteMsg.NumRows
	it.result.InsertCnt = int64(it.upsertMsg.InsertMsg.NumRows)
	if it.result.DeleteCnt != it.result.InsertCnt {
		log.Info("DeleteCnt and InsertCnt are not the same when upsert",
			zap.Int64("DeleteCnt", it.result.DeleteCnt),
			zap.Int64("InsertCnt", it.result.InsertCnt))
	}
	it.result.UpsertCnt = it.result.InsertCnt
	log.Debug("Proxy Upsert PreExecute done")
	return nil
}

func (it *upsertTask) PostExecute(ctx context.Context) error {
	return nil
}
