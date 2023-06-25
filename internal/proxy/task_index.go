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
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	CreateIndexTaskName           = "CreateIndexTask"
	DescribeIndexTaskName         = "DescribeIndexTask"
	DropIndexTaskName             = "DropIndexTask"
	GetIndexStateTaskName         = "GetIndexStateTask"
	GetIndexBuildProgressTaskName = "GetIndexBuildProgressTask"

	AutoIndexName = "AUTOINDEX"
	DimKey        = common.DimKey
)

type createIndexTask struct {
	Condition
	req       *milvuspb.CreateIndexRequest
	ctx       context.Context
	rootCoord types.RootCoord
	datacoord types.DataCoord
	result    *commonpb.Status

	isAutoIndex    bool
	newIndexParams []*commonpb.KeyValuePair
	newTypeParams  []*commonpb.KeyValuePair
	newExtraParams []*commonpb.KeyValuePair

	collectionID UniqueID
	fieldSchema  *schemapb.FieldSchema
}

func (cit *createIndexTask) TraceCtx() context.Context {
	return cit.ctx
}

func (cit *createIndexTask) ID() UniqueID {
	return cit.req.GetBase().GetMsgID()
}

func (cit *createIndexTask) SetID(uid UniqueID) {
	cit.req.GetBase().MsgID = uid
}

func (cit *createIndexTask) Name() string {
	return CreateIndexTaskName
}

func (cit *createIndexTask) Type() commonpb.MsgType {
	return cit.req.GetBase().GetMsgType()
}

func (cit *createIndexTask) BeginTs() Timestamp {
	return cit.req.GetBase().GetTimestamp()
}

func (cit *createIndexTask) EndTs() Timestamp {
	return cit.req.GetBase().GetTimestamp()
}

func (cit *createIndexTask) SetTs(ts Timestamp) {
	cit.req.Base.Timestamp = ts
}

func (cit *createIndexTask) OnEnqueue() error {
	cit.req.Base = commonpbutil.NewMsgBase()
	return nil
}

func wrapUserIndexParams(metricType string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: AutoIndexName,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metricType,
		},
	}
}

func (cit *createIndexTask) parseIndexParams() error {
	cit.newExtraParams = cit.req.GetExtraParams()

	isVecIndex := typeutil.IsVectorType(cit.fieldSchema.DataType)
	indexParamsMap := make(map[string]string)
	if !isVecIndex {
		if cit.fieldSchema.DataType == schemapb.DataType_VarChar {
			indexParamsMap[common.IndexTypeKey] = DefaultStringIndexType
		} else {
			indexParamsMap[common.IndexTypeKey] = DefaultIndexType
		}
	}
	if cit.fieldSchema.DataType == schemapb.DataType_JSON {
		return fmt.Errorf("create index on json field is not supported")
	}

	for _, kv := range cit.req.GetExtraParams() {
		if kv.Key == common.IndexParamsKey {
			params, err := funcutil.JSONToMap(kv.Value)
			if err != nil {
				return err
			}
			for k, v := range params {
				indexParamsMap[k] = v
			}
		} else {
			indexParamsMap[kv.Key] = kv.Value
		}
	}

	if isVecIndex {
		specifyIndexType, exist := indexParamsMap[common.IndexTypeKey]
		if Params.AutoIndexConfig.Enable.GetAsBool() { // `enable` only for cloud instance.
			log.Info("create index trigger AutoIndex",
				zap.String("original type", specifyIndexType),
				zap.String("final type", Params.AutoIndexConfig.AutoIndexTypeName.GetValue()))

			metricType, metricTypeExist := indexParamsMap[common.MetricTypeKey]

			// override params by autoindex
			for k, v := range Params.AutoIndexConfig.IndexParams.GetAsJSONMap() {
				indexParamsMap[k] = v
			}

			if metricTypeExist {
				// make the users' metric type first class citizen.
				indexParamsMap[common.MetricTypeKey] = metricType
			}
		} else { // behavior change after 2.2.9, adapt autoindex logic here.
			autoIndexConfig := Params.AutoIndexConfig.IndexParams.GetAsJSONMap()

			useAutoIndex := func() {
				fields := make([]zap.Field, 0, len(autoIndexConfig))
				for k, v := range autoIndexConfig {
					indexParamsMap[k] = v
					fields = append(fields, zap.String(k, v))
				}
				log.Ctx(cit.ctx).Info("AutoIndex triggered", fields...)
			}

			handle := func(numberParams int) error {
				// empty case.
				if len(indexParamsMap) == numberParams {
					// though we already know there must be metric type, how to make this safer to avoid crash?
					metricType := autoIndexConfig[common.MetricTypeKey]
					cit.newExtraParams = wrapUserIndexParams(metricType)
					useAutoIndex()
					return nil
				}

				metricType, metricTypeExist := indexParamsMap[common.MetricTypeKey]

				if len(indexParamsMap) > numberParams+1 {
					return fmt.Errorf("only metric type can be passed when use AutoIndex")
				}

				if len(indexParamsMap) == numberParams+1 {
					if !metricTypeExist {
						return fmt.Errorf("only metric type can be passed when use AutoIndex")
					}

					// only metric type is passed.
					cit.newExtraParams = wrapUserIndexParams(metricType)
					useAutoIndex()
					// make the users' metric type first class citizen.
					indexParamsMap[common.MetricTypeKey] = metricType
				}

				return nil
			}

			if !exist {
				if err := handle(0); err != nil {
					return err
				}
			} else if specifyIndexType == AutoIndexName {
				if err := handle(1); err != nil {
					return err
				}
			}
		}

		indexType, exist := indexParamsMap[common.IndexTypeKey]
		if !exist {
			return fmt.Errorf("IndexType not specified")
		}
		if indexType == indexparamcheck.IndexDISKANN {
			err := indexparams.FillDiskIndexParams(Params, indexParamsMap)
			if err != nil {
				return err
			}
		}

		err := checkTrain(cit.fieldSchema, indexParamsMap)
		if err != nil {
			return err
		}
	}
	typeParams := cit.fieldSchema.GetTypeParams()
	typeParamsMap := make(map[string]string)
	for _, pair := range typeParams {
		typeParamsMap[pair.Key] = pair.Value
	}

	for k, v := range indexParamsMap {
		//Currently, it is required that type_params and index_params do not have same keys.
		if k == DimKey || k == common.MaxLengthKey {
			delete(indexParamsMap, k)
			continue
		}
		cit.newIndexParams = append(cit.newIndexParams, &commonpb.KeyValuePair{Key: k, Value: v})
	}

	for k, v := range typeParamsMap {
		if _, ok := indexParamsMap[k]; ok {
			continue
		}
		cit.newTypeParams = append(cit.newTypeParams, &commonpb.KeyValuePair{Key: k, Value: v})
	}

	return nil
}

func (cit *createIndexTask) getIndexedField(ctx context.Context) (*schemapb.FieldSchema, error) {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, cit.req.GetDbName(), cit.req.GetCollectionName())
	if err != nil {
		log.Error("failed to get collection schema", zap.Error(err))
		return nil, fmt.Errorf("failed to get collection schema: %s", err)
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		log.Error("failed to parse collection schema", zap.Error(err))
		return nil, fmt.Errorf("failed to parse collection schema: %s", err)
	}
	field, err := schemaHelper.GetFieldFromName(cit.req.GetFieldName())
	if err != nil {
		log.Error("create index on non-exist field", zap.Error(err))
		return nil, fmt.Errorf("cannot create index on non-exist field: %s", cit.req.GetFieldName())
	}
	return field, nil
}

func fillDimension(field *schemapb.FieldSchema, indexParams map[string]string) error {
	vecDataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
	}
	if !funcutil.SliceContain(vecDataTypes, field.GetDataType()) {
		return nil
	}
	params := make([]*commonpb.KeyValuePair, 0, len(field.GetTypeParams())+len(field.GetIndexParams()))
	params = append(params, field.GetTypeParams()...)
	params = append(params, field.GetIndexParams()...)
	dimensionInSchema, err := funcutil.GetAttrByKeyFromRepeatedKV(DimKey, params)
	if err != nil {
		return fmt.Errorf("dimension not found in schema")
	}
	dimension, exist := indexParams[DimKey]
	if exist {
		if dimensionInSchema != dimension {
			return fmt.Errorf("dimension mismatch, dimension in schema: %s, dimension: %s", dimensionInSchema, dimension)
		}
	} else {
		indexParams[DimKey] = dimensionInSchema
	}
	return nil
}

func checkTrain(field *schemapb.FieldSchema, indexParams map[string]string) error {
	indexType := indexParams[common.IndexTypeKey]
	// skip params check of non-vector field.
	vecDataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
	}
	if !funcutil.SliceContain(vecDataTypes, field.GetDataType()) {
		return indexparamcheck.CheckIndexValid(field.GetDataType(), indexType, indexParams)
	}

	checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(indexType)
	if err != nil {
		log.Warn("Failed to get index checker", zap.String(common.IndexTypeKey, indexType))
		return fmt.Errorf("invalid index type: %s", indexType)
	}

	if err := fillDimension(field, indexParams); err != nil {
		return err
	}

	if err := checker.CheckValidDataType(field.GetDataType()); err != nil {
		log.Info("create index with invalid data type", zap.Error(err), zap.String("data_type", field.GetDataType().String()))
		return err
	}

	if err := checker.CheckTrain(indexParams); err != nil {
		log.Info("create index with invalid parameters", zap.Error(err))
		return err
	}

	return nil
}

func (cit *createIndexTask) PreExecute(ctx context.Context) error {
	cit.req.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.req.Base.SourceID = paramtable.GetNodeID()

	collName := cit.req.GetCollectionName()

	collID, err := globalMetaCache.GetCollectionID(ctx, cit.req.GetDbName(), collName)
	if err != nil {
		return err
	}
	cit.collectionID = collID

	if err = validateIndexName(cit.req.GetIndexName()); err != nil {
		return err
	}

	field, err := cit.getIndexedField(ctx)
	if err != nil {
		return err
	}
	cit.fieldSchema = field
	// check index param, not accurate, only some static rules
	err = cit.parseIndexParams()
	if err != nil {
		return err
	}

	return nil
}

func (cit *createIndexTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy create index", zap.Int64("collID", cit.collectionID), zap.Int64("fieldID", cit.fieldSchema.GetFieldID()),
		zap.String("indexName", cit.req.GetIndexName()), zap.Any("typeParams", cit.fieldSchema.GetTypeParams()),
		zap.Any("indexParams", cit.req.GetExtraParams()),
		zap.Any("newExtraParams", cit.newExtraParams),
	)

	if cit.req.GetIndexName() == "" {
		cit.req.IndexName = Params.CommonCfg.DefaultIndexName.GetValue() + "_" + strconv.FormatInt(cit.fieldSchema.GetFieldID(), 10)
	}
	var err error
	req := &indexpb.CreateIndexRequest{
		CollectionID:    cit.collectionID,
		FieldID:         cit.fieldSchema.GetFieldID(),
		IndexName:       cit.req.GetIndexName(),
		TypeParams:      cit.newTypeParams,
		IndexParams:     cit.newIndexParams,
		IsAutoIndex:     cit.isAutoIndex,
		UserIndexParams: cit.newExtraParams,
		Timestamp:       cit.BeginTs(),
	}
	cit.result, err = cit.datacoord.CreateIndex(ctx, req)
	if err != nil {
		return err
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
	datacoord types.DataCoord
	result    *milvuspb.DescribeIndexResponse

	collectionID UniqueID
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
	dit.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dit *describeIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DescribeIndex
	dit.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(dit.CollectionName); err != nil {
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, dit.GetDbName(), dit.CollectionName)
	if err != nil {
		return err
	}
	dit.collectionID = collID
	return nil
}

func (dit *describeIndexTask) Execute(ctx context.Context) error {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dit.GetDbName(), dit.GetCollectionName())
	if err != nil {
		log.Error("failed to get collection schema", zap.Error(err))
		return fmt.Errorf("failed to get collection schema: %s", err)
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		log.Error("failed to parse collection schema", zap.Error(err))
		return fmt.Errorf("failed to parse collection schema: %s", err)
	}

	resp, err := dit.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: dit.collectionID, IndexName: dit.IndexName})
	if err != nil || resp == nil {
		return err
	}
	dit.result = &milvuspb.DescribeIndexResponse{}
	dit.result.Status = resp.GetStatus()
	if dit.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Status.Reason)
	}
	for _, indexInfo := range resp.IndexInfos {
		field, err := schemaHelper.GetFieldFromID(indexInfo.FieldID)
		if err != nil {
			log.Error("failed to get collection field", zap.Error(err))
			return fmt.Errorf("failed to get collection field: %d", indexInfo.FieldID)
		}
		params := indexInfo.GetUserIndexParams()
		if params == nil {
			metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, indexInfo.GetIndexParams())
			if err == nil {
				params = wrapUserIndexParams(metricType)
			}
		}
		desc := &milvuspb.IndexDescription{
			IndexName:            indexInfo.GetIndexName(),
			IndexID:              indexInfo.GetIndexID(),
			FieldName:            field.Name,
			Params:               params,
			IndexedRows:          indexInfo.GetIndexedRows(),
			TotalRows:            indexInfo.GetTotalRows(),
			PendingIndexRows:     indexInfo.GetPendingIndexRows(),
			State:                indexInfo.GetState(),
			IndexStateFailReason: indexInfo.GetIndexStateFailReason(),
		}
		dit.result.IndexDescriptions = append(dit.result.IndexDescriptions, desc)
	}
	return err
}

func (dit *describeIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type getIndexStatisticsTask struct {
	Condition
	*milvuspb.GetIndexStatisticsRequest
	ctx       context.Context
	datacoord types.DataCoord
	result    *milvuspb.GetIndexStatisticsResponse

	nodeID       int64
	collectionID UniqueID
}

func (dit *getIndexStatisticsTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *getIndexStatisticsTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *getIndexStatisticsTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *getIndexStatisticsTask) Name() string {
	return DescribeIndexTaskName
}

func (dit *getIndexStatisticsTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *getIndexStatisticsTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *getIndexStatisticsTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *getIndexStatisticsTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *getIndexStatisticsTask) OnEnqueue() error {
	dit.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dit *getIndexStatisticsTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_GetIndexStatistics
	dit.Base.SourceID = dit.nodeID

	if err := validateCollectionName(dit.CollectionName); err != nil {
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, dit.GetDbName(), dit.CollectionName)
	if err != nil {
		return err
	}
	dit.collectionID = collID
	return nil
}

func (dit *getIndexStatisticsTask) Execute(ctx context.Context) error {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dit.GetDbName(), dit.GetCollectionName())
	if err != nil {
		log.Error("failed to get collection schema", zap.String("collection_name", dit.GetCollectionName()), zap.Error(err))
		return fmt.Errorf("failed to get collection schema: %s", dit.GetCollectionName())
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		log.Error("failed to parse collection schema", zap.String("collection_name", schema.GetName()), zap.Error(err))
		return fmt.Errorf("failed to parse collection schema: %s", dit.GetCollectionName())
	}

	resp, err := dit.datacoord.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{
		CollectionID: dit.collectionID, IndexName: dit.IndexName})
	if err != nil || resp == nil {
		return err
	}
	dit.result = &milvuspb.GetIndexStatisticsResponse{}
	dit.result.Status = resp.GetStatus()
	if dit.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Status.Reason)
	}
	for _, indexInfo := range resp.IndexInfos {
		field, err := schemaHelper.GetFieldFromID(indexInfo.FieldID)
		if err != nil {
			log.Error("failed to get collection field", zap.Int64("field_id", indexInfo.FieldID), zap.Error(err))
			return fmt.Errorf("failed to get collection field: %d", indexInfo.FieldID)
		}
		params := indexInfo.GetUserIndexParams()
		if params == nil {
			params = indexInfo.GetIndexParams()
		}
		desc := &milvuspb.IndexDescription{
			IndexName:            indexInfo.GetIndexName(),
			IndexID:              indexInfo.GetIndexID(),
			FieldName:            field.Name,
			Params:               params,
			IndexedRows:          indexInfo.GetIndexedRows(),
			TotalRows:            indexInfo.GetTotalRows(),
			State:                indexInfo.GetState(),
			IndexStateFailReason: indexInfo.GetIndexStateFailReason(),
		}
		dit.result.IndexDescriptions = append(dit.result.IndexDescriptions, desc)
	}
	return err
}

func (dit *getIndexStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropIndexTask struct {
	Condition
	ctx context.Context
	*milvuspb.DropIndexRequest
	dataCoord  types.DataCoord
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
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
	dit.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dit *dropIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DropIndex
	dit.Base.SourceID = paramtable.GetNodeID()

	collName, fieldName := dit.CollectionName, dit.FieldName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if fieldName != "" {
		if err := validateFieldName(fieldName); err != nil {
			return err
		}
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, dit.GetDbName(), dit.CollectionName)
	if err != nil {
		return err
	}
	dit.collectionID = collID

	loaded, err := isCollectionLoaded(ctx, dit.queryCoord, collID)
	if err != nil {
		return err
	}

	if loaded {
		return errors.New("index cannot be dropped, collection is loaded, please release it first")
	}

	return nil
}

func (dit *dropIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.dataCoord.DropIndex(ctx, &indexpb.DropIndexRequest{
		CollectionID: dit.collectionID,
		PartitionIDs: nil,
		IndexName:    dit.IndexName,
		DropAll:      false,
	})
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

// Deprecated: use describeIndexTask instead
type getIndexBuildProgressTask struct {
	Condition
	*milvuspb.GetIndexBuildProgressRequest
	ctx       context.Context
	rootCoord types.RootCoord
	dataCoord types.DataCoord
	result    *milvuspb.GetIndexBuildProgressResponse

	collectionID UniqueID
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
	gibpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (gibpt *getIndexBuildProgressTask) PreExecute(ctx context.Context) error {
	gibpt.Base.MsgType = commonpb.MsgType_GetIndexBuildProgress
	gibpt.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(gibpt.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) Execute(ctx context.Context) error {
	collectionName := gibpt.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, gibpt.GetDbName(), collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	gibpt.collectionID = collectionID

	if gibpt.IndexName == "" {
		gibpt.IndexName = Params.CommonCfg.DefaultIndexName.GetValue()
	}

	resp, err := gibpt.dataCoord.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{
		CollectionID: collectionID,
		IndexName:    gibpt.IndexName,
	})
	if err != nil {
		return err
	}

	gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
		Status:      resp.Status,
		TotalRows:   resp.GetTotalRows(),
		IndexedRows: resp.GetIndexedRows(),
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) PostExecute(ctx context.Context) error {
	return nil
}

// Deprecated: use describeIndexTask instead
type getIndexStateTask struct {
	Condition
	*milvuspb.GetIndexStateRequest
	ctx       context.Context
	dataCoord types.DataCoord
	rootCoord types.RootCoord
	result    *milvuspb.GetIndexStateResponse

	collectionID UniqueID
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
	gist.Base = commonpbutil.NewMsgBase()
	return nil
}

func (gist *getIndexStateTask) PreExecute(ctx context.Context) error {
	gist.Base.MsgType = commonpb.MsgType_GetIndexState
	gist.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(gist.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gist *getIndexStateTask) Execute(ctx context.Context) error {
	collectionID, err := globalMetaCache.GetCollectionID(ctx, gist.GetDbName(), gist.CollectionName)
	if err != nil {
		return err
	}

	state, err := gist.dataCoord.GetIndexState(ctx, &indexpb.GetIndexStateRequest{
		CollectionID: collectionID,
		IndexName:    gist.IndexName,
	})
	if err != nil {
		return err
	}

	gist.result = &milvuspb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		State:      state.GetState(),
		FailReason: state.GetFailReason(),
	}
	return nil
}

func (gist *getIndexStateTask) PostExecute(ctx context.Context) error {
	return nil
}
