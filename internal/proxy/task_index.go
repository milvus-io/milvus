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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/federpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/indexparams"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	CreateIndexTaskName              = "CreateIndexTask"
	DescribeIndexTaskName            = "DescribeIndexTask"
	DropIndexTaskName                = "DropIndexTask"
	GetIndexStateTaskName            = "GetIndexStateTask"
	GetIndexBuildProgressTaskName    = "GetIndexBuildProgressTask"
	ListIndexedSegmentTaskName       = "ListIndexedSegment"
	DescribeSegmentIndexDataTaskName = "DescribeSegmentIndexData"

	AutoIndexName = "AUTOINDEX"
	DimKey        = common.DimKey
)

type createIndexTask struct {
	Condition
	req        *milvuspb.CreateIndexRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	datacoord  types.DataCoord
	queryCoord types.QueryCoord
	result     *commonpb.Status

	isAutoIndex    bool
	newIndexParams []*commonpb.KeyValuePair

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

func (cit *createIndexTask) parseIndexParams() error {
	isVecIndex := typeutil.IsVectorType(cit.fieldSchema.DataType)
	indexParamsMap := make(map[string]string)
	if !isVecIndex {
		if cit.fieldSchema.DataType == schemapb.DataType_VarChar {
			indexParamsMap[common.IndexTypeKey] = DefaultStringIndexType
		} else {
			indexParamsMap[common.IndexTypeKey] = DefaultIndexType
		}
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
		if Params.AutoIndexConfig.Enable.GetAsBool() {
			if exist {
				if specifyIndexType != AutoIndexName {
					return fmt.Errorf("IndexType should be %s", AutoIndexName)
				}
			}
			log.Debug("create index trigger AutoIndex",
				zap.String("type", Params.AutoIndexConfig.AutoIndexTypeName.GetValue()))
			// override params
			for k, v := range Params.AutoIndexConfig.IndexParams.GetAsJSONMap() {
				indexParamsMap[k] = v
			}
		} else {
			if !exist {
				return fmt.Errorf("IndexType not specified")
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
	typeParamsMap := make(map[string]interface{})
	for _, pair := range typeParams {
		typeParamsMap[pair.Key] = struct{}{}
	}

	for k, v := range indexParamsMap {
		//Currently, it is required that type_params and index_params do not have same keys.
		_, ok := typeParamsMap[k]
		if ok {
			continue
		}
		cit.newIndexParams = append(cit.newIndexParams, &commonpb.KeyValuePair{Key: k, Value: v})
	}

	return nil
}

func (cit *createIndexTask) getIndexedField(ctx context.Context) (*schemapb.FieldSchema, error) {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, cit.req.GetCollectionName())
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

	adapter, err := indexparamcheck.GetConfAdapterMgrInstance().GetAdapter(indexType)
	if err != nil {
		log.Warn("Failed to get conf adapter", zap.String(common.IndexTypeKey, indexType))
		return fmt.Errorf("invalid index type: %s", indexType)
	}

	if err := fillDimension(field, indexParams); err != nil {
		return err
	}

	ok := adapter.CheckValidDataType(field.GetDataType())
	if !ok {
		log.Warn("Field data type don't support the index build type", zap.String("fieldDataType", field.GetDataType().String()), zap.String("indexType", indexType))
		return fmt.Errorf("field data type %s don't support the index build type %s", field.GetDataType().String(), indexType)
	}

	ok = adapter.CheckTrain(indexParams)
	if !ok {
		log.Warn("Create index with invalid params", zap.Any("index_params", indexParams))
		return fmt.Errorf("invalid index params: %v", indexParams)
	}

	return nil
}

func (cit *createIndexTask) PreExecute(ctx context.Context) error {
	cit.req.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.req.Base.SourceID = paramtable.GetNodeID()

	collName := cit.req.GetCollectionName()

	collID, err := globalMetaCache.GetCollectionID(ctx, collName)
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
	log.Debug("proxy create index", zap.Int64("collID", cit.collectionID), zap.Int64("fieldID", cit.fieldSchema.GetFieldID()),
		zap.String("indexName", cit.req.GetIndexName()), zap.Any("typeParams", cit.fieldSchema.GetTypeParams()),
		zap.Any("indexParams", cit.req.GetExtraParams()))

	if cit.req.GetIndexName() == "" {
		cit.req.IndexName = Params.CommonCfg.DefaultIndexName.GetValue() + "_" + strconv.FormatInt(cit.fieldSchema.GetFieldID(), 10)
	}
	var err error
	req := &indexpb.CreateIndexRequest{
		CollectionID:    cit.collectionID,
		FieldID:         cit.fieldSchema.GetFieldID(),
		IndexName:       cit.req.GetIndexName(),
		TypeParams:      cit.fieldSchema.GetTypeParams(),
		IndexParams:     cit.newIndexParams,
		IsAutoIndex:     cit.isAutoIndex,
		UserIndexParams: cit.req.GetExtraParams(),
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

	collID, err := globalMetaCache.GetCollectionID(ctx, dit.CollectionName)
	if err != nil {
		return err
	}
	dit.collectionID = collID
	return nil
}

func (dit *describeIndexTask) Execute(ctx context.Context) error {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dit.GetCollectionName())
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

func (dit *describeIndexTask) PostExecute(ctx context.Context) error {
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

	collID, err := globalMetaCache.GetCollectionID(ctx, dit.CollectionName)
	if err != nil {
		return err
	}
	dit.collectionID = collID

	loaded, err := checkIfLoaded(ctx, dit.queryCoord, dit.GetCollectionName(), nil)
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
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
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

	if gist.IndexName == "" {
		gist.IndexName = Params.CommonCfg.DefaultIndexName.GetValue()
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, gist.CollectionName)
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

type listIndexedSegmentTask struct {
	Condition
	*federpb.ListIndexedSegmentRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    []int64

	collectionID UniqueID
}

func (list *listIndexedSegmentTask) TraceCtx() context.Context {
	return list.ctx
}

func (list *listIndexedSegmentTask) ID() UniqueID {
	return list.Base.GetMsgID()
}

func (list *listIndexedSegmentTask) SetID(uid UniqueID) {
	list.Base.MsgID = uid
}

func (list *listIndexedSegmentTask) Name() string {
	return ListIndexedSegmentTaskName
}

func (list *listIndexedSegmentTask) Type() commonpb.MsgType {
	return list.Base.GetMsgType()
}

func (list *listIndexedSegmentTask) BeginTs() Timestamp {
	return list.Base.GetTimestamp()
}

func (list *listIndexedSegmentTask) EndTs() Timestamp {
	return list.Base.GetTimestamp()
}

func (list *listIndexedSegmentTask) SetTs(ts Timestamp) {
	list.Base.Timestamp = ts
}

func (list *listIndexedSegmentTask) OnEnqueue() error {
	list.Base = commonpbutil.NewMsgBase()
	return nil
}

func (list *listIndexedSegmentTask) PreExecute(ctx context.Context) error {
	collectionID, err := globalMetaCache.GetCollectionID(ctx, list.GetCollectionName())
	if err != nil {
		return err
	}
	list.collectionID = collectionID
	return nil
}

func (list *listIndexedSegmentTask) Execute(ctx context.Context) error {
	resp, err := list.dataCoord.ListIndexedSegment(ctx, &datapb.ListIndexedSegmentRequest{
		CollectionID: list.collectionID,
		IndexName:    list.GetIndexName(),
	})
	if err != nil {
		return err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return errors.New(resp.GetStatus().GetReason())
	}
	list.result = resp.GetSegmentIDs()
	return nil
}

func (list *listIndexedSegmentTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeSegmentIndexDataTask struct {
	Condition
	*federpb.DescribeSegmentIndexDataRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord
	result     map[int64]*federpb.SegmentIndexData

	shardMgr          *shardClientMgr
	collectionID      UniqueID
	fieldID           UniqueID
	searchShardPolicy pickShardPolicy
	resultBuf         chan *querypb.DescribeSegmentIndexDataResponse
}

func (dsidt *describeSegmentIndexDataTask) TraceCtx() context.Context {
	return dsidt.ctx
}

func (dsidt *describeSegmentIndexDataTask) ID() UniqueID {
	return dsidt.Base.GetMsgID()
}

func (dsidt *describeSegmentIndexDataTask) SetID(uid UniqueID) {
	dsidt.Base.MsgID = uid
}

func (dsidt *describeSegmentIndexDataTask) Name() string {
	return ListIndexedSegmentTaskName
}

func (dsidt *describeSegmentIndexDataTask) Type() commonpb.MsgType {
	return dsidt.Base.GetMsgType()
}

func (dsidt *describeSegmentIndexDataTask) BeginTs() Timestamp {
	return dsidt.Base.GetTimestamp()
}

func (dsidt *describeSegmentIndexDataTask) EndTs() Timestamp {
	return dsidt.Base.GetTimestamp()
}

func (dsidt *describeSegmentIndexDataTask) SetTs(ts Timestamp) {
	dsidt.Base.Timestamp = ts
}

func (dsidt *describeSegmentIndexDataTask) OnEnqueue() error {
	dsidt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dsidt *describeSegmentIndexDataTask) PreExecute(ctx context.Context) error {
	collectionID, err := globalMetaCache.GetCollectionID(ctx, dsidt.GetCollectionName())
	if err != nil {
		return err
	}
	dsidt.collectionID = collectionID
	loaded, err := checkIfLoaded(ctx, dsidt.queryCoord, dsidt.GetCollectionName(), nil)
	if err != nil {
		return err
	}

	if !loaded {
		return errors.New("collection is not loaded, please load collection first")
	}
	if dsidt.searchShardPolicy == nil {
		dsidt.searchShardPolicy = mergeRoundRobinPolicy
	}
	describeIndexReq := &indexpb.DescribeIndexRequest{
		CollectionID: collectionID,
		IndexName:    dsidt.GetIndexName(),
	}
	describeIndexResp, err := dsidt.dataCoord.DescribeIndex(ctx, describeIndexReq)
	if err != nil {
		return err
	}
	if describeIndexResp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return errors.New(describeIndexResp.GetStatus().GetReason())
	}
	dsidt.fieldID = describeIndexResp.GetIndexInfos()[0].GetFieldID()
	return nil
}

func (dsidt *describeSegmentIndexDataTask) searchShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs []string) error {
	req := &querypb.DescribeSegmentIndexDataRequest{
		Base:         dsidt.GetBase(),
		CollectionID: dsidt.collectionID,
		FieldID:      dsidt.fieldID,
		SegmentIDs:   dsidt.GetSegmentsIDs(),
		DmlChannels:  channelIDs,
	}
	req.GetBase().TargetID = nodeID

	queryNode := querynode.GetQueryNode()
	var resp *querypb.DescribeSegmentIndexDataResponse
	var err error

	if queryNode != nil && queryNode.IsStandAlone {
		resp, err = queryNode.DescribeSegmentIndexData(ctx, req)
	} else {
		resp, err = qn.DescribeSegmentIndexData(ctx, req)
	}
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode describe segment index data return error",
			zap.Int64("nodeID", nodeID),
			zap.Error(err))
		return err
	}
	if resp.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader",
			zap.Int64("nodeID", nodeID))
		return errInvalidShardLeaders
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode describe segment index data return error",
			zap.Int64("nodeID", nodeID),
			zap.String("reason", resp.GetStatus().GetReason()))
		return fmt.Errorf("fail to describe segment index data, QueryNode ID=%d, reason=%s", nodeID, resp.GetStatus().GetReason())
	}
	dsidt.resultBuf <- resp
	return nil
}

func (dsidt *describeSegmentIndexDataTask) Execute(ctx context.Context) error {
	executeSearch := func(withCache bool) error {
		shard2Leaders, err := globalMetaCache.GetShards(ctx, withCache, dsidt.GetCollectionName())
		if err != nil {
			return err
		}
		dsidt.resultBuf = make(chan *querypb.DescribeSegmentIndexDataResponse, len(shard2Leaders))
		if err := dsidt.searchShardPolicy(ctx, dsidt.shardMgr, dsidt.searchShard, shard2Leaders); err != nil {
			log.Warn("failed to do search", zap.Error(err), zap.String("Shards", fmt.Sprintf("%v", shard2Leaders)))
			return err
		}
		return nil
	}

	err := executeSearch(WithCache)
	if err != nil {
		log.Warn("first search failed, updating shardleader caches and retry search",
			zap.Error(err))
		// invalidate cache first, since ctx may be canceled or timeout here
		globalMetaCache.DeprecateShardCache(dsidt.GetCollectionName())
		err = executeSearch(WithoutCache)
	}
	return nil
}

func (dsidt *describeSegmentIndexDataTask) PostExecute(ctx context.Context) error {
	dsidt.result = make(map[int64]*federpb.SegmentIndexData)
	select {
	case <-dsidt.TraceCtx().Done():
		log.Ctx(ctx).Warn("search task wait to finish timeout!")
		return fmt.Errorf("describeSegmentIndexData task wait to finish timeout, msgID=%d", dsidt.ID())
	default:
		log.Ctx(ctx).Debug("all describeSegmentIndexData requests are finished or canceled")
		close(dsidt.resultBuf)
		for res := range dsidt.resultBuf {
			log.Ctx(ctx).Debug("proxy receives one describeSegmentIndexData result")
			for segID, indexData := range res.GetIndexDatas() {
				if _, ok := dsidt.result[segID]; ok {
					log.Warn("there are multiple index data if the segment", zap.Int64("segID", segID))
					return fmt.Errorf("there are multiple index data if the segment: %d", segID)
				}
				dsidt.result[segID] = indexData
			}
		}
	}
	return nil
}
