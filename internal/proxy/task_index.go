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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	CreateIndexTaskName           = "CreateIndexTask"
	AlterIndexTaskName            = "AlterIndexTask"
	DescribeIndexTaskName         = "DescribeIndexTask"
	DropIndexTaskName             = "DropIndexTask"
	GetIndexStateTaskName         = "GetIndexStateTask"
	GetIndexBuildProgressTaskName = "GetIndexBuildProgressTask"

	AutoIndexName = common.AutoIndexName
	DimKey        = common.DimKey
	IsSparseKey   = common.IsSparseKey
)

type createIndexTask struct {
	baseTask
	Condition
	req       *milvuspb.CreateIndexRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	datacoord types.DataCoordClient
	result    *commonpb.Status

	replicateMsgStream msgstream.MsgStream

	isAutoIndex    bool
	newIndexParams []*commonpb.KeyValuePair
	newTypeParams  []*commonpb.KeyValuePair
	newExtraParams []*commonpb.KeyValuePair

	collectionID                     UniqueID
	fieldSchema                      *schemapb.FieldSchema
	userAutoIndexMetricTypeSpecified bool
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
	if cit.req.Base == nil {
		cit.req.Base = commonpbutil.NewMsgBase()
	}
	cit.req.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.req.Base.SourceID = paramtable.GetNodeID()
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

	if err := ValidateAutoIndexMmapConfig(isVecIndex, indexParamsMap); err != nil {
		return err
	}

	specifyIndexType, exist := indexParamsMap[common.IndexTypeKey]
	if exist && specifyIndexType != "" {
		if err := indexparamcheck.ValidateMmapIndexParams(specifyIndexType, indexParamsMap); err != nil {
			log.Ctx(cit.ctx).Warn("Invalid mmap type params", zap.String(common.IndexTypeKey, specifyIndexType), zap.Error(err))
			return merr.WrapErrParameterInvalidMsg("invalid mmap type params", err.Error())
		}
		checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(specifyIndexType)
		// not enable hybrid index for user, used in milvus internally
		if err != nil || indexparamcheck.IsHYBRIDChecker(checker) {
			log.Ctx(cit.ctx).Warn("Failed to get index checker", zap.String(common.IndexTypeKey, specifyIndexType))
			return merr.WrapErrParameterInvalid("valid index", fmt.Sprintf("invalid index type: %s", specifyIndexType))
		}
	}

	if !isVecIndex {
		specifyIndexType, exist := indexParamsMap[common.IndexTypeKey]
		autoIndexEnable := Params.AutoIndexConfig.ScalarAutoIndexEnable.GetAsBool()

		if autoIndexEnable || !exist || specifyIndexType == AutoIndexName {
			getPrimitiveIndexType := func(dataType schemapb.DataType) string {
				if typeutil.IsBoolType(dataType) {
					return Params.AutoIndexConfig.ScalarBoolIndexType.GetValue()
				} else if typeutil.IsIntegerType(dataType) {
					return Params.AutoIndexConfig.ScalarIntIndexType.GetValue()
				} else if typeutil.IsFloatingType(dataType) {
					return Params.AutoIndexConfig.ScalarFloatIndexType.GetValue()
				}
				return Params.AutoIndexConfig.ScalarVarcharIndexType.GetValue()
			}

			indexType, err := func() (string, error) {
				dataType := cit.fieldSchema.DataType
				if typeutil.IsPrimitiveType(dataType) {
					return getPrimitiveIndexType(dataType), nil
				} else if typeutil.IsArrayType(dataType) {
					return getPrimitiveIndexType(cit.fieldSchema.ElementType), nil
				}
				return "", fmt.Errorf("create auto index on type:%s is not supported", dataType.String())
			}()
			if err != nil {
				return merr.WrapErrParameterInvalid("supported field", err.Error())
			}

			indexParamsMap[common.IndexTypeKey] = indexType
			cit.isAutoIndex = true
		}
	} else {
		specifyIndexType, exist := indexParamsMap[common.IndexTypeKey]
		if Params.AutoIndexConfig.Enable.GetAsBool() { // `enable` only for cloud instance.
			log.Info("create index trigger AutoIndex",
				zap.String("original type", specifyIndexType),
				zap.String("final type", Params.AutoIndexConfig.AutoIndexTypeName.GetValue()))

			metricType, metricTypeExist := indexParamsMap[common.MetricTypeKey]

			if typeutil.IsDenseFloatVectorType(cit.fieldSchema.DataType) {
				// override float vector index params by autoindex
				for k, v := range Params.AutoIndexConfig.IndexParams.GetAsJSONMap() {
					indexParamsMap[k] = v
				}
			} else if typeutil.IsSparseFloatVectorType(cit.fieldSchema.DataType) {
				// override sparse float vector index params by autoindex
				for k, v := range Params.AutoIndexConfig.SparseIndexParams.GetAsJSONMap() {
					indexParamsMap[k] = v
				}
			} else if typeutil.IsBinaryVectorType(cit.fieldSchema.DataType) {
				// override binary vector index params by autoindex
				for k, v := range Params.AutoIndexConfig.BinaryIndexParams.GetAsJSONMap() {
					indexParamsMap[k] = v
				}
			}

			if metricTypeExist {
				// make the users' metric type first class citizen.
				indexParamsMap[common.MetricTypeKey] = metricType
				cit.userAutoIndexMetricTypeSpecified = true
			}
		} else { // behavior change after 2.2.9, adapt autoindex logic here.
			useAutoIndex := func(autoIndexConfig map[string]string) {
				fields := make([]zap.Field, 0, len(autoIndexConfig))
				for k, v := range autoIndexConfig {
					indexParamsMap[k] = v
					fields = append(fields, zap.String(k, v))
				}
				log.Ctx(cit.ctx).Info("AutoIndex triggered", fields...)
			}

			handle := func(numberParams int, autoIndexConfig map[string]string) error {
				// empty case.
				if len(indexParamsMap) == numberParams {
					// though we already know there must be metric type, how to make this safer to avoid crash?
					metricType := autoIndexConfig[common.MetricTypeKey]
					cit.newExtraParams = wrapUserIndexParams(metricType)
					useAutoIndex(autoIndexConfig)
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
					useAutoIndex(autoIndexConfig)
					// make the users' metric type first class citizen.
					indexParamsMap[common.MetricTypeKey] = metricType
					cit.userAutoIndexMetricTypeSpecified = true
				}

				return nil
			}

			var config map[string]string
			if typeutil.IsDenseFloatVectorType(cit.fieldSchema.DataType) {
				// override float vector index params by autoindex
				config = Params.AutoIndexConfig.IndexParams.GetAsJSONMap()
			} else if typeutil.IsSparseFloatVectorType(cit.fieldSchema.DataType) {
				// override sparse float vector index params by autoindex
				config = Params.AutoIndexConfig.SparseIndexParams.GetAsJSONMap()
			} else if typeutil.IsBinaryVectorType(cit.fieldSchema.DataType) {
				// override binary vector index params by autoindex
				config = Params.AutoIndexConfig.BinaryIndexParams.GetAsJSONMap()
			}
			if !exist {
				if err := handle(0, config); err != nil {
					return err
				}
			} else if specifyIndexType == AutoIndexName {
				if err := handle(1, config); err != nil {
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
		metricType, metricTypeExist := indexParamsMap[common.MetricTypeKey]
		if !metricTypeExist {
			return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "metric type not set for vector index")
		}
		if typeutil.IsDenseFloatVectorType(cit.fieldSchema.DataType) {
			if !funcutil.SliceContain(indexparamcheck.FloatVectorMetrics, metricType) {
				return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "float vector index does not support metric type: "+metricType)
			}
		} else if typeutil.IsSparseFloatVectorType(cit.fieldSchema.DataType) {
			if metricType != metric.IP {
				return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "only IP is the supported metric type for sparse index")
			}
		} else if typeutil.IsBinaryVectorType(cit.fieldSchema.DataType) {
			if !funcutil.SliceContain(indexparamcheck.BinaryVectorMetrics, metricType) {
				return merr.WrapErrParameterInvalid("valid index params", "invalid index params", "binary vector index does not support metric type: "+metricType)
			}
		}
	}

	err := checkTrain(cit.fieldSchema, indexParamsMap)
	if err != nil {
		return merr.WrapErrParameterInvalid("valid index params", "invalid index params", err.Error())
	}

	typeParams := cit.fieldSchema.GetTypeParams()
	typeParamsMap := make(map[string]string)
	for _, pair := range typeParams {
		typeParamsMap[pair.Key] = pair.Value
	}

	for k, v := range indexParamsMap {
		// Currently, it is required that type_params and index_params do not have same keys.
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
	field, err := schema.schemaHelper.GetFieldFromName(cit.req.GetFieldName())
	if err != nil {
		log.Error("create index on non-exist field", zap.Error(err))
		return nil, fmt.Errorf("cannot create index on non-exist field: %s", cit.req.GetFieldName())
	}
	return field, nil
}

func fillDimension(field *schemapb.FieldSchema, indexParams map[string]string) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
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

	if indexType == indexparamcheck.IndexHybrid {
		_, exist := indexParams[common.BitmapCardinalityLimitKey]
		if !exist {
			indexParams[common.BitmapCardinalityLimitKey] = paramtable.Get().CommonCfg.BitmapIndexCardinalityBound.GetValue()
		}
	}
	checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(indexType)
	if err != nil {
		log.Warn("Failed to get index checker", zap.String(common.IndexTypeKey, indexType))
		return fmt.Errorf("invalid index type: %s", indexType)
	}

	if typeutil.IsVectorType(field.DataType) && indexType != indexparamcheck.AutoIndex {
		exist := CheckVecIndexWithDataTypeExist(indexType, field.DataType)
		if !exist {
			return fmt.Errorf("data type %d can't build with this index %s", field.DataType, indexType)
		}
	}

	isSparse := typeutil.IsSparseFloatVectorType(field.DataType)

	if !isSparse {
		if err := fillDimension(field, indexParams); err != nil {
			return err
		}
	} else {
		// used only for checker, should be deleted after checking
		indexParams[IsSparseKey] = "true"
	}

	if err := checker.CheckValidDataType(field); err != nil {
		log.Info("create index with invalid data type", zap.Error(err), zap.String("data_type", field.GetDataType().String()))
		return err
	}

	if err := checker.CheckTrain(indexParams); err != nil {
		log.Info("create index with invalid parameters", zap.Error(err))
		return err
	}

	if isSparse {
		delete(indexParams, IsSparseKey)
	}

	return nil
}

func (cit *createIndexTask) PreExecute(ctx context.Context) error {
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
	log.Ctx(ctx).Info("proxy create index", zap.Int64("collectionID", cit.collectionID), zap.Int64("fieldID", cit.fieldSchema.GetFieldID()),
		zap.String("indexName", cit.req.GetIndexName()), zap.Any("typeParams", cit.fieldSchema.GetTypeParams()),
		zap.Any("indexParams", cit.req.GetExtraParams()),
		zap.Any("newExtraParams", cit.newExtraParams),
	)

	var err error
	req := &indexpb.CreateIndexRequest{
		CollectionID:                     cit.collectionID,
		FieldID:                          cit.fieldSchema.GetFieldID(),
		IndexName:                        cit.req.GetIndexName(),
		TypeParams:                       cit.newTypeParams,
		IndexParams:                      cit.newIndexParams,
		IsAutoIndex:                      cit.isAutoIndex,
		UserIndexParams:                  cit.newExtraParams,
		Timestamp:                        cit.BeginTs(),
		UserAutoindexMetricTypeSpecified: cit.userAutoIndexMetricTypeSpecified,
	}
	cit.result, err = cit.datacoord.CreateIndex(ctx, req)
	if err = merr.CheckRPCCall(cit.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, cit.replicateMsgStream, cit.req)
	return nil
}

func (cit *createIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterIndexTask struct {
	baseTask
	Condition
	req        *milvuspb.AlterIndexRequest
	ctx        context.Context
	datacoord  types.DataCoordClient
	querycoord types.QueryCoordClient
	result     *commonpb.Status

	replicateMsgStream msgstream.MsgStream

	collectionID UniqueID
}

func (t *alterIndexTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterIndexTask) ID() UniqueID {
	return t.req.GetBase().GetMsgID()
}

func (t *alterIndexTask) SetID(uid UniqueID) {
	t.req.GetBase().MsgID = uid
}

func (t *alterIndexTask) Name() string {
	return CreateIndexTaskName
}

func (t *alterIndexTask) Type() commonpb.MsgType {
	return t.req.GetBase().GetMsgType()
}

func (t *alterIndexTask) BeginTs() Timestamp {
	return t.req.GetBase().GetTimestamp()
}

func (t *alterIndexTask) EndTs() Timestamp {
	return t.req.GetBase().GetTimestamp()
}

func (t *alterIndexTask) SetTs(ts Timestamp) {
	t.req.Base.Timestamp = ts
}

func (t *alterIndexTask) OnEnqueue() error {
	if t.req.Base == nil {
		t.req.Base = commonpbutil.NewMsgBase()
	}
	t.req.Base.MsgType = commonpb.MsgType_AlterIndex
	t.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *alterIndexTask) PreExecute(ctx context.Context) error {
	for _, param := range t.req.GetExtraParams() {
		if !indexparams.IsConfigableIndexParam(param.GetKey()) {
			return merr.WrapErrParameterInvalidMsg("%s is not configable index param", param.GetKey())
		}
	}

	collName := t.req.GetCollectionName()

	collection, err := globalMetaCache.GetCollectionID(ctx, t.req.GetDbName(), collName)
	if err != nil {
		return err
	}
	t.collectionID = collection

	if len(t.req.GetIndexName()) == 0 {
		return merr.WrapErrParameterInvalidMsg("index name is empty")
	}

	if err = validateIndexName(t.req.GetIndexName()); err != nil {
		return err
	}

	// TODO fubang should implement it when the alter index is reconstructed
	// typeParams := funcutil.KeyValuePair2Map(t.req.GetExtraParams())
	// if err = ValidateAutoIndexMmapConfig(typeParams); err != nil {
	// 	return err
	// }

	loaded, err := isCollectionLoaded(ctx, t.querycoord, collection)
	if err != nil {
		return err
	}
	if loaded {
		return merr.WrapErrCollectionLoaded(collName, "can't alter index on loaded collection, please release the collection first")
	}

	return nil
}

func (t *alterIndexTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(
		zap.String("collection", t.req.GetCollectionName()),
		zap.String("indexName", t.req.GetIndexName()),
		zap.Any("params", t.req.GetExtraParams()),
	)

	log.Info("alter index")

	var err error
	req := &indexpb.AlterIndexRequest{
		CollectionID: t.collectionID,
		IndexName:    t.req.GetIndexName(),
		Params:       t.req.GetExtraParams(),
	}
	t.result, err = t.datacoord.AlterIndex(ctx, req)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.req)
	return nil
}

func (t *alterIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeIndexTask struct {
	baseTask
	Condition
	*milvuspb.DescribeIndexRequest
	ctx       context.Context
	datacoord types.DataCoordClient
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
	dit.Base.MsgType = commonpb.MsgType_DescribeIndex
	dit.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dit *describeIndexTask) PreExecute(ctx context.Context) error {
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

	resp, err := dit.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: dit.collectionID, IndexName: dit.IndexName, Timestamp: dit.Timestamp})
	if err != nil {
		return err
	}

	dit.result = &milvuspb.DescribeIndexResponse{}
	dit.result.Status = resp.GetStatus()
	err = merr.Error(resp.GetStatus())
	if err != nil {
		if errors.Is(err, merr.ErrIndexNotFound) && len(dit.GetIndexName()) == 0 {
			err = merr.WrapErrIndexNotFoundForCollection(dit.GetCollectionName())
			dit.result.Status = merr.Status(err)
		}
		return err
	}
	for _, indexInfo := range resp.IndexInfos {
		field, err := schema.schemaHelper.GetFieldFromID(indexInfo.FieldID)
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
	baseTask
	Condition
	*milvuspb.GetIndexStatisticsRequest
	ctx       context.Context
	datacoord types.DataCoordClient
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
	dit.Base.MsgType = commonpb.MsgType_GetIndexStatistics
	dit.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dit *getIndexStatisticsTask) PreExecute(ctx context.Context) error {
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
	schemaHelper := schema.schemaHelper

	resp, err := dit.datacoord.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{
		CollectionID: dit.collectionID, IndexName: dit.IndexName,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	dit.result = &milvuspb.GetIndexStatisticsResponse{}
	dit.result.Status = resp.GetStatus()
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
	baseTask
	Condition
	ctx context.Context
	*milvuspb.DropIndexRequest
	dataCoord  types.DataCoordClient
	queryCoord types.QueryCoordClient
	result     *commonpb.Status

	collectionID UniqueID

	replicateMsgStream msgstream.MsgStream
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
	if dit.Base == nil {
		dit.Base = commonpbutil.NewMsgBase()
	}
	dit.Base.MsgType = commonpb.MsgType_DropIndex
	dit.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dit *dropIndexTask) PreExecute(ctx context.Context) error {
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
	ctxLog := log.Ctx(ctx)
	ctxLog.Info("proxy drop index", zap.Int64("collID", dit.collectionID),
		zap.String("field_name", dit.FieldName),
		zap.String("index_name", dit.IndexName),
		zap.String("db_name", dit.DbName),
	)

	var err error
	dit.result, err = dit.dataCoord.DropIndex(ctx, &indexpb.DropIndexRequest{
		CollectionID: dit.collectionID,
		PartitionIDs: nil,
		IndexName:    dit.IndexName,
		DropAll:      false,
	})
	if err = merr.CheckRPCCall(dit.result, err); err != nil {
		ctxLog.Warn("drop index failed", zap.Error(err))
		return err
	}
	SendReplicateMessagePack(ctx, dit.replicateMsgStream, dit.DropIndexRequest)
	return nil
}

func (dit *dropIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

// Deprecated: use describeIndexTask instead
type getIndexBuildProgressTask struct {
	baseTask
	Condition
	*milvuspb.GetIndexBuildProgressRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	dataCoord types.DataCoordClient
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
	gibpt.Base.MsgType = commonpb.MsgType_GetIndexBuildProgress
	gibpt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (gibpt *getIndexBuildProgressTask) PreExecute(ctx context.Context) error {
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

	resp, err := gibpt.dataCoord.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{
		CollectionID: collectionID,
		IndexName:    gibpt.IndexName,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
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
	baseTask
	Condition
	*milvuspb.GetIndexStateRequest
	ctx       context.Context
	dataCoord types.DataCoordClient
	rootCoord types.RootCoordClient
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
	gist.Base.MsgType = commonpb.MsgType_GetIndexState
	gist.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (gist *getIndexStateTask) PreExecute(ctx context.Context) error {
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
	if err = merr.CheckRPCCall(state, err); err != nil {
		return err
	}

	gist.result = &milvuspb.GetIndexStateResponse{
		Status:     merr.Success(),
		State:      state.GetState(),
		FailReason: state.GetFailReason(),
	}
	return nil
}

func (gist *getIndexStateTask) PostExecute(ctx context.Context) error {
	return nil
}
