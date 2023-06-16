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
	"math"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	IgnoreGrowingKey                = "ignore_growing"
	IterationExtensionReduceRateKey = "iteration_extension_reduce_rate"
	AnnsFieldKey                    = "anns_field"
	TopKKey                         = "topk"
	NQKey                           = "nq"
	MetricTypeKey                   = common.MetricTypeKey
	SearchParamsKey                 = "params"
	RoundDecimalKey                 = "round_decimal"
	OffsetKey                       = "offset"
	LimitKey                        = "limit"

	InsertTaskName                = "InsertTask"
	CreateCollectionTaskName      = "CreateCollectionTask"
	DropCollectionTaskName        = "DropCollectionTask"
	HasCollectionTaskName         = "HasCollectionTask"
	DescribeCollectionTaskName    = "DescribeCollectionTask"
	ShowCollectionTaskName        = "ShowCollectionTask"
	CreatePartitionTaskName       = "CreatePartitionTask"
	DropPartitionTaskName         = "DropPartitionTask"
	HasPartitionTaskName          = "HasPartitionTask"
	ShowPartitionTaskName         = "ShowPartitionTask"
	FlushTaskName                 = "FlushTask"
	LoadCollectionTaskName        = "LoadCollectionTask"
	ReleaseCollectionTaskName     = "ReleaseCollectionTask"
	LoadPartitionTaskName         = "LoadPartitionsTask"
	ReleasePartitionTaskName      = "ReleasePartitionsTask"
	DeleteTaskName                = "DeleteTask"
	CreateAliasTaskName           = "CreateAliasTask"
	DropAliasTaskName             = "DropAliasTask"
	AlterAliasTaskName            = "AlterAliasTask"
	AlterCollectionTaskName       = "AlterCollectionTask"
	UpsertTaskName                = "UpsertTask"
	CreateResourceGroupTaskName   = "CreateResourceGroupTask"
	DropResourceGroupTaskName     = "DropResourceGroupTask"
	TransferNodeTaskName          = "TransferNodeTask"
	TransferReplicaTaskName       = "TransferReplicaTask"
	ListResourceGroupsTaskName    = "ListResourceGroupsTask"
	DescribeResourceGroupTaskName = "DescribeResourceGroupTask"

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
	setChannels() error
	getChannels() []pChan
}

type BaseInsertTask = msgstream.InsertMsg

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
	cct.Base = commonpbutil.NewMsgBase()
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cct *createCollectionTask) validatePartitionKey() error {
	idx := -1
	for i, field := range cct.schema.Fields {
		if field.GetIsPartitionKey() {
			if idx != -1 {
				return fmt.Errorf("there are more than one partition key, field name = %s, %s", cct.schema.Fields[idx].Name, field.Name)
			}

			if field.GetIsPrimaryKey() {
				return errors.New("the partition key field must not be primary field")
			}

			// The type of the partition key field can only be int64 and varchar
			if field.DataType != schemapb.DataType_Int64 && field.DataType != schemapb.DataType_VarChar {
				return errors.New("the data type of partition key should be Int64 or VarChar")
			}

			if cct.GetNumPartitions() < 0 {
				return errors.New("the specified partitions should be greater than 0 if partition key is used")
			}

			// set default physical partitions num if enable partition key mode
			if cct.GetNumPartitions() == 0 {
				cct.NumPartitions = common.DefaultPartitionsWithPartitionKey
			}

			idx = i
		}
	}

	if idx == -1 {
		if cct.GetNumPartitions() != 0 {
			return fmt.Errorf("num_partitions should only be specified with partition key field enabled")
		}
	} else {
		log.Info("create collection with partition key mode",
			zap.String("collectionName", cct.CollectionName),
			zap.Int64("numDefaultPartitions", cct.GetNumPartitions()))
	}

	return nil
}

func (cct *createCollectionTask) PreExecute(ctx context.Context) error {
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = paramtable.GetNodeID()

	cct.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(cct.Schema, cct.schema)
	if err != nil {
		return err
	}
	cct.schema.AutoID = false

	if cct.ShardsNum > Params.ProxyCfg.MaxShardNum.GetAsInt32() {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum.GetAsInt())
	}

	if len(cct.schema.Fields) > Params.ProxyCfg.MaxFieldNum.GetAsInt() {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum.GetAsInt())
	}

	// validate collection name
	if err := validateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	// validate whether field names duplicates
	if err := validateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	// validate primary key definition
	if err := validatePrimaryKey(cct.schema); err != nil {
		return err
	}

	// validate dynamic field
	if err := validateDynamicField(cct.schema); err != nil {
		return err
	}

	// validate auto id definition
	if err := ValidateFieldAutoID(cct.schema); err != nil {
		return err
	}

	// validate field type definition
	if err := validateFieldType(cct.schema); err != nil {
		return err
	}

	// validate partition key mode
	if err := cct.validatePartitionKey(); err != nil {
		return err
	}

	for _, field := range cct.schema.Fields {
		// validate field name
		if err := validateFieldName(field.Name); err != nil {
			return err
		}
		// validate vector field type parameters
		if field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector {
			err = validateDimension(field)
			if err != nil {
				return err
			}
		}
		// valid max length per row parameters
		// if max_length not specified, return error
		if field.DataType == schemapb.DataType_VarChar {
			err = validateMaxLengthPerRow(cct.schema.Name, field)
			if err != nil {
				return err
			}
		}
	}

	if err := validateMultipleVectorFields(cct.schema); err != nil {
		return err
	}

	cct.CreateCollectionRequest.Schema, err = proto.Marshal(cct.schema)
	if err != nil {
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
	dct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dct *dropCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DropCollection
	dct.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *dropCollectionTask) Execute(ctx context.Context) error {
	var err error
	dct.result, err = dct.rootCoord.DropCollection(ctx, dct.DropCollectionRequest)
	return err
}

func (dct *dropCollectionTask) PostExecute(ctx context.Context) error {
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
	hct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (hct *hasCollectionTask) PreExecute(ctx context.Context) error {
	hct.Base.MsgType = commonpb.MsgType_HasCollection
	hct.Base.SourceID = paramtable.GetNodeID()

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
	dct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dct *describeCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DescribeCollection
	dct.Base.SourceID = paramtable.GetNodeID()

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
		CollectionName:       dct.GetCollectionName(),
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
		dct.result.Schema.EnableDynamicField = result.Schema.EnableDynamicField
		dct.result.CollectionID = result.CollectionID
		dct.result.VirtualChannelNames = result.VirtualChannelNames
		dct.result.PhysicalChannelNames = result.PhysicalChannelNames
		dct.result.CreatedTimestamp = result.CreatedTimestamp
		dct.result.CreatedUtcTimestamp = result.CreatedUtcTimestamp
		dct.result.ShardsNum = result.ShardsNum
		dct.result.ConsistencyLevel = result.ConsistencyLevel
		dct.result.Aliases = result.Aliases
		dct.result.Properties = result.Properties
		dct.result.NumPartitions = result.NumPartitions
		for _, field := range result.Schema.Fields {
			if field.IsDynamic {
				continue
			}
			if field.FieldID >= common.StartOfUserFieldID {
				dct.result.Schema.Fields = append(dct.result.Schema.Fields, &schemapb.FieldSchema{
					FieldID:        field.FieldID,
					Name:           field.Name,
					IsPrimaryKey:   field.IsPrimaryKey,
					AutoID:         field.AutoID,
					Description:    field.Description,
					DataType:       field.DataType,
					TypeParams:     field.TypeParams,
					IndexParams:    field.IndexParams,
					IsDynamic:      field.IsDynamic,
					IsPartitionKey: field.IsPartitionKey,
					DefaultValue:   field.DefaultValue,
				})
			}
		}
	}
	return nil
}

func (dct *describeCollectionTask) PostExecute(ctx context.Context) error {
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
	sct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (sct *showCollectionsTask) PreExecute(ctx context.Context) error {
	sct.Base.MsgType = commonpb.MsgType_ShowCollections
	sct.Base.SourceID = paramtable.GetNodeID()
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
			Base: commonpbutil.UpdateMsgBase(
				sct.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
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
			// update collectionID to collection name, and return new error info to sdk
			newErrorReason := resp.Status.Reason
			for _, collectionID := range collectionIDs {
				newErrorReason = ReplaceID2Name(newErrorReason, collectionID, IDs2Names[collectionID])
			}
			return errors.New(newErrorReason)
		}

		sct.result = &milvuspb.ShowCollectionsResponse{
			Status:                resp.Status,
			CollectionNames:       make([]string, 0, len(resp.CollectionIDs)),
			CollectionIds:         make([]int64, 0, len(resp.CollectionIDs)),
			CreatedTimestamps:     make([]uint64, 0, len(resp.CollectionIDs)),
			CreatedUtcTimestamps:  make([]uint64, 0, len(resp.CollectionIDs)),
			InMemoryPercentages:   make([]int64, 0, len(resp.CollectionIDs)),
			QueryServiceAvailable: make([]bool, 0, len(resp.CollectionIDs)),
		}

		for offset, id := range resp.CollectionIDs {
			collectionName, ok := IDs2Names[id]
			if !ok {
				log.Debug("Failed to get collection info. This collection may be not released",
					zap.Any("collectionID", id),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				continue
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
			sct.result.QueryServiceAvailable = append(sct.result.QueryServiceAvailable, resp.QueryServiceAvailable[offset])
		}
	} else {
		sct.result = respFromRootCoord
	}

	return nil
}

func (sct *showCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterCollectionTask struct {
	Condition
	*milvuspb.AlterCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (act *alterCollectionTask) TraceCtx() context.Context {
	return act.ctx
}

func (act *alterCollectionTask) ID() UniqueID {
	return act.Base.MsgID
}

func (act *alterCollectionTask) SetID(uid UniqueID) {
	act.Base.MsgID = uid
}

func (act *alterCollectionTask) Name() string {
	return AlterCollectionTaskName
}

func (act *alterCollectionTask) Type() commonpb.MsgType {
	return act.Base.MsgType
}

func (act *alterCollectionTask) BeginTs() Timestamp {
	return act.Base.Timestamp
}

func (act *alterCollectionTask) EndTs() Timestamp {
	return act.Base.Timestamp
}

func (act *alterCollectionTask) SetTs(ts Timestamp) {
	act.Base.Timestamp = ts
}

func (act *alterCollectionTask) OnEnqueue() error {
	act.Base = commonpbutil.NewMsgBase()
	return nil
}

func (act *alterCollectionTask) PreExecute(ctx context.Context) error {
	act.Base.MsgType = commonpb.MsgType_AlterCollection
	act.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (act *alterCollectionTask) Execute(ctx context.Context) error {
	var err error
	act.result, err = act.rootCoord.AlterCollection(ctx, act.AlterCollectionRequest)
	return err
}

func (act *alterCollectionTask) PostExecute(ctx context.Context) error {
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
	cpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (cpt *createPartitionTask) PreExecute(ctx context.Context) error {
	cpt.Base.MsgType = commonpb.MsgType_CreatePartition
	cpt.Base.SourceID = paramtable.GetNodeID()

	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable create partition if partition key mode is used")
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *createPartitionTask) Execute(ctx context.Context) (err error) {
	cpt.result, err = cpt.rootCoord.CreatePartition(ctx, cpt.CreatePartitionRequest)
	if err != nil {
		return err
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
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *commonpb.Status
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
	dpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (dpt *dropPartitionTask) PreExecute(ctx context.Context) error {
	dpt.Base.MsgType = commonpb.MsgType_DropPartition
	dpt.Base.SourceID = paramtable.GetNodeID()

	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable drop partition if partition key mode is used")
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, dpt.GetCollectionName())
	if err != nil {
		return err
	}
	partID, err := globalMetaCache.GetPartitionID(ctx, dpt.GetCollectionName(), dpt.GetPartitionName())
	if err != nil {
		if errors.Is(merr.ErrPartitionNotFound, err) {
			return nil
		}
		return err
	}

	collLoaded, err := isCollectionLoaded(ctx, dpt.queryCoord, collID)
	if err != nil {
		return err
	}
	if collLoaded {
		loaded, err := isPartitionLoaded(ctx, dpt.queryCoord, collID, []int64{partID})
		if err != nil {
			return err
		}
		if loaded {
			return errors.New("partition cannot be dropped, partition is loaded, please release it first")
		}
	}

	return nil
}

func (dpt *dropPartitionTask) Execute(ctx context.Context) (err error) {
	dpt.result, err = dpt.rootCoord.DropPartition(ctx, dpt.DropPartitionRequest)
	if err != nil {
		return err
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
	hpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (hpt *hasPartitionTask) PreExecute(ctx context.Context) error {
	hpt.Base.MsgType = commonpb.MsgType_HasPartition
	hpt.Base.SourceID = paramtable.GetNodeID()

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
	if err != nil {
		return err
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
	spt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (spt *showPartitionsTask) PreExecute(ctx context.Context) error {
	spt.Base.MsgType = commonpb.MsgType_ShowPartitions
	spt.Base.SourceID = paramtable.GetNodeID()

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
			Base: commonpbutil.UpdateMsgBase(
				spt.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
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
	ft.Base = commonpbutil.NewMsgBase()
	return nil
}

func (ft *flushTask) PreExecute(ctx context.Context) error {
	ft.Base.MsgType = commonpb.MsgType_Flush
	ft.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ft *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	for _, collName := range ft.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: commonpbutil.UpdateMsgBase(
				ft.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			),
			DbID:         0,
			CollectionID: collID,
			IsImport:     false,
		}
		resp, err := ft.dataCoord.Flush(ctx, flushReq)
		if err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = resp.GetTimeOfSeal()
	}
	ft.result = &milvuspb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:          "",
		CollSegIDs:      coll2Segments,
		FlushCollSegIDs: flushColl2Segments,
		CollSealTimes:   coll2SealTimes,
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
	datacoord  types.DataCoord
	result     *commonpb.Status

	collectionID UniqueID
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
	lct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (lct *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Ctx(ctx).Debug("loadCollectionTask PreExecute",
		zap.String("role", typeutil.ProxyRole))
	lct.Base.MsgType = commonpb.MsgType_LoadCollection
	lct.Base.SourceID = paramtable.GetNodeID()

	collName := lct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	// To compat with LoadCollcetion before Milvus@2.1
	if lct.ReplicaNumber == 0 {
		lct.ReplicaNumber = 1
	}

	return nil
}

func (lct *loadCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("collectionID", collID))

	log.Debug("loadCollectionTask Execute")
	if err != nil {
		return err
	}

	lct.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lct.CollectionName)
	if err != nil {
		return err
	}
	// check index
	indexResponse, err := lct.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collID,
		IndexName:    "",
	})
	if err != nil {
		return err
	}
	if indexResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(indexResponse.Status.Reason)
	}

	hasVecIndex := false
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
		for _, field := range collSchema.Fields {
			if index.FieldID == field.FieldID && (field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector) {
				hasVecIndex = true
			}
		}
	}
	if !hasVecIndex {
		errMsg := fmt.Sprintf("there is no vector index on collection: %s, please create index firstly", lct.LoadCollectionRequest.CollectionName)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	request := &querypb.LoadCollectionRequest{
		Base: commonpbutil.UpdateMsgBase(
			lct.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_LoadCollection),
		),
		DbID:           0,
		CollectionID:   collID,
		Schema:         collSchema,
		ReplicaNumber:  lct.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        lct.Refresh,
		ResourceGroups: lct.ResourceGroups,
	}
	log.Debug("send LoadCollectionRequest to query coordinator",
		zap.Any("schema", request.Schema))
	lct.result, err = lct.queryCoord.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query coordinator LoadCollection: %s", err)
	}
	return nil
}

func (lct *loadCollectionTask) PostExecute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)
	log.Ctx(ctx).Debug("loadCollectionTask PostExecute",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("collectionID", collID))
	if err != nil {
		return err
	}
	return nil
}

type releaseCollectionTask struct {
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
	chMgr      channelsMgr

	collectionID UniqueID
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
	rct.Base = commonpbutil.NewMsgBase()
	return nil
}

func (rct *releaseCollectionTask) PreExecute(ctx context.Context) error {
	rct.Base.MsgType = commonpb.MsgType_ReleaseCollection
	rct.Base.SourceID = paramtable.GetNodeID()

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
	rct.collectionID = collID
	request := &querypb.ReleaseCollectionRequest{
		Base: commonpbutil.UpdateMsgBase(
			rct.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_ReleaseCollection),
		),
		DbID:         0,
		CollectionID: collID,
	}

	rct.result, err = rct.queryCoord.ReleaseCollection(ctx, request)

	globalMetaCache.RemoveCollection(ctx, rct.CollectionName)

	return err
}

func (rct *releaseCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.DeprecateShardCache(rct.CollectionName)
	return nil
}

type loadPartitionsTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	datacoord  types.DataCoord
	result     *commonpb.Status

	collectionID UniqueID
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
	lpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (lpt *loadPartitionsTask) PreExecute(ctx context.Context) error {
	lpt.Base.MsgType = commonpb.MsgType_LoadPartitions
	lpt.Base.SourceID = paramtable.GetNodeID()

	collName := lpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable load partitions if partition key mode is used")
	}

	return nil
}

func (lpt *loadPartitionsTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	lpt.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	// check index
	indexResponse, err := lpt.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collID,
		IndexName:    "",
	})
	if err != nil {
		return err
	}
	if indexResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(indexResponse.Status.Reason)
	}

	hasVecIndex := false
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
		for _, field := range collSchema.Fields {
			if index.FieldID == field.FieldID && (field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector) {
				hasVecIndex = true
			}
		}
	}
	if !hasVecIndex {
		errMsg := fmt.Sprintf("there is no vector index on collection: %s, please create index firstly", lpt.LoadPartitionsRequest.CollectionName)
		log.Ctx(ctx).Error(errMsg)
		return errors.New(errMsg)
	}
	for _, partitionName := range lpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, lpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	if len(partitionIDs) == 0 {
		return errors.New("failed to load partition, due to no partition specified")
	}
	request := &querypb.LoadPartitionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			lpt.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_LoadPartitions),
		),
		DbID:           0,
		CollectionID:   collID,
		PartitionIDs:   partitionIDs,
		Schema:         collSchema,
		ReplicaNumber:  lpt.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        lpt.Refresh,
		ResourceGroups: lpt.ResourceGroups,
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

	collectionID UniqueID
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
	rpt.Base = commonpbutil.NewMsgBase()
	return nil
}

func (rpt *releasePartitionsTask) PreExecute(ctx context.Context) error {
	rpt.Base.MsgType = commonpb.MsgType_ReleasePartitions
	rpt.Base.SourceID = paramtable.GetNodeID()

	collName := rpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable release partitions if partition key mode is used")
	}

	return nil
}

func (rpt *releasePartitionsTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, rpt.CollectionName)
	if err != nil {
		return err
	}
	rpt.collectionID = collID
	for _, partitionName := range rpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, rpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			rpt.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions),
		),
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	rpt.result, err = rpt.queryCoord.ReleasePartitions(ctx, request)
	return err
}

func (rpt *releasePartitionsTask) PostExecute(ctx context.Context) error {
	globalMetaCache.DeprecateShardCache(rpt.CollectionName)
	return nil
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
	c.Base = commonpbutil.NewMsgBase()
	return nil
}

// PreExecute defines the action before task execution
func (c *CreateAliasTask) PreExecute(ctx context.Context) error {
	c.Base.MsgType = commonpb.MsgType_CreateAlias
	c.Base.SourceID = paramtable.GetNodeID()

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

// TraceCtx returns the context for trace
func (d *DropAliasTask) TraceCtx() context.Context {
	return d.ctx
}

// ID returns the MsgID
func (d *DropAliasTask) ID() UniqueID {
	return d.Base.MsgID
}

// SetID sets the MsgID
func (d *DropAliasTask) SetID(uid UniqueID) {
	d.Base.MsgID = uid
}

// Name returns the name of the task
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
	d.Base = commonpbutil.NewMsgBase()
	return nil
}

func (d *DropAliasTask) PreExecute(ctx context.Context) error {
	d.Base.MsgType = commonpb.MsgType_DropAlias
	d.Base.SourceID = paramtable.GetNodeID()
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
	a.Base = commonpbutil.NewMsgBase()
	return nil
}

func (a *AlterAliasTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_AlterAlias
	a.Base.SourceID = paramtable.GetNodeID()

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

type CreateResourceGroupTask struct {
	Condition
	*milvuspb.CreateResourceGroupRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (t *CreateResourceGroupTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *CreateResourceGroupTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *CreateResourceGroupTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *CreateResourceGroupTask) Name() string {
	return CreateResourceGroupTaskName
}

func (t *CreateResourceGroupTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *CreateResourceGroupTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *CreateResourceGroupTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *CreateResourceGroupTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *CreateResourceGroupTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *CreateResourceGroupTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_CreateResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *CreateResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.queryCoord.CreateResourceGroup(ctx, t.CreateResourceGroupRequest)
	return err
}

func (t *CreateResourceGroupTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropResourceGroupTask struct {
	Condition
	*milvuspb.DropResourceGroupRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (t *DropResourceGroupTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *DropResourceGroupTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *DropResourceGroupTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *DropResourceGroupTask) Name() string {
	return DropResourceGroupTaskName
}

func (t *DropResourceGroupTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *DropResourceGroupTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropResourceGroupTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropResourceGroupTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *DropResourceGroupTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *DropResourceGroupTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DropResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *DropResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.queryCoord.DropResourceGroup(ctx, t.DropResourceGroupRequest)
	return err
}

func (t *DropResourceGroupTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeResourceGroupTask struct {
	Condition
	*milvuspb.DescribeResourceGroupRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *milvuspb.DescribeResourceGroupResponse
}

func (t *DescribeResourceGroupTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *DescribeResourceGroupTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *DescribeResourceGroupTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *DescribeResourceGroupTask) Name() string {
	return DescribeResourceGroupTaskName
}

func (t *DescribeResourceGroupTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *DescribeResourceGroupTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DescribeResourceGroupTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DescribeResourceGroupTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *DescribeResourceGroupTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *DescribeResourceGroupTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DescribeResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *DescribeResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	resp, err := t.queryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
		ResourceGroup: t.ResourceGroup,
	})

	if err != nil {
		return err
	}

	getCollectionNameFunc := func(value int32, key int64) string {
		name, err := globalMetaCache.GetCollectionName(ctx, key)
		if err != nil {
			// unreachable logic path
			return "unavailable_collection"
		}
		return name
	}

	if resp.Status.ErrorCode == commonpb.ErrorCode_Success {
		rgInfo := resp.GetResourceGroup()

		loadReplicas := lo.MapKeys(rgInfo.NumLoadedReplica, getCollectionNameFunc)
		outgoingNodes := lo.MapKeys(rgInfo.NumOutgoingNode, getCollectionNameFunc)
		incomingNodes := lo.MapKeys(rgInfo.NumIncomingNode, getCollectionNameFunc)

		t.result = &milvuspb.DescribeResourceGroupResponse{
			Status: resp.Status,
			ResourceGroup: &milvuspb.ResourceGroup{
				Name:             rgInfo.GetName(),
				Capacity:         rgInfo.GetCapacity(),
				NumAvailableNode: rgInfo.NumAvailableNode,
				NumLoadedReplica: loadReplicas,
				NumOutgoingNode:  outgoingNodes,
				NumIncomingNode:  incomingNodes,
			},
		}
	} else {
		t.result = &milvuspb.DescribeResourceGroupResponse{
			Status: resp.Status,
		}
	}

	return nil
}

func (t *DescribeResourceGroupTask) PostExecute(ctx context.Context) error {
	return nil
}

type TransferNodeTask struct {
	Condition
	*milvuspb.TransferNodeRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (t *TransferNodeTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *TransferNodeTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *TransferNodeTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *TransferNodeTask) Name() string {
	return TransferNodeTaskName
}

func (t *TransferNodeTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *TransferNodeTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *TransferNodeTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *TransferNodeTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *TransferNodeTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *TransferNodeTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_TransferNode
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *TransferNodeTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.queryCoord.TransferNode(ctx, t.TransferNodeRequest)
	return err
}

func (t *TransferNodeTask) PostExecute(ctx context.Context) error {
	return nil
}

type TransferReplicaTask struct {
	Condition
	*milvuspb.TransferReplicaRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
}

func (t *TransferReplicaTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *TransferReplicaTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *TransferReplicaTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *TransferReplicaTask) Name() string {
	return TransferReplicaTaskName
}

func (t *TransferReplicaTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *TransferReplicaTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *TransferReplicaTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *TransferReplicaTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *TransferReplicaTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *TransferReplicaTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_TransferReplica
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *TransferReplicaTask) Execute(ctx context.Context) error {
	var err error
	collID, err := globalMetaCache.GetCollectionID(ctx, t.CollectionName)
	if err != nil {
		return err
	}
	t.result, err = t.queryCoord.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: t.SourceResourceGroup,
		TargetResourceGroup: t.TargetResourceGroup,
		CollectionID:        collID,
		NumReplica:          t.NumReplica,
	})
	return err
}

func (t *TransferReplicaTask) PostExecute(ctx context.Context) error {
	return nil
}

type ListResourceGroupsTask struct {
	Condition
	*milvuspb.ListResourceGroupsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *milvuspb.ListResourceGroupsResponse
}

func (t *ListResourceGroupsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *ListResourceGroupsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *ListResourceGroupsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *ListResourceGroupsTask) Name() string {
	return ListResourceGroupsTaskName
}

func (t *ListResourceGroupsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *ListResourceGroupsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *ListResourceGroupsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *ListResourceGroupsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *ListResourceGroupsTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *ListResourceGroupsTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_ListResourceGroups
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *ListResourceGroupsTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.queryCoord.ListResourceGroups(ctx, t.ListResourceGroupsRequest)
	return err
}

func (t *ListResourceGroupsTask) PostExecute(ctx context.Context) error {
	return nil
}
