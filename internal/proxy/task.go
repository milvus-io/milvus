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
	IgnoreGrowingKey     = "ignore_growing"
	ReduceStopForBestKey = "reduce_stop_for_best"
	AnnsFieldKey         = "anns_field"
	TopKKey              = "topk"
	NQKey                = "nq"
	MetricTypeKey        = common.MetricTypeKey
	SearchParamsKey      = "params"
	RoundDecimalKey      = "round_decimal"
	OffsetKey            = "offset"
	LimitKey             = "limit"

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

	CreateDatabaseTaskName = "CreateCollectionTask"
	DropDatabaseTaskName   = "DropDatabaseTaskName"
	ListDatabaseTaskName   = "ListDatabaseTaskName"

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
	rootCoord types.RootCoordClient
	result    *commonpb.Status
	schema    *schemapb.CollectionSchema
}

func (t *createCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *createCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *createCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *createCollectionTask) Name() string {
	return CreateCollectionTaskName
}

func (t *createCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *createCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *createCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *createCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *createCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_CreateCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *createCollectionTask) validatePartitionKey() error {
	idx := -1
	for i, field := range t.schema.Fields {
		if field.GetIsPartitionKey() {
			if idx != -1 {
				return fmt.Errorf("there are more than one partition key, field name = %s, %s", t.schema.Fields[idx].Name, field.Name)
			}

			if field.GetIsPrimaryKey() {
				return errors.New("the partition key field must not be primary field")
			}

			// The type of the partition key field can only be int64 and varchar
			if field.DataType != schemapb.DataType_Int64 && field.DataType != schemapb.DataType_VarChar {
				return errors.New("the data type of partition key should be Int64 or VarChar")
			}

			if t.GetNumPartitions() < 0 {
				return errors.New("the specified partitions should be greater than 0 if partition key is used")
			}

			// set default physical partitions num if enable partition key mode
			if t.GetNumPartitions() == 0 {
				t.NumPartitions = common.DefaultPartitionsWithPartitionKey
			}

			idx = i
		}
	}

	if idx == -1 {
		if t.GetNumPartitions() != 0 {
			return fmt.Errorf("num_partitions should only be specified with partition key field enabled")
		}
	} else {
		log.Info("create collection with partition key mode",
			zap.String("collectionName", t.CollectionName),
			zap.Int64("numDefaultPartitions", t.GetNumPartitions()))
	}

	return nil
}

func (t *createCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_CreateCollection
	t.Base.SourceID = paramtable.GetNodeID()

	t.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(t.Schema, t.schema)
	if err != nil {
		return err
	}
	t.schema.AutoID = false

	if t.ShardsNum > Params.ProxyCfg.MaxShardNum.GetAsInt32() {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum.GetAsInt())
	}

	if len(t.schema.Fields) > Params.ProxyCfg.MaxFieldNum.GetAsInt() {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum.GetAsInt())
	}

	// validate collection name
	if err := validateCollectionName(t.schema.Name); err != nil {
		return err
	}

	// validate whether field names duplicates
	if err := validateDuplicatedFieldName(t.schema.Fields); err != nil {
		return err
	}

	// validate primary key definition
	if err := validatePrimaryKey(t.schema); err != nil {
		return err
	}

	// validate dynamic field
	if err := validateDynamicField(t.schema); err != nil {
		return err
	}

	// validate auto id definition
	if err := ValidateFieldAutoID(t.schema); err != nil {
		return err
	}

	// validate field type definition
	if err := validateFieldType(t.schema); err != nil {
		return err
	}

	// validate partition key mode
	if err := t.validatePartitionKey(); err != nil {
		return err
	}

	for _, field := range t.schema.Fields {
		// validate field name
		if err := validateFieldName(field.Name); err != nil {
			return err
		}
		// validate vector field type parameters
		if isVectorType(field.DataType) {
			err = validateDimension(field)
			if err != nil {
				return err
			}
		}
		// valid max length per row parameters
		// if max_length not specified, return error
		if field.DataType == schemapb.DataType_VarChar ||
			(field.GetDataType() == schemapb.DataType_Array && field.GetElementType() == schemapb.DataType_VarChar) {
			err = validateMaxLengthPerRow(t.schema.Name, field)
			if err != nil {
				return err
			}
		}
		// valid max capacity for array per row parameters
		// if max_capacity not specified, return error
		if field.DataType == schemapb.DataType_Array {
			if err = validateMaxCapacityPerRow(t.schema.Name, field); err != nil {
				return err
			}
		}
	}

	if err := validateMultipleVectorFields(t.schema); err != nil {
		return err
	}

	t.CreateCollectionRequest.Schema, err = proto.Marshal(t.schema)
	if err != nil {
		return err
	}

	return nil
}

func (t *createCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.CreateCollection(ctx, t.CreateCollectionRequest)
	return err
}

func (t *createCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
}

func (t *dropCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *dropCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *dropCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *dropCollectionTask) Name() string {
	return DropCollectionTaskName
}

func (t *dropCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *dropCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *dropCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *dropCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DropCollection
	t.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(t.CollectionName); err != nil {
		return err
	}
	return nil
}

func (t *dropCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.DropCollection(ctx, t.DropCollectionRequest)
	return err
}

func (t *dropCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasCollectionTask struct {
	Condition
	*milvuspb.HasCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.BoolResponse
}

func (t *hasCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *hasCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *hasCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *hasCollectionTask) Name() string {
	return HasCollectionTaskName
}

func (t *hasCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *hasCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hasCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hasCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *hasCollectionTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *hasCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_HasCollection
	t.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(t.CollectionName); err != nil {
		return err
	}
	return nil
}

func (t *hasCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.HasCollection(ctx, t.HasCollectionRequest)
	if err != nil {
		return err
	}
	if t.result == nil {
		return errors.New("has collection resp is nil")
	}
	if t.result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(t.result.GetStatus())
	}
	return nil
}

func (t *hasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeCollectionTask struct {
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.DescribeCollectionResponse
}

func (t *describeCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *describeCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *describeCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *describeCollectionTask) Name() string {
	return DescribeCollectionTaskName
}

func (t *describeCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *describeCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *describeCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *describeCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *describeCollectionTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *describeCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DescribeCollection
	t.Base.SourceID = paramtable.GetNodeID()

	if t.CollectionID != 0 && len(t.CollectionName) == 0 {
		return nil
	}

	return validateCollectionName(t.CollectionName)
}

func (t *describeCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result = &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
		CollectionName:       t.GetCollectionName(),
		DbName:               t.GetDbName(),
	}

	result, err := t.rootCoord.DescribeCollection(ctx, t.DescribeCollectionRequest)
	if err != nil {
		return err
	}

	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		t.result.Status = result.Status

		// compatibility with PyMilvus existing implementation
		err := merr.Error(t.result.GetStatus())
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// nolint
			t.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			// nolint
			t.result.Status.Reason = "can't find collection " + t.result.GetStatus().GetReason()
		}
	} else {
		t.result.Schema.Name = result.Schema.Name
		t.result.Schema.Description = result.Schema.Description
		t.result.Schema.AutoID = result.Schema.AutoID
		t.result.Schema.EnableDynamicField = result.Schema.EnableDynamicField
		t.result.CollectionID = result.CollectionID
		t.result.VirtualChannelNames = result.VirtualChannelNames
		t.result.PhysicalChannelNames = result.PhysicalChannelNames
		t.result.CreatedTimestamp = result.CreatedTimestamp
		t.result.CreatedUtcTimestamp = result.CreatedUtcTimestamp
		t.result.ShardsNum = result.ShardsNum
		t.result.ConsistencyLevel = result.ConsistencyLevel
		t.result.Aliases = result.Aliases
		t.result.Properties = result.Properties
		t.result.DbName = result.GetDbName()
		t.result.NumPartitions = result.NumPartitions
		for _, field := range result.Schema.Fields {
			if field.IsDynamic {
				continue
			}
			if field.FieldID >= common.StartOfUserFieldID {
				t.result.Schema.Fields = append(t.result.Schema.Fields, &schemapb.FieldSchema{
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
					ElementType:    field.ElementType,
				})
			}
		}
	}
	return nil
}

func (t *describeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showCollectionsTask struct {
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx        context.Context
	rootCoord  types.RootCoordClient
	queryCoord types.QueryCoordClient
	result     *milvuspb.ShowCollectionsResponse
}

func (t *showCollectionsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *showCollectionsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *showCollectionsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *showCollectionsTask) Name() string {
	return ShowCollectionTaskName
}

func (t *showCollectionsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *showCollectionsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *showCollectionsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *showCollectionsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *showCollectionsTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *showCollectionsTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_ShowCollections
	t.Base.SourceID = paramtable.GetNodeID()
	if t.GetType() == milvuspb.ShowType_InMemory {
		for _, collectionName := range t.CollectionNames {
			if err := validateCollectionName(collectionName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *showCollectionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := t.rootCoord.ShowCollections(ctx, t.ShowCollectionsRequest)
	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show collections")
	}

	if respFromRootCoord.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(respFromRootCoord.GetStatus())
	}

	if t.GetType() == milvuspb.ShowType_InMemory {
		IDs2Names := make(map[UniqueID]string)
		for offset, collectionName := range respFromRootCoord.CollectionNames {
			collectionID := respFromRootCoord.CollectionIds[offset]
			IDs2Names[collectionID] = collectionName
		}
		collectionIDs := make([]UniqueID, 0)
		for _, collectionName := range t.CollectionNames {
			collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), collectionName)
			if err != nil {
				log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			collectionIDs = append(collectionIDs, collectionID)
			IDs2Names[collectionID] = collectionName
		}

		resp, err := t.queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
			// DbID: t.ShowCollectionsRequest.DbName,
			CollectionIDs: collectionIDs,
		})
		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show collections")
		}

		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			// update collectionID to collection name, and return new error info to sdk
			newErrorReason := resp.GetStatus().GetReason()
			for _, collectionID := range collectionIDs {
				newErrorReason = ReplaceID2Name(newErrorReason, collectionID, IDs2Names[collectionID])
			}
			return errors.New(newErrorReason)
		}

		t.result = &milvuspb.ShowCollectionsResponse{
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
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showCollections"))
				continue
			}
			collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, t.GetDbName(), collectionName, id)
			if err != nil {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			t.result.CollectionIds = append(t.result.CollectionIds, id)
			t.result.CollectionNames = append(t.result.CollectionNames, collectionName)
			t.result.CreatedTimestamps = append(t.result.CreatedTimestamps, collectionInfo.createdTimestamp)
			t.result.CreatedUtcTimestamps = append(t.result.CreatedUtcTimestamps, collectionInfo.createdUtcTimestamp)
			t.result.InMemoryPercentages = append(t.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
			t.result.QueryServiceAvailable = append(t.result.QueryServiceAvailable, resp.QueryServiceAvailable[offset])
		}
	} else {
		t.result = respFromRootCoord
	}

	return nil
}

func (t *showCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterCollectionTask struct {
	Condition
	*milvuspb.AlterCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
}

func (t *alterCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *alterCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *alterCollectionTask) Name() string {
	return AlterCollectionTaskName
}

func (t *alterCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *alterCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *alterCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *alterCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_AlterCollection
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *alterCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.AlterCollection(ctx, t.AlterCollectionRequest)
	return err
}

func (t *alterCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type createPartitionTask struct {
	Condition
	*milvuspb.CreatePartitionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
}

func (t *createPartitionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *createPartitionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *createPartitionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *createPartitionTask) Name() string {
	return CreatePartitionTaskName
}

func (t *createPartitionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *createPartitionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *createPartitionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *createPartitionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *createPartitionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *createPartitionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_CreatePartition
	t.Base.SourceID = paramtable.GetNodeID()

	collName, partitionTag := t.CollectionName, t.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), collName)
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

func (t *createPartitionTask) Execute(ctx context.Context) (err error) {
	t.result, err = t.rootCoord.CreatePartition(ctx, t.CreatePartitionRequest)
	if err != nil {
		return err
	}
	if t.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(t.result.Reason)
	}
	return err
}

func (t *createPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropPartitionTask struct {
	Condition
	*milvuspb.DropPartitionRequest
	ctx        context.Context
	rootCoord  types.RootCoordClient
	queryCoord types.QueryCoordClient
	result     *commonpb.Status
}

func (t *dropPartitionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *dropPartitionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *dropPartitionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *dropPartitionTask) Name() string {
	return DropPartitionTaskName
}

func (t *dropPartitionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *dropPartitionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropPartitionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropPartitionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *dropPartitionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *dropPartitionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DropPartition
	t.Base.SourceID = paramtable.GetNodeID()

	collName, partitionTag := t.CollectionName, t.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable drop partition if partition key mode is used")
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		return err
	}
	partID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), t.GetCollectionName(), t.GetPartitionName())
	if err != nil {
		if errors.Is(merr.ErrPartitionNotFound, err) {
			return nil
		}
		return err
	}

	collLoaded, err := isCollectionLoaded(ctx, t.queryCoord, collID)
	if err != nil {
		return err
	}
	if collLoaded {
		loaded, err := isPartitionLoaded(ctx, t.queryCoord, collID, []int64{partID})
		if err != nil {
			return err
		}
		if loaded {
			return errors.New("partition cannot be dropped, partition is loaded, please release it first")
		}
	}

	return nil
}

func (t *dropPartitionTask) Execute(ctx context.Context) (err error) {
	t.result, err = t.rootCoord.DropPartition(ctx, t.DropPartitionRequest)
	if err != nil {
		return err
	}
	if t.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(t.result.Reason)
	}
	return err
}

func (t *dropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasPartitionTask struct {
	Condition
	*milvuspb.HasPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.BoolResponse
}

func (t *hasPartitionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *hasPartitionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *hasPartitionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *hasPartitionTask) Name() string {
	return HasPartitionTaskName
}

func (t *hasPartitionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *hasPartitionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hasPartitionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hasPartitionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *hasPartitionTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *hasPartitionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_HasPartition
	t.Base.SourceID = paramtable.GetNodeID()

	collName, partitionTag := t.CollectionName, t.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (t *hasPartitionTask) Execute(ctx context.Context) (err error) {
	t.result, err = t.rootCoord.HasPartition(ctx, t.HasPartitionRequest)
	if err != nil {
		return err
	}
	if t.result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(t.result.GetStatus())
	}
	return err
}

func (t *hasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showPartitionsTask struct {
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx        context.Context
	rootCoord  types.RootCoordClient
	queryCoord types.QueryCoordClient
	result     *milvuspb.ShowPartitionsResponse
}

func (t *showPartitionsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *showPartitionsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *showPartitionsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *showPartitionsTask) Name() string {
	return ShowPartitionTaskName
}

func (t *showPartitionsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *showPartitionsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *showPartitionsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *showPartitionsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *showPartitionsTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	return nil
}

func (t *showPartitionsTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_ShowPartitions
	t.Base.SourceID = paramtable.GetNodeID()

	if err := validateCollectionName(t.CollectionName); err != nil {
		return err
	}

	if t.GetType() == milvuspb.ShowType_InMemory {
		for _, partitionName := range t.PartitionNames {
			if err := validatePartitionTag(partitionName, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *showPartitionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := t.rootCoord.ShowPartitions(ctx, t.ShowPartitionsRequest)
	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show partitions")
	}

	if respFromRootCoord.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(respFromRootCoord.GetStatus())
	}

	if t.GetType() == milvuspb.ShowType_InMemory {
		collectionName := t.CollectionName
		collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), collectionName)
		if err != nil {
			log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
				zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showPartitions"))
			return err
		}
		IDs2Names := make(map[UniqueID]string)
		for offset, partitionName := range respFromRootCoord.PartitionNames {
			partitionID := respFromRootCoord.PartitionIDs[offset]
			IDs2Names[partitionID] = partitionName
		}
		partitionIDs := make([]UniqueID, 0)
		for _, partitionName := range t.PartitionNames {
			partitionID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			partitionIDs = append(partitionIDs, partitionID)
			IDs2Names[partitionID] = partitionName
		}
		resp, err := t.queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
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

		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return merr.Error(resp.GetStatus())
		}

		t.result = &milvuspb.ShowPartitionsResponse{
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
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return errors.New("failed to show partitions")
			}
			partitionInfo, err := globalMetaCache.GetPartitionInfo(ctx, t.GetDbName(), collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			t.result.PartitionIDs = append(t.result.PartitionIDs, id)
			t.result.PartitionNames = append(t.result.PartitionNames, partitionName)
			t.result.CreatedTimestamps = append(t.result.CreatedTimestamps, partitionInfo.createdTimestamp)
			t.result.CreatedUtcTimestamps = append(t.result.CreatedUtcTimestamps, partitionInfo.createdUtcTimestamp)
			t.result.InMemoryPercentages = append(t.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
		}
	} else {
		t.result = respFromRootCoord
	}

	return nil
}

func (t *showPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type flushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoordClient
	result    *milvuspb.FlushResponse

	replicateMsgStream msgstream.MsgStream
}

func (t *flushTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *flushTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *flushTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *flushTask) Name() string {
	return FlushTaskName
}

func (t *flushTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *flushTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *flushTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *flushTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_Flush
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	coll2FlushTs := make(map[string]Timestamp)
	for _, collName := range t.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			),
			CollectionID: collID,
			IsImport:     false,
		}
		resp, err := t.dataCoord.Flush(ctx, flushReq)
		if err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return merr.Error(resp.GetStatus())
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = resp.GetTimeOfSeal()
		coll2FlushTs[collName] = resp.GetFlushTs()
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.FlushRequest)
	t.result = &milvuspb.FlushResponse{
		Status:          merr.Success(),
		DbName:          t.GetDbName(),
		CollSegIDs:      coll2Segments,
		FlushCollSegIDs: flushColl2Segments,
		CollSealTimes:   coll2SealTimes,
		CollFlushTs:     coll2FlushTs,
	}
	return nil
}

func (t *flushTask) PostExecute(ctx context.Context) error {
	return nil
}

type loadCollectionTask struct {
	Condition
	*milvuspb.LoadCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoordClient
	datacoord  types.DataCoordClient
	result     *commonpb.Status

	collectionID       UniqueID
	replicateMsgStream msgstream.MsgStream
}

func (t *loadCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *loadCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *loadCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *loadCollectionTask) Name() string {
	return LoadCollectionTaskName
}

func (t *loadCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *loadCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *loadCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *loadCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *loadCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Ctx(ctx).Debug("loadCollectionTask PreExecute",
		zap.String("role", typeutil.ProxyRole))
	t.Base.MsgType = commonpb.MsgType_LoadCollection
	t.Base.SourceID = paramtable.GetNodeID()

	collName := t.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	// To compat with LoadCollcetion before Milvus@2.1
	if t.ReplicaNumber == 0 {
		t.ReplicaNumber = 1
	}

	return nil
}

func (t *loadCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("collectionID", collID))

	log.Debug("loadCollectionTask Execute")
	if err != nil {
		return err
	}

	t.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	// check index
	indexResponse, err := t.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collID,
		IndexName:    "",
	})
	if err == nil {
		err = merr.Error(indexResponse.GetStatus())
	}
	if err != nil {
		if errors.Is(err, merr.ErrIndexNotFound) {
			err = merr.WrapErrIndexNotFoundForCollection(t.GetCollectionName())
		}
		return err
	}

	hasVecIndex := false
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
		for _, field := range collSchema.Fields {
			if index.FieldID == field.FieldID && (field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_Float16Vector) {
				hasVecIndex = true
			}
		}
	}
	if !hasVecIndex {
		errMsg := fmt.Sprintf("there is no vector index on collection: %s, please create index firstly", t.LoadCollectionRequest.CollectionName)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	request := &querypb.LoadCollectionRequest{
		Base: commonpbutil.UpdateMsgBase(
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_LoadCollection),
		),
		DbID:           0,
		CollectionID:   collID,
		Schema:         collSchema,
		ReplicaNumber:  t.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        t.Refresh,
		ResourceGroups: t.ResourceGroups,
	}
	log.Debug("send LoadCollectionRequest to query coordinator",
		zap.Any("schema", request.Schema))
	t.result, err = t.queryCoord.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query coordinator LoadCollection: %s", err)
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.LoadCollectionRequest)
	return nil
}

func (t *loadCollectionTask) PostExecute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
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
	queryCoord types.QueryCoordClient
	result     *commonpb.Status

	collectionID       UniqueID
	replicateMsgStream msgstream.MsgStream
}

func (t *releaseCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *releaseCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *releaseCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *releaseCollectionTask) Name() string {
	return ReleaseCollectionTaskName
}

func (t *releaseCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *releaseCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *releaseCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *releaseCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *releaseCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *releaseCollectionTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_ReleaseCollection
	t.Base.SourceID = paramtable.GetNodeID()

	collName := t.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (t *releaseCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	t.collectionID = collID
	request := &querypb.ReleaseCollectionRequest{
		Base: commonpbutil.UpdateMsgBase(
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_ReleaseCollection),
		),
		DbID:         0,
		CollectionID: collID,
	}

	t.result, err = t.queryCoord.ReleaseCollection(ctx, request)

	globalMetaCache.RemoveCollection(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.ReleaseCollectionRequest)
	return err
}

func (t *releaseCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.DeprecateShardCache(t.GetDbName(), t.CollectionName)
	return nil
}

type loadPartitionsTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoordClient
	datacoord  types.DataCoordClient
	result     *commonpb.Status

	collectionID UniqueID
}

func (t *loadPartitionsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *loadPartitionsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *loadPartitionsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *loadPartitionsTask) Name() string {
	return LoadPartitionTaskName
}

func (t *loadPartitionsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *loadPartitionsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *loadPartitionsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *loadPartitionsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *loadPartitionsTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *loadPartitionsTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_LoadPartitions
	t.Base.SourceID = paramtable.GetNodeID()

	collName := t.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable load partitions if partition key mode is used")
	}

	return nil
}

func (t *loadPartitionsTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	t.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	// check index
	indexResponse, err := t.datacoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collID,
		IndexName:    "",
	})
	if err == nil {
		err = merr.Error(indexResponse.GetStatus())
	}
	if err != nil {
		if errors.Is(err, merr.ErrIndexNotFound) {
			err = merr.WrapErrIndexNotFoundForCollection(t.GetCollectionName())
		}
		return err
	}

	hasVecIndex := false
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
		for _, field := range collSchema.Fields {
			if index.FieldID == field.FieldID && (field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_Float16Vector) {
				hasVecIndex = true
			}
		}
	}
	if !hasVecIndex {
		errMsg := fmt.Sprintf("there is no vector index on collection: %s, please create index firstly", t.LoadPartitionsRequest.CollectionName)
		log.Ctx(ctx).Error(errMsg)
		return errors.New(errMsg)
	}
	for _, partitionName := range t.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), t.CollectionName, partitionName)
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
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_LoadPartitions),
		),
		DbID:           0,
		CollectionID:   collID,
		PartitionIDs:   partitionIDs,
		Schema:         collSchema,
		ReplicaNumber:  t.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        t.Refresh,
		ResourceGroups: t.ResourceGroups,
	}
	t.result, err = t.queryCoord.LoadPartitions(ctx, request)
	return err
}

func (t *loadPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type releasePartitionsTask struct {
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoordClient
	result     *commonpb.Status

	collectionID UniqueID
}

func (t *releasePartitionsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *releasePartitionsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *releasePartitionsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *releasePartitionsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *releasePartitionsTask) Name() string {
	return ReleasePartitionTaskName
}

func (t *releasePartitionsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *releasePartitionsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *releasePartitionsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *releasePartitionsTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *releasePartitionsTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_ReleasePartitions
	t.Base.SourceID = paramtable.GetNodeID()

	collName := t.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), collName)
	if err != nil {
		return err
	}
	if partitionKeyMode {
		return errors.New("disable release partitions if partition key mode is used")
	}

	return nil
}

func (t *releasePartitionsTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	t.collectionID = collID
	for _, partitionName := range t.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), t.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions),
		),
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	t.result, err = t.queryCoord.ReleasePartitions(ctx, request)
	return err
}

func (t *releasePartitionsTask) PostExecute(ctx context.Context) error {
	globalMetaCache.DeprecateShardCache(t.GetDbName(), t.CollectionName)
	return nil
}

// CreateAliasTask contains task information of CreateAlias
type CreateAliasTask struct {
	Condition
	*milvuspb.CreateAliasRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
}

// TraceCtx returns the trace context of the task.
func (t *CreateAliasTask) TraceCtx() context.Context {
	return t.ctx
}

// ID return the id of the task
func (t *CreateAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

// SetID sets the id of the task
func (t *CreateAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

// Name returns the name of the task
func (t *CreateAliasTask) Name() string {
	return CreateAliasTaskName
}

// Type returns the type of the task
func (t *CreateAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

// BeginTs returns the ts
func (t *CreateAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

// EndTs returns the ts
func (t *CreateAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

// SetTs sets the ts
func (t *CreateAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

// OnEnqueue defines the behavior task enqueued
func (t *CreateAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

// PreExecute defines the tion before task execution
func (t *CreateAliasTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_CreateAlias
	t.Base.SourceID = paramtable.GetNodeID()

	collAlias := t.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := t.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}
	return nil
}

// Execute defines the tual execution of create alias
func (t *CreateAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.CreateAlias(ctx, t.CreateAliasRequest)
	return err
}

// PostExecute defines the post execution, do nothing for create alias
func (t *CreateAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DropAliasTask is the task to drop alias
type DropAliasTask struct {
	Condition
	*milvuspb.DropAliasRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
}

// TraceCtx returns the context for trace
func (t *DropAliasTask) TraceCtx() context.Context {
	return t.ctx
}

// ID returns the MsgID
func (t *DropAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

// SetID sets the MsgID
func (t *DropAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

// Name returns the name of the task
func (t *DropAliasTask) Name() string {
	return DropAliasTaskName
}

func (t *DropAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *DropAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *DropAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *DropAliasTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_DropAlias
	t.Base.SourceID = paramtable.GetNodeID()
	collAlias := t.Alias
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}
	return nil
}

func (t *DropAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.DropAlias(ctx, t.DropAliasRequest)
	return err
}

func (t *DropAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// AlterAliasTask is the task to alter alias
type AlterAliasTask struct {
	Condition
	*milvuspb.AlterAliasRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status
}

func (t *AlterAliasTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *AlterAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *AlterAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *AlterAliasTask) Name() string {
	return AlterAliasTaskName
}

func (t *AlterAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *AlterAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *AlterAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *AlterAliasTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_AlterAlias
	t.Base.SourceID = paramtable.GetNodeID()

	collAlias := t.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := t.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (t *AlterAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.AlterAlias(ctx, t.AlterAliasRequest)
	return err
}

func (t *AlterAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

type CreateResourceGroupTask struct {
	Condition
	*milvuspb.CreateResourceGroupRequest
	ctx        context.Context
	queryCoord types.QueryCoordClient
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
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
	queryCoord types.QueryCoordClient
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
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
	queryCoord types.QueryCoordClient
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

	getCollectionName := func(collections map[int64]int32) (map[string]int32, error) {
		ret := make(map[string]int32)
		for key, value := range collections {
			name, err := globalMetaCache.GetCollectionName(ctx, "", key)
			if err != nil {
				log.Warn("failed to get collection name",
					zap.Int64("collectionID", key),
					zap.Error(err))

				// if collection has been dropped, skip it
				if errors.Is(err, merr.ErrCollectionNotFound) {
					continue
				}
				return nil, err
			}
			ret[name] = value
		}
		return ret, nil
	}

	if resp.GetStatus().GetErrorCode() == commonpb.ErrorCode_Success {
		rgInfo := resp.GetResourceGroup()

		numLoadedReplica, err := getCollectionName(rgInfo.NumLoadedReplica)
		if err != nil {
			return err
		}
		numOutgoingNode, err := getCollectionName(rgInfo.NumOutgoingNode)
		if err != nil {
			return err
		}
		numIncomingNode, err := getCollectionName(rgInfo.NumIncomingNode)
		if err != nil {
			return err
		}

		t.result = &milvuspb.DescribeResourceGroupResponse{
			Status: resp.Status,
			ResourceGroup: &milvuspb.ResourceGroup{
				Name:             rgInfo.GetName(),
				Capacity:         rgInfo.GetCapacity(),
				NumAvailableNode: rgInfo.NumAvailableNode,
				NumLoadedReplica: numLoadedReplica,
				NumOutgoingNode:  numOutgoingNode,
				NumIncomingNode:  numIncomingNode,
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
	queryCoord types.QueryCoordClient
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
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
	queryCoord types.QueryCoordClient
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	return nil
}

func (t *TransferReplicaTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_TransferReplica
	t.Base.SourceID = paramtable.GetNodeID()

	return nil
}

func (t *TransferReplicaTask) Execute(ctx context.Context) error {
	var err error
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
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
	queryCoord types.QueryCoordClient
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
