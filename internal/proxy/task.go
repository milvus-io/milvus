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
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	SumScorer string = "sum"
	MaxScorer string = "max"
	AvgScorer string = "avg"
)

const (
	IgnoreGrowingKey     = "ignore_growing"
	ReduceStopForBestKey = "reduce_stop_for_best"
	IteratorField        = "iterator"
	CollectionID         = "collection_id"
	GroupByFieldKey      = "group_by_field"
	GroupSizeKey         = "group_size"
	StrictGroupSize      = "strict_group_size"
	RankGroupScorer      = "rank_group_scorer"
	AnnsFieldKey         = "anns_field"
	TopKKey              = "topk"
	NQKey                = "nq"
	MetricTypeKey        = common.MetricTypeKey
	ParamsKey            = common.ParamsKey
	ExprParamsKey        = "expr_params"
	RoundDecimalKey      = "round_decimal"
	OffsetKey            = "offset"
	LimitKey             = "limit"

	SearchIterV2Key        = "search_iter_v2"
	SearchIterBatchSizeKey = "search_iter_batch_size"
	SearchIterLastBoundKey = "search_iter_last_bound"
	SearchIterIdKey        = "search_iter_id"

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
	FlushAllTaskName              = "FlushAllTask"
	LoadCollectionTaskName        = "LoadCollectionTask"
	ReleaseCollectionTaskName     = "ReleaseCollectionTask"
	LoadPartitionTaskName         = "LoadPartitionsTask"
	ReleasePartitionTaskName      = "ReleasePartitionsTask"
	DeleteTaskName                = "DeleteTask"
	CreateAliasTaskName           = "CreateAliasTask"
	DropAliasTaskName             = "DropAliasTask"
	AlterAliasTaskName            = "AlterAliasTask"
	DescribeAliasTaskName         = "DescribeAliasTask"
	ListAliasesTaskName           = "ListAliasesTask"
	AlterCollectionTaskName       = "AlterCollectionTask"
	AlterCollectionFieldTaskName  = "AlterCollectionFieldTask"
	UpsertTaskName                = "UpsertTask"
	CreateResourceGroupTaskName   = "CreateResourceGroupTask"
	UpdateResourceGroupsTaskName  = "UpdateResourceGroupsTask"
	DropResourceGroupTaskName     = "DropResourceGroupTask"
	TransferNodeTaskName          = "TransferNodeTask"
	TransferReplicaTaskName       = "TransferReplicaTask"
	ListResourceGroupsTaskName    = "ListResourceGroupsTask"
	DescribeResourceGroupTaskName = "DescribeResourceGroupTask"
	RunAnalyzerTaskName           = "RunAnalyzer"

	CreateDatabaseTaskName   = "CreateCollectionTask"
	DropDatabaseTaskName     = "DropDatabaseTaskName"
	ListDatabaseTaskName     = "ListDatabaseTaskName"
	AlterDatabaseTaskName    = "AlterDatabaseTaskName"
	DescribeDatabaseTaskName = "DescribeDatabaseTaskName"

	AddFieldTaskName = "AddFieldTaskName"

	// minFloat32 minimum float.
	minFloat32 = -1 * float32(math.MaxFloat32)

	RankTypeKey      = "strategy"
	RRFParamsKey     = "k"
	WeightsParamsKey = "weights"
	NormScoreKey     = "norm_score"
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
	CanSkipAllocTimestamp() bool
	SetOnEnqueueTime()
	GetDurationInQueue() time.Duration
	IsSubTask() bool
	SetExecutingTime()
	GetDurationInExecuting() time.Duration
}

type baseTask struct {
	onEnqueueTime time.Time
	executingTime time.Time
}

func (bt *baseTask) CanSkipAllocTimestamp() bool {
	return false
}

func (bt *baseTask) SetOnEnqueueTime() {
	bt.onEnqueueTime = time.Now()
}

func (bt *baseTask) GetDurationInQueue() time.Duration {
	return time.Since(bt.onEnqueueTime)
}

func (bt *baseTask) IsSubTask() bool {
	return false
}

func (bt *baseTask) SetExecutingTime() {
	bt.executingTime = time.Now()
}

func (bt *baseTask) GetDurationInExecuting() time.Duration {
	return time.Since(bt.executingTime)
}

type dmlTask interface {
	task
	setChannels() error
	getChannels() []pChan
}

type BaseInsertTask = msgstream.InsertMsg

type createCollectionTask struct {
	baseTask
	Condition
	*milvuspb.CreateCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
	schema   *schemapb.CollectionSchema
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

func (t *createCollectionTask) validatePartitionKey(ctx context.Context) error {
	idx := -1
	for i, field := range t.schema.Fields {
		if field.GetIsPartitionKey() {
			if idx != -1 {
				return fmt.Errorf("there are more than one partition key, field name = %s, %s", t.schema.Fields[idx].Name, field.Name)
			}

			if field.GetIsPrimaryKey() {
				return errors.New("the partition key field must not be primary field")
			}

			if field.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("partition key field not support nullable")
			}

			// The type of the partition key field can only be int64 and varchar
			if field.DataType != schemapb.DataType_Int64 && field.DataType != schemapb.DataType_VarChar {
				return errors.New("the data type of partition key should be Int64 or VarChar")
			}

			if t.GetNumPartitions() < 0 {
				return errors.New("the specified partitions should be greater than 0 if partition key is used")
			}

			maxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt64()
			if t.GetNumPartitions() > maxPartitionNum {
				return merr.WrapErrParameterInvalidMsg("partition number (%d) exceeds max configuration (%d)",
					t.GetNumPartitions(), maxPartitionNum)
			}

			// set default physical partitions num if enable partition key mode
			if t.GetNumPartitions() == 0 {
				defaultNum := common.DefaultPartitionsWithPartitionKey
				if defaultNum > maxPartitionNum {
					defaultNum = maxPartitionNum
				}
				t.NumPartitions = defaultNum
			}

			idx = i
		}
	}

	// Fields in StructArrayFields should not be partition key
	for _, field := range t.schema.StructArrayFields {
		for _, subField := range field.Fields {
			if subField.GetIsPartitionKey() {
				return merr.WrapErrCollectionIllegalSchema(t.CollectionName,
					fmt.Sprintf("partition key is not supported for struct field, field name = %s", subField.Name))
			}
		}
	}

	mustPartitionKey := Params.ProxyCfg.MustUsePartitionKey.GetAsBool()
	if mustPartitionKey && idx == -1 {
		return merr.WrapErrParameterInvalidMsg("partition key must be set when creating the collection" +
			" because the mustUsePartitionKey config is true")
	}

	if idx == -1 {
		if t.GetNumPartitions() != 0 {
			return errors.New("num_partitions should only be specified with partition key field enabled")
		}
	} else {
		log.Ctx(ctx).Info("create collection with partition key mode",
			zap.String("collectionName", t.CollectionName),
			zap.Int64("numDefaultPartitions", t.GetNumPartitions()))
	}

	return nil
}

func (t *createCollectionTask) validateClusteringKey(ctx context.Context) error {
	idx := -1
	for i, field := range t.schema.Fields {
		if field.GetIsClusteringKey() {
			if typeutil.IsVectorType(field.GetDataType()) &&
				!paramtable.Get().CommonCfg.EnableVectorClusteringKey.GetAsBool() {
				return merr.WrapErrCollectionVectorClusteringKeyNotAllowed(t.CollectionName)
			}
			if idx != -1 {
				return merr.WrapErrCollectionIllegalSchema(t.CollectionName,
					fmt.Sprintf("there are more than one clustering key, field name = %s, %s", t.schema.Fields[idx].Name, field.Name))
			}
			idx = i
		}
	}

	// Fields in StructArrayFields should not be clustering key
	for _, field := range t.schema.StructArrayFields {
		for _, subField := range field.Fields {
			if subField.GetIsClusteringKey() {
				return merr.WrapErrCollectionIllegalSchema(t.CollectionName,
					fmt.Sprintf("clustering key is not supported for struct field, field name = %s", subField.Name))
			}
		}
	}
	if idx != -1 {
		log.Ctx(ctx).Info("create collection with clustering key",
			zap.String("collectionName", t.CollectionName),
			zap.String("clusteringKeyField", t.schema.Fields[idx].Name))
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

	if err := validateFunction(t.schema); err != nil {
		return err
	}

	if t.ShardsNum > Params.ProxyCfg.MaxShardNum.GetAsInt32() {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum.GetAsInt())
	}

	totalFieldsNum := typeutil.GetTotalFieldsNum(t.schema)
	if totalFieldsNum > Params.ProxyCfg.MaxFieldNum.GetAsInt() {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum.GetAsInt())
	}

	vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema))
	if vectorFields > Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
		return fmt.Errorf("maximum vector field's number should be limited to %d", Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
	}

	if vectorFields == 0 {
		return merr.WrapErrParameterInvalidMsg("schema does not contain vector field")
	}

	// validate collection name
	if err := validateCollectionName(t.schema.Name); err != nil {
		return err
	}

	// validate whether field names duplicates
	if err := validateDuplicatedFieldName(t.schema); err != nil {
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
	if err := t.validatePartitionKey(ctx); err != nil {
		return err
	}

	hasPartitionKey := hasPartitionKeyModeField(t.schema)
	if _, err := validatePartitionKeyIsolation(ctx, t.CollectionName, hasPartitionKey, t.GetProperties()...); err != nil {
		return err
	}

	// validate clustering key
	if err := t.validateClusteringKey(ctx); err != nil {
		return err
	}

	for _, field := range t.schema.Fields {
		if err := ValidateField(field, t.schema); err != nil {
			return err
		}
	}

	for _, structArrayField := range t.schema.StructArrayFields {
		if err := ValidateStructArrayField(structArrayField, t.schema); err != nil {
			return err
		}
	}

	if err := validateMultipleVectorFields(t.schema); err != nil {
		return err
	}

	if err := validateLoadFieldsList(t.schema); err != nil {
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
	t.result, err = t.mixCoord.CreateCollection(ctx, t.CreateCollectionRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *createCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type addCollectionFieldTask struct {
	baseTask
	Condition
	*milvuspb.AddCollectionFieldRequest
	ctx         context.Context
	mixCoord    types.MixCoordClient
	result      *commonpb.Status
	fieldSchema *schemapb.FieldSchema
	oldSchema   *schemapb.CollectionSchema
}

func (t *addCollectionFieldTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *addCollectionFieldTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *addCollectionFieldTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *addCollectionFieldTask) Name() string {
	return AddFieldTaskName
}

func (t *addCollectionFieldTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *addCollectionFieldTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *addCollectionFieldTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *addCollectionFieldTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *addCollectionFieldTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AddCollectionField
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *addCollectionFieldTask) PreExecute(ctx context.Context) error {
	if t.oldSchema == nil {
		return merr.WrapErrParameterInvalidMsg("empty old schema in add field task")
	}
	t.fieldSchema = &schemapb.FieldSchema{}
	err := proto.Unmarshal(t.GetSchema(), t.fieldSchema)
	if err != nil {
		return err
	}
	fieldList := typeutil.NewSet[string]()
	for _, schema := range t.oldSchema.Fields {
		fieldList.Insert(schema.Name)
	}

	if len(fieldList) >= Params.ProxyCfg.MaxFieldNum.GetAsInt() {
		msg := fmt.Sprintf("The number of fields has reached the maximum value %d", Params.ProxyCfg.MaxFieldNum.GetAsInt())
		return merr.WrapErrParameterInvalidMsg(msg)
	}

	if _, ok := schemapb.DataType_name[int32(t.fieldSchema.DataType)]; !ok || t.fieldSchema.GetDataType() == schemapb.DataType_None {
		return merr.WrapErrParameterInvalid("valid field", fmt.Sprintf("field data type: %s is not supported", t.fieldSchema.GetDataType()))
	}

	if typeutil.IsVectorType(t.fieldSchema.DataType) {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support to add vector field, field name = %s", t.fieldSchema.Name))
	}
	if funcutil.SliceContain([]string{common.RowIDFieldName, common.TimeStampFieldName, common.MetaFieldName}, t.fieldSchema.GetName()) {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support to add system field, field name = %s", t.fieldSchema.Name))
	}
	if t.fieldSchema.IsPrimaryKey {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support to add pk field, field name = %s", t.fieldSchema.Name))
	}
	if !t.fieldSchema.Nullable {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("added field must be nullable, please check it, field name = %s", t.fieldSchema.Name))
	}
	if t.fieldSchema.AutoID {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("only primary field can speficy AutoID with true, field name = %s", t.fieldSchema.Name))
	}
	if t.fieldSchema.IsPartitionKey {
		return merr.WrapErrParameterInvalidMsg("not support to add partition key field, field name  = %s", t.fieldSchema.Name)
	}
	if t.fieldSchema.GetIsClusteringKey() {
		for _, f := range t.oldSchema.Fields {
			if f.GetIsClusteringKey() {
				return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("already has another clutering key field, field name: %s", t.fieldSchema.GetName()))
			}
		}
	}
	if err := ValidateField(t.fieldSchema, t.oldSchema); err != nil {
		return err
	}
	if fieldList.Contain(t.fieldSchema.Name) {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("duplicate field name: %s", t.fieldSchema.GetName()))
	}

	log.Info("PreExecute addField task done", zap.Any("field schema", t.fieldSchema))
	return nil
}

func (t *addCollectionFieldTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.AddCollectionField(ctx, t.AddCollectionFieldRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *addCollectionFieldTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionTask struct {
	baseTask
	Condition
	*milvuspb.DropCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
	chMgr    channelsMgr
	chTicker channelsTimeTicker
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
	t.Base.MsgType = commonpb.MsgType_DropCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *dropCollectionTask) PreExecute(ctx context.Context) error {
	// No need to check collection name
	// Validation shall be preformed in `CreateCollection`
	// also permit drop collection one with bad collection name
	return nil
}

func (t *dropCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.DropCollection(ctx, t.DropCollectionRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *dropCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasCollectionTask struct {
	baseTask
	Condition
	*milvuspb.HasCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.BoolResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_HasCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *hasCollectionTask) PreExecute(ctx context.Context) error {
	if err := validateCollectionName(t.CollectionName); err != nil {
		return err
	}
	return nil
}

func (t *hasCollectionTask) Execute(ctx context.Context) error {
	t.result = &milvuspb.BoolResponse{
		Status: merr.Success(),
	}
	_, err := globalMetaCache.GetCollectionID(ctx, t.HasCollectionRequest.GetDbName(), t.HasCollectionRequest.GetCollectionName())
	// error other than
	if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
		t.result.Status = merr.Status(err)
		return err
	}
	// if collection not nil, means error is ErrCollectionNotFound, result is false
	// otherwise, result is true
	t.result.Value = (err == nil)
	return nil
}

func (t *hasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeCollectionTask struct {
	baseTask
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeCollectionResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DescribeCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *describeCollectionTask) PreExecute(ctx context.Context) error {
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
			Name:              "",
			Description:       "",
			AutoID:            false,
			Fields:            make([]*schemapb.FieldSchema, 0),
			Functions:         make([]*schemapb.FunctionSchema, 0),
			StructArrayFields: make([]*schemapb.StructArrayFieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
		CollectionName:       t.GetCollectionName(),
		DbName:               t.GetDbName(),
	}

	ctx = AppendUserInfoForRPC(ctx)
	result, err := t.mixCoord.DescribeCollection(ctx, t.DescribeCollectionRequest)
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
			t.result.Status.Reason = fmt.Sprintf("can't find collection[database=%s][collection=%s]", t.GetDbName(), t.GetCollectionName())
			t.result.Status.ExtraInfo = map[string]string{merr.InputErrorFlagKey: "true"}
		}
		return nil
	}

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
	t.result.UpdateTimestamp = result.UpdateTimestamp
	t.result.UpdateTimestampStr = result.UpdateTimestampStr
	copyFieldSchema := func(field *schemapb.FieldSchema) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			FieldID:          field.FieldID,
			Name:             field.Name,
			IsPrimaryKey:     field.IsPrimaryKey,
			AutoID:           field.AutoID,
			Description:      field.Description,
			DataType:         field.DataType,
			TypeParams:       field.TypeParams,
			IndexParams:      field.IndexParams,
			IsDynamic:        field.IsDynamic,
			IsPartitionKey:   field.IsPartitionKey,
			IsClusteringKey:  field.IsClusteringKey,
			DefaultValue:     field.DefaultValue,
			ElementType:      field.ElementType,
			Nullable:         field.Nullable,
			IsFunctionOutput: field.IsFunctionOutput,
		}
	}

	for _, field := range result.Schema.Fields {
		if field.IsDynamic {
			continue
		}
		if field.FieldID >= common.StartOfUserFieldID {
			t.result.Schema.Fields = append(t.result.Schema.Fields, copyFieldSchema(field))
		}
	}

	for i, structArrayField := range result.Schema.StructArrayFields {
		t.result.Schema.StructArrayFields = append(t.result.Schema.StructArrayFields, &schemapb.StructArrayFieldSchema{
			FieldID:     structArrayField.FieldID,
			Name:        structArrayField.Name,
			Description: structArrayField.Description,
			Fields:      make([]*schemapb.FieldSchema, 0, len(structArrayField.Fields)),
		})
		for _, field := range structArrayField.Fields {
			t.result.Schema.StructArrayFields[i].Fields = append(t.result.Schema.StructArrayFields[i].Fields, copyFieldSchema(field))
		}
	}

	for _, function := range result.Schema.Functions {
		t.result.Schema.Functions = append(t.result.Schema.Functions, proto.Clone(function).(*schemapb.FunctionSchema))
	}
	return nil
}

func (t *describeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showCollectionsTask struct {
	baseTask
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ShowCollectionsResponse
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
	t.Base.MsgType = commonpb.MsgType_ShowCollections
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *showCollectionsTask) PreExecute(ctx context.Context) error {
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
	ctx = AppendUserInfoForRPC(ctx)
	respFromRootCoord, err := t.mixCoord.ShowCollections(ctx, t.ShowCollectionsRequest)
	if err = merr.CheckRPCCall(respFromRootCoord, err); err != nil {
		return err
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
				log.Ctx(ctx).Debug("Failed to get collection id.", zap.String("collectionName", collectionName),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showCollections"))
				return err
			}
			collectionIDs = append(collectionIDs, collectionID)
			IDs2Names[collectionID] = collectionName
		}

		resp, err := t.mixCoord.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
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
				log.Ctx(ctx).Debug("Failed to get collection info. This collection may be not released",
					zap.Int64("collectionID", id),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showCollections"))
				continue
			}
			collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, t.GetDbName(), collectionName, id)
			if err != nil {
				log.Ctx(ctx).Debug("Failed to get collection info.", zap.String("collectionName", collectionName),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showCollections"))
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
	baseTask
	Condition
	*milvuspb.AlterCollectionRequest
	ctx                context.Context
	mixCoord           types.MixCoordClient
	result             *commonpb.Status
	replicateMsgStream msgstream.MsgStream
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
	t.Base.MsgType = commonpb.MsgType_AlterCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func hasMmapProp(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.MmapEnabledKey {
			return true
		}
	}
	return false
}

func hasLazyLoadProp(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.LazyLoadEnableKey {
			return true
		}
	}
	return false
}

func hasPropInDeletekeys(keys []string) string {
	for _, key := range keys {
		if key == common.MmapEnabledKey || key == common.LazyLoadEnableKey {
			return key
		}
	}
	return ""
}

func validatePartitionKeyIsolation(ctx context.Context, colName string, isPartitionKeyEnabled bool, props ...*commonpb.KeyValuePair) (bool, error) {
	iso, err := common.IsPartitionKeyIsolationKvEnabled(props...)
	if err != nil {
		return false, err
	}

	// partition key isolation is not set, skip
	if !iso {
		return false, nil
	}

	if !isPartitionKeyEnabled {
		return false, merr.WrapErrCollectionIllegalSchema(colName,
			"partition key isolation mode is enabled but no partition key field is set. Please set the partition key first")
	}

	if !paramtable.Get().CommonCfg.EnableMaterializedView.GetAsBool() {
		return false, merr.WrapErrCollectionIllegalSchema(colName,
			"partition key isolation mode is enabled but current Milvus does not support it. Please contact us")
	}

	log.Ctx(ctx).Info("validated with partition key isolation", zap.String("collectionName", colName))

	return true, nil
}

func (t *alterCollectionTask) PreExecute(ctx context.Context) error {
	if len(t.GetProperties()) > 0 && len(t.GetDeleteKeys()) > 0 {
		return merr.WrapErrParameterInvalidMsg("cannot provide both DeleteKeys and ExtraParams")
	}

	collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}

	t.CollectionID = collectionID

	if len(t.GetProperties()) > 0 {
		if hasMmapProp(t.Properties...) || hasLazyLoadProp(t.Properties...) {
			loaded, err := isCollectionLoaded(ctx, t.mixCoord, t.CollectionID)
			if err != nil {
				return err
			}
			if loaded {
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not alter mmap properties if collection loaded")
			}
		}
	} else if len(t.GetDeleteKeys()) > 0 {
		key := hasPropInDeletekeys(t.DeleteKeys)
		if key != "" {
			loaded, err := isCollectionLoaded(ctx, t.mixCoord, t.CollectionID)
			if err != nil {
				return err
			}
			if loaded {
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not delete mmap properties if collection loaded")
			}
		}
	}

	isPartitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	// check if the new partition key isolation is valid to use
	newIsoValue, err := validatePartitionKeyIsolation(ctx, t.CollectionName, isPartitionKeyMode, t.Properties...)
	if err != nil {
		return err
	}
	collBasicInfo, err := globalMetaCache.GetCollectionInfo(t.ctx, t.GetDbName(), t.CollectionName, t.CollectionID)
	if err != nil {
		return err
	}
	oldIsoValue := collBasicInfo.partitionKeyIsolation

	log.Ctx(ctx).Info("alter collection pre check with partition key isolation",
		zap.String("collectionName", t.CollectionName),
		zap.Bool("isPartitionKeyMode", isPartitionKeyMode),
		zap.Bool("newIsoValue", newIsoValue),
		zap.Bool("oldIsoValue", oldIsoValue))

	// if the isolation flag in properties is not set, meta cache will assign partitionKeyIsolation in collection info to false
	//   - None|false -> false, skip
	//   - None|false -> true, check if the collection has vector index
	//   - true -> false, check if the collection has vector index
	//   - false -> true, check if the collection has vector index
	//   - true -> true, skip
	if oldIsoValue != newIsoValue {
		collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.CollectionName)
		if err != nil {
			return err
		}

		hasVecIndex := false
		indexName := ""
		indexResponse, err := t.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
			CollectionID: t.CollectionID,
			IndexName:    "",
		})
		if err != nil {
			return merr.WrapErrServiceInternal("describe index failed", err.Error())
		}
		for _, index := range indexResponse.IndexInfos {
			for _, field := range collSchema.Fields {
				if index.FieldID == field.FieldID && typeutil.IsVectorType(field.DataType) {
					hasVecIndex = true
					indexName = field.GetName()
				}
			}
		}
		if hasVecIndex {
			return merr.WrapErrIndexDuplicate(indexName,
				"can not alter partition key isolation mode if the collection already has a vector index. Please drop the index first")
		}
	}

	_, ok := common.IsReplicateEnabled(t.Properties)
	if ok {
		return merr.WrapErrParameterInvalidMsg("can't set the replicate.id property")
	}
	endTS, ok := common.GetReplicateEndTS(t.Properties)
	if ok && collBasicInfo.replicateID != "" {
		allocResp, err := t.mixCoord.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
			Count:          1,
			BlockTimestamp: endTS,
		})
		if err = merr.CheckRPCCall(allocResp, err); err != nil {
			return merr.WrapErrServiceInternal("alloc timestamp failed", err.Error())
		}
		if allocResp.GetTimestamp() <= endTS {
			return merr.WrapErrServiceInternal("alter collection: alloc timestamp failed, timestamp is not greater than endTS",
				fmt.Sprintf("timestamp = %d, endTS = %d", allocResp.GetTimestamp(), endTS))
		}
	}

	return nil
}

func (t *alterCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.AlterCollection(ctx, t.AlterCollectionRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.AlterCollectionRequest)
	return nil
}

func (t *alterCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterCollectionFieldTask struct {
	baseTask
	Condition
	*milvuspb.AlterCollectionFieldRequest
	ctx                context.Context
	mixCoord           types.MixCoordClient
	result             *commonpb.Status
	replicateMsgStream msgstream.MsgStream
}

func (t *alterCollectionFieldTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterCollectionFieldTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *alterCollectionFieldTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *alterCollectionFieldTask) Name() string {
	return AlterCollectionTaskName
}

func (t *alterCollectionFieldTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *alterCollectionFieldTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionFieldTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionFieldTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *alterCollectionFieldTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterCollectionField
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

const (
	MmapEnabledKey = "mmap_enabled"
)

var allowedAlterProps = []string{
	common.MaxLengthKey,
	common.MmapEnabledKey,
	common.MaxCapacityKey,
}

var allowedDropProps = []string{
	common.MmapEnabledKey,
}

func IsKeyAllowAlter(key string) bool {
	for _, allowedKey := range allowedAlterProps {
		if key == allowedKey {
			return true
		}
	}
	return false
}

func IsKeyAllowDrop(key string) bool {
	for _, allowedKey := range allowedDropProps {
		if key == allowedKey {
			return true
		}
	}
	return false
}

func updateKey(key string) string {
	var updatedKey string
	if key == MmapEnabledKey {
		updatedKey = common.MmapEnabledKey
	} else {
		updatedKey = key
	}
	return updatedKey
}

func updatePropertiesKeys(oldProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string)
	for _, prop := range oldProps {
		updatedKey := updateKey(prop.Key)
		props[updatedKey] = prop.Value
	}

	propKV := make([]*commonpb.KeyValuePair, 0)
	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}

func (t *alterCollectionFieldTask) PreExecute(ctx context.Context) error {
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}

	isCollectionLoadedFn := func() (bool, error) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
		if err != nil {
			return false, err
		}
		loaded, err1 := isCollectionLoaded(ctx, t.mixCoord, collectionID)
		if err1 != nil {
			return false, err1
		}
		return loaded, nil
	}

	t.Properties = updatePropertiesKeys(t.Properties)
	for _, prop := range t.Properties {
		if !IsKeyAllowAlter(prop.Key) {
			return merr.WrapErrParameterInvalidMsg("%s does not allow update in collection field param", prop.Key)
		}
		// Check the value type based on the key
		switch prop.Key {
		case common.MmapEnabledKey:
			loaded, err := isCollectionLoadedFn()
			if err != nil {
				return err
			}
			if loaded {
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not alter collection field properties if collection loaded")
			}

		case common.MaxLengthKey:
			IsStringType := false
			fieldName := ""
			for _, field := range collSchema.Fields {
				if field.GetName() == t.FieldName && (typeutil.IsStringType(field.DataType) || typeutil.IsArrayContainStringElementType(field.DataType, field.ElementType)) {
					IsStringType = true
					fieldName = field.GetName()
					break
				}
			}
			if !IsStringType {
				return merr.WrapErrParameterInvalidMsg("%s can not modify the maxlength for non-string types", fieldName)
			}
			value, err := strconv.Atoi(prop.Value)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("%s should be an integer, but got %T", prop.Key, prop.Value)
			}

			defaultMaxVarCharLength := Params.ProxyCfg.MaxVarCharLength.GetAsInt64()
			if int64(value) > defaultMaxVarCharLength {
				return merr.WrapErrParameterInvalidMsg("%s exceeds the maximum allowed value %s", prop.Value, strconv.FormatInt(defaultMaxVarCharLength, 10))
			}
		case common.MaxCapacityKey:
			IsArrayType := false
			fieldName := ""
			for _, field := range collSchema.Fields {
				if field.GetName() == t.FieldName && typeutil.IsArrayType(field.DataType) {
					IsArrayType = true
					fieldName = field.GetName()
					break
				}
			}
			if !IsArrayType {
				return merr.WrapErrParameterInvalidMsg("%s can not modify the maxcapacity for non-array types", fieldName)
			}

			maxCapacityPerRow, err := strconv.ParseInt(prop.Value, 10, 64)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("the value for %s of field %s must be an integer", common.MaxCapacityKey, fieldName)
			}
			if maxCapacityPerRow > defaultMaxArrayCapacity || maxCapacityPerRow <= 0 {
				return merr.WrapErrParameterInvalidMsg("the maximum capacity specified for a Array should be in (0, %d]", defaultMaxArrayCapacity)
			}
		}
	}

	deleteKeys := make([]string, 0)
	for _, key := range t.DeleteKeys {
		updatedKey := updateKey(key)
		if !IsKeyAllowDrop(updatedKey) {
			return merr.WrapErrParameterInvalidMsg("%s is not allowed to drop in collection field param", key)
		}

		if updatedKey == common.MmapEnabledKey {
			loaded, err := isCollectionLoadedFn()
			if err != nil {
				return err
			}
			if loaded {
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not drop collection field properties if collection loaded")
			}
		}

		deleteKeys = append(deleteKeys, updatedKey)
	}
	t.DeleteKeys = deleteKeys

	return nil
}

func (t *alterCollectionFieldTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.AlterCollectionField(ctx, t.AlterCollectionFieldRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.AlterCollectionFieldRequest)
	return nil
}

func (t *alterCollectionFieldTask) PostExecute(ctx context.Context) error {
	return nil
}

type createPartitionTask struct {
	baseTask
	Condition
	*milvuspb.CreatePartitionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_CreatePartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *createPartitionTask) PreExecute(ctx context.Context) error {
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
	t.result, err = t.mixCoord.CreatePartition(ctx, t.CreatePartitionRequest)
	if err := merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		t.result = merr.Status(err)
		return err
	}
	partitionID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), t.GetCollectionName(), t.GetPartitionName())
	if err != nil {
		t.result = merr.Status(err)
		return err
	}
	t.result, err = t.mixCoord.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions)),
		CollectionID: collectionID,
		PartitionID:  partitionID,
	})
	return merr.CheckRPCCall(t.result, err)
}

func (t *createPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropPartitionTask struct {
	baseTask
	Condition
	*milvuspb.DropPartitionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_DropPartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *dropPartitionTask) PreExecute(ctx context.Context) error {
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

	collLoaded, err := isCollectionLoaded(ctx, t.mixCoord, collID)
	if err != nil {
		return err
	}
	if collLoaded {
		loaded, err := isPartitionLoaded(ctx, t.mixCoord, collID, partID)
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
	t.result, err = t.mixCoord.DropPartition(ctx, t.DropPartitionRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *dropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasPartitionTask struct {
	baseTask
	Condition
	*milvuspb.HasPartitionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.BoolResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_HasPartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *hasPartitionTask) PreExecute(ctx context.Context) error {
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
	t.result, err = t.mixCoord.HasPartition(ctx, t.HasPartitionRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *hasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showPartitionsTask struct {
	baseTask
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ShowPartitionsResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_ShowPartitions
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *showPartitionsTask) PreExecute(ctx context.Context) error {
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
	respFromRootCoord, err := t.mixCoord.ShowPartitions(ctx, t.ShowPartitionsRequest)
	if err = merr.CheckRPCCall(respFromRootCoord, err); err != nil {
		return err
	}

	if t.GetType() == milvuspb.ShowType_InMemory {
		collectionName := t.CollectionName
		collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), collectionName)
		if err != nil {
			log.Ctx(ctx).Debug("Failed to get collection id.", zap.String("collectionName", collectionName),
				zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showPartitions"))
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
				log.Ctx(ctx).Debug("Failed to get partition id.", zap.String("partitionName", partitionName),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showPartitions"))
				return err
			}
			partitionIDs = append(partitionIDs, partitionID)
			IDs2Names[partitionID] = partitionName
		}
		resp, err := t.mixCoord.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		})
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
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
				log.Ctx(ctx).Debug("Failed to get partition id.", zap.String("partitionName", partitionName),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showPartitions"))
				return errors.New("failed to show partitions")
			}
			partitionInfo, err := globalMetaCache.GetPartitionInfo(ctx, t.GetDbName(), collectionName, partitionName)
			if err != nil {
				log.Ctx(ctx).Debug("Failed to get partition id.", zap.String("partitionName", partitionName),
					zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "showPartitions"))
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

const LoadPriorityName = "load_priority"

type loadCollectionTask struct {
	baseTask
	Condition
	*milvuspb.LoadCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

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
	t.Base.MsgType = commonpb.MsgType_LoadCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Ctx(ctx).Debug("loadCollectionTask PreExecute",
		zap.String("role", typeutil.ProxyRole))

	collName := t.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (t *loadCollectionTask) GetLoadPriority() commonpb.LoadPriority {
	loadPriority := commonpb.LoadPriority_HIGH
	loadPriorityStr, ok := t.LoadCollectionRequest.LoadParams[LoadPriorityName]
	if ok && loadPriorityStr == "low" {
		loadPriority = commonpb.LoadPriority_LOW
	}
	return loadPriority
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
	// prepare load field list
	loadFields, err := collSchema.GetLoadFieldIDs(t.GetLoadFields(), t.GetSkipLoadDynamicField())
	if err != nil {
		return err
	}

	// check index
	indexResponse, err := t.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
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

	// not support multiple indexes on one field
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
	}

	loadFieldsSet := typeutil.NewSet(loadFields...)
	unindexedVecFields := make([]string, 0)
	for _, field := range collSchema.GetFields() {
		if typeutil.IsVectorType(field.GetDataType()) && loadFieldsSet.Contain(field.GetFieldID()) {
			if _, ok := fieldIndexIDs[field.GetFieldID()]; !ok {
				unindexedVecFields = append(unindexedVecFields, field.GetName())
			}
		}
	}

	// todo(SpadeA): check vector field in StructArrayField when index is implemented

	if len(unindexedVecFields) != 0 {
		errMsg := fmt.Sprintf("there is no vector index on field: %v, please create index firstly", unindexedVecFields)
		log.Debug(errMsg)
		return errors.New(errMsg)
	}
	request := &querypb.LoadCollectionRequest{
		Base: commonpbutil.UpdateMsgBase(
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_LoadCollection),
		),
		DbID:           0,
		CollectionID:   collID,
		Schema:         collSchema.CollectionSchema,
		ReplicaNumber:  t.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        t.Refresh,
		ResourceGroups: t.ResourceGroups,
		LoadFields:     loadFields,
		Priority:       t.GetLoadPriority(),
	}
	log.Info("send LoadCollectionRequest to query coordinator",
		zap.Any("schema", request.Schema),
		zap.Int32("priority", int32(request.GetPriority())))
	t.result, err = t.mixCoord.LoadCollection(ctx, request)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
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
	baseTask
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

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
	t.Base.MsgType = commonpb.MsgType_ReleaseCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *releaseCollectionTask) PreExecute(ctx context.Context) error {
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

	t.result, err = t.mixCoord.ReleaseCollection(ctx, request)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}

	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.ReleaseCollectionRequest)
	return nil
}

func (t *releaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type loadPartitionsTask struct {
	baseTask
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID       UniqueID
	replicateMsgStream msgstream.MsgStream
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
	t.Base.MsgType = commonpb.MsgType_LoadPartitions
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *loadPartitionsTask) PreExecute(ctx context.Context) error {
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

func (t *loadPartitionsTask) GetLoadPriority() commonpb.LoadPriority {
	loadPriority := commonpb.LoadPriority_HIGH
	loadPriorityStr, ok := t.LoadPartitionsRequest.LoadParams[LoadPriorityName]
	if ok && loadPriorityStr == "low" {
		loadPriority = commonpb.LoadPriority_LOW
	}
	return loadPriority
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
	// prepare load field list
	loadFields, err := collSchema.GetLoadFieldIDs(t.GetLoadFields(), t.GetSkipLoadDynamicField())
	if err != nil {
		return err
	}
	// check index
	indexResponse, err := t.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
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

	// not support multiple indexes on one field
	fieldIndexIDs := make(map[int64]int64)
	for _, index := range indexResponse.IndexInfos {
		fieldIndexIDs[index.FieldID] = index.IndexID
	}

	loadFieldsSet := typeutil.NewSet(loadFields...)
	unindexedVecFields := make([]string, 0)
	for _, field := range collSchema.GetFields() {
		if typeutil.IsVectorType(field.GetDataType()) && loadFieldsSet.Contain(field.GetFieldID()) {
			if _, ok := fieldIndexIDs[field.GetFieldID()]; !ok {
				unindexedVecFields = append(unindexedVecFields, field.GetName())
			}
		}
	}

	// todo(SpadeA): check vector field in StructArrayField when index is implemented

	if len(unindexedVecFields) != 0 {
		errMsg := fmt.Sprintf("there is no vector index on field: %v, please create index firstly", unindexedVecFields)
		log.Ctx(ctx).Debug(errMsg)
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
		Schema:         collSchema.CollectionSchema,
		ReplicaNumber:  t.ReplicaNumber,
		FieldIndexID:   fieldIndexIDs,
		Refresh:        t.Refresh,
		ResourceGroups: t.ResourceGroups,
		LoadFields:     loadFields,
		Priority:       t.GetLoadPriority(),
	}
	log.Info("send LoadPartitionRequest to query coordinator",
		zap.Any("schema", request.Schema),
		zap.Int32("priority", int32(request.GetPriority())))
	t.result, err = t.mixCoord.LoadPartitions(ctx, request)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.LoadPartitionsRequest)

	return nil
}

func (t *loadPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type releasePartitionsTask struct {
	baseTask
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID       UniqueID
	replicateMsgStream msgstream.MsgStream
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
	t.Base.MsgType = commonpb.MsgType_ReleasePartitions
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *releasePartitionsTask) PreExecute(ctx context.Context) error {
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
	t.result, err = t.mixCoord.ReleasePartitions(ctx, request)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.ReleasePartitionsRequest)
	return nil
}

func (t *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type CreateResourceGroupTask struct {
	baseTask
	Condition
	*milvuspb.CreateResourceGroupRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_CreateResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *CreateResourceGroupTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *CreateResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.CreateResourceGroup(ctx, t.CreateResourceGroupRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *CreateResourceGroupTask) PostExecute(ctx context.Context) error {
	return nil
}

type UpdateResourceGroupsTask struct {
	baseTask
	Condition
	*milvuspb.UpdateResourceGroupsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (t *UpdateResourceGroupsTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *UpdateResourceGroupsTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *UpdateResourceGroupsTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *UpdateResourceGroupsTask) Name() string {
	return UpdateResourceGroupsTaskName
}

func (t *UpdateResourceGroupsTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *UpdateResourceGroupsTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *UpdateResourceGroupsTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *UpdateResourceGroupsTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *UpdateResourceGroupsTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_UpdateResourceGroups
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *UpdateResourceGroupsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *UpdateResourceGroupsTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.UpdateResourceGroups(ctx, &querypb.UpdateResourceGroupsRequest{
		Base:           t.UpdateResourceGroupsRequest.GetBase(),
		ResourceGroups: t.UpdateResourceGroupsRequest.GetResourceGroups(),
	})
	return merr.CheckRPCCall(t.result, err)
}

func (t *UpdateResourceGroupsTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropResourceGroupTask struct {
	baseTask
	Condition
	*milvuspb.DropResourceGroupRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_DropResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *DropResourceGroupTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *DropResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.DropResourceGroup(ctx, t.DropResourceGroupRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *DropResourceGroupTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeResourceGroupTask struct {
	baseTask
	Condition
	*milvuspb.DescribeResourceGroupRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeResourceGroupResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DescribeResourceGroup
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *DescribeResourceGroupTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *DescribeResourceGroupTask) Execute(ctx context.Context) error {
	var err error
	resp, err := t.mixCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
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
				log.Ctx(ctx).Warn("failed to get collection name",
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
				Config:           rgInfo.Config,
				Nodes:            rgInfo.Nodes,
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
	baseTask
	Condition
	*milvuspb.TransferNodeRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_TransferNode
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *TransferNodeTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *TransferNodeTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.TransferNode(ctx, t.TransferNodeRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *TransferNodeTask) PostExecute(ctx context.Context) error {
	return nil
}

type TransferReplicaTask struct {
	baseTask
	Condition
	*milvuspb.TransferReplicaRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	t.Base.MsgType = commonpb.MsgType_TransferReplica
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *TransferReplicaTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *TransferReplicaTask) Execute(ctx context.Context) error {
	var err error
	collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	t.result, err = t.mixCoord.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: t.SourceResourceGroup,
		TargetResourceGroup: t.TargetResourceGroup,
		CollectionID:        collID,
		NumReplica:          t.NumReplica,
	})
	return merr.CheckRPCCall(t.result, err)
}

func (t *TransferReplicaTask) PostExecute(ctx context.Context) error {
	return nil
}

type ListResourceGroupsTask struct {
	baseTask
	Condition
	*milvuspb.ListResourceGroupsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListResourceGroupsResponse
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
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_ListResourceGroups
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *ListResourceGroupsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *ListResourceGroupsTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.ListResourceGroups(ctx, t.ListResourceGroupsRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *ListResourceGroupsTask) PostExecute(ctx context.Context) error {
	return nil
}

type RunAnalyzerTask struct {
	baseTask
	Condition
	*milvuspb.RunAnalyzerRequest
	ctx          context.Context
	collectionID typeutil.UniqueID
	fieldID      typeutil.UniqueID
	dbName       string
	lb           LBPolicy

	result *milvuspb.RunAnalyzerResponse
}

func (t *RunAnalyzerTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *RunAnalyzerTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *RunAnalyzerTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *RunAnalyzerTask) Name() string {
	return RunAnalyzerTaskName
}

func (t *RunAnalyzerTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *RunAnalyzerTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *RunAnalyzerTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *RunAnalyzerTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *RunAnalyzerTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_RunAnalyzer
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *RunAnalyzerTask) PreExecute(ctx context.Context) error {
	t.dbName = t.GetDbName()

	collID, err := globalMetaCache.GetCollectionID(ctx, t.dbName, t.GetCollectionName())
	if err != nil { // err is not nil if collection not exists
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}

	t.collectionID = collID

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.dbName, t.GetCollectionName())
	if err != nil { // err is not nil if collection not exists
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}

	fieldId, ok := schema.MapFieldID(t.GetFieldName())
	if !ok {
		return merr.WrapErrAsInputError(merr.WrapErrFieldNotFound(t.GetFieldName()))
	}

	t.fieldID = fieldId
	t.result = &milvuspb.RunAnalyzerResponse{}
	return nil
}

func (t *RunAnalyzerTask) runAnalyzerOnShardleader(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	resp, err := qn.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
		Channel:       channel,
		FieldId:       t.fieldID,
		AnalyzerNames: t.GetAnalyzerNames(),
		Placeholder:   t.GetPlaceholder(),
		WithDetail:    t.GetWithDetail(),
		WithHash:      t.GetWithHash(),
	})
	if err != nil {
		return err
	}

	if err := merr.Error(resp.GetStatus()); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *RunAnalyzerTask) Execute(ctx context.Context) error {
	err := t.lb.ExecuteOneChannel(ctx, CollectionWorkLoad{
		db:             t.dbName,
		collectionName: t.GetCollectionName(),
		collectionID:   t.collectionID,
		nq:             int64(len(t.GetPlaceholder())),
		exec:           t.runAnalyzerOnShardleader,
	})

	return err
}

func (t *RunAnalyzerTask) PostExecute(ctx context.Context) error {
	return nil
}

// isIgnoreGrowing is used to check if the request should ignore growing
func isIgnoreGrowing(params []*commonpb.KeyValuePair) (bool, error) {
	for _, kv := range params {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err := strconv.ParseBool(kv.GetValue())
			if err != nil {
				return false, errors.New("parse ignore growing field failed")
			}
			return ignoreGrowing, nil
		}
	}
	return false, nil
}
