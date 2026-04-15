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
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/timestamptz"
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
	JSONPath             = "json_path"
	JSONType             = "json_type"
	StrictCastKey        = "strict_cast"
	RankGroupScorer      = "rank_group_scorer"
	AnnsFieldKey         = "anns_field"
	AnalyzerKey          = "analyzer_name"
	TopKKey              = "topk"
	NQKey                = "nq"
	MetricTypeKey        = common.MetricTypeKey
	ParamsKey            = common.ParamsKey
	ExprParamsKey        = "expr_params"
	RoundDecimalKey      = "round_decimal"
	OffsetKey            = "offset"
	LimitKey             = "limit"
	// key for timestamptz translation
	TimefieldsKey = "time_fields"

	SearchIterV2Key        = "search_iter_v2"
	SearchIterBatchSizeKey = "search_iter_batch_size"
	SearchIterLastBoundKey = "search_iter_last_bound"
	SearchIterIdKey        = "search_iter_id"
	QueryIterLastPKKey     = "query_iter_last_pk"
	QueryIterLastOffsetKey = "query_iter_last_element_offset"
	GroupByFieldsKey       = "group_by_fields"
	OrderByFieldsKey       = "order_by_fields"
	PipelineTraceKey       = "pipeline_trace"

	InsertTaskName                = "InsertTask"
	CreateCollectionTaskName      = "CreateCollectionTask"
	DropCollectionTaskName        = "DropCollectionTask"
	TruncateCollectionTaskName    = "TruncateCollectionTask"
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
	AddCollectionFunctionTask     = "AddCollectionFunctionTask"
	AlterCollectionFunctionTask   = "AlterCollectionFunctionTask"
	DropCollectionFunctionTask    = "DropCollectionFunctionTask"
	UpsertTaskName                = "UpsertTask"
	CreateResourceGroupTaskName   = "CreateResourceGroupTask"
	UpdateResourceGroupsTaskName  = "UpdateResourceGroupsTask"
	DropResourceGroupTaskName     = "DropResourceGroupTask"
	TransferNodeTaskName          = "TransferNodeTask"
	TransferReplicaTaskName       = "TransferReplicaTask"
	ListResourceGroupsTaskName    = "ListResourceGroupsTask"
	DescribeResourceGroupTaskName = "DescribeResourceGroupTask"
	RunAnalyzerTaskName           = "RunAnalyzer"
	HighlightTaskName             = "Highlight"

	CreateDatabaseTaskName   = "CreateCollectionTask"
	DropDatabaseTaskName     = "DropDatabaseTaskName"
	ListDatabaseTaskName     = "ListDatabaseTaskName"
	AlterDatabaseTaskName    = "AlterDatabaseTaskName"
	DescribeDatabaseTaskName = "DescribeDatabaseTaskName"

	AddFieldTaskName              = "AddFieldTaskName"
	AlterCollectionSchemaTaskName = "AlterCollectionSchemaTaskName"

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
			if !typeutil.IsClusteringKeyType(field.GetDataType()) {
				return merr.WrapErrCollectionIllegalSchema(t.CollectionName,
					fmt.Sprintf("clustering key field %s has unsupported data type %s", field.Name, field.GetDataType().String()))
			}
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

func validateCollectionTTL(props []*commonpb.KeyValuePair) (bool, error) {
	for _, pair := range props {
		if pair.Key == common.CollectionTTLConfigKey {
			val, err := strconv.Atoi(pair.Value)
			if err != nil {
				return true, merr.WrapErrParameterInvalidMsg("collection TTL is not a valid positive integer")
			}
			if val < -1 || val > common.MaxTTLSeconds {
				return true, merr.WrapErrParameterInvalidMsg("collection TTL is out of range, expect [-1, 3155760000], got %d", val)
			}
			return true, nil
		}
	}
	return false, nil
}

func validateTTLField(props []*commonpb.KeyValuePair, fields []*schemapb.FieldSchema) (bool, error) {
	for _, pair := range props {
		if pair.Key == common.CollectionTTLFieldKey {
			fieldName := pair.Value
			for _, field := range fields {
				if field.Name == fieldName {
					if field.DataType != schemapb.DataType_Timestamptz {
						return true, merr.WrapErrParameterInvalidMsg("ttl field must be timestamptz, field name = %s", fieldName)
					}
					return true, nil
				}
			}
			return true, merr.WrapErrParameterInvalidMsg("ttl field name %s not found in schema", fieldName)
		}
	}
	return false, nil
}

func (t *createCollectionTask) validateTTL() error {
	hasCollectionTTL, err := validateCollectionTTL(t.GetProperties())
	if err != nil {
		return err
	}

	hasTTLField, err := validateTTLField(t.GetProperties(), t.schema.Fields)
	if err != nil {
		return err
	}

	if hasCollectionTTL && hasTTLField {
		return merr.WrapErrParameterInvalidMsg("collection TTL and ttl field cannot be set at the same time")
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
	t.schema.DbName = t.GetDbName()

	isExternalCollection := typeutil.IsExternalCollection(t.schema)
	if err := typeutil.NormalizeAndValidateExternalCollectionSchema(t.schema); err != nil {
		return err
	}

	disableCheck, err := common.IsDisableFuncRuntimeCheck(t.GetProperties()...)
	if err != nil {
		return err
	}
	if err := validateFunction(t.schema, "", disableCheck); err != nil {
		return err
	}

	// External collections must be single-shard: the refresh mechanism assigns all
	// segments to VChannelNames[0], so multiple shards would leave segments orphaned.
	if isExternalCollection && t.ShardsNum > 1 {
		return fmt.Errorf("external collection does not support multiple shards, got ShardsNum=%d", t.ShardsNum)
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

	// Reject reserved system field names supplied by the user before any
	// server-side injection runs. __virtual_pk__ is the internal PK name
	// auto-injected for external collections; RowID and Timestamp are
	// segcore-internal columns. Allowing a user field under these names
	// would create a namespace trap for any tooling that assumes they are
	// system-owned (issue #49314). Must run before
	// injectVirtualPKForExternalCollection so the check only sees
	// user-supplied fields.
	if err := validateReservedFieldNames(t.schema); err != nil {
		return err
	}

	// For external collections, inject virtual PK field if no PK exists
	if isExternalCollection {
		if err := injectVirtualPKForExternalCollection(t.schema); err != nil {
			return err
		}
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

	// validate query mode
	if err := common.ValidateQueryMode(t.GetProperties()...); err != nil {
		return err
	}

	// Validate timezone
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.GetProperties())
	if exist && !timestamptz.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}

	// Validate collection ttl
	_, err = common.GetCollectionTTL(t.GetProperties())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("collection ttl property value not valid, parse error: %s", err.Error())
	}

	// Validate warmup policy for all warmup keys
	if hasWarmupProp(t.GetProperties()...) {
		for _, prop := range t.GetProperties() {
			if common.IsFieldWarmupKey(prop.GetKey()) {
				return merr.WrapErrParameterInvalidMsg("warmup key '%s' is only allowed at field level, use warmup.scalarField/warmup.scalarIndex/warmup.vectorField/warmup.vectorIndex at collection level", prop.GetKey())
			}
			if common.IsCollectionWarmupKey(prop.GetKey()) {
				if err := common.ValidateWarmupPolicy(prop.GetValue()); err != nil {
					return merr.WrapErrParameterInvalidMsg("invalid warmup value for key %s: %s", prop.GetKey(), err.Error())
				}
			}
		}
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

	// Transform struct field names to ensure global uniqueness
	// This allows different structs to have fields with the same name
	err = transformStructFieldNames(t.schema)
	if err != nil {
		return fmt.Errorf("failed to transform struct field names: %v", err)
	}

	// validate whether field names duplicates (after transformation)
	if err := validateDuplicatedFieldName(t.schema); err != nil {
		return err
	}

	if err := validateMultipleVectorFields(t.schema); err != nil {
		return err
	}

	if err := validateLoadFieldsList(t.schema); err != nil {
		return err
	}

	if err := t.validateTTL(); err != nil {
		return err
	}

	t.Schema, err = proto.Marshal(t.schema)
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
	if err := proto.Unmarshal(t.GetSchema(), t.fieldSchema); err != nil {
		return err
	}
	if err := validateAddFieldRequest(t.oldSchema, t.fieldSchema); err != nil {
		return err
	}
	// User-added fields must be nullable so that old segments without this field can return
	// NULL rather than causing a schema inconsistency at query time.
	if !t.fieldSchema.GetNullable() {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("added field must be nullable, please check it, field name = %s", t.fieldSchema.GetName()))
	}
	if err := ValidateField(t.fieldSchema, t.oldSchema); err != nil {
		return err
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

// validateAddFieldRequest validates both the old schema constraints and the new field properties
// for an AddCollectionField request. It is the single source of truth for add-field validation.
func validateAddFieldRequest(schema *schemapb.CollectionSchema, newFieldSchema *schemapb.FieldSchema) error {
	// --- old schema constraints ---
	fieldList := typeutil.NewSet[string]()
	for _, f := range schema.Fields {
		fieldList.Insert(f.Name)
	}
	if len(fieldList) >= Params.ProxyCfg.MaxFieldNum.GetAsInt() {
		msg := fmt.Sprintf("The number of fields has reached the maximum value %d", Params.ProxyCfg.MaxFieldNum.GetAsInt())
		return merr.WrapErrParameterInvalidMsg(msg)
	}
	if fieldList.Contain(newFieldSchema.GetName()) {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("duplicated field name %s", newFieldSchema.GetName()))
	}

	// --- new field property constraints ---
	if _, ok := schemapb.DataType_name[int32(newFieldSchema.GetDataType())]; !ok || newFieldSchema.GetDataType() == schemapb.DataType_None {
		return merr.WrapErrParameterInvalid("valid field", fmt.Sprintf("field data type: %s is not supported", newFieldSchema.GetDataType()))
	}
	if funcutil.SliceContain([]string{common.RowIDFieldName, common.TimeStampFieldName, common.MetaFieldName, common.NamespaceFieldName, common.VirtualPKFieldName}, newFieldSchema.GetName()) {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support to add system field, field name = %s", newFieldSchema.GetName()))
	}
	if newFieldSchema.GetIsPrimaryKey() {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support to add pk field, field name = %s", newFieldSchema.GetName()))
	}
	if newFieldSchema.GetAutoID() {
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("only primary field can speficy AutoID with true, field name = %s", newFieldSchema.GetName()))
	}
	if newFieldSchema.GetIsPartitionKey() {
		return merr.WrapErrParameterInvalidMsg("not support to add partition key field, field name  = %s", newFieldSchema.GetName())
	}
	if newFieldSchema.GetIsClusteringKey() {
		if !typeutil.IsClusteringKeyType(newFieldSchema.GetDataType()) {
			return merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("clustering key field %s has unsupported data type %s",
					newFieldSchema.GetName(), newFieldSchema.GetDataType().String()))
		}
		for _, f := range schema.GetFields() {
			if f.GetIsClusteringKey() {
				return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("already has another clustering key field, field name: %s", newFieldSchema.GetName()))
			}
		}
	}
	if typeutil.IsVectorType(newFieldSchema.DataType) {
		vectorFields := len(typeutil.GetVectorFieldSchemas(schema))
		if vectorFields >= Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
			return fmt.Errorf("maximum vector field's number should be limited to %d", Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
		}
	}

	// NOTE: The nullable requirement for added fields is enforced by callers that deal with
	// user-defined fields (e.g. addCollectionFieldTask). Function output fields managed by
	// alterCollectionSchemaTask are explicitly prohibited from being nullable by validateFunction,
	// so the check is intentionally left to callers rather than enforced here universally.
	//
	// Dense vector types require a dimension TypeParam. This check applies unconditionally
	// (regardless of nullable) because a vector field without dimension is always invalid.
	// SparseFloatVector is excluded because it does not have a fixed dimension by design.
	if typeutil.IsVectorType(newFieldSchema.DataType) {
		if newFieldSchema.DataType == schemapb.DataType_FloatVector ||
			newFieldSchema.DataType == schemapb.DataType_Float16Vector ||
			newFieldSchema.DataType == schemapb.DataType_BFloat16Vector ||
			newFieldSchema.DataType == schemapb.DataType_BinaryVector ||
			newFieldSchema.DataType == schemapb.DataType_Int8Vector {
			if len(newFieldSchema.TypeParams) == 0 {
				return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("vector field must have dimension specified, field name = %s", newFieldSchema.GetName()))
			}
		}
	}
	return nil
}

type alterCollectionSchemaTask struct {
	baseTask
	Condition
	*milvuspb.AlterCollectionSchemaRequest
	*milvuspb.AlterCollectionSchemaResponse
	ctx       context.Context
	mixCoord  types.MixCoordClient
	oldSchema *schemapb.CollectionSchema
}

func (t *alterCollectionSchemaTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterCollectionSchemaTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *alterCollectionSchemaTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *alterCollectionSchemaTask) Name() string {
	return AlterCollectionSchemaTaskName
}

func (t *alterCollectionSchemaTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *alterCollectionSchemaTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionSchemaTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionSchemaTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *alterCollectionSchemaTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterCollectionSchema
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *alterCollectionSchemaTask) PreExecute(ctx context.Context) error {
	if t.oldSchema == nil {
		return merr.WrapErrParameterInvalidMsg("empty old schema in alter collection schema task")
	}

	action := t.GetAction()
	if action == nil {
		return merr.WrapErrParameterInvalidMsg("action is nil in alter schema task")
	}
	addRequest := action.GetAddRequest()
	if addRequest == nil {
		return merr.WrapErrParameterInvalidMsg("add_request is nil, only add operation is supported for now")
	}

	fieldInfos := addRequest.GetFieldInfos()
	funcSchemas := addRequest.GetFuncSchema()

	// AlterCollectionSchema currently only supports adding exactly one function with its output fields.
	// RootCoord enforces the same constraint (ddl_callbacks_alter_collection_schema.go);
	// validate early at Proxy to give clearer error messages.
	if len(funcSchemas) != 1 || funcSchemas[0] == nil {
		return merr.WrapErrParameterInvalidMsg("For now, exactly one function schema is required in alter schema task")
	}
	if len(fieldInfos) == 0 {
		return merr.WrapErrParameterInvalidMsg("fieldInfos is empty, function output fields are required")
	}

	if len(fieldInfos) != 1 {
		return merr.WrapErrParameterInvalidMsg("For now, only one field info is supported in alter schema task")
	}
	newFieldSchema := fieldInfos[0].GetFieldSchema()
	if newFieldSchema == nil {
		return merr.WrapErrParameterInvalidMsg("empty new field schema in alter schema task")
	}
	if err := validateAddFieldRequest(t.oldSchema, newFieldSchema); err != nil {
		return err
	}
	if err := ValidateField(newFieldSchema, t.oldSchema); err != nil {
		return err
	}

	// Verify that every OutputFieldName of the function refers to one of the newly-added
	// fields (from fieldInfos), not to a field that already exists in the old schema.
	// This prevents a caller from wiring function output to an existing field, which
	// would corrupt that field's data silently.
	newFieldNames := make(map[string]struct{}, len(fieldInfos))
	for _, fi := range fieldInfos {
		if fi.GetFieldSchema() != nil {
			newFieldNames[fi.GetFieldSchema().GetName()] = struct{}{}
		}
	}
	for _, outName := range funcSchemas[0].GetOutputFieldNames() {
		if _, isNew := newFieldNames[outName]; !isNew {
			return merr.WrapErrParameterInvalidMsg(
				"function output field %q must be one of the newly-added fields, not an existing field",
				outName,
			)
		}
	}

	// Physical backfill is currently only implemented for BM25 in the datanode backfill
	// compactor. Reject unsupported types early so the request never reaches RootCoord
	// and no segment is left in an unrecoverable stale-schema state.
	if addRequest.GetDoPhysicalBackfill() && funcSchemas[0].GetType() != schemapb.FunctionType_BM25 {
		return merr.WrapErrParameterInvalidMsg(
			"physical backfill is currently only supported for BM25 functions, got %s",
			funcSchemas[0].GetType().String())
	}

	// Validate function-field type compatibility (e.g., BM25 requires varchar input,
	// SparseFloatVector output). Construct a merged schema with old fields + new fields
	// + new function, then validate only the new function to avoid re-checking existing
	// functions' runtime providers.
	// Deep-copy each new field schema so validateFunction's mutations (e.g. clearing
	// IsFunctionOutput) do not affect the original request objects.
	mergedSchema := proto.Clone(t.oldSchema).(*schemapb.CollectionSchema)
	for _, fieldInfo := range fieldInfos {
		mergedSchema.Fields = append(mergedSchema.Fields, proto.Clone(fieldInfo.GetFieldSchema()).(*schemapb.FieldSchema))
	}
	mergedSchema.Functions = append(mergedSchema.Functions, funcSchemas[0])
	if err := validateFunction(mergedSchema, funcSchemas[0].GetName(), false); err != nil {
		return err
	}

	return nil
}

func (t *alterCollectionSchemaTask) Execute(ctx context.Context) error {
	action := t.GetAction()
	if action != nil {
		addRequest := action.GetAddRequest()
		if addRequest != nil {
			for _, fieldInfo := range addRequest.GetFieldInfos() {
				if fieldInfo != nil && fieldInfo.GetFieldSchema() != nil {
					fieldInfo.GetFieldSchema().IsFunctionOutput = true
				}
			}
		}
	}
	var err error
	t.AlterCollectionSchemaResponse, err = t.mixCoord.AlterCollectionSchema(ctx, t.AlterCollectionSchemaRequest)
	return merr.CheckRPCCall(t.GetAlterStatus(), err)
}

func (t *alterCollectionSchemaTask) PostExecute(ctx context.Context) error {
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
	_, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrDatabaseNotFound) {
			// make dropping collection idempotent.
			log.Ctx(ctx).Warn("drop non-existent collection", zap.String("collection", t.GetCollectionName()), zap.String("database", t.GetDbName()))
			return nil
		}
		return err
	}

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

type truncateCollectionTask struct {
	baseTask
	Condition
	*milvuspb.TruncateCollectionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.TruncateCollectionResponse
	chMgr    channelsMgr
}

func (t *truncateCollectionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *truncateCollectionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *truncateCollectionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *truncateCollectionTask) Name() string {
	return TruncateCollectionTaskName
}

func (t *truncateCollectionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *truncateCollectionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *truncateCollectionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *truncateCollectionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *truncateCollectionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_TruncateCollection
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *truncateCollectionTask) PreExecute(ctx context.Context) error {
	if err := validateCollectionName(t.CollectionName); err != nil {
		return err
	}
	// Truncate is a destructive op on internal segments. External collections
	// have no internal data to truncate — their authoritative data lives in
	// the user's object store and is materialized by RefreshExternalCollection.
	// Allowing truncate would either no-op silently (misleading) or wipe the
	// generated segment metadata while leaving the external source intact,
	// putting the collection in an inconsistent state from which the next
	// load/search would fail. Reject up front; users who want to reset the
	// view should use RefreshExternalCollection or DropCollection.
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		return err
	}
	if typeutil.IsExternalCollection(collSchema.CollectionSchema) {
		return merr.WrapErrParameterInvalidMsg(
			"truncate is not supported on external collections; use RefreshExternalCollection to refresh the data view, or DropCollection to remove it")
	}
	return nil
}

func (t *truncateCollectionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.TruncateCollection(ctx, t.TruncateCollectionRequest)
	return merr.CheckRPCCall(t.result, err)
}

func (t *truncateCollectionTask) PostExecute(ctx context.Context) error {
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
	_, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
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
	t.result.Schema.ExternalSource = result.Schema.ExternalSource
	// Pass spec through unredacted; the public proxy.DescribeCollection
	// entry point applies RedactExternalSpec uniformly across cached and
	// remote provider paths so internal-only callers of this task path
	// (if any) still observe raw creds for FFI auth.
	t.result.Schema.ExternalSpec = result.Schema.ExternalSpec
	t.result.Schema.EnableNamespace = result.Schema.EnableNamespace
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
	t.result.DbId = result.GetDbId()
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
			ExternalField:    field.GetExternalField(),
		}
	}

	for _, field := range result.Schema.Fields {
		if field.IsDynamic || field.Name == common.NamespaceFieldName {
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

	if err := restoreStructFieldNames(t.result.Schema); err != nil {
		return fmt.Errorf("failed to restore struct field names: %v", err)
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
			ShardsNum:             make([]int32, 0, len(resp.CollectionIDs)),
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
			t.result.ShardsNum = append(t.result.ShardsNum, collectionInfo.shardsNum)
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
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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

func hasTTLProp(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.CollectionTTLConfigKey {
			return true
		}
	}
	return false
}

func hasTTLFieldProp(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.CollectionTTLFieldKey {
			return true
		}
	}
	return false
}

func hasWarmupProp(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if common.IsWarmupKey(p.GetKey()) {
			return true
		}
	}
	return false
}

func hasPropInDeletekeys(keys []string) string {
	for _, key := range keys {
		if key == common.MmapEnabledKey || common.IsWarmupKey(key) {
			return key
		}
	}
	return ""
}

// checkVectorIndexExist checks if the collection has any vector index.
// Returns the vector field name that has an index, or empty string if none.
func checkVectorIndexExist(ctx context.Context, dbName, collectionName string, collectionID int64, mixCoord types.MixCoordClient) (string, error) {
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, collectionName)
	if err != nil {
		return "", err
	}

	indexResponse, err := mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collectionID,
		IndexName:    "",
	})
	if err = merr.CheckRPCCall(indexResponse, err); err != nil && !errors.Is(err, merr.ErrIndexNotFound) {
		return "", merr.WrapErrServiceInternal("describe index failed", err.Error())
	}
	for _, index := range indexResponse.IndexInfos {
		for _, field := range collSchema.Fields {
			if index.FieldID == field.FieldID && typeutil.IsVectorType(field.DataType) {
				return field.GetName(), nil
			}
		}
	}
	return "", nil
}

// detectBoolPropChange detects whether a boolean collection property is being
// changed via Properties or DeleteKeys. parseFn validates and parses the new
// value when the key is found in Properties.
// only one of properties or deleteKeys should be provided
func detectBoolPropChange(
	oldValue bool,
	propKey string,
	properties []*commonpb.KeyValuePair,
	deleteKeys []string,
	parseFn func() (bool, error),
) (newValue bool, changed bool, err error) {
	// this is duplicated with the check in alterCollectionTask PreExecute
	if len(properties) > 0 && len(deleteKeys) > 0 {
		return false, false, merr.WrapErrParameterInvalidMsg("cannot provide both DeleteKeys and ExtraParams")
	}
	newValue = oldValue
	if _, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(propKey, properties); ok {
		newValue, err = parseFn()
		if err != nil {
			return false, false, err
		}
		changed = oldValue != newValue
	}
	for _, key := range deleteKeys {
		if key == propKey {
			newValue = false
			changed = oldValue != newValue
			break
		}
	}
	return newValue, changed, nil
}

// detectQueryModeChange detects whether the query_mode collection property is
// being changed via Properties or DeleteKeys. Returns the new query mode string
// (empty string means no query mode) and whether it changed.
func detectQueryModeChange(
	oldQueryMode string,
	properties []*commonpb.KeyValuePair,
	deleteKeys []string,
) (newQueryMode string, changed bool, err error) {
	// this is duplicated with the check in alterCollectionTask PreExecute
	if len(properties) > 0 && len(deleteKeys) > 0 {
		return "", false, merr.WrapErrParameterInvalidMsg("cannot provide both DeleteKeys and ExtraParams")
	}
	newQueryMode = oldQueryMode
	if common.IsQueryModeKeyExists(properties...) {
		if err := common.ValidateQueryMode(properties...); err != nil {
			return "", false, err
		}
		newQueryMode = common.GetQueryMode(properties...)
		changed = oldQueryMode != newQueryMode
	}
	for _, key := range deleteKeys {
		if key == common.QueryModeKey {
			newQueryMode = ""
			changed = oldQueryMode != newQueryMode
			break
		}
	}
	return newQueryMode, changed, nil
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

	// External source/spec form an atomic tuple bound to the physical data
	// layout. The only supported way to change them is RefreshExternalCollection,
	// which applies them atomically and re-runs the data load pipeline.
	for _, prop := range t.GetProperties() {
		if prop.GetKey() == common.CollectionExternalSource || prop.GetKey() == common.CollectionExternalSpec {
			return merr.WrapErrParameterInvalidMsg(
				"cannot alter %s via alter_collection_properties; use RefreshExternalCollection instead",
				prop.GetKey())
		}
	}
	for _, key := range t.GetDeleteKeys() {
		if key == common.CollectionExternalSource || key == common.CollectionExternalSpec {
			return merr.WrapErrParameterInvalidMsg(
				"cannot delete %s; external source/spec are immutable post-create except via RefreshExternalCollection",
				key)
		}
	}

	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}

	t.CollectionID = collectionID

	if len(t.GetProperties()) > 0 {
		hasMmap := hasMmapProp(t.Properties...)
		hasWarmup := hasWarmupProp(t.Properties...)
		if hasMmap || hasWarmup {
			loaded, err := isCollectionLoaded(ctx, t.mixCoord, t.CollectionID)
			if err != nil {
				return err
			}
			if loaded {
				// keeping the original error msg here for compatibility
				if hasMmap {
					return merr.WrapErrCollectionLoaded(t.CollectionName, "can not alter mmap properties if collection loaded")
				}
				if hasWarmup {
					return merr.WrapErrCollectionLoaded(t.CollectionName, "can not alter warmup properties if collection loaded")
				}
			}
		}

		enabled, _ := common.IsAllowInsertAutoID(t.Properties...)
		if enabled {
			primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(collSchema.CollectionSchema)
			if err != nil {
				return err
			}
			if !primaryFieldSchema.AutoID {
				return merr.WrapErrParameterInvalidMsg("the value for %s must be false when autoID is false", common.AllowInsertAutoIDKey)
			}
		}
		// Check the validation of timezone
		userDefinedTimezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.Properties)
		if exist && !timestamptz.IsTimezoneValid(userDefinedTimezone) {
			return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", userDefinedTimezone)
		}

		hasTTL, err := validateCollectionTTL(t.GetProperties())
		if err != nil {
			return err
		}
		hasTTLField, err := validateTTLField(t.GetProperties(), collSchema.GetFields())
		if err != nil {
			return err
		}
		if hasTTL && hasTTLField {
			return merr.WrapErrParameterInvalidMsg("collection TTL and ttl field cannot be set at the same time")
		}
		if hasTTL && hasTTLFieldProp(collSchema.GetProperties()...) {
			return merr.WrapErrParameterInvalidMsg("ttl field is already exists, cannot be set collection TTL")
		}
		if hasTTLField && hasTTLProp(collSchema.GetProperties()...) {
			return merr.WrapErrParameterInvalidMsg("collection TTL is already set, cannot be set ttl field")
		}

		// Validate warmup policy for all warmup keys
		if hasWarmupProp(t.Properties...) {
			for _, prop := range t.Properties {
				if common.IsFieldWarmupKey(prop.GetKey()) {
					return merr.WrapErrParameterInvalidMsg("warmup key '%s' is only allowed at field level, use warmup.scalarField/warmup.scalarIndex/warmup.vectorField/warmup.vectorIndex at collection level", prop.GetKey())
				}
				if common.IsCollectionWarmupKey(prop.GetKey()) {
					if err := common.ValidateWarmupPolicy(prop.GetValue()); err != nil {
						return merr.WrapErrParameterInvalidMsg("invalid warmup value for key %s: %s", prop.GetKey(), err.Error())
					}
				}
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
				if key == common.MmapEnabledKey {
					return merr.WrapErrCollectionLoaded(t.CollectionName, "can not delete mmap properties if collection loaded")
				}
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not delete %s properties if collection loaded", key)
			}
		}
	}

	isPartitionKeyMode, err := isPartitionKeyMode(ctx, t.GetDbName(), t.CollectionName)
	if err != nil {
		return err
	}
	collBasicInfo, err := globalMetaCache.GetCollectionInfo(t.ctx, t.GetDbName(), t.CollectionName, t.CollectionID)
	if err != nil {
		return err
	}
	newIsoValue, isoChanged, err := detectBoolPropChange(
		collBasicInfo.partitionKeyIsolation, common.PartitionKeyIsolationKey,
		t.Properties, t.GetDeleteKeys(),
		func() (bool, error) {
			return validatePartitionKeyIsolation(ctx, t.CollectionName, isPartitionKeyMode, t.Properties...)
		},
	)
	if err != nil {
		return err
	}

	newQueryMode, queryModeChanged, err := detectQueryModeChange(
		collBasicInfo.queryMode,
		t.Properties, t.GetDeleteKeys(),
	)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info("alter collection pre check with partition key isolation/query mode",
		zap.String("collectionName", t.CollectionName),
		zap.Bool("isPartitionKeyMode", isPartitionKeyMode),
		zap.Bool("newIsoValue", newIsoValue),
		zap.Bool("oldIsoValue", collBasicInfo.partitionKeyIsolation),
		zap.String("newQueryMode", newQueryMode),
		zap.String("oldQueryMode", collBasicInfo.queryMode))

	// If partition key isolation or query_mode changed, check for existing vector index.
	// Changing these properties requires dropping the vector index first.
	if isoChanged || queryModeChanged {
		if vecField, err := checkVectorIndexExist(ctx, t.GetDbName(), t.CollectionName, t.CollectionID, t.mixCoord); err != nil {
			return err
		} else if vecField != "" {
			if isoChanged {
				return merr.WrapErrIndexDuplicate(vecField,
					"can not alter partition key isolation mode if the collection already has a vector index. Please drop the index first")
			}
			if queryModeChanged {
				return merr.WrapErrIndexDuplicate(vecField,
					"can not alter "+common.QueryModeKey+" if the collection already has a vector index. Please drop the index first")
			}
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
	return nil
}

func (t *alterCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterCollectionFieldTask struct {
	baseTask
	Condition
	*milvuspb.AlterCollectionFieldRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	common.FieldDescriptionKey,
	common.WarmupKey,
	common.WarmupScalarFieldKey,
	common.WarmupScalarIndexKey,
	common.WarmupVectorFieldKey,
	common.WarmupVectorIndexKey,
}

var allowedDropProps = []string{
	common.MmapEnabledKey,
	common.WarmupKey,
	common.WarmupScalarFieldKey,
	common.WarmupScalarIndexKey,
	common.WarmupVectorFieldKey,
	common.WarmupVectorIndexKey,
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

		case common.WarmupKey:
			loaded, err := isCollectionLoadedFn()
			if err != nil {
				return err
			}
			if loaded {
				return merr.WrapErrCollectionLoaded(t.CollectionName, "can not alter warmup if collection loaded")
			}
			if err := common.ValidateWarmupPolicy(prop.Value); err != nil {
				return merr.WrapErrParameterInvalidMsg(err.Error())
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

		if updatedKey == common.MmapEnabledKey || common.IsFieldWarmupKey(updatedKey) {
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

	// Check partition key mode
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), collName)
	if err != nil {
		return err
	}
	if typeutil.HasPartitionKey(collSchema.CollectionSchema) {
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

	// Check partition key mode
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, t.GetDbName(), collName)
	if err != nil {
		return err
	}
	if typeutil.HasPartitionKey(collSchema.CollectionSchema) {
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
		if errors.Is(merr.ErrPartitionNotFound, err) || errors.Is(merr.ErrCollectionNotFound, err) || errors.Is(merr.ErrDatabaseNotFound, err) {
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

	collectionID UniqueID
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
	loadPriorityStr, ok := t.LoadParams[LoadPriorityName]
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
	allFields := typeutil.GetAllFieldSchemas(collSchema.CollectionSchema)
	for _, field := range allFields {
		if typeutil.IsVectorType(field.GetDataType()) && loadFieldsSet.Contain(field.GetFieldID()) {
			if _, ok := fieldIndexIDs[field.GetFieldID()]; !ok {
				unindexedVecFields = append(unindexedVecFields, field.GetName())
			}
		}
	}

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

	collectionID UniqueID
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
	loadPriorityStr, ok := t.LoadParams[LoadPriorityName]
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
	allFields := typeutil.GetAllFieldSchemas(collSchema.CollectionSchema)
	for _, field := range allFields {
		if typeutil.IsVectorType(field.GetDataType()) && loadFieldsSet.Contain(field.GetFieldID()) {
			if _, ok := fieldIndexIDs[field.GetFieldID()]; !ok {
				unindexedVecFields = append(unindexedVecFields, field.GetName())
			}
		}
	}

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
		Base:           t.GetBase(),
		ResourceGroups: t.GetResourceGroups(),
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
	lb           shardclient.LBPolicy

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
	err := t.lb.ExecuteOneChannel(ctx, shardclient.CollectionWorkLoad{
		Db:             t.dbName,
		CollectionName: t.GetCollectionName(),
		CollectionID:   t.collectionID,
		Nq:             int64(len(t.GetPlaceholder())),
		Exec:           t.runAnalyzerOnShardleader,
	})

	return err
}

func (t *RunAnalyzerTask) PostExecute(ctx context.Context) error {
	return nil
}

// git highlight after search
type HighlightTask struct {
	baseTask
	Condition
	*querypb.GetHighlightRequest
	ctx            context.Context
	collectionName string
	collectionID   typeutil.UniqueID
	dbName         string
	lb             shardclient.LBPolicy

	result *querypb.GetHighlightResponse
}

func (t *HighlightTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *HighlightTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *HighlightTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *HighlightTask) Name() string {
	return HighlightTaskName
}

func (t *HighlightTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *HighlightTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *HighlightTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Undefined
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *HighlightTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *HighlightTask) getHighlightOnShardleader(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	ctx = retry.WithMaxAttemptsContext(ctx, 1)
	t.Channel = channel
	resp, err := qn.GetHighlight(ctx, t.GetHighlightRequest)
	if err != nil {
		return err
	}

	if err := merr.Error(resp.GetStatus()); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *HighlightTask) Execute(ctx context.Context) error {
	err := t.lb.ExecuteOneChannel(ctx, shardclient.CollectionWorkLoad{
		Db:             t.dbName,
		CollectionName: t.collectionName,
		CollectionID:   t.collectionID,
		Nq:             int64(len(t.GetTopks()) * len(t.GetTasks())),
		Exec:           t.getHighlightOnShardleader,
	})

	return err
}

func (t *HighlightTask) PostExecute(ctx context.Context) error {
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
