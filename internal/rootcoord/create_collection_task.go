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

package rootcoord

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	ms "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type collectionChannels struct {
	virtualChannels  []string
	physicalChannels []string
}

type createCollectionTask struct {
	baseTask
	Req            *milvuspb.CreateCollectionRequest
	schema         *schemapb.CollectionSchema
	collID         UniqueID
	partIDs        []UniqueID
	channels       collectionChannels
	dbID           UniqueID
	partitionNames []string
}

func (t *createCollectionTask) validate() error {
	if t.Req == nil {
		return errors.New("empty requests")
	}

	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreateCollection); err != nil {
		return err
	}

	// 1. check shard number
	shardsNum := t.Req.GetShardsNum()
	cfgMaxShardNum := Params.RootCoordCfg.DmlChannelNum.GetAsInt32()
	if shardsNum > cfgMaxShardNum {
		return fmt.Errorf("shard num (%d) exceeds max configuration (%d)", shardsNum, cfgMaxShardNum)
	}

	cfgShardLimit := Params.ProxyCfg.MaxShardNum.GetAsInt32()
	if shardsNum > cfgShardLimit {
		return fmt.Errorf("shard num (%d) exceeds system limit (%d)", shardsNum, cfgShardLimit)
	}

	// 2. check db-collection capacity
	db2CollIDs := t.core.meta.ListAllAvailCollections(t.ctx)
	if err := t.checkMaxCollectionsPerDB(db2CollIDs); err != nil {
		return err
	}

	// 3. check total collection number
	totalCollections := 0
	for _, collIDs := range db2CollIDs {
		totalCollections += len(collIDs)
	}

	maxCollectionNum := Params.QuotaConfig.MaxCollectionNum.GetAsInt()
	if totalCollections >= maxCollectionNum {
		log.Warn("unable to create collection because the number of collection has reached the limit", zap.Int("max_collection_num", maxCollectionNum))
		return merr.WrapErrCollectionNumLimitExceeded(t.Req.GetDbName(), maxCollectionNum)
	}

	// 4. check collection * shard * partition
	var newPartNum int64 = 1
	if t.Req.GetNumPartitions() > 0 {
		newPartNum = t.Req.GetNumPartitions()
	}
	return checkGeneralCapacity(t.ctx, 1, newPartNum, t.Req.GetShardsNum(), t.core, t.ts)
}

// checkMaxCollectionsPerDB DB properties take precedence over quota configurations for max collections.
func (t *createCollectionTask) checkMaxCollectionsPerDB(db2CollIDs map[int64][]int64) error {
	collIDs, ok := db2CollIDs[t.dbID]
	if !ok {
		log.Warn("can not found DB ID", zap.String("collection", t.Req.GetCollectionName()), zap.String("dbName", t.Req.GetDbName()))
		return merr.WrapErrDatabaseNotFound(t.Req.GetDbName(), "failed to create collection")
	}

	db, err := t.core.meta.GetDatabaseByName(t.ctx, t.Req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		log.Warn("can not found DB ID", zap.String("collection", t.Req.GetCollectionName()), zap.String("dbName", t.Req.GetDbName()))
		return merr.WrapErrDatabaseNotFound(t.Req.GetDbName(), "failed to create collection")
	}

	check := func(maxColNumPerDB int) error {
		if len(collIDs) >= maxColNumPerDB {
			log.Warn("unable to create collection because the number of collection has reached the limit in DB", zap.Int("maxCollectionNumPerDB", maxColNumPerDB))
			return merr.WrapErrCollectionNumLimitExceeded(t.Req.GetDbName(), maxColNumPerDB)
		}
		return nil
	}

	maxColNumPerDBStr := db.GetProperty(common.DatabaseMaxCollectionsKey)
	if maxColNumPerDBStr != "" {
		maxColNumPerDB, err := strconv.Atoi(maxColNumPerDBStr)
		if err != nil {
			log.Warn("parse value of property fail", zap.String("key", common.DatabaseMaxCollectionsKey),
				zap.String("value", maxColNumPerDBStr), zap.Error(err))
			return fmt.Errorf(fmt.Sprintf("parse value of property fail, key:%s, value:%s", common.DatabaseMaxCollectionsKey, maxColNumPerDBStr))
		}
		return check(maxColNumPerDB)
	}

	maxColNumPerDB := Params.QuotaConfig.MaxCollectionNumPerDB.GetAsInt()
	return check(maxColNumPerDB)
}

func checkFieldSchema(schema *schemapb.CollectionSchema) error {
	for _, fieldSchema := range schema.Fields {
		if fieldSchema.GetNullable() && typeutil.IsVectorType(fieldSchema.GetDataType()) {
			msg := fmt.Sprintf("vector type not support null, type:%s, name:%s", fieldSchema.GetDataType().String(), fieldSchema.GetName())
			return merr.WrapErrParameterInvalidMsg(msg)
		}
		if fieldSchema.GetDefaultValue() != nil {
			switch fieldSchema.GetDefaultValue().Data.(type) {
			case *schemapb.ValueField_BoolData:
				if fieldSchema.GetDataType() != schemapb.DataType_Bool {
					return merr.WrapErrParameterInvalid("DataType_Bool", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_IntData:
				if fieldSchema.GetDataType() != schemapb.DataType_Int32 &&
					fieldSchema.GetDataType() != schemapb.DataType_Int16 &&
					fieldSchema.GetDataType() != schemapb.DataType_Int8 {
					return merr.WrapErrParameterInvalid("DataType_Int", "not match", "default value type mismatches field schema type")
				}
				defaultValue := fieldSchema.GetDefaultValue().GetIntData()
				if fieldSchema.GetDataType() == schemapb.DataType_Int16 {
					if defaultValue > math.MaxInt16 || defaultValue < math.MinInt16 {
						return merr.WrapErrParameterInvalidRange(math.MinInt16, math.MaxInt16, defaultValue, "default value out of range")
					}
				}
				if fieldSchema.GetDataType() == schemapb.DataType_Int8 {
					if defaultValue > math.MaxInt8 || defaultValue < math.MinInt8 {
						return merr.WrapErrParameterInvalidRange(math.MinInt8, math.MaxInt8, defaultValue, "default value out of range")
					}
				}
			case *schemapb.ValueField_LongData:
				if fieldSchema.GetDataType() != schemapb.DataType_Int64 {
					return merr.WrapErrParameterInvalid("DataType_Int64", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_FloatData:
				if fieldSchema.GetDataType() != schemapb.DataType_Float {
					return merr.WrapErrParameterInvalid("DataType_Float", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_DoubleData:
				if fieldSchema.GetDataType() != schemapb.DataType_Double {
					return merr.WrapErrParameterInvalid("DataType_Double", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_StringData:
				if fieldSchema.GetDataType() != schemapb.DataType_VarChar {
					return merr.WrapErrParameterInvalid("DataType_VarChar", "not match", "default value type mismatches field schema type")
				}
				maxLength, err := parameterutil.GetMaxLength(fieldSchema)
				if err != nil {
					return err
				}
				defaultValueLength := len(fieldSchema.GetDefaultValue().GetStringData())
				if int64(defaultValueLength) > maxLength {
					msg := fmt.Sprintf("the length (%d) of string exceeds max length (%d)", defaultValueLength, maxLength)
					return merr.WrapErrParameterInvalid("valid length string", "string length exceeds max length", msg)
				}
			default:
				panic("default value unsupport data type")
			}
		}
	}

	return nil
}

func hasSystemFields(schema *schemapb.CollectionSchema, systemFields []string) bool {
	for _, f := range schema.GetFields() {
		if funcutil.SliceContain(systemFields, f.GetName()) {
			return true
		}
	}
	return false
}

func validateFieldDataType(schema *schemapb.CollectionSchema) error {
	for _, field := range schema.GetFields() {
		if _, ok := schemapb.DataType_name[int32(field.GetDataType())]; !ok || field.GetDataType() == schemapb.DataType_None {
			return merr.WrapErrParameterInvalid("valid field", fmt.Sprintf("field data type: %s is not supported", field.GetDataType()))
		}
	}
	return nil
}

func (t *createCollectionTask) validateSchema(schema *schemapb.CollectionSchema) error {
	log.With(zap.String("CollectionName", t.Req.CollectionName))
	if t.Req.GetCollectionName() != schema.GetName() {
		log.Error("collection name not matches schema name", zap.String("SchemaName", schema.Name))
		msg := fmt.Sprintf("collection name = %s, schema.Name=%s", t.Req.GetCollectionName(), schema.Name)
		return merr.WrapErrParameterInvalid("collection name matches schema name", "don't match", msg)
	}

	if err := checkFieldSchema(schema); err != nil {
		return err
	}

	if hasSystemFields(schema, []string{RowIDFieldName, TimeStampFieldName, MetaFieldName}) {
		log.Error("schema contains system field",
			zap.String("RowIDFieldName", RowIDFieldName),
			zap.String("TimeStampFieldName", TimeStampFieldName),
			zap.String("MetaFieldName", MetaFieldName))
		msg := fmt.Sprintf("schema contains system field: %s, %s, %s", RowIDFieldName, TimeStampFieldName, MetaFieldName)
		return merr.WrapErrParameterInvalid("schema don't contains system field", "contains", msg)
	}
	return validateFieldDataType(schema)
}

func (t *createCollectionTask) assignFieldID(schema *schemapb.CollectionSchema) {
	for idx := range schema.GetFields() {
		schema.Fields[idx].FieldID = int64(idx + StartOfUserFieldID)
	}
}

func (t *createCollectionTask) appendDynamicField(schema *schemapb.CollectionSchema) {
	if schema.EnableDynamicField {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:        MetaFieldName,
			Description: "dynamic schema",
			DataType:    schemapb.DataType_JSON,
			IsDynamic:   true,
		})
		log.Info("append dynamic field", zap.String("collection", schema.Name))
	}
}

func (t *createCollectionTask) appendSysFields(schema *schemapb.CollectionSchema) {
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(RowIDField),
		Name:         RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(TimeStampField),
		Name:         TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	})
}

func (t *createCollectionTask) prepareSchema() error {
	var schema schemapb.CollectionSchema
	if err := proto.Unmarshal(t.Req.GetSchema(), &schema); err != nil {
		return err
	}
	if err := t.validateSchema(&schema); err != nil {
		return err
	}
	t.appendDynamicField(&schema)
	t.assignFieldID(&schema)
	t.appendSysFields(&schema)
	t.schema = &schema
	return nil
}

func (t *createCollectionTask) assignShardsNum() {
	if t.Req.GetShardsNum() <= 0 {
		t.Req.ShardsNum = common.DefaultShardsNum
	}
}

func (t *createCollectionTask) assignCollectionID() error {
	var err error
	t.collID, err = t.core.idAllocator.AllocOne()
	return err
}

func (t *createCollectionTask) assignPartitionIDs() error {
	t.partitionNames = make([]string, 0)
	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()

	_, err := typeutil.GetPartitionKeyFieldSchema(t.schema)
	if err == nil {
		partitionNums := t.Req.GetNumPartitions()
		// double check, default num of physical partitions should be greater than 0
		if partitionNums <= 0 {
			return errors.New("the specified partitions should be greater than 0 if partition key is used")
		}

		cfgMaxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt64()
		if partitionNums > cfgMaxPartitionNum {
			return fmt.Errorf("partition number (%d) exceeds max configuration (%d), collection: %s",
				partitionNums, cfgMaxPartitionNum, t.Req.CollectionName)
		}

		for i := int64(0); i < partitionNums; i++ {
			t.partitionNames = append(t.partitionNames, fmt.Sprintf("%s_%d", defaultPartitionName, i))
		}
	} else {
		// compatible with old versions <= 2.2.8
		t.partitionNames = append(t.partitionNames, defaultPartitionName)
	}

	t.partIDs = make([]UniqueID, len(t.partitionNames))
	start, end, err := t.core.idAllocator.Alloc(uint32(len(t.partitionNames)))
	if err != nil {
		return err
	}

	for i := start; i < end; i++ {
		t.partIDs[i-start] = i
	}
	log.Info("assign partitions when create collection",
		zap.String("collectionName", t.Req.GetCollectionName()),
		zap.Strings("partitionNames", t.partitionNames))

	return nil
}

func (t *createCollectionTask) assignChannels() error {
	vchanNames := make([]string, t.Req.GetShardsNum())
	// physical channel names
	chanNames := t.core.chanTimeTick.getDmlChannelNames(int(t.Req.GetShardsNum()))

	if int32(len(chanNames)) < t.Req.GetShardsNum() {
		return fmt.Errorf("no enough channels, want: %d, got: %d", t.Req.GetShardsNum(), len(chanNames))
	}

	shardNum := int(t.Req.GetShardsNum())
	for i := 0; i < shardNum; i++ {
		vchanNames[i] = funcutil.GetVirtualChannel(chanNames[i], t.collID, i)
	}
	t.channels = collectionChannels{
		virtualChannels:  vchanNames,
		physicalChannels: chanNames,
	}
	return nil
}

func (t *createCollectionTask) Prepare(ctx context.Context) error {
	db, err := t.core.meta.GetDatabaseByName(ctx, t.Req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	t.dbID = db.ID

	if err := t.validate(); err != nil {
		return err
	}

	if err := t.prepareSchema(); err != nil {
		return err
	}

	t.assignShardsNum()

	if err := t.assignCollectionID(); err != nil {
		return err
	}

	if err := t.assignPartitionIDs(); err != nil {
		return err
	}

	return t.assignChannels()
}

func (t *createCollectionTask) genCreateCollectionMsg(ctx context.Context, ts uint64) *ms.MsgPack {
	msgPack := ms.MsgPack{}
	msg := &ms.CreateCollectionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		CreateCollectionRequest: t.genCreateCollectionRequest(),
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	return &msgPack
}

func (t *createCollectionTask) genCreateCollectionRequest() *msgpb.CreateCollectionRequest {
	collectionID := t.collID
	partitionIDs := t.partIDs
	// error won't happen here.
	marshaledSchema, _ := proto.Marshal(t.schema)
	pChannels := t.channels.physicalChannels
	vChannels := t.channels.virtualChannels
	return &msgpb.CreateCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreateCollection),
			commonpbutil.WithTimeStamp(t.ts),
		),
		CollectionID:         collectionID,
		PartitionIDs:         partitionIDs,
		Schema:               marshaledSchema,
		VirtualChannelNames:  vChannels,
		PhysicalChannelNames: pChannels,
	}
}

func (t *createCollectionTask) addChannelsAndGetStartPositions(ctx context.Context, ts uint64) (map[string][]byte, error) {
	t.core.chanTimeTick.addDmlChannels(t.channels.physicalChannels...)
	if streamingutil.IsStreamingServiceEnabled() {
		return t.broadcastCreateCollectionMsgIntoStreamingService(ctx, ts)
	}
	msg := t.genCreateCollectionMsg(ctx, ts)
	return t.core.chanTimeTick.broadcastMarkDmlChannels(t.channels.physicalChannels, msg)
}

func (t *createCollectionTask) broadcastCreateCollectionMsgIntoStreamingService(ctx context.Context, ts uint64) (map[string][]byte, error) {
	req := t.genCreateCollectionRequest()
	// dispatch the createCollectionMsg into all vchannel.
	msgs := make([]message.MutableMessage, 0, len(req.VirtualChannelNames))
	for _, vchannel := range req.VirtualChannelNames {
		msg, err := message.NewCreateCollectionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.CreateCollectionMessageHeader{
				CollectionId: req.CollectionID,
				PartitionIds: req.GetPartitionIDs(),
			}).
			WithBody(req).
			BuildMutable()
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	// send the createCollectionMsg into streaming service.
	// ts is used as initial checkpoint at datacoord,
	// it must be set as barrier time tick.
	// The timetick of create message in wal must be greater than ts, to avoid data read loss at read side.
	resps := streaming.WAL().AppendMessagesWithOption(ctx, streaming.AppendOption{
		BarrierTimeTick: ts,
	}, msgs...)
	if err := resps.UnwrapFirstError(); err != nil {
		return nil, err
	}
	// make the old message stream serialized id.
	startPositions := make(map[string][]byte)
	for idx, resp := range resps.Responses {
		// The key is pchannel here
		startPositions[req.PhysicalChannelNames[idx]] = adaptor.MustGetMQWrapperIDFromMessage(resp.AppendResult.MessageID).Serialize()
	}
	return startPositions, nil
}

func (t *createCollectionTask) getCreateTs() (uint64, error) {
	replicateInfo := t.Req.GetBase().GetReplicateInfo()
	if !replicateInfo.GetIsReplicate() {
		return t.GetTs(), nil
	}
	if replicateInfo.GetMsgTimestamp() == 0 {
		log.Warn("the cdc timestamp is not set in the request for the backup instance")
		return 0, merr.WrapErrParameterInvalidMsg("the cdc timestamp is not set in the request for the backup instance")
	}
	return replicateInfo.GetMsgTimestamp(), nil
}

func (t *createCollectionTask) Execute(ctx context.Context) error {
	collID := t.collID
	partIDs := t.partIDs
	ts, err := t.getCreateTs()
	if err != nil {
		return err
	}

	vchanNames := t.channels.virtualChannels
	chanNames := t.channels.physicalChannels

	startPositions, err := t.addChannelsAndGetStartPositions(ctx, ts)
	if err != nil {
		// ugly here, since we must get start positions first.
		t.core.chanTimeTick.removeDmlChannels(t.channels.physicalChannels...)
		return err
	}

	partitions := make([]*model.Partition, len(partIDs))
	for i, partID := range partIDs {
		partitions[i] = &model.Partition{
			PartitionID:               partID,
			PartitionName:             t.partitionNames[i],
			PartitionCreatedTimestamp: ts,
			CollectionID:              collID,
			State:                     pb.PartitionState_PartitionCreated,
		}
	}

	collInfo := model.Collection{
		CollectionID:         collID,
		DBID:                 t.dbID,
		Name:                 t.schema.Name,
		Description:          t.schema.Description,
		AutoID:               t.schema.AutoID,
		Fields:               model.UnmarshalFieldModels(t.schema.Fields),
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
		ShardsNum:            t.Req.ShardsNum,
		ConsistencyLevel:     t.Req.ConsistencyLevel,
		StartPositions:       toKeyDataPairs(startPositions),
		CreateTime:           ts,
		State:                pb.CollectionState_CollectionCreating,
		Partitions:           partitions,
		Properties:           t.Req.Properties,
		EnableDynamicField:   t.schema.EnableDynamicField,
	}

	// We cannot check the idempotency inside meta table when adding collection, since we'll execute duplicate steps
	// if add collection successfully due to idempotency check. Some steps may be risky to be duplicate executed if they
	// are not promised idempotent.
	clone := collInfo.Clone()
	// need double check in meta table if we can't promise the sequence execution.
	existedCollInfo, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err == nil {
		equal := existedCollInfo.Equal(*clone)
		if !equal {
			return fmt.Errorf("create duplicate collection with different parameters, collection: %s", t.Req.GetCollectionName())
		}
		// make creating collection idempotent.
		log.Warn("add duplicate collection", zap.String("collection", t.Req.GetCollectionName()), zap.Uint64("ts", ts))
		return nil
	}

	undoTask := newBaseUndoTask(t.core.stepExecutor)
	undoTask.AddStep(&expireCacheStep{
		baseStep:        baseStep{core: t.core},
		dbName:          t.Req.GetDbName(),
		collectionNames: []string{t.Req.GetCollectionName()},
		collectionID:    InvalidCollectionID,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_DropCollection)},
	}, &nullStep{})
	undoTask.AddStep(&nullStep{}, &removeDmlChannelsStep{
		baseStep:  baseStep{core: t.core},
		pChannels: chanNames,
	}) // remove dml channels if any error occurs.
	undoTask.AddStep(&addCollectionMetaStep{
		baseStep: baseStep{core: t.core},
		coll:     &collInfo,
	}, &deleteCollectionMetaStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collID,
		// When we undo createCollectionTask, this ts may be less than the ts when unwatch channels.
		ts: ts,
	})
	// serve for this case: watching channels succeed in datacoord but failed due to network failure.
	undoTask.AddStep(&nullStep{}, &unwatchChannelsStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collID,
		channels:     t.channels,
		isSkip:       !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	undoTask.AddStep(&watchChannelsStep{
		baseStep: baseStep{core: t.core},
		info: &watchInfo{
			ts:             ts,
			collectionID:   collID,
			vChannels:      t.channels.virtualChannels,
			startPositions: toKeyDataPairs(startPositions),
			schema: &schemapb.CollectionSchema{
				Name:        collInfo.Name,
				Description: collInfo.Description,
				AutoID:      collInfo.AutoID,
				Fields:      model.MarshalFieldModels(collInfo.Fields),
			},
		},
	}, &nullStep{})
	undoTask.AddStep(&changeCollectionStateStep{
		baseStep:     baseStep{core: t.core},
		collectionID: collID,
		state:        pb.CollectionState_CollectionCreated,
		ts:           ts,
	}, &nullStep{}) // We'll remove the whole collection anyway.

	return undoTask.Execute(ctx)
}
