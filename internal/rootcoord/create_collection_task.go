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

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	ms "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type collectionChannels struct {
	virtualChannels  []string
	physicalChannels []string
}

type createCollectionTask struct {
	baseTask
	Req      *milvuspb.CreateCollectionRequest
	schema   *schemapb.CollectionSchema
	collID   UniqueID
	partID   UniqueID
	channels collectionChannels
}

func (t *createCollectionTask) validate() error {
	if t.Req == nil {
		return errors.New("empty requests")
	}

	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreateCollection); err != nil {
		return err
	}

	shardsNum := t.Req.GetShardsNum()

	cfgMaxShardNum := Params.RootCoordCfg.DmlChannelNum.GetAsInt32()
	if shardsNum > cfgMaxShardNum {
		return fmt.Errorf("shard num (%d) exceeds max configuration (%d)", shardsNum, cfgMaxShardNum)
	}

	cfgShardLimit := Params.ProxyCfg.MaxShardNum.GetAsInt32()
	if shardsNum > cfgShardLimit {
		return fmt.Errorf("shard num (%d) exceeds system limit (%d)", shardsNum, cfgShardLimit)
	}

	return nil
}

func defaultValueTypeMatch(schema *schemapb.CollectionSchema) error {
	for _, fieldSchema := range schema.Fields {
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

func (t *createCollectionTask) validateSchema(schema *schemapb.CollectionSchema) error {
	log.With(zap.String("CollectionName", t.Req.CollectionName))
	if t.Req.GetCollectionName() != schema.GetName() {
		log.Error("collection name not matches schema name", zap.String("SchemaName", schema.Name))
		msg := fmt.Sprintf("collection name = %s, schema.Name=%s", t.Req.GetCollectionName(), schema.Name)
		return merr.WrapErrParameterInvalid("collection name matches schema name", "don't match", msg)
	}

	err := defaultValueTypeMatch(schema)
	if err != nil {
		log.Error("default value type mismatch field schema type")
		return err
	}

	if hasSystemFields(schema, []string{RowIDFieldName, TimeStampFieldName}) {
		log.Error("schema contains system field", zap.String("RowIDFieldName", RowIDFieldName), zap.String("TimeStampFieldName", TimeStampFieldName))
		msg := fmt.Sprintf("schema contains system field: %s, %s", RowIDFieldName, TimeStampFieldName)
		return merr.WrapErrParameterInvalid("schema don't contains system field", "contains", msg)
	}
	return nil
}

func (t *createCollectionTask) assignFieldID(schema *schemapb.CollectionSchema) {
	for idx := range schema.GetFields() {
		schema.Fields[idx].FieldID = int64(idx + StartOfUserFieldID)
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

func (t *createCollectionTask) assignPartitionID() error {
	var err error
	t.partID, err = t.core.idAllocator.AllocOne()
	return err
}

func (t *createCollectionTask) assignChannels() error {
	vchanNames := make([]string, t.Req.GetShardsNum())
	//physical channel names
	chanNames := t.core.chanTimeTick.getDmlChannelNames(int(t.Req.GetShardsNum()))

	if int32(len(chanNames)) < t.Req.GetShardsNum() {
		return fmt.Errorf("no enough channels, want: %d, got: %d", t.Req.GetShardsNum(), len(chanNames))
	}

	for i := int32(0); i < t.Req.GetShardsNum(); i++ {
		vchanNames[i] = fmt.Sprintf("%s_%dv%d", chanNames[i], t.collID, i)
	}
	t.channels = collectionChannels{
		virtualChannels:  vchanNames,
		physicalChannels: chanNames,
	}
	return nil
}

func (t *createCollectionTask) Prepare(ctx context.Context) error {
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

	if err := t.assignPartitionID(); err != nil {
		return err
	}

	return t.assignChannels()
}

func (t *createCollectionTask) genCreateCollectionMsg(ctx context.Context) *ms.MsgPack {
	ts := t.GetTs()
	collectionID := t.collID
	partitionID := t.partID
	// error won't happen here.
	marshaledSchema, _ := proto.Marshal(t.schema)
	pChannels := t.channels.physicalChannels
	vChannels := t.channels.virtualChannels

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	msg := &ms.CreateCollectionMsg{
		BaseMsg: baseMsg,
		CreateCollectionRequest: msgpb.CreateCollectionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_CreateCollection),
				commonpbutil.WithTimeStamp(ts),
			),
			CollectionID:         collectionID,
			PartitionID:          partitionID,
			Schema:               marshaledSchema,
			VirtualChannelNames:  vChannels,
			PhysicalChannelNames: pChannels,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	return &msgPack
}

func (t *createCollectionTask) addChannelsAndGetStartPositions(ctx context.Context) (map[string][]byte, error) {
	t.core.chanTimeTick.addDmlChannels(t.channels.physicalChannels...)
	msg := t.genCreateCollectionMsg(ctx)
	return t.core.chanTimeTick.broadcastMarkDmlChannels(t.channels.physicalChannels, msg)
}

func (t *createCollectionTask) Execute(ctx context.Context) error {
	collID := t.collID
	partID := t.partID
	ts := t.GetTs()

	vchanNames := t.channels.virtualChannels
	chanNames := t.channels.physicalChannels

	startPositions, err := t.addChannelsAndGetStartPositions(ctx)
	if err != nil {
		// ugly here, since we must get start positions first.
		t.core.chanTimeTick.removeDmlChannels(t.channels.physicalChannels...)
		return err
	}

	collInfo := model.Collection{
		CollectionID:         collID,
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
		Partitions: []*model.Partition{
			{
				PartitionID:               partID,
				PartitionName:             Params.CommonCfg.DefaultPartitionName.GetValue(),
				PartitionCreatedTimestamp: ts,
				CollectionID:              collID,
				State:                     pb.PartitionState_PartitionCreated,
			},
		},
		Properties: t.Req.Properties,
	}

	// We cannot check the idempotency inside meta table when adding collection, since we'll execute duplicate steps
	// if add collection successfully due to idempotency check. Some steps may be risky to be duplicate executed if they
	// are not promised idempotent.
	clone := collInfo.Clone()
	clone.Partitions = []*model.Partition{{PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue()}}
	// need double check in meta table if we can't promise the sequence execution.
	existedCollInfo, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err == nil {
		equal := existedCollInfo.Equal(*clone)
		if !equal {
			return fmt.Errorf("create duplicate collection with different parameters, collection: %s", t.Req.GetCollectionName())
		}
		// make creating collection idempotent.
		log.Warn("add duplicate collection", zap.String("collection", t.Req.GetCollectionName()), zap.Uint64("ts", t.GetTs()))
		return nil
	}

	// check collection number quota for the entire the instance
	existedCollInfos, err := t.core.meta.ListCollections(ctx, typeutil.MaxTimestamp)
	if err != nil {
		log.Warn("fail to list collections for checking the collection count", zap.Error(err))
		return fmt.Errorf("fail to list collections for checking the collection count")
	}
	maxCollectionNum := Params.QuotaConfig.MaxCollectionNum.GetAsInt()
	if len(existedCollInfos) >= maxCollectionNum {
		log.Warn("unable to create collection because the number of collection has reached the limit", zap.Int("max_collection_num", maxCollectionNum))
		return merr.WrapErrCollectionResourceLimitExceeded(fmt.Sprintf("Failed to create collection, limit={%d}", maxCollectionNum))
	}
	// check collection number quota for DB
	existedColsInDB := lo.Filter(existedCollInfos, func(collection *model.Collection, _ int) bool {
		return t.Req.GetDbName() != "" && collection.DBName == t.Req.GetDbName()
	})
	maxColNumPerDB := Params.QuotaConfig.MaxCollectionNumPerDB.GetAsInt()
	if len(existedColsInDB) >= maxColNumPerDB {
		log.Warn("unable to create collection because the number of collection has reached the limit in DB", zap.Int("maxCollectionNumPerDB", maxColNumPerDB))
		return merr.WrapErrCollectionResourceLimitExceeded(fmt.Sprintf("Failed to create collection, maxCollectionNumPerDB={%d}", maxColNumPerDB))
	}

	undoTask := newBaseUndoTask(t.core.stepExecutor)
	undoTask.AddStep(&expireCacheStep{
		baseStep:        baseStep{core: t.core},
		collectionNames: []string{t.Req.GetCollectionName()},
		collectionID:    InvalidCollectionID,
		ts:              ts,
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
