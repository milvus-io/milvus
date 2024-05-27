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

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	ms "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type copyCollectionTask struct {
	baseTask
	Req            *rootcoordpb.TruncateCollectionRequest
	collectionName string
	collectionID   UniqueID
	collInfo       *model.Collection
	partitions     []*model.Partition
	partitionIDs   []UniqueID
	channels       collectionChannels
}

func (t *copyCollectionTask) validate() error {
	// todo
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropCollection); err != nil {
		return err
	}
	return nil
}

func (t *copyCollectionTask) assignCollectionID() error {
	var err error
	t.collectionID, err = t.core.idAllocator.AllocOne()
	return err
}

func (t *copyCollectionTask) assignPartitionIDs() error {
	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()

	partitionNums := len(t.collInfo.Partitions)
	start, end, err := t.core.idAllocator.Alloc(uint32(partitionNums))
	if err != nil {
		return err
	}

	t.partitions = make([]*model.Partition, partitionNums)
	t.partitionIDs = make([]UniqueID, partitionNums)
	for i := start; i < end; i++ {
		// todo
		t.partitions[i-start] = t.collInfo.Partitions[i-start]
		t.partitionIDs[i-start] = i
		t.partitions[i-start].PartitionID = i
		t.partitions[i-start].CollectionID = t.collectionID
		t.partitions[i-start].PartitionName = fmt.Sprintf("%s_%d", defaultPartitionName, i-start)
	}
	log.Info("assign partitions when create collection",
		zap.String("collectionName", t.Req.GetCollectionName()))

	return nil
}

func (t *copyCollectionTask) assignChannels() error {
	vchanNames := make([]string, t.collInfo.ShardsNum)
	// physical channel names
	chanNames := t.core.chanTimeTick.getDmlChannelNames(int(t.collInfo.ShardsNum))

	if int32(len(chanNames)) < t.collInfo.ShardsNum {
		return fmt.Errorf("no enough channels, want: %d, got: %d", t.collInfo.ShardsNum, len(chanNames))
	}

	for i := int32(0); i < t.collInfo.ShardsNum; i++ {
		vchanNames[i] = fmt.Sprintf("%s_%dv%d", chanNames[i], t.collectionID, i)
	}
	t.channels = collectionChannels{
		virtualChannels:  vchanNames,
		physicalChannels: chanNames,
	}
	return nil
}

func (t *copyCollectionTask) Prepare(ctx context.Context) error {
	err := t.validate()
	if err != nil {
		return err
	}

	t.collInfo, err = t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	t.collectionName = util.GenerateTempCollectionName(t.Req.GetCollectionName())

	if err := t.assignCollectionID(); err != nil {
		return err
	}

	if err := t.assignPartitionIDs(); err != nil {
		return err
	}

	return t.assignChannels()
}

func (t *copyCollectionTask) genCreateCollectionMsg(ctx context.Context, ts uint64) *ms.MsgPack {
	// error won't happen here.
	schemaFeilds := make([]*schemapb.FieldSchema, 0)
	for _, field := range t.collInfo.Fields {
		schemaFeilds = append(schemaFeilds, &schemapb.FieldSchema{
			FieldID:         field.FieldID,
			Name:            field.Name,
			IsPrimaryKey:    field.IsPrimaryKey,
			Description:     field.Description,
			DataType:        field.DataType,
			TypeParams:      field.TypeParams,
			IndexParams:     field.IndexParams,
			AutoID:          field.AutoID,
			State:           field.State,
			ElementType:     field.ElementType,
			DefaultValue:    field.DefaultValue,
			IsDynamic:       field.IsDynamic,
			IsPartitionKey:  field.IsPartitionKey,
			IsClusteringKey: field.IsClusteringKey,
		})
	}
	marshaledSchema, _ := proto.Marshal(&schemapb.CollectionSchema{
		Name:               t.collectionName,
		Description:        t.collInfo.Description,
		AutoID:             t.collInfo.AutoID,
		Fields:             schemaFeilds,
		EnableDynamicField: t.collInfo.EnableDynamicField,
		Properties:         t.collInfo.Properties,
	})
	pChannels := t.channels.physicalChannels
	vChannels := t.channels.virtualChannels

	msgPack := ms.MsgPack{}
	msg := &ms.CreateCollectionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		CreateCollectionRequest: msgpb.CreateCollectionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_CreateCollection),
				commonpbutil.WithTimeStamp(ts),
			),
			CollectionID:         t.collectionID,
			PartitionIDs:         t.partitionIDs,
			Schema:               marshaledSchema,
			VirtualChannelNames:  vChannels,
			PhysicalChannelNames: pChannels,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	return &msgPack
}

func (t *copyCollectionTask) addChannelsAndGetStartPositions(ctx context.Context, ts uint64) (map[string][]byte, error) {
	t.core.chanTimeTick.addDmlChannels(t.channels.physicalChannels...)
	msg := t.genCreateCollectionMsg(ctx, ts)
	return t.core.chanTimeTick.broadcastMarkDmlChannels(t.channels.physicalChannels, msg)
}

func (t *copyCollectionTask) Execute(ctx context.Context) error {
	_, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.collectionName, typeutil.MaxTimestamp)
	if err == nil {
		return fmt.Errorf("create duplicate temp collection")
	}

	startPositions, err := t.addChannelsAndGetStartPositions(ctx, t.collInfo.CreateTime)
	if err != nil {
		// ugly here, since we must get start positions first.
		t.core.chanTimeTick.removeDmlChannels(t.channels.physicalChannels...)
		return err
	}
	undoTask := newBaseUndoTask(t.core.stepExecutor)
	undoTask.AddStep(&expireCacheStep{
		baseStep:        baseStep{core: t.core},
		dbName:          t.Req.GetDbName(),
		collectionNames: []string{t.collectionName},
		collectionID:    InvalidCollectionID,
		ts:              t.collInfo.CreateTime,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_DropCollection)},
	}, &nullStep{})
	undoTask.AddStep(&nullStep{}, &removeDmlChannelsStep{
		baseStep:  baseStep{core: t.core},
		pChannels: t.channels.physicalChannels,
	}) // remove dml channels if any error occurs.
	undoTask.AddStep(&addCollectionMetaStep{
		baseStep: baseStep{core: t.core},
		coll: &model.Collection{
			CollectionID:         t.collectionID,
			DBID:                 t.collInfo.DBID,
			Name:                 t.collectionName,
			Description:          t.collInfo.Description,
			AutoID:               t.collInfo.AutoID,
			Fields:               t.collInfo.Fields,
			VirtualChannelNames:  t.channels.virtualChannels,
			PhysicalChannelNames: t.channels.physicalChannels,
			ShardsNum:            t.collInfo.ShardsNum,
			ConsistencyLevel:     t.collInfo.ConsistencyLevel,
			StartPositions:       toKeyDataPairs(startPositions),
			CreateTime:           t.collInfo.CreateTime,
			State:                pb.CollectionState_CollectionCreating,
			Partitions:           t.partitions,
			Properties:           t.collInfo.Properties,
			EnableDynamicField:   t.collInfo.EnableDynamicField,
		},
	}, &deleteCollectionMetaStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collectionID,
		// When we undo copyCollectionTask, this ts may be less than the ts when unwatch channels.
		ts: t.collInfo.CreateTime,
	})
	// serve for this case: watching channels succeed in datacoord but failed due to network failure.
	undoTask.AddStep(&nullStep{}, &unwatchChannelsStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collectionID,
		channels:     t.channels,
		isSkip:       !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	undoTask.AddStep(&watchChannelsStep{
		baseStep: baseStep{core: t.core},
		info: &watchInfo{
			ts:             t.collInfo.CreateTime,
			collectionID:   t.collectionID,
			vChannels:      t.channels.virtualChannels,
			startPositions: toKeyDataPairs(startPositions),
			schema: &schemapb.CollectionSchema{
				Name:        t.collectionName,
				Description: t.collInfo.Description,
				AutoID:      t.collInfo.AutoID,
				Fields:      model.MarshalFieldModels(t.collInfo.Fields),
			},
		},
	}, &nullStep{})
	undoTask.AddStep(&changeCollectionStateStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collectionID,
		state:        pb.CollectionState_CollectionDropping,
		ts:           t.collInfo.CreateTime,
	}, &nullStep{}) // We'll remove the whole collection anyway.

	return undoTask.Execute(ctx)
}
