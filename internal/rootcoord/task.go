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
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type reqTask interface {
	Ctx() context.Context
	Type() commonpb.MsgType
	Execute(ctx context.Context) error
	Core() *Core
}

type baseReqTask struct {
	ctx  context.Context
	core *Core
}

func (b *baseReqTask) Core() *Core {
	return b.core
}

func (b *baseReqTask) Ctx() context.Context {
	return b.ctx
}

func executeTask(t reqTask) error {
	errChan := make(chan error)

	go func() {
		err := t.Execute(t.Ctx())
		errChan <- err
	}()
	select {
	case <-t.Core().ctx.Done():
		return fmt.Errorf("context canceled")
	case <-t.Ctx().Done():
		return fmt.Errorf("context canceled")
	case err := <-errChan:
		if t.Core().ctx.Err() != nil || t.Ctx().Err() != nil {
			return fmt.Errorf("context canceled")
		}
		return err
	}
}

// CreateCollectionReqTask create collection request task
type CreateCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.CreateCollectionRequest
}

// Type return msg type
func (t *CreateCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *CreateCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_CreateCollection {
		return fmt.Errorf("create collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(t.Req.Schema, &schema)
	if err != nil {
		return fmt.Errorf("unmarshal schema error= %w", err)
	}

	if t.Req.CollectionName != schema.Name {
		return fmt.Errorf("collection name = %s, schema.Name=%s", t.Req.CollectionName, schema.Name)
	}
	if t.Req.ShardsNum <= 0 {
		t.Req.ShardsNum = common.DefaultShardsNum
	}
	log.Debug("CreateCollectionReqTask Execute", zap.Any("CollectionName", t.Req.CollectionName),
		zap.Int32("ShardsNum", t.Req.ShardsNum),
		zap.String("ConsistencyLevel", t.Req.ConsistencyLevel.String()))

	for idx, field := range schema.Fields {
		field.FieldID = int64(idx + StartOfUserFieldID)
	}
	rowIDField := &schemapb.FieldSchema{
		FieldID:      int64(RowIDField),
		Name:         RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	}
	timeStampField := &schemapb.FieldSchema{
		FieldID:      int64(TimeStampField),
		Name:         TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	}
	schema.Fields = append(schema.Fields, rowIDField, timeStampField)

	collID, _, err := t.core.IDAllocator(1)
	if err != nil {
		return fmt.Errorf("alloc collection id error = %w", err)
	}
	partID, _, err := t.core.IDAllocator(1)
	if err != nil {
		return fmt.Errorf("alloc partition id error = %w", err)
	}

	log.Debug("collection name -> id",
		zap.String("collection name", t.Req.CollectionName),
		zap.Int64("collection_id", collID),
		zap.Int64("default partition id", partID))

	vchanNames := make([]string, t.Req.ShardsNum)
	chanNames := make([]string, t.Req.ShardsNum)
	deltaChanNames := make([]string, t.Req.ShardsNum)
	for i := int32(0); i < t.Req.ShardsNum; i++ {
		vchanNames[i] = fmt.Sprintf("%s_%dv%d", t.core.chanTimeTick.getDmlChannelName(), collID, i)
		chanNames[i] = ToPhysicalChannel(vchanNames[i])

		deltaChanNames[i] = t.core.chanTimeTick.getDeltaChannelName()
		deltaChanName, err1 := ConvertChannelName(chanNames[i], Params.RootCoordCfg.DmlChannelName, Params.RootCoordCfg.DeltaChannelName)
		if err1 != nil || deltaChanName != deltaChanNames[i] {
			return fmt.Errorf("dmlChanName %s and deltaChanName %s mis-match", chanNames[i], deltaChanNames[i])
		}
	}

	collInfo := etcdpb.CollectionInfo{
		ID:                         collID,
		Schema:                     &schema,
		PartitionIDs:               []typeutil.UniqueID{partID},
		PartitionNames:             []string{Params.CommonCfg.DefaultPartitionName},
		FieldIndexes:               make([]*etcdpb.FieldIndexInfo, 0, 16),
		VirtualChannelNames:        vchanNames,
		PhysicalChannelNames:       chanNames,
		ShardsNum:                  t.Req.ShardsNum,
		PartitionCreatedTimestamps: []uint64{0},
		ConsistencyLevel:           t.Req.ConsistencyLevel,
	}

	idxInfo := make([]*etcdpb.IndexInfo, 0, 16)

	// schema is modified (add RowIDField and TimestampField),
	// so need Marshal again
	schemaBytes, err := proto.Marshal(&schema)
	if err != nil {
		return fmt.Errorf("marshal schema error = %w", err)
	}

	ddCollReq := internalpb.CreateCollectionRequest{
		Base:                 t.Req.Base,
		DbName:               t.Req.DbName,
		CollectionName:       t.Req.CollectionName,
		PartitionName:        Params.CommonCfg.DefaultPartitionName,
		DbID:                 0, //TODO,not used
		CollectionID:         collID,
		PartitionID:          partID,
		Schema:               schemaBytes,
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
	}

	reason := fmt.Sprintf("create collection %d", collID)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("tso alloc fail, error = %w", err)
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddCollReq.Base.Timestamp = ts
	ddOpStr, err := EncodeDdOperation(&ddCollReq, CreateCollectionDDType)
	if err != nil {
		return fmt.Errorf("encodeDdOperation fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	createCollectionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.addDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.removeDdlTimeTick(ts, reason)

		// add dml channel before send dd msg
		t.core.chanTimeTick.addDmlChannels(chanNames...)

		// also add delta channels
		t.core.chanTimeTick.addDeltaChannels(deltaChanNames...)

		ids, err := t.core.SendDdCreateCollectionReq(ctx, &ddCollReq, chanNames)
		if err != nil {
			return fmt.Errorf("send dd create collection req failed, error = %w", err)
		}
		for _, pchan := range collInfo.PhysicalChannelNames {
			collInfo.StartPositions = append(collInfo.StartPositions, &commonpb.KeyDataPair{
				Key:  pchan,
				Data: ids[pchan],
			})
		}

		// update meta table after send dd operation
		if err = t.core.MetaTable.AddCollection(&collInfo, ts, idxInfo, ddOpStr); err != nil {
			t.core.chanTimeTick.removeDmlChannels(chanNames...)
			t.core.chanTimeTick.removeDeltaChannels(deltaChanNames...)
			// it's ok just to leave create collection message sent, datanode and querynode does't process CreateCollection logic
			return fmt.Errorf("meta table add collection failed,error = %w", err)
		}

		// use addDdlTimeTick and removeDdlTimeTick to mark DDL operation in process
		t.core.chanTimeTick.removeDdlTimeTick(ts, reason)
		errTimeTick := t.core.SendTimeTick(ts, reason)
		if errTimeTick != nil {
			log.Warn("Failed to send timetick", zap.Error(errTimeTick))
		}
		return nil
	}

	if err = createCollectionFn(); err != nil {
		return err
	}

	if err = t.core.CallWatchChannels(ctx, collID, vchanNames); err != nil {
		return err
	}

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

// DropCollectionReqTask drop collection request task
type DropCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DropCollectionRequest
}

// Type return msg type
func (t *DropCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DropCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropCollection {
		return fmt.Errorf("drop collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	if t.core.MetaTable.IsAlias(t.Req.CollectionName) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}

	collMeta, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	if err != nil {
		return err
	}

	ddReq := internalpb.DropCollectionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		DbID:           0, //not used
		CollectionID:   collMeta.ID,
	}

	reason := fmt.Sprintf("drop collection %d", collMeta.ID)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddReq.Base.Timestamp = ts
	ddOpStr, err := EncodeDdOperation(&ddReq, DropCollectionDDType)
	if err != nil {
		return fmt.Errorf("encodeDdOperation fail, error = %w", err)
	}

	// drop all indices
	if err = t.core.RemoveIndex(ctx, t.Req.CollectionName, ""); err != nil {
		return err
	}

	// get all aliases before meta table updated
	aliases := t.core.MetaTable.ListAliases(collMeta.ID)

	// use lambda function here to guarantee all resources to be released
	dropCollectionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.addDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.removeDdlTimeTick(ts, reason)

		if err = t.core.SendDdDropCollectionReq(ctx, &ddReq, collMeta.PhysicalChannelNames); err != nil {
			return err
		}

		// update meta table after send dd operation
		if err = t.core.MetaTable.DeleteCollection(collMeta.ID, ts, ddOpStr); err != nil {
			return err
		}

		// use addDdlTimeTick and removeDdlTimeTick to mark DDL operation in process
		t.core.chanTimeTick.removeDdlTimeTick(ts, reason)
		errTimeTick := t.core.SendTimeTick(ts, reason)
		if errTimeTick != nil {
			log.Warn("Failed to send timetick", zap.Error(errTimeTick))
		}
		// send tt into deleted channels to tell data_node to clear flowgragh
		t.core.chanTimeTick.sendTimeTickToChannel(collMeta.PhysicalChannelNames, ts)

		// remove dml channel after send dd msg
		t.core.chanTimeTick.removeDmlChannels(collMeta.PhysicalChannelNames...)

		// remove delta channels
		deltaChanNames := make([]string, len(collMeta.PhysicalChannelNames))
		for i, chanName := range collMeta.PhysicalChannelNames {
			if deltaChanNames[i], err = ConvertChannelName(chanName, Params.RootCoordCfg.DmlChannelName, Params.RootCoordCfg.DeltaChannelName); err != nil {
				return err
			}
		}
		t.core.chanTimeTick.removeDeltaChannels(deltaChanNames...)
		return nil
	}

	if err = dropCollectionFn(); err != nil {
		return err
	}

	//notify query service to release collection
	if err = t.core.CallReleaseCollectionService(t.core.ctx, ts, 0, collMeta.ID); err != nil {
		log.Error("Failed to CallReleaseCollectionService", zap.Error(err))
		return err
	}

	t.core.ExpireMetaCache(ctx, []string{t.Req.CollectionName}, ts)
	t.core.ExpireMetaCache(ctx, aliases, ts)

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

// HasCollectionReqTask has collection request task
type HasCollectionReqTask struct {
	baseReqTask
	Req           *milvuspb.HasCollectionRequest
	HasCollection bool
}

// Type return msg type
func (t *HasCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *HasCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_HasCollection {
		return fmt.Errorf("has collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	_, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, t.Req.TimeStamp)
	if err == nil {
		t.HasCollection = true
	} else {
		t.HasCollection = false
	}
	return nil
}

// DescribeCollectionReqTask describe collection request task
type DescribeCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeCollectionRequest
	Rsp *milvuspb.DescribeCollectionResponse
}

// Type return msg type
func (t *DescribeCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DescribeCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DescribeCollection {
		return fmt.Errorf("describe collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	var collInfo *etcdpb.CollectionInfo
	var err error

	if t.Req.CollectionName != "" {
		collInfo, err = t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, t.Req.TimeStamp)
		if err != nil {
			return err
		}
	} else {
		collInfo, err = t.core.MetaTable.GetCollectionByID(t.Req.CollectionID, t.Req.TimeStamp)
		if err != nil {
			return err
		}
	}

	t.Rsp.Schema = proto.Clone(collInfo.Schema).(*schemapb.CollectionSchema)
	t.Rsp.CollectionID = collInfo.ID
	t.Rsp.VirtualChannelNames = collInfo.VirtualChannelNames
	t.Rsp.PhysicalChannelNames = collInfo.PhysicalChannelNames
	if collInfo.ShardsNum == 0 {
		collInfo.ShardsNum = int32(len(collInfo.VirtualChannelNames))
	}
	t.Rsp.ShardsNum = collInfo.ShardsNum
	t.Rsp.ConsistencyLevel = collInfo.ConsistencyLevel

	t.Rsp.CreatedTimestamp = collInfo.CreateTime
	createdPhysicalTime, _ := tsoutil.ParseHybridTs(collInfo.CreateTime)
	t.Rsp.CreatedUtcTimestamp = uint64(createdPhysicalTime)
	t.Rsp.Aliases = t.core.MetaTable.ListAliases(collInfo.ID)
	t.Rsp.StartPositions = collInfo.GetStartPositions()
	return nil
}

// ShowCollectionReqTask show collection request task
type ShowCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowCollectionsRequest
	Rsp *milvuspb.ShowCollectionsResponse
}

// Type return msg type
func (t *ShowCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *ShowCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_ShowCollections {
		return fmt.Errorf("show collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	coll, err := t.core.MetaTable.ListCollections(t.Req.TimeStamp)
	if err != nil {
		return err
	}
	for name, meta := range coll {
		t.Rsp.CollectionNames = append(t.Rsp.CollectionNames, name)
		t.Rsp.CollectionIds = append(t.Rsp.CollectionIds, meta.ID)
		t.Rsp.CreatedTimestamps = append(t.Rsp.CreatedTimestamps, meta.CreateTime)
		physical, _ := tsoutil.ParseHybridTs(meta.CreateTime)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}
	return nil
}

// CreatePartitionReqTask create partition request task
type CreatePartitionReqTask struct {
	baseReqTask
	Req *milvuspb.CreatePartitionRequest
}

// Type return msg type
func (t *CreatePartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *CreatePartitionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_CreatePartition {
		return fmt.Errorf("create partition, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	collMeta, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	if err != nil {
		return err
	}
	partID, _, err := t.core.IDAllocator(1)
	if err != nil {
		return err
	}

	ddReq := internalpb.CreatePartitionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		PartitionName:  t.Req.PartitionName,
		DbID:           0, // todo, not used
		CollectionID:   collMeta.ID,
		PartitionID:    partID,
	}

	reason := fmt.Sprintf("create partition %s", t.Req.PartitionName)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddReq.Base.Timestamp = ts
	ddOpStr, err := EncodeDdOperation(&ddReq, CreatePartitionDDType)
	if err != nil {
		return fmt.Errorf("encodeDdOperation fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	createPartitionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.addDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.removeDdlTimeTick(ts, reason)

		if err = t.core.SendDdCreatePartitionReq(ctx, &ddReq, collMeta.PhysicalChannelNames); err != nil {
			return err
		}

		// update meta table after send dd operation
		if err = t.core.MetaTable.AddPartition(collMeta.ID, t.Req.PartitionName, partID, ts, ddOpStr); err != nil {
			return err
		}

		// use addDdlTimeTick and removeDdlTimeTick to mark DDL operation in process
		t.core.chanTimeTick.removeDdlTimeTick(ts, reason)
		errTimeTick := t.core.SendTimeTick(ts, reason)
		if errTimeTick != nil {
			log.Warn("Failed to send timetick", zap.Error(errTimeTick))
		}
		return nil
	}

	if err = createPartitionFn(); err != nil {
		return err
	}

	t.core.ExpireMetaCache(ctx, []string{t.Req.CollectionName}, ts)

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

// DropPartitionReqTask drop partition request task
type DropPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.DropPartitionRequest
}

// Type return msg type
func (t *DropPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DropPartitionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropPartition {
		return fmt.Errorf("drop partition, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	collInfo, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	if err != nil {
		return err
	}
	partID, err := t.core.MetaTable.GetPartitionByName(collInfo.ID, t.Req.PartitionName, 0)
	if err != nil {
		return err
	}

	ddReq := internalpb.DropPartitionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		PartitionName:  t.Req.PartitionName,
		DbID:           0, //todo,not used
		CollectionID:   collInfo.ID,
		PartitionID:    partID,
	}

	reason := fmt.Sprintf("drop partition %s", t.Req.PartitionName)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddReq.Base.Timestamp = ts
	ddOpStr, err := EncodeDdOperation(&ddReq, DropPartitionDDType)
	if err != nil {
		return fmt.Errorf("encodeDdOperation fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	dropPartitionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.addDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.removeDdlTimeTick(ts, reason)

		if err = t.core.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames); err != nil {
			return err
		}

		// update meta table after send dd operation
		if _, err = t.core.MetaTable.DeletePartition(collInfo.ID, t.Req.PartitionName, ts, ddOpStr); err != nil {
			return err
		}

		// use addDdlTimeTick and removeDdlTimeTick to mark DDL operation in process
		t.core.chanTimeTick.removeDdlTimeTick(ts, reason)
		errTimeTick := t.core.SendTimeTick(ts, reason)
		if errTimeTick != nil {
			log.Warn("Failed to send timetick", zap.Error(errTimeTick))
		}
		return nil
	}

	if err = dropPartitionFn(); err != nil {
		return err
	}

	t.core.ExpireMetaCache(ctx, []string{t.Req.CollectionName}, ts)

	//notify query service to release partition
	// TODO::xige-16, reOpen when queryCoord support release partitions after load collection
	//if err = t.core.CallReleasePartitionService(t.core.ctx, ts, 0, collInfo.ID, []typeutil.UniqueID{partID}); err != nil {
	//	log.Error("Failed to CallReleaseCollectionService", zap.Error(err))
	//	return err
	//}

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

// HasPartitionReqTask has partition request task
type HasPartitionReqTask struct {
	baseReqTask
	Req          *milvuspb.HasPartitionRequest
	HasPartition bool
}

// Type return msg type
func (t *HasPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *HasPartitionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_HasPartition {
		return fmt.Errorf("has partition, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	coll, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	if err != nil {
		return err
	}
	t.HasPartition = t.core.MetaTable.HasPartition(coll.ID, t.Req.PartitionName, 0)
	return nil
}

// ShowPartitionReqTask show partition request task
type ShowPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowPartitionsRequest
	Rsp *milvuspb.ShowPartitionsResponse
}

// Type return msg type
func (t *ShowPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *ShowPartitionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_ShowPartitions {
		return fmt.Errorf("show partition, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	var coll *etcdpb.CollectionInfo
	var err error
	if t.Req.CollectionName == "" {
		coll, err = t.core.MetaTable.GetCollectionByID(t.Req.CollectionID, 0)
	} else {
		coll, err = t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	}
	if err != nil {
		return err
	}
	t.Rsp.PartitionIDs = coll.PartitionIDs
	t.Rsp.PartitionNames = coll.PartitionNames
	t.Rsp.CreatedTimestamps = coll.PartitionCreatedTimestamps
	t.Rsp.CreatedUtcTimestamps = make([]uint64, 0, len(coll.PartitionCreatedTimestamps))
	for _, ts := range coll.PartitionCreatedTimestamps {
		physical, _ := tsoutil.ParseHybridTs(ts)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}

	return nil
}

// DescribeSegmentReqTask describe segment request task
type DescribeSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeSegmentRequest
	Rsp *milvuspb.DescribeSegmentResponse //TODO,return repeated segment id in the future
}

// Type return msg type
func (t *DescribeSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DescribeSegmentReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DescribeSegment {
		return fmt.Errorf("describe segment, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	coll, err := t.core.MetaTable.GetCollectionByID(t.Req.CollectionID, 0)
	if err != nil {
		return err
	}
	exist := false
	segIDs, err := t.core.CallGetFlushedSegmentsService(ctx, t.Req.CollectionID, -1)
	if err != nil {
		log.Debug("Get flushed segment from data coord failed", zap.String("collection_name", coll.Schema.Name), zap.Error(err))
		exist = true
	} else {
		for _, id := range segIDs {
			if id == t.Req.SegmentID {
				exist = true
				break
			}
		}
	}

	if !exist {
		return fmt.Errorf("segment id %d not belong to collection id %d", t.Req.SegmentID, t.Req.CollectionID)
	}
	//TODO, get filed_id and index_name from request
	segIdxInfo, err := t.core.MetaTable.GetSegmentIndexInfoByID(t.Req.SegmentID, -1, "")
	log.Debug("RootCoord DescribeSegmentReqTask, MetaTable.GetSegmentIndexInfoByID", zap.Any("SegmentID", t.Req.SegmentID),
		zap.Any("segIdxInfo", segIdxInfo), zap.Error(err))
	if err != nil {
		return err
	}
	t.Rsp.IndexID = segIdxInfo.IndexID
	t.Rsp.BuildID = segIdxInfo.BuildID
	t.Rsp.EnableIndex = segIdxInfo.EnableIndex
	t.Rsp.FieldID = segIdxInfo.FieldID
	return nil
}

// ShowSegmentReqTask show segment request task
type ShowSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.ShowSegmentsRequest
	Rsp *milvuspb.ShowSegmentsResponse
}

// Type return msg type
func (t *ShowSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *ShowSegmentReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_ShowSegments {
		return fmt.Errorf("show segments, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	coll, err := t.core.MetaTable.GetCollectionByID(t.Req.CollectionID, 0)
	if err != nil {
		return err
	}
	exist := false
	for _, partID := range coll.PartitionIDs {
		if partID == t.Req.PartitionID {
			exist = true
			break
		}
	}
	if !exist {
		return fmt.Errorf("partition id = %d not belong to collection id = %d", t.Req.PartitionID, t.Req.CollectionID)
	}
	segIDs, err := t.core.CallGetFlushedSegmentsService(ctx, t.Req.CollectionID, t.Req.PartitionID)
	if err != nil {
		log.Debug("Get flushed segments from data coord failed", zap.String("collection name", coll.Schema.Name), zap.Int64("partition id", t.Req.PartitionID), zap.Error(err))
		return err
	}

	t.Rsp.SegmentIDs = append(t.Rsp.SegmentIDs, segIDs...)
	return nil
}

// CreateIndexReqTask create index request task
type CreateIndexReqTask struct {
	baseReqTask
	Req *milvuspb.CreateIndexRequest
}

// Type return msg type
func (t *CreateIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *CreateIndexReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_CreateIndex {
		return fmt.Errorf("create index, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	indexName := Params.CommonCfg.DefaultIndexName //TODO, get name from request
	indexID, _, err := t.core.IDAllocator(1)
	log.Debug("RootCoord CreateIndexReqTask", zap.Any("indexID", indexID), zap.Error(err))
	if err != nil {
		return err
	}
	idxInfo := &etcdpb.IndexInfo{
		IndexName:   indexName,
		IndexID:     indexID,
		IndexParams: t.Req.ExtraParams,
	}
	collMeta, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName, 0)
	if err != nil {
		return err
	}
	segID2PartID, err := t.core.getSegments(ctx, collMeta.ID)
	flushedSegs := make([]typeutil.UniqueID, 0, len(segID2PartID))
	for k := range segID2PartID {
		flushedSegs = append(flushedSegs, k)
	}
	if err != nil {
		log.Debug("Get flushed segments from data coord failed", zap.String("collection_name", collMeta.Schema.Name), zap.Error(err))
		return err
	}

	segIDs, field, err := t.core.MetaTable.GetNotIndexedSegments(t.Req.CollectionName, t.Req.FieldName, idxInfo, flushedSegs)
	if err != nil {
		log.Debug("RootCoord CreateIndexReqTask metaTable.GetNotIndexedSegments", zap.Error(err))
		return err
	}
	if field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
		return fmt.Errorf("field name = %s, data type = %s", t.Req.FieldName, schemapb.DataType_name[int32(field.DataType)])
	}

	for _, segID := range segIDs {
		info := etcdpb.SegmentIndexInfo{
			CollectionID: collMeta.ID,
			PartitionID:  segID2PartID[segID],
			SegmentID:    segID,
			FieldID:      field.FieldID,
			IndexID:      idxInfo.IndexID,
			EnableIndex:  false,
		}
		info.BuildID, err = t.core.BuildIndex(ctx, segID, &field, idxInfo, false)
		if err != nil {
			return err
		}
		if info.BuildID != 0 {
			info.EnableIndex = true
		}
		if err := t.core.MetaTable.AddIndex(&info); err != nil {
			log.Debug("Add index into meta table failed", zap.Int64("collection_id", collMeta.ID), zap.Int64("index_id", info.IndexID), zap.Int64("build_id", info.BuildID), zap.Error(err))
		}
	}

	return nil
}

// DescribeIndexReqTask describe index request task
type DescribeIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeIndexRequest
	Rsp *milvuspb.DescribeIndexResponse
}

// Type return msg type
func (t *DescribeIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DescribeIndexReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DescribeIndex {
		return fmt.Errorf("describe index, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	coll, idx, err := t.core.MetaTable.GetIndexByName(t.Req.CollectionName, t.Req.IndexName)
	if err != nil {
		return err
	}
	for _, i := range idx {
		f, err := GetFieldSchemaByIndexID(&coll, typeutil.UniqueID(i.IndexID))
		if err != nil {
			log.Warn("Get field schema by index id failed", zap.String("collection name", t.Req.CollectionName), zap.String("index name", t.Req.IndexName), zap.Error(err))
			continue
		}
		desc := &milvuspb.IndexDescription{
			IndexName: i.IndexName,
			Params:    i.IndexParams,
			IndexID:   i.IndexID,
			FieldName: f.Name,
		}
		t.Rsp.IndexDescriptions = append(t.Rsp.IndexDescriptions, desc)
	}
	return nil
}

// DropIndexReqTask drop index request task
type DropIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DropIndexRequest
}

// Type return msg type
func (t *DropIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DropIndexReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropIndex {
		return fmt.Errorf("drop index, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	if err := t.core.RemoveIndex(ctx, t.Req.CollectionName, t.Req.IndexName); err != nil {
		return err
	}
	_, _, err := t.core.MetaTable.DropIndex(t.Req.CollectionName, t.Req.FieldName, t.Req.IndexName)
	return err
}

// CreateAliasReqTask create alias request task
type CreateAliasReqTask struct {
	baseReqTask
	Req *milvuspb.CreateAliasRequest
}

// Type return msg type
func (t *CreateAliasReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *CreateAliasReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_CreateAlias {
		return fmt.Errorf("create alias, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}

	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}
	err = t.core.MetaTable.AddAlias(t.Req.Alias, t.Req.CollectionName, ts)
	if err != nil {
		return fmt.Errorf("meta table add alias failed, error = %w", err)
	}

	return nil
}

// DropAliasReqTask drop alias request task
type DropAliasReqTask struct {
	baseReqTask
	Req *milvuspb.DropAliasRequest
}

// Type return msg type
func (t *DropAliasReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *DropAliasReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropAlias {
		return fmt.Errorf("create alias, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}

	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}
	err = t.core.MetaTable.DropAlias(t.Req.Alias, ts)
	if err != nil {
		return fmt.Errorf("meta table drop alias failed, error = %w", err)
	}

	t.core.ExpireMetaCache(ctx, []string{t.Req.Alias}, ts)

	return nil
}

// AlterAliasReqTask alter alias request task
type AlterAliasReqTask struct {
	baseReqTask
	Req *milvuspb.AlterAliasRequest
}

// Type return msg type
func (t *AlterAliasReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

// Execute task execution
func (t *AlterAliasReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_AlterAlias {
		return fmt.Errorf("alter alias, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}

	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}
	err = t.core.MetaTable.AlterAlias(t.Req.Alias, t.Req.CollectionName, ts)
	if err != nil {
		return fmt.Errorf("meta table alter alias failed, error = %w", err)
	}

	t.core.ExpireMetaCache(ctx, []string{t.Req.Alias}, ts)

	return nil
}
