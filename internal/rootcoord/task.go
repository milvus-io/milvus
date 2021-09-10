// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

type CreateCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.CreateCollectionRequest
}

func (t *CreateCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
		t.Req.ShardsNum = DefaultShardsNum
	}
	log.Debug("CreateCollectionReqTask Execute", zap.Any("CollectionName", t.Req.CollectionName),
		zap.Any("ShardsNum", t.Req.ShardsNum))

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
	for i := int32(0); i < t.Req.ShardsNum; i++ {
		vchanNames[i] = fmt.Sprintf("%s_%d_%d_v%d", t.Req.CollectionName, collID, i, i)
		chanNames[i] = ToPhysicalChannel(vchanNames[i])
	}

	collInfo := etcdpb.CollectionInfo{
		ID:                         collID,
		Schema:                     &schema,
		PartitionIDs:               []typeutil.UniqueID{partID},
		PartitionNames:             []string{Params.DefaultPartitionName},
		FieldIndexes:               make([]*etcdpb.FieldIndexInfo, 0, 16),
		VirtualChannelNames:        vchanNames,
		PhysicalChannelNames:       chanNames,
		ShardsNum:                  t.Req.ShardsNum,
		PartitionCreatedTimestamps: []uint64{0},
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
		PartitionName:        Params.DefaultPartitionName,
		DbID:                 0, //TODO,not used
		CollectionID:         collID,
		PartitionID:          partID,
		Schema:               schemaBytes,
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddCollReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddCollReq, CreateCollectionDDType)
	}

	reason := fmt.Sprintf("create collection %d", collID)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	createCollectionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.AddDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)

		err = t.core.MetaTable.AddCollection(&collInfo, ts, idxInfo, ddOp)
		if err != nil {
			return fmt.Errorf("meta table add collection failed,error = %w", err)
		}

		// add dml channel before send dd msg
		t.core.dmlChannels.AddProducerChannels(chanNames...)

		err = t.core.SendDdCreateCollectionReq(ctx, &ddCollReq, chanNames)
		if err != nil {
			return fmt.Errorf("send dd create collection req failed, error = %w", err)
		}

		t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)
		t.core.SendTimeTick(ts, reason)
		return nil
	}

	err = createCollectionFn()
	if err != nil {
		return err
	}

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

type DropCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DropCollectionRequest
}

func (t *DropCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DropCollectionReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropCollection {
		return fmt.Errorf("drop collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
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

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddReq, DropCollectionDDType)
	}

	reason := fmt.Sprintf("drop collection %d", collMeta.ID)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	dropCollectionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.AddDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)

		err = t.core.MetaTable.DeleteCollection(collMeta.ID, ts, ddOp)
		if err != nil {
			return err
		}

		err = t.core.SendDdDropCollectionReq(ctx, &ddReq, collMeta.PhysicalChannelNames)
		if err != nil {
			return err
		}

		t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)
		t.core.SendTimeTick(ts, reason)

		for _, chanName := range collMeta.PhysicalChannelNames {
			// send tt into deleted channels to tell data_node to clear flowgragh
			t.core.chanTimeTick.SendChannelTimeTick(chanName, ts)
		}

		// remove dml channel after send dd msg
		t.core.dmlChannels.RemoveProducerChannels(collMeta.PhysicalChannelNames...)
		return nil
	}

	err = dropCollectionFn()
	if err != nil {
		return err
	}

	//notify query service to release collection
	if err = t.core.CallReleaseCollectionService(t.core.ctx, ts, 0, collMeta.ID); err != nil {
		log.Error("CallReleaseCollectionService failed", zap.String("error", err.Error()))
		return err
	}

	req := proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:   0, //TODO, msg type
			MsgID:     0, //TODO, msg id
			Timestamp: ts,
			SourceID:  t.core.session.ServerID,
		},
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
	}
	// error doesn't matter here
	t.core.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

type HasCollectionReqTask struct {
	baseReqTask
	Req           *milvuspb.HasCollectionRequest
	HasCollection bool
}

func (t *HasCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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

type DescribeCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeCollectionRequest
	Rsp *milvuspb.DescribeCollectionResponse
}

func (t *DescribeCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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

	t.Rsp.CreatedTimestamp = collInfo.CreateTime
	createdPhysicalTime, _ := tsoutil.ParseHybridTs(collInfo.CreateTime)
	t.Rsp.CreatedUtcTimestamp = createdPhysicalTime

	return nil
}

type ShowCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowCollectionsRequest
	Rsp *milvuspb.ShowCollectionsResponse
}

func (t *ShowCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, physical)
	}
	return nil
}

type CreatePartitionReqTask struct {
	baseReqTask
	Req *milvuspb.CreatePartitionRequest
}

func (t *CreatePartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddReq, CreatePartitionDDType)
	}

	reason := fmt.Sprintf("create partition %s", t.Req.PartitionName)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	createPartitionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.AddDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)

		err = t.core.MetaTable.AddPartition(collMeta.ID, t.Req.PartitionName, partID, ts, ddOp)
		if err != nil {
			return err
		}

		err = t.core.SendDdCreatePartitionReq(ctx, &ddReq, collMeta.PhysicalChannelNames)
		if err != nil {
			return err
		}

		t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)
		t.core.SendTimeTick(ts, reason)
		return nil
	}

	err = createPartitionFn()
	if err != nil {
		return err
	}

	req := proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:   0, //TODO, msg type
			MsgID:     0, //TODO, msg id
			Timestamp: ts,
			SourceID:  t.core.session.ServerID,
		},
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
	}
	// error doesn't matter here
	t.core.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

type DropPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.DropPartitionRequest
}

func (t *DropPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddReq, DropPartitionDDType)
	}

	reason := fmt.Sprintf("drop partition %s", t.Req.PartitionName)
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return fmt.Errorf("TSO alloc fail, error = %w", err)
	}

	// use lambda function here to guarantee all resources to be released
	dropPartitionFn := func() error {
		// lock for ddl operation
		t.core.ddlLock.Lock()
		defer t.core.ddlLock.Unlock()

		t.core.chanTimeTick.AddDdlTimeTick(ts, reason)
		// clear ddl timetick in all conditions
		defer t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)

		_, err = t.core.MetaTable.DeletePartition(collInfo.ID, t.Req.PartitionName, ts, ddOp)
		if err != nil {
			return err
		}

		err = t.core.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames)
		if err != nil {
			return err
		}

		t.core.chanTimeTick.RemoveDdlTimeTick(ts, reason)
		t.core.SendTimeTick(ts, reason)
		return nil
	}

	err = dropPartitionFn()
	if err != nil {
		return err
	}

	req := proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType:   0, //TODO, msg type
			MsgID:     0, //TODO, msg id
			Timestamp: ts,
			SourceID:  t.core.session.ServerID,
		},
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
	}
	// error doesn't matter here
	t.core.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req)

	//notify query service to release partition
	if err = t.core.CallReleasePartitionService(t.core.ctx, ts, 0, collInfo.ID, []typeutil.UniqueID{partID}); err != nil {
		log.Error("CallReleaseCollectionService failed", zap.String("error", err.Error()))
		return err
	}

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

type HasPartitionReqTask struct {
	baseReqTask
	Req          *milvuspb.HasPartitionRequest
	HasPartition bool
}

func (t *HasPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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

type ShowPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowPartitionsRequest
	Rsp *milvuspb.ShowPartitionsResponse
}

func (t *ShowPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, physical)
	}

	return nil
}

type DescribeSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeSegmentRequest
	Rsp *milvuspb.DescribeSegmentResponse //TODO,return repeated segment id in the future
}

func (t *DescribeSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
		log.Debug("get flushed segment from data coord failed", zap.String("collection_name", coll.Schema.Name), zap.Error(err))
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
	return nil
}

type ShowSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.ShowSegmentsRequest
	Rsp *milvuspb.ShowSegmentsResponse
}

func (t *ShowSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
		log.Debug("get flushed segments from data coord failed", zap.String("collection name", coll.Schema.Name), zap.Int64("partition id", t.Req.PartitionID), zap.Error(err))
		return err
	}

	t.Rsp.SegmentIDs = append(t.Rsp.SegmentIDs, segIDs...)
	return nil
}

type CreateIndexReqTask struct {
	baseReqTask
	Req *milvuspb.CreateIndexRequest
}

func (t *CreateIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *CreateIndexReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_CreateIndex {
		return fmt.Errorf("create index, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	indexName := Params.DefaultIndexName //TODO, get name from request
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
		log.Debug("get flushed segments from data coord failed", zap.String("collection_name", collMeta.Schema.Name), zap.Error(err))
		return err
	}

	segIDs, field, err := t.core.MetaTable.GetNotIndexedSegments(t.Req.CollectionName, t.Req.FieldName, idxInfo, flushedSegs, t.Req.Base.GetTimestamp())
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
		ts, _ := t.core.TSOAllocator(1)
		if err := t.core.MetaTable.AddIndex(&info, ts); err != nil {
			log.Debug("Add index into meta table failed", zap.Int64("collection_id", collMeta.ID), zap.Int64("index_id", info.IndexID), zap.Int64("build_id", info.BuildID), zap.Error(err))
		}
	}

	return nil
}

type DescribeIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeIndexRequest
	Rsp *milvuspb.DescribeIndexResponse
}

func (t *DescribeIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

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
			log.Warn("get field schema by index id failed", zap.String("collection name", t.Req.CollectionName), zap.String("index name", t.Req.IndexName), zap.Error(err))
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

type DropIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DropIndexRequest
}

func (t *DropIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DropIndexReqTask) Execute(ctx context.Context) error {
	if t.Type() != commonpb.MsgType_DropIndex {
		return fmt.Errorf("drop index, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	_, info, err := t.core.MetaTable.GetIndexByName(t.Req.CollectionName, t.Req.IndexName)
	if err != nil {
		log.Warn("GetIndexByName failed,", zap.String("collection name", t.Req.CollectionName), zap.String("field name", t.Req.FieldName), zap.String("index name", t.Req.IndexName), zap.Error(err))
		return err
	}
	if len(info) == 0 {
		return nil
	}
	if len(info) != 1 {
		return fmt.Errorf("len(index) = %d", len(info))
	}
	err = t.core.CallDropIndexService(ctx, info[0].IndexID)
	if err != nil {
		return err
	}
	ts, _ := t.core.TSOAllocator(1)
	_, _, err = t.core.MetaTable.DropIndex(t.Req.CollectionName, t.Req.FieldName, t.Req.IndexName, ts)
	return err
}
