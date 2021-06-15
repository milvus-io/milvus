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

package masterservice

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type reqTask interface {
	Ctx() context.Context
	Type() commonpb.MsgType
	Execute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type baseReqTask struct {
	ctx  context.Context
	cv   chan error
	core *Core
}

func (bt *baseReqTask) Notify(err error) {
	bt.cv <- err
}

func (bt *baseReqTask) WaitToFinish() error {
	select {
	case <-bt.core.ctx.Done():
		return fmt.Errorf("core context done, %w", bt.core.ctx.Err())
	case <-bt.ctx.Done():
		return fmt.Errorf("request context done, %w", bt.ctx.Err())
	case err, ok := <-bt.cv:
		if !ok {
			return fmt.Errorf("notify chan closed")
		}
		return err
	}
}

type TimetickTask struct {
	baseReqTask
}

func (t *TimetickTask) Ctx() context.Context {
	return t.ctx
}

func (t *TimetickTask) Type() commonpb.MsgType {
	return commonpb.MsgType_TimeTick
}

func (t *TimetickTask) Execute(ctx context.Context) error {
	ts, err := t.core.TSOAllocator(1)
	if err != nil {
		return err
	}
	return t.core.SendTimeTick(ts)
}

type CreateCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.CreateCollectionRequest
}

func (t *CreateCollectionReqTask) Ctx() context.Context {
	return t.ctx
}

func (t *CreateCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *CreateCollectionReqTask) Execute(ctx context.Context) error {
	const defaultShardsNum = 2

	if t.Type() != commonpb.MsgType_CreateCollection {
		return fmt.Errorf("create collection, msg type = %s", commonpb.MsgType_name[int32(t.Type())])
	}
	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(t.Req.Schema, &schema)
	if err != nil {
		return err
	}

	if t.Req.CollectionName != schema.Name {
		return fmt.Errorf("collection name = %s, schema.Name=%s", t.Req.CollectionName, schema.Name)
	}

	if t.Req.ShardsNum <= 0 {
		log.Debug("Set ShardsNum to default", zap.String("collection name", t.Req.CollectionName),
			zap.Int32("defaultShardsNum", defaultShardsNum))
		t.Req.ShardsNum = defaultShardsNum
	}

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
		return err
	}
	collTs := t.Req.Base.Timestamp
	partID, _, err := t.core.IDAllocator(1)
	if err != nil {
		return err
	}

	vchanNames := make([]string, t.Req.ShardsNum)
	chanNames := make([]string, t.Req.ShardsNum)
	for i := int32(0); i < t.Req.ShardsNum; i++ {
		vchanNames[i] = fmt.Sprintf("%s_%d_v%d", t.Req.CollectionName, collID, i)
		chanNames[i] = fmt.Sprintf("%s_%d_c%d", t.Req.CollectionName, collID, i)
	}

	collInfo := etcdpb.CollectionInfo{
		ID:                   collID,
		Schema:               &schema,
		CreateTime:           collTs,
		PartitionIDs:         make([]typeutil.UniqueID, 0, 16),
		FieldIndexes:         make([]*etcdpb.FieldIndexInfo, 0, 16),
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
	}

	// every collection has _default partition
	partInfo := etcdpb.PartitionInfo{
		PartitionName: Params.DefaultPartitionName,
		PartitionID:   partID,
		SegmentIDs:    make([]typeutil.UniqueID, 0, 16),
	}
	idxInfo := make([]*etcdpb.IndexInfo, 0, 16)
	/////////////////////// ignore index param from create_collection /////////////////////////
	//for _, field := range schema.Fields {
	//	if field.DataType == schemapb.DataType_VectorFloat || field.DataType == schemapb.DataType_VectorBinary {
	//		if len(field.IndexParams) > 0 {
	//			idxID, err := t.core.idAllocator.AllocOne()
	//			if err != nil {
	//				return err
	//			}
	//			filedIdx := &etcdpb.FieldIndexInfo{
	//				FiledID: field.FieldID,
	//				IndexID: idxID,
	//			}
	//			idx := &etcdpb.IndexInfo{
	//				IndexName:   fmt.Sprintf("%s_index_%d", collMeta.Schema.Name, field.FieldID),
	//				IndexID:     idxID,
	//				IndexParams: field.IndexParams,
	//			}
	//			idxInfo = append(idxInfo, idx)
	//			collMeta.FieldIndexes = append(collMeta.FieldIndexes, filedIdx)
	//		}
	//	}
	//}

	// schema is modified (add RowIDField and TimestampField),
	// so need Marshal again
	schemaBytes, err := proto.Marshal(&schema)
	if err != nil {
		return err
	}

	ddCollReq := internalpb.CreateCollectionRequest{
		Base:                 t.Req.Base,
		DbName:               t.Req.DbName,
		CollectionName:       t.Req.CollectionName,
		DbID:                 0, //TODO,not used
		CollectionID:         collID,
		Schema:               schemaBytes,
		VirtualChannelNames:  vchanNames,
		PhysicalChannelNames: chanNames,
	}

	ddPartReq := internalpb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     t.Req.Base.MsgID, //TODO, msg id
			Timestamp: t.Req.Base.Timestamp + 1,
			SourceID:  t.Req.Base.SourceID,
		},
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		PartitionName:  Params.DefaultPartitionName,
		DbID:           0, //TODO, not used
		CollectionID:   collInfo.ID,
		PartitionID:    partInfo.PartitionID,
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddCollReq.Base.Timestamp = ts
		ddPartReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddCollReq, &ddPartReq, CreateCollectionDDType)
	}

	ts, err := t.core.MetaTable.AddCollection(&collInfo, &partInfo, idxInfo, ddOp)
	if err != nil {
		return err
	}

	// add dml channel before send dd msg
	t.core.dmlChannels.AddProducerChannels(chanNames...)

	err = t.core.SendDdCreateCollectionReq(ctx, &ddCollReq, chanNames)
	if err != nil {
		return err
	}
	err = t.core.SendDdCreatePartitionReq(ctx, &ddPartReq, chanNames)
	if err != nil {
		return err
	}

	t.core.SendTimeTick(ts)

	// Update DDOperation in etcd
	return t.core.setDdMsgSendFlag(true)
}

type DropCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DropCollectionRequest
}

func (t *DropCollectionReqTask) Ctx() context.Context {
	return t.ctx
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
		return EncodeDdOperation(&ddReq, nil, DropCollectionDDType)
	}

	ts, err := t.core.MetaTable.DeleteCollection(collMeta.ID, ddOp)
	if err != nil {
		return err
	}

	err = t.core.SendDdDropCollectionReq(ctx, &ddReq, collMeta.PhysicalChannelNames)
	if err != nil {
		return err
	}

	t.core.SendTimeTick(ts)

	// remove dml channel after send dd msg
	t.core.dmlChannels.RemoveProducerChannels(collMeta.PhysicalChannelNames...)

	//notify query service to release collection
	go func() {
		if err = t.core.CallReleaseCollectionService(t.core.ctx, ts, 0, collMeta.ID); err != nil {
			log.Warn("CallReleaseCollectionService failed", zap.String("error", err.Error()))
		}
	}()

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

func (t *HasCollectionReqTask) Ctx() context.Context {
	return t.ctx
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

func (t *DescribeCollectionReqTask) Ctx() context.Context {
	return t.ctx
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
	//var newField []*schemapb.FieldSchema
	//for _, field := range t.Rsp.Schema.Fields {
	//	if field.FieldID >= StartOfUserFieldID {
	//		newField = append(newField, field)
	//	}
	//}
	//t.Rsp.Schema.Fields = newField

	t.Rsp.VirtualChannelNames = collInfo.VirtualChannelNames
	t.Rsp.PhysicalChannelNames = collInfo.PhysicalChannelNames
	return nil
}

type ShowCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowCollectionsRequest
	Rsp *milvuspb.ShowCollectionsResponse
}

func (t *ShowCollectionReqTask) Ctx() context.Context {
	return t.ctx
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
	for name, id := range coll {
		t.Rsp.CollectionNames = append(t.Rsp.CollectionNames, name)
		t.Rsp.CollectionIds = append(t.Rsp.CollectionIds, id)
	}
	return nil
}

type CreatePartitionReqTask struct {
	baseReqTask
	Req *milvuspb.CreatePartitionRequest
}

func (t *CreatePartitionReqTask) Ctx() context.Context {
	return t.ctx
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
		return EncodeDdOperation(&ddReq, nil, CreatePartitionDDType)
	}

	ts, err := t.core.MetaTable.AddPartition(collMeta.ID, t.Req.PartitionName, partID, ddOp)
	if err != nil {
		return err
	}

	err = t.core.SendDdCreatePartitionReq(ctx, &ddReq, collMeta.PhysicalChannelNames)
	if err != nil {
		return err
	}

	t.core.SendTimeTick(ts)

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

func (t *DropPartitionReqTask) Ctx() context.Context {
	return t.ctx
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
	partInfo, err := t.core.MetaTable.GetPartitionByName(collInfo.ID, t.Req.PartitionName, 0)
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
		PartitionID:    partInfo.PartitionID,
	}

	// build DdOperation and save it into etcd, when ddmsg send fail,
	// system can restore ddmsg from etcd and re-send
	ddOp := func(ts typeutil.Timestamp) (string, error) {
		ddReq.Base.Timestamp = ts
		return EncodeDdOperation(&ddReq, nil, DropPartitionDDType)
	}

	ts, _, err := t.core.MetaTable.DeletePartition(collInfo.ID, t.Req.PartitionName, ddOp)
	if err != nil {
		return err
	}

	err = t.core.SendDdDropPartitionReq(ctx, &ddReq, collInfo.PhysicalChannelNames)
	if err != nil {
		return err
	}

	t.core.SendTimeTick(ts)

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

type HasPartitionReqTask struct {
	baseReqTask
	Req          *milvuspb.HasPartitionRequest
	HasPartition bool
}

func (t *HasPartitionReqTask) Ctx() context.Context {
	return t.ctx
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

func (t *ShowPartitionReqTask) Ctx() context.Context {
	return t.ctx
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
	for _, partID := range coll.PartitionIDs {
		partMeta, err := t.core.MetaTable.GetPartitionByID(coll.ID, partID, 0)
		if err != nil {
			return err
		}
		t.Rsp.PartitionIDs = append(t.Rsp.PartitionIDs, partMeta.PartitionID)
		t.Rsp.PartitionNames = append(t.Rsp.PartitionNames, partMeta.PartitionName)
	}
	return nil
}

type DescribeSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeSegmentRequest
	Rsp *milvuspb.DescribeSegmentResponse //TODO,return repeated segment id in the future
}

func (t *DescribeSegmentReqTask) Ctx() context.Context {
	return t.ctx
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
	for _, partID := range coll.PartitionIDs {
		if exist {
			break
		}
		partMeta, err := t.core.MetaTable.GetPartitionByID(coll.ID, partID, 0)
		if err != nil {
			return err
		}
		for _, e := range partMeta.SegmentIDs {
			if e == t.Req.SegmentID {
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
	log.Debug("MasterService DescribeSegmentReqTask, MetaTable.GetSegmentIndexInfoByID", zap.Any("SegmentID", t.Req.SegmentID),
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

func (t *ShowSegmentReqTask) Ctx() context.Context {
	return t.ctx
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
	partMeta, err := t.core.MetaTable.GetPartitionByID(coll.ID, t.Req.PartitionID, 0)
	if err != nil {
		return err
	}
	t.Rsp.SegmentIDs = append(t.Rsp.SegmentIDs, partMeta.SegmentIDs...)
	return nil
}

type CreateIndexReqTask struct {
	baseReqTask
	Req *milvuspb.CreateIndexRequest
}

func (t *CreateIndexReqTask) Ctx() context.Context {
	return t.ctx
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
	log.Debug("MasterService CreateIndexReqTask", zap.Any("indexID", indexID), zap.Error(err))
	if err != nil {
		return err
	}
	idxInfo := &etcdpb.IndexInfo{
		IndexName:   indexName,
		IndexID:     indexID,
		IndexParams: t.Req.ExtraParams,
	}
	segIDs, field, err := t.core.MetaTable.GetNotIndexedSegments(t.Req.CollectionName, t.Req.FieldName, idxInfo)
	log.Debug("MasterService CreateIndexReqTask metaTable.GetNotIndexedSegments", zap.Error(err))
	if err != nil {
		return err
	}
	if field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
		return fmt.Errorf("field name = %s, data type = %s", t.Req.FieldName, schemapb.DataType_name[int32(field.DataType)])
	}

	var segIdxInfos []*etcdpb.SegmentIndexInfo
	for _, segID := range segIDs {
		info := etcdpb.SegmentIndexInfo{
			SegmentID:   segID,
			FieldID:     field.FieldID,
			IndexID:     idxInfo.IndexID,
			EnableIndex: false,
		}
		info.BuildID, err = t.core.BuildIndex(segID, &field, idxInfo, false)
		if err != nil {
			return err
		}
		if info.BuildID != 0 {
			info.EnableIndex = true
		}
		segIdxInfos = append(segIdxInfos, &info)
	}

	_, err = t.core.MetaTable.AddIndex(segIdxInfos, "", "")
	log.Debug("MasterService CreateIndexReq", zap.Any("segIdxInfos", segIdxInfos), zap.Error(err))
	return err
}

type DescribeIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeIndexRequest
	Rsp *milvuspb.DescribeIndexResponse
}

func (t *DescribeIndexReqTask) Ctx() context.Context {
	return t.ctx
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

func (t *DropIndexReqTask) Ctx() context.Context {
	return t.ctx
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
	_, _, _, err = t.core.MetaTable.DropIndex(t.Req.CollectionName, t.Req.FieldName, t.Req.IndexName)
	return err
}
