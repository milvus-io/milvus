// // Licensed to the LF AI & Data foundation under one
// // or more contributor license agreements. See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership. The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License. You may obtain a copy of the License at
// //
// //	http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
package proxy

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type upsertTask struct {
	Condition

	upsertMsg *msgstream.UpsertMsg
	req       *milvuspb.UpsertRequest
	baseMsg   msgstream.BaseMsg

	ctx context.Context

	timestamps    []uint64
	rowIDs        []int64
	result        *milvuspb.MutationResult
	idAllocator   *allocator.IDAllocator
	segIDAssigner *segIDAssigner
	collectionID  UniqueID
	chMgr         channelsMgr
	chTicker      channelsTimeTicker
	vChannels     []vChan
	pChannels     []pChan
	schema        *schemapb.CollectionSchema
}

// TraceCtx returns upsertTask context
func (it *upsertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *upsertTask) ID() UniqueID {
	return it.req.Base.MsgID
}

func (it *upsertTask) SetID(uid UniqueID) {
	it.req.Base.MsgID = uid
}

func (it *upsertTask) Name() string {
	return UpsertTaskName
}

func (it *upsertTask) Type() commonpb.MsgType {
	return it.req.Base.MsgType
}

func (it *upsertTask) BeginTs() Timestamp {
	return it.baseMsg.BeginTimestamp
}

func (it *upsertTask) SetTs(ts Timestamp) {
	it.baseMsg.BeginTimestamp = ts
	it.baseMsg.EndTimestamp = ts
}

func (it *upsertTask) EndTs() Timestamp {
	return it.baseMsg.EndTimestamp
}

func (it *upsertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels, err := it.getChannels()
	if err != nil {
		return ret, err
	}

	beginTs := it.BeginTs()
	endTs := it.EndTs()

	for _, channel := range channels {
		ret[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	return ret, nil
}

func (it *upsertTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.req.CollectionName)
	if err != nil {
		return nil, err
	}
	return it.chMgr.getChannels(collID)
}

func (it *upsertTask) OnEnqueue() error {
	return nil
}

func (it *upsertTask) insertPreExecute(ctx context.Context) error {
	collectionName := it.upsertMsg.InsertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Error("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	partitionTag := it.upsertMsg.InsertMsg.PartitionName
	if err := validatePartitionTag(partitionTag, true); err != nil {
		log.Error("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
		return err
	}
	rowNums := uint32(it.upsertMsg.InsertMsg.NRows())
	// set upsertTask.insertRequest.rowIDs
	tr := timerecord.NewTimeRecorder("applyPK")
	rowIDBegin, rowIDEnd, _ := it.idAllocator.Alloc(rowNums)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan()))

	it.upsertMsg.InsertMsg.RowIDs = make([]UniqueID, rowNums)
	it.rowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.upsertMsg.InsertMsg.RowIDs[offset] = i
		it.rowIDs[offset] = i
	}
	// set upsertTask.insertRequest.timeStamps
	rowNum := it.upsertMsg.InsertMsg.NRows()
	it.upsertMsg.InsertMsg.Timestamps = make([]uint64, rowNum)
	it.timestamps = make([]uint64, rowNum)
	for index := range it.timestamps {
		it.upsertMsg.InsertMsg.Timestamps[index] = it.BeginTs()
		it.timestamps[index] = it.BeginTs()
	}
	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	// check primaryFieldData whether autoID is true or not
	// only allow support autoID == false
	var err error
	it.result.IDs, err = checkPrimaryFieldData(it.schema, it.result, it.upsertMsg.InsertMsg, false)
	log := log.Ctx(ctx).With(zap.String("collectionName", it.upsertMsg.InsertMsg.CollectionName))
	if err != nil {
		log.Error("check primary field data and hash primary key failed when upsert",
			zap.Error(err))
		return err
	}
	// set field ID to insert field data
	err = fillFieldIDBySchema(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema)
	if err != nil {
		log.Error("insert set fieldID to fieldData failed when upsert",
			zap.Error(err))
		return err
	}

	// check that all field's number rows are equal
	if err = it.upsertMsg.InsertMsg.CheckAligned(); err != nil {
		log.Error("field data is not aligned when upsert",
			zap.Error(err))
		return err
	}
	log.Debug("Proxy Upsert insertPreExecute done")

	return nil
}

func (it *upsertTask) deletePreExecute(ctx context.Context) error {
	collName := it.upsertMsg.DeleteMsg.CollectionName
	log := log.Ctx(ctx).With(
		zap.String("collectionName", collName))

	if err := validateCollectionName(collName); err != nil {
		log.Info("Invalid collection name", zap.Error(err))
		return err
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, collName)
	if err != nil {
		log.Info("Failed to get collection id", zap.Error(err))
		return err
	}
	it.upsertMsg.DeleteMsg.CollectionID = collID
	it.collectionID = collID

	// If partitionName is not empty, partitionID will be set.
	if len(it.upsertMsg.DeleteMsg.PartitionName) > 0 {
		partName := it.upsertMsg.DeleteMsg.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Info("Invalid partition name", zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, collName, partName)
		if err != nil {
			log.Info("Failed to get partition id", zap.String("collectionName", collName), zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		it.upsertMsg.DeleteMsg.PartitionID = partID
	} else {
		it.upsertMsg.DeleteMsg.PartitionID = common.InvalidPartitionID
	}

	it.upsertMsg.DeleteMsg.Timestamps = make([]uint64, it.upsertMsg.DeleteMsg.NumRows)
	for index := range it.upsertMsg.DeleteMsg.Timestamps {
		it.upsertMsg.DeleteMsg.Timestamps[index] = it.BeginTs()
	}
	log.Debug("Proxy Upsert deletePreExecute done")
	return nil
}

func (it *upsertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-PreExecute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", it.req.CollectionName))

	it.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.req.CollectionName)
	if err != nil {
		log.Info("Failed to get collection schema", zap.Error(err))
		return err
	}
	it.schema = schema

	it.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{
			InsertRequest: msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: it.req.CollectionName,
				PartitionName:  it.req.PartitionName,
				FieldsData:     it.req.FieldsData,
				NumRows:        uint64(it.req.NumRows),
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		},
		DeleteMsg: &msgstream.DeleteMsg{
			DeleteRequest: msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				DbName:         it.req.DbName,
				CollectionName: it.req.CollectionName,
				NumRows:        int64(it.req.NumRows),
				PartitionName:  it.req.PartitionName,
				CollectionID:   it.collectionID,
			},
		},
	}
	err = it.insertPreExecute(ctx)
	if err != nil {
		log.Info("Fail to insertPreExecute", zap.Error(err))
		return err
	}

	err = it.deletePreExecute(ctx)
	if err != nil {
		log.Info("Fail to deletePreExecute", zap.Error(err))
		return err
	}

	it.result.DeleteCnt = it.upsertMsg.DeleteMsg.NumRows
	it.result.InsertCnt = int64(it.upsertMsg.InsertMsg.NumRows)
	if it.result.DeleteCnt != it.result.InsertCnt {
		log.Error("DeleteCnt and InsertCnt are not the same when upsert",
			zap.Int64("DeleteCnt", it.result.DeleteCnt),
			zap.Int64("InsertCnt", it.result.InsertCnt))
	}
	it.result.UpsertCnt = it.result.InsertCnt
	log.Debug("Proxy Upsert PreExecute done")
	return nil
}

func (it *upsertTask) insertExecute(ctx context.Context, msgPack *msgstream.MsgPack) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", it.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collectionName := it.upsertMsg.InsertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		return err
	}
	it.upsertMsg.InsertMsg.CollectionID = collID
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	var partitionID UniqueID
	if len(it.upsertMsg.InsertMsg.PartitionName) > 0 {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, it.req.PartitionName)
		if err != nil {
			return err
		}
	} else {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, Params.CommonCfg.DefaultPartitionName.GetValue())
		if err != nil {
			return err
		}
	}
	it.upsertMsg.InsertMsg.PartitionID = partitionID
	tr.Record("get collection id & partition id from cache when insertExecute")

	_, err = it.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}
	tr.Record("get used message stream when insertExecute")

	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Error("get vChannels failed when insertExecute",
			zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}

	log.Debug("send insert request to virtual channels when insertExecute",
		zap.String("collection", it.req.GetCollectionName()),
		zap.String("partition", it.req.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Int64("partition_id", partitionID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()))

	// assign segmentID for insert data and repack data by segmentID
	insertMsgPack, err := assignSegmentID(it.TraceCtx(), it.upsertMsg.InsertMsg, it.result, channelNames, it.idAllocator, it.segIDAssigner)
	if err != nil {
		log.Error("assign segmentID and repack insert data failed when insertExecute",
			zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	log.Debug("assign segmentID for insert data success when insertExecute",
		zap.String("collectionName", it.req.CollectionName))
	tr.Record("assign segment id")
	msgPack.Msgs = append(msgPack.Msgs, insertMsgPack.Msgs...)

	log.Debug("Proxy Insert Execute done when upsert",
		zap.String("collectionName", collectionName))

	return nil
}

func (it *upsertTask) deleteExecute(ctx context.Context, msgPack *msgstream.MsgPack) (err error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", it.ID()))
	defer tr.Elapse("delete execute done when upsert")

	collID := it.upsertMsg.DeleteMsg.CollectionID
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	// hash primary keys to channels
	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when deleteExecute", zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	it.upsertMsg.DeleteMsg.PrimaryKeys = it.result.IDs
	it.upsertMsg.DeleteMsg.HashValues = typeutil.HashPK2Channels(it.upsertMsg.DeleteMsg.PrimaryKeys, channelNames)

	log.Debug("send delete request to virtual channels when deleteExecute",
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames))

	tr.Record("get vchannels")
	// repack delete msg by dmChannel
	result := make(map[uint32]msgstream.TsMsg)
	collectionName := it.upsertMsg.DeleteMsg.CollectionName
	collectionID := it.upsertMsg.DeleteMsg.CollectionID
	partitionID := it.upsertMsg.DeleteMsg.PartitionID
	partitionName := it.upsertMsg.DeleteMsg.PartitionName
	proxyID := it.upsertMsg.DeleteMsg.Base.SourceID
	for index, key := range it.upsertMsg.DeleteMsg.HashValues {
		ts := it.upsertMsg.DeleteMsg.Timestamps[index]
		_, ok := result[key]
		if !ok {
			sliceRequest := msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithTimeStamp(ts),
					commonpbutil.WithSourceID(proxyID),
				),
				CollectionID:   collectionID,
				PartitionID:    partitionID,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				PrimaryKeys:    &schemapb.IDs{},
			}
			deleteMsg := &msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{
					Ctx: ctx,
				},
				DeleteRequest: sliceRequest,
			}
			result[key] = deleteMsg
		}
		curMsg := result[key].(*msgstream.DeleteMsg)
		curMsg.HashValues = append(curMsg.HashValues, it.upsertMsg.DeleteMsg.HashValues[index])
		curMsg.Timestamps = append(curMsg.Timestamps, it.upsertMsg.DeleteMsg.Timestamps[index])
		typeutil.AppendIDs(curMsg.PrimaryKeys, it.upsertMsg.DeleteMsg.PrimaryKeys, index)
		curMsg.NumRows++
		curMsg.ShardName = channelNames[key]
	}

	// send delete request to log broker
	deleteMsgPack := &msgstream.MsgPack{
		BeginTs: it.upsertMsg.DeleteMsg.BeginTs(),
		EndTs:   it.upsertMsg.DeleteMsg.EndTs(),
	}
	for _, msg := range result {
		if msg != nil {
			deleteMsgPack.Msgs = append(deleteMsgPack.Msgs, msg)
		}
	}
	msgPack.Msgs = append(msgPack.Msgs, deleteMsgPack.Msgs...)

	log.Debug("Proxy Upsert deleteExecute done")
	return nil
}

func (it *upsertTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", it.req.CollectionName))

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute upsert %d", it.ID()))
	stream, err := it.chMgr.getOrCreateDmlStream(it.collectionID)
	if err != nil {
		return err
	}
	msgPack := &msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
	}
	err = it.insertExecute(ctx, msgPack)
	if err != nil {
		log.Info("Fail to insertExecute", zap.Error(err))
		return err
	}

	err = it.deleteExecute(ctx, msgPack)
	if err != nil {
		log.Info("Fail to deleteExecute", zap.Error(err))
		return err
	}

	tr.Record("pack messages in upsert")
	err = stream.Produce(msgPack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	sendMsgDur := tr.Record("send upsert request to dml channels")
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.UpsertLabel).Observe(float64(sendMsgDur.Milliseconds()))
	log.Debug("Proxy Upsert Execute done")
	return nil
}

func (it *upsertTask) PostExecute(ctx context.Context) error {
	return nil
}
