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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	InsertTaskName                  = "InsertTask"
	CreateCollectionTaskName        = "CreateCollectionTask"
	DropCollectionTaskName          = "DropCollectionTask"
	SearchTaskName                  = "SearchTask"
	RetrieveTaskName                = "RetrieveTask"
	QueryTaskName                   = "QueryTask"
	AnnsFieldKey                    = "anns_field"
	TopKKey                         = "topk"
	MetricTypeKey                   = "metric_type"
	SearchParamsKey                 = "params"
	RoundDecimalKey                 = "round_decimal"
	HasCollectionTaskName           = "HasCollectionTask"
	DescribeCollectionTaskName      = "DescribeCollectionTask"
	GetCollectionStatisticsTaskName = "GetCollectionStatisticsTask"
	GetPartitionStatisticsTaskName  = "GetPartitionStatisticsTask"
	ShowCollectionTaskName          = "ShowCollectionTask"
	CreatePartitionTaskName         = "CreatePartitionTask"
	DropPartitionTaskName           = "DropPartitionTask"
	HasPartitionTaskName            = "HasPartitionTask"
	ShowPartitionTaskName           = "ShowPartitionTask"
	CreateIndexTaskName             = "CreateIndexTask"
	DescribeIndexTaskName           = "DescribeIndexTask"
	DropIndexTaskName               = "DropIndexTask"
	GetIndexStateTaskName           = "GetIndexStateTask"
	GetIndexBuildProgressTaskName   = "GetIndexBuildProgressTask"
	FlushTaskName                   = "FlushTask"
	LoadCollectionTaskName          = "LoadCollectionTask"
	ReleaseCollectionTaskName       = "ReleaseCollectionTask"
	LoadPartitionTaskName           = "LoadPartitionsTask"
	ReleasePartitionTaskName        = "ReleasePartitionsTask"
	deleteTaskName                  = "DeleteTask"
	CreateAliasTaskName             = "CreateAliasTask"
	DropAliasTaskName               = "DropAliasTask"
	AlterAliasTaskName              = "AlterAliasTask"

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
	getChannels() ([]pChan, error)
	getPChanStats() (map[pChan]pChanStatistics, error)
}

type BaseInsertTask = msgstream.InsertMsg

type insertTask struct {
	BaseInsertTask
	// req *milvuspb.InsertRequest
	Condition
	ctx context.Context

	result         *milvuspb.MutationResult
	rowIDAllocator *allocator.IDAllocator
	segIDAssigner  *segIDAssigner
	chMgr          channelsMgr
	chTicker       channelsTimeTicker
	vChannels      []vChan
	pChannels      []pChan
	schema         *schemapb.CollectionSchema
}

// TraceCtx returns insertTask context
func (it *insertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *insertTask) ID() UniqueID {
	return it.Base.MsgID
}

func (it *insertTask) SetID(uid UniqueID) {
	it.Base.MsgID = uid
}

func (it *insertTask) Name() string {
	return InsertTaskName
}

func (it *insertTask) Type() commonpb.MsgType {
	return it.Base.MsgType
}

func (it *insertTask) BeginTs() Timestamp {
	return it.BeginTimestamp
}

func (it *insertTask) SetTs(ts Timestamp) {
	it.BeginTimestamp = ts
	it.EndTimestamp = ts
}

func (it *insertTask) EndTs() Timestamp {
	return it.EndTimestamp
}

func (it *insertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
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

func (it *insertTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.CollectionName)
	if err != nil {
		return nil, err
	}
	return it.chMgr.getChannels(collID)
}

func (it *insertTask) OnEnqueue() error {
	return nil
}

func (it *insertTask) checkLengthOfFieldsData() error {
	neededFieldsNum := 0
	for _, field := range it.schema.Fields {
		if !field.AutoID {
			neededFieldsNum++
		}
	}

	if len(it.FieldsData) < neededFieldsNum {
		return errFieldsLessThanNeeded(len(it.FieldsData), neededFieldsNum)
	}

	return nil
}

func (it *insertTask) checkPrimaryFieldData() error {
	rowNums := uint32(it.NRows())
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if it.NRows() <= 0 {
		return errNumRowsLessThanOrEqualToZero(rowNums)
	}

	if err := it.checkLengthOfFieldsData(); err != nil {
		return err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(it.schema)
	if err != nil {
		log.Error("get primary field schema failed", zap.String("collection name", it.CollectionName), zap.Any("schema", it.schema), zap.Error(err))
		return err
	}

	// get primaryFieldData whether autoID is true or not
	var primaryFieldData *schemapb.FieldData
	if !primaryFieldSchema.AutoID {
		primaryFieldData, err = typeutil.GetPrimaryFieldData(it.GetFieldsData(), primaryFieldSchema)
		if err != nil {
			log.Error("get primary field data failed", zap.String("collection name", it.CollectionName), zap.Error(err))
			return err
		}
	} else {
		// if autoID == true, currently only support autoID for int64 PrimaryField
		primaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, it.RowIDs)
		if err != nil {
			log.Error("generate primary field data failed when autoID == true", zap.String("collection name", it.CollectionName), zap.Error(err))
			return err
		}
		// if autoID == true, set the primary field data
		it.FieldsData = append(it.FieldsData, primaryFieldData)
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	it.result.IDs, err = parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Error("parse primary field data to IDs failed", zap.String("collection name", it.CollectionName), zap.Error(err))
		return err
	}

	return nil
}

func (it *insertTask) PreExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(it.ctx, "Proxy-Insert-PreExecute")
	defer sp.Finish()

	it.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	collectionName := it.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Error("valid collection name failed", zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	partitionTag := it.PartitionName
	if err := validatePartitionTag(partitionTag, true); err != nil {
		log.Error("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
		return err
	}

	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
	if err != nil {
		log.Error("get collection schema from global meta cache failed", zap.String("collection name", collectionName), zap.Error(err))
		return err
	}
	it.schema = collSchema

	rowNums := uint32(it.NRows())
	// set insertTask.rowIDs
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	tr := timerecord.NewTimeRecorder("applyPK")
	rowIDBegin, rowIDEnd, _ = it.rowIDAllocator.Alloc(rowNums)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan()))

	it.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.RowIDs[offset] = i
	}
	// set insertTask.timeStamps
	rowNum := it.NRows()
	it.Timestamps = make([]uint64, rowNum)
	for index := range it.Timestamps {
		it.Timestamps[index] = it.BeginTimestamp
	}

	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	// check primaryFieldData whether autoID is true or not
	// set rowIDs as primary data if autoID == true
	err = it.checkPrimaryFieldData()
	if err != nil {
		log.Error("check primary field data and hash primary key failed", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldIDBySchema(it.GetFieldsData(), collSchema)
	if err != nil {
		log.Error("set fieldID to fieldData failed", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	// check that all field's number rows are equal
	if err = it.CheckAligned(); err != nil {
		log.Error("field data is not aligned", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	log.Debug("Proxy Insert PreExecute done", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName))

	return nil
}

func (it *insertTask) assignSegmentID(channelNames []string) (*msgstream.MsgPack, error) {
	threshold := Params.PulsarCfg.MaxMessageSize

	result := &msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
	}

	// generate hash value for every primary key
	if len(it.HashValues) != 0 {
		log.Warn("the hashvalues passed through client is not supported now, and will be overwritten")
	}
	it.HashValues = typeutil.HashPK2Channels(it.result.IDs, channelNames)
	// groupedHashKeys represents the dmChannel index
	channel2RowOffsets := make(map[string][]int)  //   channelName to count
	channelMaxTSMap := make(map[string]Timestamp) //  channelName to max Timestamp

	// assert len(it.hashValues) < maxInt
	for offset, channelID := range it.HashValues {
		channelName := channelNames[channelID]
		if _, ok := channel2RowOffsets[channelName]; !ok {
			channel2RowOffsets[channelName] = []int{}
		}
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], offset)

		if _, ok := channelMaxTSMap[channelName]; !ok {
			channelMaxTSMap[channelName] = typeutil.ZeroTimestamp
		}
		ts := it.Timestamps[offset]
		if channelMaxTSMap[channelName] < ts {
			channelMaxTSMap[channelName] = ts
		}
	}

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string) *msgstream.InsertMsg {
		insertReq := internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     it.Base.MsgID,
				Timestamp: it.BeginTimestamp, // entity's timestamp was set to equal it.BeginTimestamp in preExecute()
				SourceID:  it.Base.SourceID,
			},
			CollectionID:   it.CollectionID,
			PartitionID:    it.PartitionID,
			CollectionName: it.CollectionName,
			PartitionName:  it.PartitionName,
			SegmentID:      segmentID,
			ShardName:      channelName,
			Version:        internalpb.InsertDataVersion_ColumnBased,
		}
		insertReq.FieldsData = make([]*schemapb.FieldData, len(it.GetFieldsData()))

		insertMsg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx: it.TraceCtx(),
			},
			InsertRequest: insertReq,
		}

		return insertMsg
	}

	// repack the row data corresponding to the offset to insertMsg
	getInsertMsgsBySegmentID := func(segmentID UniqueID, rowOffsets []int, channelName string, mexMessageSize int) ([]msgstream.TsMsg, error) {
		repackedMsgs := make([]msgstream.TsMsg, 0)
		requestSize := 0
		insertMsg := createInsertMsg(segmentID, channelName)
		for _, offset := range rowOffsets {
			curRowMessageSize, err := typeutil.EstimateEntitySize(it.InsertRequest.GetFieldsData(), offset)
			if err != nil {
				return nil, err
			}

			// if insertMsg's size is greater than the threshold, split into multiple insertMsgs
			if requestSize+curRowMessageSize >= mexMessageSize {
				repackedMsgs = append(repackedMsgs, insertMsg)
				insertMsg = createInsertMsg(segmentID, channelName)
				requestSize = 0
			}

			typeutil.AppendFieldData(insertMsg.FieldsData, it.GetFieldsData(), int64(offset))
			insertMsg.HashValues = append(insertMsg.HashValues, it.HashValues[offset])
			insertMsg.Timestamps = append(insertMsg.Timestamps, it.Timestamps[offset])
			insertMsg.RowIDs = append(insertMsg.RowIDs, it.RowIDs[offset])
			insertMsg.NumRows++
			requestSize += curRowMessageSize
		}
		repackedMsgs = append(repackedMsgs, insertMsg)

		return repackedMsgs, nil
	}

	// get allocated segmentID info for every dmChannel and repack insertMsgs for every segmentID
	for channelName, rowOffsets := range channel2RowOffsets {
		assignedSegmentInfos, err := it.segIDAssigner.GetSegmentID(it.CollectionID, it.PartitionID, channelName, uint32(len(rowOffsets)), channelMaxTSMap[channelName])
		if err != nil {
			log.Error("allocate segmentID for insert data failed",
				zap.Int64("collectionID", it.CollectionID),
				zap.String("channel name", channelName),
				zap.Int("allocate count", len(rowOffsets)),
				zap.Error(err))
			return nil, err
		}

		startPos := 0
		for segmentID, count := range assignedSegmentInfos {
			subRowOffsets := rowOffsets[startPos : startPos+int(count)]
			insertMsgs, err := getInsertMsgsBySegmentID(segmentID, subRowOffsets, channelName, threshold)
			if err != nil {
				log.Error("repack insert data to insert msgs failed",
					zap.Int64("collectionID", it.CollectionID),
					zap.Error(err))
				return nil, err
			}
			result.Msgs = append(result.Msgs, insertMsgs...)
			startPos += int(count)
		}
	}

	return result, nil
}

func (it *insertTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(it.ctx, "Proxy-Insert-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert %d", it.ID()))
	defer tr.Elapse("insert execute done")

	collectionName := it.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		return err
	}
	it.CollectionID = collID
	var partitionID UniqueID
	if len(it.PartitionName) > 0 {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, it.PartitionName)
		if err != nil {
			return err
		}
	} else {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, Params.CommonCfg.DefaultPartitionName)
		if err != nil {
			return err
		}
	}
	it.PartitionID = partitionID
	tr.Record("get collection id & partition id from cache")

	stream, err := it.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}
	tr.Record("get used message stream")

	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Error("get vChannels failed", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}

	log.Info("send insert request to virtual channels",
		zap.String("collection", it.GetCollectionName()),
		zap.String("partition", it.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Int64("partition_id", partitionID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()))

	// assign segmentID for insert data and repack data by segmentID
	msgPack, err := it.assignSegmentID(channelNames)
	if err != nil {
		log.Error("assign segmentID and repack insert data failed", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	log.Debug("assign segmentID for insert data success", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID), zap.String("collection name", it.CollectionName))
	tr.Record("assign segment id")
	err = stream.Produce(msgPack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	sendMsgDur := tr.Record("send insert request to dml channel")
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.InsertLabel).Observe(float64(sendMsgDur.Milliseconds()))

	log.Debug("Proxy Insert Execute done", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName))

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}

type createCollectionTask struct {
	Condition
	*milvuspb.CreateCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	schema    *schemapb.CollectionSchema
}

func (cct *createCollectionTask) TraceCtx() context.Context {
	return cct.ctx
}

func (cct *createCollectionTask) ID() UniqueID {
	return cct.Base.MsgID
}

func (cct *createCollectionTask) SetID(uid UniqueID) {
	cct.Base.MsgID = uid
}

func (cct *createCollectionTask) Name() string {
	return CreateCollectionTaskName
}

func (cct *createCollectionTask) Type() commonpb.MsgType {
	return cct.Base.MsgType
}

func (cct *createCollectionTask) BeginTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) EndTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) SetTs(ts Timestamp) {
	cct.Base.Timestamp = ts
}

func (cct *createCollectionTask) OnEnqueue() error {
	cct.Base = &commonpb.MsgBase{}
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (cct *createCollectionTask) PreExecute(ctx context.Context) error {
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	cct.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(cct.Schema, cct.schema)
	if err != nil {
		return err
	}
	cct.schema.AutoID = false

	if cct.ShardsNum > Params.ProxyCfg.MaxShardNum {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum)
	}

	if int64(len(cct.schema.Fields)) > Params.ProxyCfg.MaxFieldNum {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum)
	}

	// validate collection name
	if err := validateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	// validate whether field names duplicates
	if err := validateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	// validate primary key definition
	if err := validatePrimaryKey(cct.schema); err != nil {
		return err
	}

	// validate auto id definition
	if err := ValidateFieldAutoID(cct.schema); err != nil {
		return err
	}

	// validate field type definition
	if err := validateFieldType(cct.schema); err != nil {
		return err
	}

	for _, field := range cct.schema.Fields {
		// validate field name
		if err := validateFieldName(field.Name); err != nil {
			return err
		}
		// validate vector field type parameters
		if field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector {
			err = validateDimension(field)
			if err != nil {
				return err
			}
		}
		// valid max length per row parameters
		// if max_length not specified, return error
		if field.DataType == schemapb.DataType_VarChar {
			err = validateMaxLengthPerRow(cct.schema.Name, field)
			if err != nil {
				return err
			}
		}
	}

	if err := validateMultipleVectorFields(cct.schema); err != nil {
		return err
	}

	cct.CreateCollectionRequest.Schema, err = proto.Marshal(cct.schema)
	if err != nil {
		return err
	}

	return nil
}

func (cct *createCollectionTask) Execute(ctx context.Context) error {
	var err error
	cct.result, err = cct.rootCoord.CreateCollection(ctx, cct.CreateCollectionRequest)
	return err
}

func (cct *createCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
}

func (dct *dropCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *dropCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *dropCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *dropCollectionTask) Name() string {
	return DropCollectionTaskName
}

func (dct *dropCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *dropCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *dropCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *dropCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DropCollection
	dct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *dropCollectionTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, dct.CollectionName)
	if err != nil {
		return err
	}

	dct.result, err = dct.rootCoord.DropCollection(ctx, dct.DropCollectionRequest)
	if err != nil {
		return err
	}

	_ = dct.chMgr.removeDMLStream(collID)
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

func (dct *dropCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

// Support wildcard in output fields:
//   "*" - all scalar fields
//   "%" - all vector fields
// For example, A and B are scalar fields, C and D are vector fields, duplicated fields will automatically be removed.
//   output_fields=["*"] 	 ==> [A,B]
//   output_fields=["%"] 	 ==> [C,D]
//   output_fields=["*","%"] ==> [A,B,C,D]
//   output_fields=["*",A] 	 ==> [A,B]
//   output_fields=["*",C]   ==> [A,B,C]
func translateOutputFields(outputFields []string, schema *schemapb.CollectionSchema, addPrimary bool) ([]string, error) {
	var primaryFieldName string
	scalarFieldNameMap := make(map[string]bool)
	vectorFieldNameMap := make(map[string]bool)
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)

	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vectorFieldNameMap[field.Name] = true
		} else {
			scalarFieldNameMap[field.Name] = true
		}
	}

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == "*" {
			for fieldName := range scalarFieldNameMap {
				resultFieldNameMap[fieldName] = true
			}
		} else if outputFieldName == "%" {
			for fieldName := range vectorFieldNameMap {
				resultFieldNameMap[fieldName] = true
			}
		} else {
			resultFieldNameMap[outputFieldName] = true
		}
	}

	if addPrimary {
		resultFieldNameMap[primaryFieldName] = true
	}

	for fieldName := range resultFieldNameMap {
		resultFieldNames = append(resultFieldNames, fieldName)
	}
	return resultFieldNames, nil
}

type hasCollectionTask struct {
	Condition
	*milvuspb.HasCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hct *hasCollectionTask) TraceCtx() context.Context {
	return hct.ctx
}

func (hct *hasCollectionTask) ID() UniqueID {
	return hct.Base.MsgID
}

func (hct *hasCollectionTask) SetID(uid UniqueID) {
	hct.Base.MsgID = uid
}

func (hct *hasCollectionTask) Name() string {
	return HasCollectionTaskName
}

func (hct *hasCollectionTask) Type() commonpb.MsgType {
	return hct.Base.MsgType
}

func (hct *hasCollectionTask) BeginTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) EndTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) SetTs(ts Timestamp) {
	hct.Base.Timestamp = ts
}

func (hct *hasCollectionTask) OnEnqueue() error {
	hct.Base = &commonpb.MsgBase{}
	return nil
}

func (hct *hasCollectionTask) PreExecute(ctx context.Context) error {
	hct.Base.MsgType = commonpb.MsgType_HasCollection
	hct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(hct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (hct *hasCollectionTask) Execute(ctx context.Context) error {
	var err error
	hct.result, err = hct.rootCoord.HasCollection(ctx, hct.HasCollectionRequest)
	if err != nil {
		return err
	}
	if hct.result == nil {
		return errors.New("has collection resp is nil")
	}
	if hct.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hct.result.Status.Reason)
	}
	return nil
}

func (hct *hasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeCollectionTask struct {
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.DescribeCollectionResponse
}

func (dct *describeCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *describeCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *describeCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *describeCollectionTask) Name() string {
	return DescribeCollectionTaskName
}

func (dct *describeCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *describeCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *describeCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *describeCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DescribeCollection
	dct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if dct.CollectionID != 0 && len(dct.CollectionName) == 0 {
		return nil
	}

	return validateCollectionName(dct.CollectionName)
}

func (dct *describeCollectionTask) Execute(ctx context.Context) error {
	var err error
	dct.result = &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
	}

	result, err := dct.rootCoord.DescribeCollection(ctx, dct.DescribeCollectionRequest)

	if err != nil {
		return err
	}

	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		dct.result.Status = result.Status
	} else {
		dct.result.Schema.Name = result.Schema.Name
		dct.result.Schema.Description = result.Schema.Description
		dct.result.Schema.AutoID = result.Schema.AutoID
		dct.result.CollectionID = result.CollectionID
		dct.result.VirtualChannelNames = result.VirtualChannelNames
		dct.result.PhysicalChannelNames = result.PhysicalChannelNames
		dct.result.CreatedTimestamp = result.CreatedTimestamp
		dct.result.CreatedUtcTimestamp = result.CreatedUtcTimestamp
		dct.result.ShardsNum = result.ShardsNum
		dct.result.ConsistencyLevel = result.ConsistencyLevel
		dct.result.Aliases = result.Aliases
		for _, field := range result.Schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID {
				dct.result.Schema.Fields = append(dct.result.Schema.Fields, &schemapb.FieldSchema{
					FieldID:      field.FieldID,
					Name:         field.Name,
					IsPrimaryKey: field.IsPrimaryKey,
					AutoID:       field.AutoID,
					Description:  field.Description,
					DataType:     field.DataType,
					TypeParams:   field.TypeParams,
					IndexParams:  field.IndexParams,
				})
			}
		}
	}
	return nil
}

func (dct *describeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type getCollectionStatisticsTask struct {
	Condition
	*milvuspb.GetCollectionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.GetCollectionStatisticsResponse

	collectionID UniqueID
}

func (g *getCollectionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getCollectionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getCollectionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getCollectionStatisticsTask) Name() string {
	return GetCollectionStatisticsTaskName
}

func (g *getCollectionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getCollectionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getCollectionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *getCollectionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetCollectionStatistics
	g.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (g *getCollectionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	g.collectionID = collID
	req := &datapb.GetCollectionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetCollectionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
	}

	result, _ := g.dataCoord.GetCollectionStatistics(ctx, req)
	if result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetCollectionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *getCollectionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type getPartitionStatisticsTask struct {
	Condition
	*milvuspb.GetPartitionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.GetPartitionStatisticsResponse

	collectionID UniqueID
}

func (g *getPartitionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getPartitionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getPartitionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getPartitionStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *getPartitionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getPartitionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getPartitionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *getPartitionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (g *getPartitionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	g.collectionID = collID
	partitionID, err := globalMetaCache.GetPartitionID(ctx, g.CollectionName, g.PartitionName)
	if err != nil {
		return err
	}
	req := &datapb.GetPartitionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetPartitionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
		PartitionID:  partitionID,
	}

	result, _ := g.dataCoord.GetPartitionStatistics(ctx, req)
	if result == nil {
		return errors.New("get partition statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *getPartitionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type showCollectionsTask struct {
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowCollectionsResponse
}

func (sct *showCollectionsTask) TraceCtx() context.Context {
	return sct.ctx
}

func (sct *showCollectionsTask) ID() UniqueID {
	return sct.Base.MsgID
}

func (sct *showCollectionsTask) SetID(uid UniqueID) {
	sct.Base.MsgID = uid
}

func (sct *showCollectionsTask) Name() string {
	return ShowCollectionTaskName
}

func (sct *showCollectionsTask) Type() commonpb.MsgType {
	return sct.Base.MsgType
}

func (sct *showCollectionsTask) BeginTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) EndTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) SetTs(ts Timestamp) {
	sct.Base.Timestamp = ts
}

func (sct *showCollectionsTask) OnEnqueue() error {
	sct.Base = &commonpb.MsgBase{}
	return nil
}

func (sct *showCollectionsTask) PreExecute(ctx context.Context) error {
	sct.Base.MsgType = commonpb.MsgType_ShowCollections
	sct.Base.SourceID = Params.ProxyCfg.GetNodeID()
	if sct.GetType() == milvuspb.ShowType_InMemory {
		for _, collectionName := range sct.CollectionNames {
			if err := validateCollectionName(collectionName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sct *showCollectionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := sct.rootCoord.ShowCollections(ctx, sct.ShowCollectionsRequest)

	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show collections")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if sct.GetType() == milvuspb.ShowType_InMemory {
		IDs2Names := make(map[UniqueID]string)
		for offset, collectionName := range respFromRootCoord.CollectionNames {
			collectionID := respFromRootCoord.CollectionIds[offset]
			IDs2Names[collectionID] = collectionName
		}
		collectionIDs := make([]UniqueID, 0)
		for _, collectionName := range sct.CollectionNames {
			collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			collectionIDs = append(collectionIDs, collectionID)
			IDs2Names[collectionID] = collectionName
		}

		resp, err := sct.queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     sct.Base.MsgID,
				Timestamp: sct.Base.Timestamp,
				SourceID:  sct.Base.SourceID,
			},
			//DbID: sct.ShowCollectionsRequest.DbName,
			CollectionIDs: collectionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show collections")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			// update collectionID to collection name, and return new error info to sdk
			newErrorReason := resp.Status.Reason
			for _, collectionID := range collectionIDs {
				newErrorReason = ReplaceID2Name(newErrorReason, collectionID, IDs2Names[collectionID])
			}
			return errors.New(newErrorReason)
		}

		sct.result = &milvuspb.ShowCollectionsResponse{
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
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return errors.New("failed to show collections")
			}
			collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			sct.result.CollectionIds = append(sct.result.CollectionIds, id)
			sct.result.CollectionNames = append(sct.result.CollectionNames, collectionName)
			sct.result.CreatedTimestamps = append(sct.result.CreatedTimestamps, collectionInfo.createdTimestamp)
			sct.result.CreatedUtcTimestamps = append(sct.result.CreatedUtcTimestamps, collectionInfo.createdUtcTimestamp)
			sct.result.InMemoryPercentages = append(sct.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
			sct.result.QueryServiceAvailable = append(sct.result.QueryServiceAvailable, resp.QueryServiceAvailable[offset])
		}
	} else {
		sct.result = respFromRootCoord
	}

	return nil
}

func (sct *showCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type createPartitionTask struct {
	Condition
	*milvuspb.CreatePartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (cpt *createPartitionTask) TraceCtx() context.Context {
	return cpt.ctx
}

func (cpt *createPartitionTask) ID() UniqueID {
	return cpt.Base.MsgID
}

func (cpt *createPartitionTask) SetID(uid UniqueID) {
	cpt.Base.MsgID = uid
}

func (cpt *createPartitionTask) Name() string {
	return CreatePartitionTaskName
}

func (cpt *createPartitionTask) Type() commonpb.MsgType {
	return cpt.Base.MsgType
}

func (cpt *createPartitionTask) BeginTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) EndTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) SetTs(ts Timestamp) {
	cpt.Base.Timestamp = ts
}

func (cpt *createPartitionTask) OnEnqueue() error {
	cpt.Base = &commonpb.MsgBase{}
	return nil
}

func (cpt *createPartitionTask) PreExecute(ctx context.Context) error {
	cpt.Base.MsgType = commonpb.MsgType_CreatePartition
	cpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *createPartitionTask) Execute(ctx context.Context) (err error) {
	cpt.result, err = cpt.rootCoord.CreatePartition(ctx, cpt.CreatePartitionRequest)
	if cpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cpt.result.Reason)
	}
	return err
}

func (cpt *createPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropPartitionTask struct {
	Condition
	*milvuspb.DropPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (dpt *dropPartitionTask) TraceCtx() context.Context {
	return dpt.ctx
}

func (dpt *dropPartitionTask) ID() UniqueID {
	return dpt.Base.MsgID
}

func (dpt *dropPartitionTask) SetID(uid UniqueID) {
	dpt.Base.MsgID = uid
}

func (dpt *dropPartitionTask) Name() string {
	return DropPartitionTaskName
}

func (dpt *dropPartitionTask) Type() commonpb.MsgType {
	return dpt.Base.MsgType
}

func (dpt *dropPartitionTask) BeginTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) EndTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) SetTs(ts Timestamp) {
	dpt.Base.Timestamp = ts
}

func (dpt *dropPartitionTask) OnEnqueue() error {
	dpt.Base = &commonpb.MsgBase{}
	return nil
}

func (dpt *dropPartitionTask) PreExecute(ctx context.Context) error {
	dpt.Base.MsgType = commonpb.MsgType_DropPartition
	dpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (dpt *dropPartitionTask) Execute(ctx context.Context) (err error) {
	dpt.result, err = dpt.rootCoord.DropPartition(ctx, dpt.DropPartitionRequest)
	if dpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dpt.result.Reason)
	}
	return err
}

func (dpt *dropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasPartitionTask struct {
	Condition
	*milvuspb.HasPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hpt *hasPartitionTask) TraceCtx() context.Context {
	return hpt.ctx
}

func (hpt *hasPartitionTask) ID() UniqueID {
	return hpt.Base.MsgID
}

func (hpt *hasPartitionTask) SetID(uid UniqueID) {
	hpt.Base.MsgID = uid
}

func (hpt *hasPartitionTask) Name() string {
	return HasPartitionTaskName
}

func (hpt *hasPartitionTask) Type() commonpb.MsgType {
	return hpt.Base.MsgType
}

func (hpt *hasPartitionTask) BeginTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) EndTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) SetTs(ts Timestamp) {
	hpt.Base.Timestamp = ts
}

func (hpt *hasPartitionTask) OnEnqueue() error {
	hpt.Base = &commonpb.MsgBase{}
	return nil
}

func (hpt *hasPartitionTask) PreExecute(ctx context.Context) error {
	hpt.Base.MsgType = commonpb.MsgType_HasPartition
	hpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := hpt.CollectionName, hpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (hpt *hasPartitionTask) Execute(ctx context.Context) (err error) {
	hpt.result, err = hpt.rootCoord.HasPartition(ctx, hpt.HasPartitionRequest)
	if hpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if hpt.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hpt.result.Status.Reason)
	}
	return err
}

func (hpt *hasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showPartitionsTask struct {
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowPartitionsResponse
}

func (spt *showPartitionsTask) TraceCtx() context.Context {
	return spt.ctx
}

func (spt *showPartitionsTask) ID() UniqueID {
	return spt.Base.MsgID
}

func (spt *showPartitionsTask) SetID(uid UniqueID) {
	spt.Base.MsgID = uid
}

func (spt *showPartitionsTask) Name() string {
	return ShowPartitionTaskName
}

func (spt *showPartitionsTask) Type() commonpb.MsgType {
	return spt.Base.MsgType
}

func (spt *showPartitionsTask) BeginTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) EndTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) SetTs(ts Timestamp) {
	spt.Base.Timestamp = ts
}

func (spt *showPartitionsTask) OnEnqueue() error {
	spt.Base = &commonpb.MsgBase{}
	return nil
}

func (spt *showPartitionsTask) PreExecute(ctx context.Context) error {
	spt.Base.MsgType = commonpb.MsgType_ShowPartitions
	spt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(spt.CollectionName); err != nil {
		return err
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		for _, partitionName := range spt.PartitionNames {
			if err := validatePartitionTag(partitionName, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (spt *showPartitionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := spt.rootCoord.ShowPartitions(ctx, spt.ShowPartitionsRequest)
	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show partitions")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		collectionName := spt.CollectionName
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		if err != nil {
			log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
				zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
			return err
		}
		IDs2Names := make(map[UniqueID]string)
		for offset, partitionName := range respFromRootCoord.PartitionNames {
			partitionID := respFromRootCoord.PartitionIDs[offset]
			IDs2Names[partitionID] = partitionName
		}
		partitionIDs := make([]UniqueID, 0)
		for _, partitionName := range spt.PartitionNames {
			partitionID, err := globalMetaCache.GetPartitionID(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			partitionIDs = append(partitionIDs, partitionID)
			IDs2Names[partitionID] = partitionName
		}
		resp, err := spt.queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     spt.Base.MsgID,
				Timestamp: spt.Base.Timestamp,
				SourceID:  spt.Base.SourceID,
			},
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show partitions")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		spt.result = &milvuspb.ShowPartitionsResponse{
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
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return errors.New("failed to show partitions")
			}
			partitionInfo, err := globalMetaCache.GetPartitionInfo(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			spt.result.PartitionIDs = append(spt.result.PartitionIDs, id)
			spt.result.PartitionNames = append(spt.result.PartitionNames, partitionName)
			spt.result.CreatedTimestamps = append(spt.result.CreatedTimestamps, partitionInfo.createdTimestamp)
			spt.result.CreatedUtcTimestamps = append(spt.result.CreatedUtcTimestamps, partitionInfo.createdUtcTimestamp)
			spt.result.InMemoryPercentages = append(spt.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
		}
	} else {
		spt.result = respFromRootCoord
	}

	return nil
}

func (spt *showPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type createIndexTask struct {
	Condition
	*milvuspb.CreateIndexRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status

	collectionID UniqueID
}

func (cit *createIndexTask) TraceCtx() context.Context {
	return cit.ctx
}

func (cit *createIndexTask) ID() UniqueID {
	return cit.Base.MsgID
}

func (cit *createIndexTask) SetID(uid UniqueID) {
	cit.Base.MsgID = uid
}

func (cit *createIndexTask) Name() string {
	return CreateIndexTaskName
}

func (cit *createIndexTask) Type() commonpb.MsgType {
	return cit.Base.MsgType
}

func (cit *createIndexTask) BeginTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *createIndexTask) EndTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *createIndexTask) SetTs(ts Timestamp) {
	cit.Base.Timestamp = ts
}

func (cit *createIndexTask) OnEnqueue() error {
	cit.Base = &commonpb.MsgBase{}
	return nil
}

func parseIndexParams(m []*commonpb.KeyValuePair) (map[string]string, error) {
	indexParams := make(map[string]string)
	for _, kv := range m {
		if kv.Key == "params" { // TODO(dragondriver): change `params` to const variable
			params, err := funcutil.ParseIndexParamsMap(kv.Value)
			if err != nil {
				return nil, err
			}
			for k, v := range params {
				indexParams[k] = v
			}
		} else {
			indexParams[kv.Key] = kv.Value
		}
	}
	_, exist := indexParams["index_type"] // TODO(dragondriver): change `index_type` to const variable
	if !exist {
		indexParams["index_type"] = indexparamcheck.IndexFaissIvfPQ // IVF_PQ is the default index type
	}
	return indexParams, nil
}

func (cit *createIndexTask) getIndexedField(ctx context.Context) (*schemapb.FieldSchema, error) {
	schema, err := globalMetaCache.GetCollectionSchema(ctx, cit.GetCollectionName())
	if err != nil {
		log.Error("failed to get collection schema", zap.Error(err))
		return nil, fmt.Errorf("failed to get collection schema: %s", err)
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		log.Error("failed to parse collection schema", zap.Error(err))
		return nil, fmt.Errorf("failed to parse collection schema: %s", err)
	}
	field, err := schemaHelper.GetFieldFromName(cit.GetFieldName())
	if err != nil {
		log.Error("create index on non-exist field", zap.Error(err))
		return nil, fmt.Errorf("cannot create index on non-exist field: %s", cit.GetFieldName())
	}
	return field, nil
}

func fillDimension(field *schemapb.FieldSchema, indexParams map[string]string) error {
	vecDataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
	}
	if !funcutil.SliceContain(vecDataTypes, field.GetDataType()) {
		return nil
	}
	params := make([]*commonpb.KeyValuePair, 0, len(field.GetTypeParams())+len(field.GetIndexParams()))
	params = append(params, field.GetTypeParams()...)
	params = append(params, field.GetIndexParams()...)
	dimensionInSchema, err := funcutil.GetAttrByKeyFromRepeatedKV("dim", params)
	if err != nil {
		return fmt.Errorf("dimension not found in schema")
	}
	dimension, exist := indexParams["dim"]
	if exist {
		if dimensionInSchema != dimension {
			return fmt.Errorf("dimension mismatch, dimension in schema: %s, dimension: %s", dimensionInSchema, dimension)
		}
	} else {
		indexParams["dim"] = dimensionInSchema
	}
	return nil
}

func checkTrain(field *schemapb.FieldSchema, indexParams map[string]string) error {
	indexType := indexParams["index_type"]

	// skip params check of non-vector field.
	vecDataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
	}
	if !funcutil.SliceContain(vecDataTypes, field.GetDataType()) {
		return indexparamcheck.CheckIndexValid(field.GetDataType(), indexType, indexParams)
	}

	adapter, err := indexparamcheck.GetConfAdapterMgrInstance().GetAdapter(indexType)
	if err != nil {
		log.Warn("Failed to get conf adapter", zap.String("index_type", indexType))
		return fmt.Errorf("invalid index type: %s", indexType)
	}

	if err := fillDimension(field, indexParams); err != nil {
		return err
	}

	ok := adapter.CheckTrain(indexParams)
	if !ok {
		log.Warn("Create index with invalid params", zap.Any("index_params", indexParams))
		return fmt.Errorf("invalid index params: %v", indexParams)
	}

	return nil
}

func (cit *createIndexTask) PreExecute(ctx context.Context) error {
	cit.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := cit.CollectionName

	collID, err := globalMetaCache.GetCollectionID(ctx, collName)
	if err != nil {
		return err
	}
	cit.collectionID = collID

	field, err := cit.getIndexedField(ctx)
	if err != nil {
		return err
	}

	// check index param, not accurate, only some static rules
	indexParams, err := parseIndexParams(cit.GetExtraParams())
	if err != nil {
		log.Error("failed to parse index params", zap.Error(err))
		return fmt.Errorf("failed to parse index params: %s", err)
	}

	return checkTrain(field, indexParams)
}

func (cit *createIndexTask) Execute(ctx context.Context) error {
	var err error
	cit.result, err = cit.rootCoord.CreateIndex(ctx, cit.CreateIndexRequest)
	if cit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cit.result.Reason)
	}
	return err
}

func (cit *createIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeIndexTask struct {
	Condition
	*milvuspb.DescribeIndexRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.DescribeIndexResponse

	collectionID UniqueID
}

func (dit *describeIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *describeIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *describeIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *describeIndexTask) Name() string {
	return DescribeIndexTaskName
}

func (dit *describeIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *describeIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *describeIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *describeIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *describeIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *describeIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DescribeIndex
	dit.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(dit.CollectionName); err != nil {
		return err
	}

	collID, _ := globalMetaCache.GetCollectionID(ctx, dit.CollectionName)
	dit.collectionID = collID
	return nil
}

func (dit *describeIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.rootCoord.DescribeIndex(ctx, dit.DescribeIndexRequest)
	if dit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dit.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Status.Reason)
	}
	return err
}

func (dit *describeIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropIndexTask struct {
	Condition
	ctx context.Context
	*milvuspb.DropIndexRequest
	rootCoord types.RootCoord
	result    *commonpb.Status

	collectionID UniqueID
}

func (dit *dropIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *dropIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *dropIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *dropIndexTask) Name() string {
	return DropIndexTaskName
}

func (dit *dropIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *dropIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *dropIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *dropIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *dropIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *dropIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DropIndex
	dit.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, fieldName := dit.CollectionName, dit.FieldName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validateFieldName(fieldName); err != nil {
		return err
	}

	if dit.IndexName == "" {
		dit.IndexName = Params.CommonCfg.DefaultIndexName
	}

	collID, _ := globalMetaCache.GetCollectionID(ctx, dit.CollectionName)
	dit.collectionID = collID

	return nil
}

func (dit *dropIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.rootCoord.DropIndex(ctx, dit.DropIndexRequest)
	if dit.result == nil {
		return errors.New("drop index resp is nil")
	}
	if dit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Reason)
	}
	return err
}

func (dit *dropIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type getIndexBuildProgressTask struct {
	Condition
	*milvuspb.GetIndexBuildProgressRequest
	ctx        context.Context
	indexCoord types.IndexCoord
	rootCoord  types.RootCoord
	dataCoord  types.DataCoord
	result     *milvuspb.GetIndexBuildProgressResponse

	collectionID UniqueID
}

func (gibpt *getIndexBuildProgressTask) TraceCtx() context.Context {
	return gibpt.ctx
}

func (gibpt *getIndexBuildProgressTask) ID() UniqueID {
	return gibpt.Base.MsgID
}

func (gibpt *getIndexBuildProgressTask) SetID(uid UniqueID) {
	gibpt.Base.MsgID = uid
}

func (gibpt *getIndexBuildProgressTask) Name() string {
	return GetIndexBuildProgressTaskName
}

func (gibpt *getIndexBuildProgressTask) Type() commonpb.MsgType {
	return gibpt.Base.MsgType
}

func (gibpt *getIndexBuildProgressTask) BeginTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *getIndexBuildProgressTask) EndTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *getIndexBuildProgressTask) SetTs(ts Timestamp) {
	gibpt.Base.Timestamp = ts
}

func (gibpt *getIndexBuildProgressTask) OnEnqueue() error {
	gibpt.Base = &commonpb.MsgBase{}
	return nil
}

func (gibpt *getIndexBuildProgressTask) PreExecute(ctx context.Context) error {
	gibpt.Base.MsgType = commonpb.MsgType_GetIndexBuildProgress
	gibpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(gibpt.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) Execute(ctx context.Context) error {
	collectionName := gibpt.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	gibpt.collectionID = collectionID

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		DbName:         gibpt.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	partitions, err := gibpt.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return err
	}

	if gibpt.IndexName == "" {
		gibpt.IndexName = Params.CommonCfg.DefaultIndexName
	}

	describeIndexReq := &milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		DbName:         gibpt.DbName,
		CollectionName: gibpt.CollectionName,
		//		IndexName:      gibpt.IndexName,
	}

	indexDescriptionResp, err2 := gibpt.rootCoord.DescribeIndex(ctx, describeIndexReq)
	if err2 != nil {
		return err2
	}

	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gibpt.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return fmt.Errorf("no index is created")
	}

	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gibpt.rootCoord.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	buildIndexMap := make(map[int64]int64)
	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := gibpt.rootCoord.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			return err
		}
		if segmentDesc.IndexID == matchIndexID {
			if segmentDesc.EnableIndex {
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, segmentDesc.BuildID)
				buildIndexMap[segmentID] = segmentDesc.BuildID
			}
		}
	}

	states, err := gibpt.indexCoord.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
			Status: states.Status,
		}
	}

	buildFinishMap := make(map[int64]bool)
	for _, state := range states.States {
		if state.State == commonpb.IndexState_Finished {
			buildFinishMap[state.IndexBuildID] = true
		}
	}

	infoResp, err := gibpt.dataCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		SegmentIDs: allSegmentIDs,
	})
	if err != nil {
		return err
	}

	total := int64(0)
	indexed := int64(0)

	for _, info := range infoResp.Infos {
		total += info.NumOfRows
		if buildFinishMap[buildIndexMap[info.ID]] {
			indexed += info.NumOfRows
		}
	}

	gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		TotalRows:   total,
		IndexedRows: indexed,
	}

	return nil
}

func (gibpt *getIndexBuildProgressTask) PostExecute(ctx context.Context) error {
	return nil
}

type getIndexStateTask struct {
	Condition
	*milvuspb.GetIndexStateRequest
	ctx        context.Context
	indexCoord types.IndexCoord
	rootCoord  types.RootCoord
	result     *milvuspb.GetIndexStateResponse

	collectionID UniqueID
}

func (gist *getIndexStateTask) TraceCtx() context.Context {
	return gist.ctx
}

func (gist *getIndexStateTask) ID() UniqueID {
	return gist.Base.MsgID
}

func (gist *getIndexStateTask) SetID(uid UniqueID) {
	gist.Base.MsgID = uid
}

func (gist *getIndexStateTask) Name() string {
	return GetIndexStateTaskName
}

func (gist *getIndexStateTask) Type() commonpb.MsgType {
	return gist.Base.MsgType
}

func (gist *getIndexStateTask) BeginTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *getIndexStateTask) EndTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *getIndexStateTask) SetTs(ts Timestamp) {
	gist.Base.Timestamp = ts
}

func (gist *getIndexStateTask) OnEnqueue() error {
	gist.Base = &commonpb.MsgBase{}
	return nil
}

func (gist *getIndexStateTask) PreExecute(ctx context.Context) error {
	gist.Base.MsgType = commonpb.MsgType_GetIndexState
	gist.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(gist.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gist *getIndexStateTask) getPartitions(ctx context.Context, collectionName string, collectionID UniqueID) (*milvuspb.ShowPartitionsResponse, error) {
	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		DbName:         gist.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	ret, err := gist.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return nil, err
	}
	if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, errors.New(ret.GetStatus().GetReason())
	}
	return ret, nil
}

func (gist *getIndexStateTask) describeIndex(ctx context.Context) (*milvuspb.DescribeIndexResponse, error) {
	describeIndexReq := milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		DbName:         gist.DbName,
		CollectionName: gist.CollectionName,
		IndexName:      gist.IndexName,
	}
	ret, err := gist.rootCoord.DescribeIndex(ctx, &describeIndexReq)
	if err != nil {
		return nil, err
	}
	if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, errors.New(ret.GetStatus().GetReason())
	}
	return ret, nil
}

func (gist *getIndexStateTask) getSegmentIDs(ctx context.Context, collectionID UniqueID, partitions *milvuspb.ShowPartitionsResponse) ([]UniqueID, error) {
	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gist.Base.MsgID,
				Timestamp: gist.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gist.rootCoord.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return nil, err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}
	return allSegmentIDs, nil
}

func (gist *getIndexStateTask) getIndexBuildIDs(ctx context.Context, collectionID UniqueID, allSegmentIDs []UniqueID, indexID UniqueID) ([]UniqueID, error) {
	indexBuildIDs := make([]UniqueID, 0)
	segmentsDesc, err := gist.rootCoord.DescribeSegments(ctx, &rootcoordpb.DescribeSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeSegments,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: collectionID,
		SegmentIDs:   allSegmentIDs,
	})
	if err != nil {
		return nil, err
	}

	if segmentsDesc.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, errors.New(segmentsDesc.GetStatus().GetReason())
	}
	for _, segmentDesc := range segmentsDesc.GetSegmentInfos() {
		for _, segmentIndexInfo := range segmentDesc.GetIndexInfos() {
			if segmentIndexInfo.IndexID == indexID && segmentIndexInfo.EnableIndex {
				indexBuildIDs = append(indexBuildIDs, segmentIndexInfo.GetBuildID())
			}
		}
	}
	return indexBuildIDs, nil
}

func (gist *getIndexStateTask) Execute(ctx context.Context) error {
	collectionName := gist.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	gist.collectionID = collectionID

	// Get partition result for the given collection.
	partitions, err := gist.getPartitions(ctx, collectionName, collectionID)
	if err != nil {
		return err
	}

	if gist.IndexName == "" {
		gist.IndexName = Params.CommonCfg.DefaultIndexName
	}

	// Retrieve index status and detailed index information.
	indexDescriptionResp, err := gist.describeIndex(ctx)
	if err != nil {
		return err
	}

	// Check if the target index name exists.
	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gist.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return fmt.Errorf("no index is created")
	}

	// Fetch segments for partitions.
	allSegmentIDs, err := gist.getSegmentIDs(ctx, collectionID, partitions)
	if err != nil {
		return err
	}

	// Get all index build ids.
	indexBuildIDs, err := gist.getIndexBuildIDs(ctx, collectionID, allSegmentIDs, matchIndexID)
	if err != nil {
		return err
	}
	log.Debug("get index state", zap.String("role", typeutil.ProxyRole), zap.Int64s("indexBuildIDs", indexBuildIDs))

	gist.result = &milvuspb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		State:      commonpb.IndexState_Finished,
		FailReason: "",
	}

	if len(indexBuildIDs) <= 0 {
		return nil
	}

	// Get index states.
	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: indexBuildIDs,
	}
	states, err := gist.indexCoord.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gist.result = &milvuspb.GetIndexStateResponse{
			Status: states.Status,
			State:  commonpb.IndexState_Failed,
		}
		return nil
	}

	for _, state := range states.States {
		if state.State != commonpb.IndexState_Finished {
			gist.result = &milvuspb.GetIndexStateResponse{
				Status:     states.Status,
				State:      state.State,
				FailReason: state.Reason,
			}
			return nil
		}
	}

	return nil
}

func (gist *getIndexStateTask) PostExecute(ctx context.Context) error {
	return nil
}

type flushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.FlushResponse
}

func (ft *flushTask) TraceCtx() context.Context {
	return ft.ctx
}

func (ft *flushTask) ID() UniqueID {
	return ft.Base.MsgID
}

func (ft *flushTask) SetID(uid UniqueID) {
	ft.Base.MsgID = uid
}

func (ft *flushTask) Name() string {
	return FlushTaskName
}

func (ft *flushTask) Type() commonpb.MsgType {
	return ft.Base.MsgType
}

func (ft *flushTask) BeginTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) EndTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) SetTs(ts Timestamp) {
	ft.Base.Timestamp = ts
}

func (ft *flushTask) OnEnqueue() error {
	ft.Base = &commonpb.MsgBase{}
	return nil
}

func (ft *flushTask) PreExecute(ctx context.Context) error {
	ft.Base.MsgType = commonpb.MsgType_Flush
	ft.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (ft *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	for _, collName := range ft.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     ft.Base.MsgID,
				Timestamp: ft.Base.Timestamp,
				SourceID:  ft.Base.SourceID,
			},
			DbID:         0,
			CollectionID: collID,
		}
		resp, err := ft.dataCoord.Flush(ctx, flushReq)
		if err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
	}
	ft.result = &milvuspb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:     "",
		CollSegIDs: coll2Segments,
	}
	return nil
}

func (ft *flushTask) PostExecute(ctx context.Context) error {
	return nil
}

type loadCollectionTask struct {
	Condition
	*milvuspb.LoadCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (lct *loadCollectionTask) TraceCtx() context.Context {
	return lct.ctx
}

func (lct *loadCollectionTask) ID() UniqueID {
	return lct.Base.MsgID
}

func (lct *loadCollectionTask) SetID(uid UniqueID) {
	lct.Base.MsgID = uid
}

func (lct *loadCollectionTask) Name() string {
	return LoadCollectionTaskName
}

func (lct *loadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) BeginTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) EndTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) SetTs(ts Timestamp) {
	lct.Base.Timestamp = ts
}

func (lct *loadCollectionTask) OnEnqueue() error {
	lct.Base = &commonpb.MsgBase{}
	return nil
}

func (lct *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PreExecute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	lct.Base.MsgType = commonpb.MsgType_LoadCollection
	lct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := lct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lct *loadCollectionTask) Execute(ctx context.Context) (err error) {
	log.Debug("loadCollectionTask Execute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)
	if err != nil {
		return err
	}
	lct.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lct.CollectionName)
	if err != nil {
		return err
	}

	request := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     lct.Base.MsgID,
			Timestamp: lct.Base.Timestamp,
			SourceID:  lct.Base.SourceID,
		},
		DbID:          0,
		CollectionID:  collID,
		Schema:        collSchema,
		ReplicaNumber: lct.ReplicaNumber,
	}
	log.Debug("send LoadCollectionRequest to query coordinator", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", request.Base.MsgID), zap.Int64("collectionID", request.CollectionID),
		zap.Any("schema", request.Schema))
	lct.result, err = lct.queryCoord.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query coordinator LoadCollection: %s", err)
	}
	return nil
}

func (lct *loadCollectionTask) PostExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PostExecute", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", lct.Base.MsgID))
	return nil
}

type releaseCollectionTask struct {
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
	chMgr      channelsMgr

	collectionID UniqueID
}

func (rct *releaseCollectionTask) TraceCtx() context.Context {
	return rct.ctx
}

func (rct *releaseCollectionTask) ID() UniqueID {
	return rct.Base.MsgID
}

func (rct *releaseCollectionTask) SetID(uid UniqueID) {
	rct.Base.MsgID = uid
}

func (rct *releaseCollectionTask) Name() string {
	return ReleaseCollectionTaskName
}

func (rct *releaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) BeginTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) EndTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) SetTs(ts Timestamp) {
	rct.Base.Timestamp = ts
}

func (rct *releaseCollectionTask) OnEnqueue() error {
	rct.Base = &commonpb.MsgBase{}
	return nil
}

func (rct *releaseCollectionTask) PreExecute(ctx context.Context) error {
	rct.Base.MsgType = commonpb.MsgType_ReleaseCollection
	rct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := rct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rct *releaseCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, rct.CollectionName)
	if err != nil {
		return err
	}
	rct.collectionID = collID
	request := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleaseCollection,
			MsgID:     rct.Base.MsgID,
			Timestamp: rct.Base.Timestamp,
			SourceID:  rct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
	}

	rct.result, err = rct.queryCoord.ReleaseCollection(ctx, request)

	globalMetaCache.RemoveCollection(ctx, rct.CollectionName)

	return err
}

func (rct *releaseCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.ClearShards(rct.CollectionName)
	return nil
}

type loadPartitionsTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (lpt *loadPartitionsTask) TraceCtx() context.Context {
	return lpt.ctx
}

func (lpt *loadPartitionsTask) ID() UniqueID {
	return lpt.Base.MsgID
}

func (lpt *loadPartitionsTask) SetID(uid UniqueID) {
	lpt.Base.MsgID = uid
}

func (lpt *loadPartitionsTask) Name() string {
	return LoadPartitionTaskName
}

func (lpt *loadPartitionsTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionsTask) BeginTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) EndTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) SetTs(ts Timestamp) {
	lpt.Base.Timestamp = ts
}

func (lpt *loadPartitionsTask) OnEnqueue() error {
	lpt.Base = &commonpb.MsgBase{}
	return nil
}

func (lpt *loadPartitionsTask) PreExecute(ctx context.Context) error {
	lpt.Base.MsgType = commonpb.MsgType_LoadPartitions
	lpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := lpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lpt *loadPartitionsTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	lpt.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range lpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, lpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadPartitions,
			MsgID:     lpt.Base.MsgID,
			Timestamp: lpt.Base.Timestamp,
			SourceID:  lpt.Base.SourceID,
		},
		DbID:          0,
		CollectionID:  collID,
		PartitionIDs:  partitionIDs,
		Schema:        collSchema,
		ReplicaNumber: lpt.ReplicaNumber,
	}
	lpt.result, err = lpt.queryCoord.LoadPartitions(ctx, request)
	return err
}

func (lpt *loadPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type releasePartitionsTask struct {
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (rpt *releasePartitionsTask) TraceCtx() context.Context {
	return rpt.ctx
}

func (rpt *releasePartitionsTask) ID() UniqueID {
	return rpt.Base.MsgID
}

func (rpt *releasePartitionsTask) SetID(uid UniqueID) {
	rpt.Base.MsgID = uid
}

func (rpt *releasePartitionsTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionsTask) Name() string {
	return ReleasePartitionTaskName
}

func (rpt *releasePartitionsTask) BeginTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) EndTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) SetTs(ts Timestamp) {
	rpt.Base.Timestamp = ts
}

func (rpt *releasePartitionsTask) OnEnqueue() error {
	rpt.Base = &commonpb.MsgBase{}
	return nil
}

func (rpt *releasePartitionsTask) PreExecute(ctx context.Context) error {
	rpt.Base.MsgType = commonpb.MsgType_ReleasePartitions
	rpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := rpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rpt *releasePartitionsTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, rpt.CollectionName)
	if err != nil {
		return err
	}
	rpt.collectionID = collID
	for _, partitionName := range rpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, rpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleasePartitions,
			MsgID:     rpt.Base.MsgID,
			Timestamp: rpt.Base.Timestamp,
			SourceID:  rpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	rpt.result, err = rpt.queryCoord.ReleasePartitions(ctx, request)
	return err
}

func (rpt *releasePartitionsTask) PostExecute(ctx context.Context) error {
	globalMetaCache.ClearShards(rpt.CollectionName)
	return nil
}

type BaseDeleteTask = msgstream.DeleteMsg

type deleteTask struct {
	Condition
	BaseDeleteTask
	ctx        context.Context
	deleteExpr string
	//req       *milvuspb.DeleteRequest
	result    *milvuspb.MutationResult
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
	vChannels []vChan
	pChannels []pChan

	collectionID UniqueID
	schema       *schemapb.CollectionSchema
}

func (dt *deleteTask) TraceCtx() context.Context {
	return dt.ctx
}

func (dt *deleteTask) ID() UniqueID {
	return dt.Base.MsgID
}

func (dt *deleteTask) SetID(uid UniqueID) {
	dt.Base.MsgID = uid
}

func (dt *deleteTask) Type() commonpb.MsgType {
	return dt.Base.MsgType
}

func (dt *deleteTask) Name() string {
	return deleteTaskName
}

func (dt *deleteTask) BeginTs() Timestamp {
	return dt.Base.Timestamp
}

func (dt *deleteTask) EndTs() Timestamp {
	return dt.Base.Timestamp
}

func (dt *deleteTask) SetTs(ts Timestamp) {
	dt.Base.Timestamp = ts
}

func (dt *deleteTask) OnEnqueue() error {
	dt.DeleteRequest.Base = &commonpb.MsgBase{}
	return nil
}

func (dt *deleteTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels, err := dt.getChannels()
	if err != nil {
		return ret, err
	}

	beginTs := dt.BeginTs()
	endTs := dt.EndTs()

	for _, channel := range channels {
		ret[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	return ret, nil
}

func (dt *deleteTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(dt.ctx, dt.CollectionName)
	if err != nil {
		return nil, err
	}
	return dt.chMgr.getChannels(collID)
}

func getPrimaryKeysFromExpr(schema *schemapb.CollectionSchema, expr string) (res *schemapb.IDs, rowNum int64, err error) {
	if len(expr) == 0 {
		log.Warn("empty expr")
		return
	}

	plan, err := createExprPlan(schema, expr)
	if err != nil {
		return res, 0, fmt.Errorf("failed to create expr plan, expr = %s", expr)
	}

	// delete request only support expr "id in [a, b]"
	termExpr, ok := plan.Node.(*planpb.PlanNode_Predicates).Predicates.Expr.(*planpb.Expr_TermExpr)
	if !ok {
		return res, 0, fmt.Errorf("invalid plan node type, only pk in [1, 2] supported")
	}

	res = &schemapb.IDs{}
	rowNum = int64(len(termExpr.TermExpr.Values))
	switch termExpr.TermExpr.ColumnInfo.GetDataType() {
	case schemapb.DataType_Int64:
		ids := make([]int64, 0)
		for _, v := range termExpr.TermExpr.Values {
			ids = append(ids, v.GetInt64Val())
		}
		res.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: ids,
			},
		}
	case schemapb.DataType_VarChar:
		ids := make([]string, 0)
		for _, v := range termExpr.TermExpr.Values {
			ids = append(ids, v.GetStringVal())
		}
		res.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: ids,
			},
		}
	default:
		return res, 0, fmt.Errorf("invalid field data type specifyed in delete expr")
	}

	return res, rowNum, nil
}

func (dt *deleteTask) PreExecute(ctx context.Context) error {
	dt.Base.MsgType = commonpb.MsgType_Delete
	dt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	dt.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: dt.BeginTs(),
	}

	collName := dt.CollectionName
	if err := validateCollectionName(collName); err != nil {
		log.Error("Invalid collection name", zap.String("collectionName", collName))
		return err
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, collName)
	if err != nil {
		log.Debug("Failed to get collection id", zap.String("collectionName", collName))
		return err
	}
	dt.DeleteRequest.CollectionID = collID
	dt.collectionID = collID

	// If partitionName is not empty, partitionID will be set.
	if len(dt.PartitionName) > 0 {
		partName := dt.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Error("Invalid partition name", zap.String("partitionName", partName))
			return err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, collName, partName)
		if err != nil {
			log.Debug("Failed to get partition id", zap.String("collectionName", collName), zap.String("partitionName", partName))
			return err
		}
		dt.DeleteRequest.PartitionID = partID
	} else {
		dt.DeleteRequest.PartitionID = common.InvalidPartitionID
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, collName)
	if err != nil {
		log.Error("Failed to get collection schema", zap.String("collectionName", collName))
		return err
	}
	dt.schema = schema

	// get delete.primaryKeys from delete expr
	primaryKeys, numRow, err := getPrimaryKeysFromExpr(schema, dt.deleteExpr)
	if err != nil {
		log.Error("Failed to get primary keys from expr", zap.Error(err))
		return err
	}

	dt.DeleteRequest.NumRows = numRow
	dt.DeleteRequest.PrimaryKeys = primaryKeys
	log.Debug("get primary keys from expr", zap.Int64("len of primary keys", dt.DeleteRequest.NumRows))

	// set result
	dt.result.IDs = primaryKeys
	dt.result.DeleteCnt = dt.DeleteRequest.NumRows

	dt.Timestamps = make([]uint64, numRow)
	for index := range dt.Timestamps {
		dt.Timestamps[index] = dt.BeginTs()
	}

	return nil
}

func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	sp, ctx := trace.StartSpanFromContextWithOperationName(dt.ctx, "Proxy-Delete-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))

	collID := dt.DeleteRequest.CollectionID
	stream, err := dt.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}

	// hash primary keys to channels
	channelNames, err := dt.chMgr.getVChannels(collID)
	if err != nil {
		log.Error("get vChannels failed", zap.Int64("collectionID", collID), zap.Error(err))
		dt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		dt.result.Status.Reason = err.Error()
		return err
	}
	dt.HashValues = typeutil.HashPK2Channels(dt.result.IDs, channelNames)

	log.Info("send delete request to virtual channels",
		zap.String("collection", dt.GetCollectionName()),
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", dt.ID()))

	tr.Record("get vchannels")
	// repack delete msg by dmChannel
	result := make(map[uint32]msgstream.TsMsg)
	collectionName := dt.CollectionName
	collectionID := dt.CollectionID
	partitionID := dt.PartitionID
	partitionName := dt.PartitionName
	proxyID := dt.Base.SourceID
	for index, key := range dt.HashValues {
		ts := dt.Timestamps[index]
		_, ok := result[key]
		if !ok {
			sliceRequest := internalpb.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Delete,
					MsgID:     dt.Base.MsgID,
					Timestamp: ts,
					SourceID:  proxyID,
				},
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
		curMsg.HashValues = append(curMsg.HashValues, dt.HashValues[index])
		curMsg.Timestamps = append(curMsg.Timestamps, dt.Timestamps[index])
		typeutil.AppendIDs(curMsg.PrimaryKeys, dt.PrimaryKeys, index)
		curMsg.NumRows++
	}

	// send delete request to log broker
	msgPack := &msgstream.MsgPack{
		BeginTs: dt.BeginTs(),
		EndTs:   dt.EndTs(),
	}
	for _, msg := range result {
		if msg != nil {
			msgPack.Msgs = append(msgPack.Msgs, msg)
		}
	}

	tr.Record("pack messages")
	err = stream.Produce(msgPack)
	if err != nil {
		dt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		dt.result.Status.Reason = err.Error()
		return err
	}
	sendMsgDur := tr.Record("send delete request to dml channels")
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.DeleteLabel).Observe(float64(sendMsgDur.Milliseconds()))

	return nil
}

func (dt *deleteTask) PostExecute(ctx context.Context) error {
	return nil
}

// CreateAliasTask contains task information of CreateAlias
type CreateAliasTask struct {
	Condition
	*milvuspb.CreateAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

// TraceCtx returns the trace context of the task.
func (c *CreateAliasTask) TraceCtx() context.Context {
	return c.ctx
}

// ID return the id of the task
func (c *CreateAliasTask) ID() UniqueID {
	return c.Base.MsgID
}

// SetID sets the id of the task
func (c *CreateAliasTask) SetID(uid UniqueID) {
	c.Base.MsgID = uid
}

// Name returns the name of the task
func (c *CreateAliasTask) Name() string {
	return CreateAliasTaskName
}

// Type returns the type of the task
func (c *CreateAliasTask) Type() commonpb.MsgType {
	return c.Base.MsgType
}

// BeginTs returns the ts
func (c *CreateAliasTask) BeginTs() Timestamp {
	return c.Base.Timestamp
}

// EndTs returns the ts
func (c *CreateAliasTask) EndTs() Timestamp {
	return c.Base.Timestamp
}

// SetTs sets the ts
func (c *CreateAliasTask) SetTs(ts Timestamp) {
	c.Base.Timestamp = ts
}

// OnEnqueue defines the behavior task enqueued
func (c *CreateAliasTask) OnEnqueue() error {
	c.Base = &commonpb.MsgBase{}
	return nil
}

// PreExecute defines the action before task execution
func (c *CreateAliasTask) PreExecute(ctx context.Context) error {
	c.Base.MsgType = commonpb.MsgType_CreateAlias
	c.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collAlias := c.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := c.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}
	return nil
}

// Execute defines the actual execution of create alias
func (c *CreateAliasTask) Execute(ctx context.Context) error {
	var err error
	c.result, err = c.rootCoord.CreateAlias(ctx, c.CreateAliasRequest)
	return err
}

// PostExecute defines the post execution, do nothing for create alias
func (c *CreateAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DropAliasTask is the task to drop alias
type DropAliasTask struct {
	Condition
	*milvuspb.DropAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

// TraceCtx returns the context for trace
func (d *DropAliasTask) TraceCtx() context.Context {
	return d.ctx
}

// ID returns the MsgID
func (d *DropAliasTask) ID() UniqueID {
	return d.Base.MsgID
}

// SetID sets the MsgID
func (d *DropAliasTask) SetID(uid UniqueID) {
	d.Base.MsgID = uid
}

// Name returns the name of the task
func (d *DropAliasTask) Name() string {
	return DropAliasTaskName
}

func (d *DropAliasTask) Type() commonpb.MsgType {
	return d.Base.MsgType
}

func (d *DropAliasTask) BeginTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) EndTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) SetTs(ts Timestamp) {
	d.Base.Timestamp = ts
}

func (d *DropAliasTask) OnEnqueue() error {
	d.Base = &commonpb.MsgBase{}
	return nil
}

func (d *DropAliasTask) PreExecute(ctx context.Context) error {
	d.Base.MsgType = commonpb.MsgType_DropAlias
	d.Base.SourceID = Params.ProxyCfg.GetNodeID()
	collAlias := d.Alias
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}
	return nil
}

func (d *DropAliasTask) Execute(ctx context.Context) error {
	var err error
	d.result, err = d.rootCoord.DropAlias(ctx, d.DropAliasRequest)
	return err
}

func (d *DropAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// AlterAliasTask is the task to alter alias
type AlterAliasTask struct {
	Condition
	*milvuspb.AlterAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (a *AlterAliasTask) TraceCtx() context.Context {
	return a.ctx
}

func (a *AlterAliasTask) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterAliasTask) SetID(uid UniqueID) {
	a.Base.MsgID = uid
}

func (a *AlterAliasTask) Name() string {
	return AlterAliasTaskName
}

func (a *AlterAliasTask) Type() commonpb.MsgType {
	return a.Base.MsgType
}

func (a *AlterAliasTask) BeginTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) EndTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) SetTs(ts Timestamp) {
	a.Base.Timestamp = ts
}

func (a *AlterAliasTask) OnEnqueue() error {
	a.Base = &commonpb.MsgBase{}
	return nil
}

func (a *AlterAliasTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_AlterAlias
	a.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collAlias := a.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := a.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (a *AlterAliasTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.AlterAlias(ctx, a.AlterAliasRequest)
	return err
}

func (a *AlterAliasTask) PostExecute(ctx context.Context) error {
	return nil
}
