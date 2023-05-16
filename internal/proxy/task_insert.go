package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type insertTask struct {
	BaseInsertTask
	// req *milvuspb.InsertRequest
	Condition
	ctx context.Context

	result             *milvuspb.MutationResult
	idAllocator        *allocator.IDAllocator
	segIDAssigner      *segIDAssigner
	chMgr              channelsMgr
	chTicker           channelsTimeTicker
	vChannels          []vChan
	pChannels          []pChan
	schema             *schemapb.CollectionSchema
	isPartitionKeyMode bool
	partitionKeys      *schemapb.FieldData
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

func (it *insertTask) getChannels() ([]pChan, error) {
	if len(it.pChannels) != 0 {
		return it.pChannels, nil
	}
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.GetDbName(), it.CollectionName)
	if err != nil {
		return nil, err
	}
	channels, err := it.chMgr.getChannels(collID)
	if err != nil {
		return nil, err
	}
	it.pChannels = channels
	return channels, nil
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
			log.Info("get primary field data failed", zap.String("collection name", it.CollectionName), zap.Error(err))
			return err
		}
	} else {
		// check primary key data not exist
		if typeutil.IsPrimaryFieldDataExist(it.GetFieldsData(), primaryFieldSchema) {
			return fmt.Errorf("can not assign primary field data when auto id enabled %v", primaryFieldSchema.Name)
		}
		// if autoID == true, currently only support autoID for int64 PrimaryField
		primaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, it.RowIDs)
		if err != nil {
			log.Warn("generate primary field data failed when autoID == true", zap.String("collection name", it.CollectionName), zap.Error(err))
			return err
		}
		// if autoID == true, set the primary field data
		it.FieldsData = append(it.FieldsData, primaryFieldData)
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	it.result.IDs, err = parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.String("collection name", it.CollectionName), zap.Error(err))
		return err
	}

	return nil
}

func (it *insertTask) checkPartitionKeys(collSchema *schemapb.CollectionSchema) error {
	partitionKeyField, err := typeutil.GetPartitionFieldSchema(collSchema)
	if err == nil {
		it.isPartitionKeyMode = true
		if len(it.PartitionName) > 0 {
			return errors.New("not support manually specifying the partition names if partition key mode is used")
		}

		for _, fieldData := range it.GetFieldsData() {
			if fieldData.GetFieldId() == partitionKeyField.FieldID {
				it.partitionKeys = fieldData
			}
		}

		if it.partitionKeys == nil {
			return errors.New("partition key not specify when insert")
		}
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
		log.Info("valid collection name failed", zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, it.GetDbName(), collectionName)
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
	rowIDBegin, rowIDEnd, _ = it.idAllocator.Alloc(rowNums)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))

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
		log.Info("check primary field data and hash primary key failed", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldIDBySchema(it.GetFieldsData(), collSchema)
	if err != nil {
		log.Info("set fieldID to fieldData failed", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	err = it.checkPartitionKeys(it.schema)
	if err != nil {
		log.Info("check partition keys failed", zap.String("collection name", collectionName), zap.Error(err))
		return err
	}

	if !it.isPartitionKeyMode {
		// set default partition name if not use partition key
		// insert to _default partition
		if len(it.PartitionName) <= 0 {
			it.PartitionName = Params.CommonCfg.DefaultPartitionName
		}

		partitionTag := it.PartitionName
		if err := validatePartitionTag(partitionTag, true); err != nil {
			log.Info("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck()).Validate(it.GetFieldsData(), it.schema, it.NRows()); err != nil {
		return err
	}

	log.Debug("Proxy Insert PreExecute done", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName))

	return nil
}

func (it *insertTask) assignChannelsByPK(channelNames []string) map[string][]int {
	// generate hash value for every primary key
	if len(it.HashValues) != 0 {
		log.Warn("the hashvalues passed through client is not supported now, and will be overwritten")
	}
	it.HashValues = typeutil.HashPK2Channels(it.result.IDs, channelNames)
	// groupedHashKeys represents the dmChannel index
	channel2RowOffsets := make(map[string][]int) //   channelName to count

	// assert len(it.hashValues) < maxInt
	for offset, channelID := range it.HashValues {
		channelName := channelNames[channelID]
		if _, ok := channel2RowOffsets[channelName]; !ok {
			channel2RowOffsets[channelName] = []int{}
		}
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], offset)
	}

	return channel2RowOffsets
}

func (it *insertTask) assignPartitionsByKey(ctx context.Context, rowOffsets []int, keys *schemapb.FieldData) (map[string][]int, error) {
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, it.GetDbName(), it.CollectionName)
	if err != nil {
		return nil, err
	}

	partition2RowOffsets := make(map[string][]int) //   partitionNames to offset
	hashValues, err := typeutil.HashKey2PartitionsWithFilter(rowOffsets, keys, partitionNames)
	if err != nil {
		return nil, err
	}

	for idx, index := range hashValues {
		partitionName := partitionNames[index]
		if _, ok := partition2RowOffsets[partitionName]; !ok {
			partition2RowOffsets[partitionName] = []int{}
		}
		partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], rowOffsets[idx])
	}

	return partition2RowOffsets, nil
}

func (it *insertTask) getInsertMsgsByPartition(segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string) ([]msgstream.TsMsg, error) {
	threshold := Params.PulsarCfg.MaxMessageSize

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string) *msgstream.InsertMsg {
		insertReq := internalpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithTimeStamp(it.BeginTimestamp), // entity's timestamp was set to equal it.BeginTimestamp in preExecute()
				commonpbutil.WithSourceID(it.Base.SourceID),
			),
			CollectionID:   it.CollectionID,
			PartitionID:    partitionID,
			CollectionName: it.CollectionName,
			PartitionName:  partitionName,
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

	repackedMsgs := make([]msgstream.TsMsg, 0)
	requestSize := 0
	insertMsg := createInsertMsg(segmentID, channelName)
	for _, offset := range rowOffsets {
		curRowMessageSize, err := typeutil.EstimateEntitySize(it.InsertRequest.GetFieldsData(), offset)
		if err != nil {
			return nil, err
		}

		// if insertMsg's size is greater than the threshold, split into multiple insertMsgs
		if requestSize+curRowMessageSize >= threshold {
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

func (it *insertTask) repackInsertDataByPartition(ctx context.Context,
	partitionName string,
	rowOffsets []int,
	channelName string) ([]msgstream.TsMsg, error) {
	res := make([]msgstream.TsMsg, 0)

	maxTs := Timestamp(0)
	for _, offset := range rowOffsets {
		ts := it.Timestamps[offset]
		if maxTs < ts {
			maxTs = ts
		}
	}

	partitionID, err := globalMetaCache.GetPartitionID(ctx, it.GetDbName(), it.GetCollectionName(), partitionName)
	if err != nil {
		return nil, err
	}
	assignedSegmentInfos, err := it.segIDAssigner.GetSegmentID(it.CollectionID, partitionID, channelName, uint32(len(rowOffsets)), maxTs)
	if err != nil {
		log.Warn("allocate segmentID for insert data failed",
			zap.Int64("collectionID", it.CollectionID),
			zap.String("channel name", channelName),
			zap.Int("allocate count", len(rowOffsets)),
			zap.Error(err))
		return nil, err
	}

	startPos := 0
	for segmentID, count := range assignedSegmentInfos {
		subRowOffsets := rowOffsets[startPos : startPos+int(count)]
		insertMsgs, err := it.getInsertMsgsByPartition(segmentID, partitionID, partitionName, subRowOffsets, channelName)
		if err != nil {
			log.Warn("repack insert data to insert msgs failed",
				zap.Int64("collectionID", it.CollectionID),
				zap.Int64("partitionID", partitionID),
				zap.Error(err))
			return nil, err
		}
		res = append(res, insertMsgs...)
		startPos += int(count)
	}

	return res, nil
}

func (it *insertTask) repackInsertDataToMsgPack(ctx context.Context,
	channelNames []string) (*msgstream.MsgPack, error) {
	msgPack := &msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
	}

	var err error
	channel2RowOffsets := it.assignChannelsByPK(channelNames)
	for channel, rowOffsets := range channel2RowOffsets {
		partition2RowOffsets := make(map[string][]int)
		if it.isPartitionKeyMode {
			// hash partition keys to multi default physical partitions
			partition2RowOffsets, err = it.assignPartitionsByKey(ctx, rowOffsets, it.partitionKeys)
			if err != nil {
				return nil, err
			}
		} else {
			// 1. The insert request specifies the partition name
			// 2. partition name not specified in insert request, use DefaultPartitionName (init when PreExecute)
			partition2RowOffsets[it.PartitionName] = rowOffsets
		}

		errGroup, _ := errgroup.WithContext(ctx)
		partition2Msgs := sync.Map{}
		for partitionName, offsets := range partition2RowOffsets {
			partitionName := partitionName
			offsets := offsets
			errGroup.Go(func() error {
				insertMsgs, err := it.repackInsertDataByPartition(ctx, partitionName, offsets, channel)
				if err != nil {
					return err
				}

				partition2Msgs.Store(partitionName, insertMsgs)
				return nil
			})
		}

		err = errGroup.Wait()
		if err != nil {
			return nil, err
		}

		partition2Msgs.Range(func(k, v interface{}) bool {
			insertMsgs := v.([]msgstream.TsMsg)
			msgPack.Msgs = append(msgPack.Msgs, insertMsgs...)
			return true
		})
	}

	return msgPack, nil
}

func (it *insertTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(it.ctx, "Proxy-Insert-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert %d", it.ID()))

	collectionName := it.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, it.GetDbName(), collectionName)
	if err != nil {
		return err
	}
	it.CollectionID = collID
	getCacheDur := tr.RecordSpan()

	stream, err := it.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}
	getMsgStreamDur := tr.RecordSpan()

	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}

	log.Ctx(ctx).Debug("send insert request to virtual channels",
		zap.String("collection", it.GetCollectionName()),
		zap.String("partition", it.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()),
		zap.Duration("get cache duration", getCacheDur),
		zap.Duration("get msgStream duration", getMsgStreamDur))

	msgPack, err := it.repackInsertDataToMsgPack(ctx, channelNames)
	if err != nil {
		log.Warn("repack insert data to msgpack failed", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	err = setMsgID(ctx, msgPack.Msgs, it.idAllocator)
	if err != nil {
		log.Error("failed to allocate msg id", zap.Error(err))
		return err
	}

	assignSegmentIDDur := tr.RecordSpan()
	log.Debug("assign segmentID for insert data success", zap.Int64("msgID", it.Base.MsgID), zap.Int64("collectionID", collID),
		zap.String("collection name", it.CollectionName),
		zap.Duration("assign segmentID duration", assignSegmentIDDur))
	err = stream.Produce(msgPack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	sendMsgDur := tr.RecordSpan()
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.InsertLabel).Observe(float64(sendMsgDur.Milliseconds()))
	totalExecDur := tr.ElapseSpan()
	log.Debug("Proxy Insert Execute done", zap.Int64("msgID", it.Base.MsgID), zap.String("collection name", collectionName),
		zap.Duration("send message duration", sendMsgDur),
		zap.Duration("execute duration", totalExecDur))

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}
