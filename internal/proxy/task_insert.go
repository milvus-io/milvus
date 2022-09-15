package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type insertTask struct {
	BaseInsertTask
	// req *milvuspb.InsertRequest
	Condition
	ctx context.Context

	result        *milvuspb.MutationResult
	idAllocator   *allocator.IDAllocator
	segIDAssigner *segIDAssigner
	chMgr         channelsMgr
	chTicker      channelsTimeTicker
	vChannels     []vChan
	pChannels     []pChan
	schema        *schemapb.CollectionSchema
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
	rowIDBegin, rowIDEnd, _ = it.idAllocator.Alloc(rowNums)
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

	// pre-alloc msg id by batch
	var idBegin, idEnd int64
	var err error

	// fetch next id, if not id available, fetch next batch
	// lazy fetch, get first batch after first getMsgID called
	getMsgID := func() (int64, error) {
		if idBegin == idEnd {
			err = retry.Do(it.ctx, func() error {
				idBegin, idEnd, err = it.idAllocator.Alloc(16)
				return err
			})
			if err != nil {
				log.Error("failed to allocate msg id", zap.Int64("base.MsgID", it.Base.MsgID), zap.Error(err))
				return 0, err
			}
		}
		result := idBegin
		idBegin++
		return result, nil
	}

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string, msgID int64) *msgstream.InsertMsg {
		insertReq := internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     msgID,
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
	getInsertMsgsBySegmentID := func(segmentID UniqueID, rowOffsets []int, channelName string, maxMessageSize int) ([]msgstream.TsMsg, error) {
		repackedMsgs := make([]msgstream.TsMsg, 0)
		requestSize := 0
		msgID, err := getMsgID()
		if err != nil {
			return nil, err
		}
		insertMsg := createInsertMsg(segmentID, channelName, msgID)
		for _, offset := range rowOffsets {
			curRowMessageSize, err := typeutil.EstimateEntitySize(it.InsertRequest.GetFieldsData(), offset)
			if err != nil {
				return nil, err
			}

			// if insertMsg's size is greater than the threshold, split into multiple insertMsgs
			if requestSize+curRowMessageSize >= maxMessageSize {
				repackedMsgs = append(repackedMsgs, insertMsg)
				msgID, err = getMsgID()
				if err != nil {
					return nil, err
				}
				insertMsg = createInsertMsg(segmentID, channelName, msgID)
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
