package proxy

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type insertTask struct {
	baseTask
	// req *milvuspb.InsertRequest
	Condition
	insertMsg *BaseInsertTask
	ctx       context.Context

	result        *milvuspb.MutationResult
	idAllocator   *allocator.IDAllocator
	segIDAssigner *segIDAssigner
	chMgr         channelsMgr
	chTicker      channelsTimeTicker
	vChannels     []vChan
	pChannels     []pChan
	schema        *schemapb.CollectionSchema
	partitionKeys *schemapb.FieldData
}

// TraceCtx returns insertTask context
func (it *insertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *insertTask) ID() UniqueID {
	return it.insertMsg.Base.MsgID
}

func (it *insertTask) SetID(uid UniqueID) {
	it.insertMsg.Base.MsgID = uid
}

func (it *insertTask) Name() string {
	return InsertTaskName
}

func (it *insertTask) Type() commonpb.MsgType {
	return it.insertMsg.Base.MsgType
}

func (it *insertTask) BeginTs() Timestamp {
	return it.insertMsg.BeginTimestamp
}

func (it *insertTask) SetTs(ts Timestamp) {
	it.insertMsg.BeginTimestamp = ts
	it.insertMsg.EndTimestamp = ts
}

func (it *insertTask) EndTs() Timestamp {
	return it.insertMsg.EndTimestamp
}

func (it *insertTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), it.insertMsg.CollectionName)
	if err != nil {
		return err
	}
	channels, err := it.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	it.pChannels = channels
	return nil
}

func (it *insertTask) getChannels() []pChan {
	return it.pChannels
}

func (it *insertTask) OnEnqueue() error {
	if it.insertMsg.Base == nil {
		it.insertMsg.Base = commonpbutil.NewMsgBase()
	}
	it.insertMsg.Base.MsgType = commonpb.MsgType_Insert
	it.insertMsg.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (it *insertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-PreExecute")
	defer sp.End()

	it.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	collectionName := it.insertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Warn("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	maxInsertSize := Params.QuotaConfig.MaxInsertSize.GetAsInt()
	if maxInsertSize != -1 && it.insertMsg.Size() > maxInsertSize {
		log.Warn("insert request size exceeds maxInsertSize",
			zap.Int("request size", it.insertMsg.Size()), zap.Int("maxInsertSize", maxInsertSize))
		return merr.WrapErrAsInputError(merr.WrapErrParameterTooLarge("insert request size exceeds maxInsertSize"))
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get collection schema from global meta cache failed", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	it.schema = schema.CollectionSchema

	rowNums := uint32(it.insertMsg.NRows())
	// set insertTask.rowIDs
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	tr := timerecord.NewTimeRecorder("applyPK")
	rowIDBegin, rowIDEnd, _ = it.idAllocator.Alloc(rowNums)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))

	it.insertMsg.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.insertMsg.RowIDs[offset] = i
	}
	// set insertTask.timeStamps
	rowNum := it.insertMsg.NRows()
	it.insertMsg.Timestamps = make([]uint64, rowNum)
	for index := range it.insertMsg.Timestamps {
		it.insertMsg.Timestamps[index] = it.insertMsg.BeginTimestamp
	}

	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	if it.schema.EnableDynamicField {
		err = checkDynamicFieldData(it.schema, it.insertMsg)
		if err != nil {
			return err
		}
	}

	// check primaryFieldData whether autoID is true or not
	// set rowIDs as primary data if autoID == true
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	it.result.IDs, err = checkPrimaryFieldData(it.schema, it.insertMsg)
	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName))
	if err != nil {
		log.Warn("check primary field data and hash primary key failed",
			zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldIDBySchema(it.insertMsg.GetFieldsData(), schema.CollectionSchema)
	if err != nil {
		log.Info("set fieldID to fieldData failed",
			zap.Error(err))
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}
	if partitionKeyMode {
		fieldSchema, _ := typeutil.GetPartitionKeyFieldSchema(it.schema)
		it.partitionKeys, err = getPartitionKeyFieldData(fieldSchema, it.insertMsg)
		if err != nil {
			log.Warn("get partition keys from insert request failed", zap.String("collectionName", collectionName), zap.Error(err))
			return err
		}
	} else {
		// set default partition name if not use partition key
		// insert to _default partition
		partitionTag := it.insertMsg.GetPartitionName()
		if len(partitionTag) <= 0 {
			partitionTag = Params.CommonCfg.DefaultPartitionName.GetValue()
			it.insertMsg.PartitionName = partitionTag
		}

		if err := validatePartitionTag(partitionTag, true); err != nil {
			log.Warn("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck(), withOverflowCheck(), withMaxLenCheck(), withMaxCapCheck()).
		Validate(it.insertMsg.GetFieldsData(), schema.schemaHelper, it.insertMsg.NRows()); err != nil {
		return merr.WrapErrAsInputError(err)
	}

	log.Debug("Proxy Insert PreExecute done")

	return nil
}

func (it *insertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-Execute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert %d", it.ID()))

	collectionName := it.insertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	log := log.Ctx(ctx)
	if err != nil {
		log.Warn("fail to get collection id", zap.Error(err))
		return err
	}
	it.insertMsg.CollectionID = collID

	getCacheDur := tr.RecordSpan()
	stream, err := it.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}
	getMsgStreamDur := tr.RecordSpan()

	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed", zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status = merr.Status(err)
		return err
	}

	log.Debug("send insert request to virtual channels",
		zap.String("partition", it.insertMsg.GetPartitionName()),
		zap.Int64("collectionID", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()),
		zap.Duration("get cache duration", getCacheDur),
		zap.Duration("get msgStream duration", getMsgStreamDur))

	// assign segmentID for insert data and repack data by segmentID
	var msgPack *msgstream.MsgPack
	if it.partitionKeys == nil {
		msgPack, err = repackInsertData(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.idAllocator, it.segIDAssigner)
	} else {
		msgPack, err = repackInsertDataWithPartitionKey(it.TraceCtx(), channelNames, it.partitionKeys, it.insertMsg, it.result, it.idAllocator, it.segIDAssigner)
	}
	if err != nil {
		log.Warn("assign segmentID and repack insert data failed", zap.Error(err))
		it.result.Status = merr.Status(err)
		return err
	}
	assignSegmentIDDur := tr.RecordSpan()

	log.Debug("assign segmentID for insert data success",
		zap.Duration("assign segmentID duration", assignSegmentIDDur))
	err = stream.Produce(msgPack)
	if err != nil {
		log.Warn("fail to produce insert msg", zap.Error(err))
		it.result.Status = merr.Status(err)
		return err
	}
	sendMsgDur := tr.RecordSpan()
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.InsertLabel).Observe(float64(sendMsgDur.Milliseconds()))
	totalExecDur := tr.ElapseSpan()
	log.Debug("Proxy Insert Execute done",
		zap.String("collectionName", collectionName),
		zap.Duration("send message duration", sendMsgDur),
		zap.Duration("execute duration", totalExecDur))

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}
