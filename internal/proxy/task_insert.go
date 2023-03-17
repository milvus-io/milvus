package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type insertTask struct {
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

func (it *insertTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.CollectionName)
	if err != nil {
		return nil, err
	}
	return it.chMgr.getChannels(collID)
}

func (it *insertTask) OnEnqueue() error {
	return nil
}

func (it *insertTask) checkVectorFieldData() error {
	// error won't happen here.
	helper, _ := typeutil.CreateSchemaHelper(it.schema)

	fields := it.insertMsg.GetFieldsData()
	for _, field := range fields {
		if field.GetType() != schemapb.DataType_FloatVector {
			continue
		}

		vectorField := field.GetVectors()
		if vectorField == nil || vectorField.GetFloatVector() == nil {
			return fmt.Errorf("float vector field '%v' is illegal, array type mismatch", field.GetFieldName())
		}

		// error won't happen here.
		f, _ := helper.GetFieldFromName(field.GetFieldName())
		dim, _ := typeutil.GetDim(f)

		floatArray := vectorField.GetFloatVector()

		// TODO: `NumRows` passed by client may be not trustable.
		if uint64(len(floatArray.GetData())) != uint64(dim)*it.insertMsg.GetNumRows() {
			return fmt.Errorf("length of inserted vector (%d) not match dim (%d)", len(floatArray.GetData()), dim)
		}

		if err := typeutil.VerifyFloats32(floatArray.GetData()); err != nil {
			return fmt.Errorf("float vector field data is illegal, error: %w", err)
		}
	}

	return nil
}

func (it *insertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Insert-PreExecute")
	defer sp.End()

	it.result = &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	collectionName := it.insertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Error("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	partitionTag := it.insertMsg.PartitionName
	if err := validatePartitionTag(partitionTag, true); err != nil {
		log.Error("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
		return err
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
	if err != nil {
		log.Error("get collection schema from global meta cache failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}
	it.schema = schema

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

	// check primaryFieldData whether autoID is true or not
	// set rowIDs as primary data if autoID == true
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	it.result.IDs, err = checkPrimaryFieldData(it.schema, it.result, it.insertMsg, true)
	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName))
	if err != nil {
		log.Error("check primary field data and hash primary key failed",
			zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldIDBySchema(it.insertMsg.GetFieldsData(), schema)
	if err != nil {
		log.Error("set fieldID to fieldData failed",
			zap.Error(err))
		return err
	}

	// check that all field's number rows are equal
	if err = it.insertMsg.CheckAligned(); err != nil {
		log.Error("field data is not aligned",
			zap.Error(err))
		return err
	}

	// check vector field data
	err = it.checkVectorFieldData()
	if err != nil {
		log.Error("vector field data is illegal", zap.Error(err))
		return err
	}

	log.Debug("Proxy Insert PreExecute done")

	return nil
}

func (it *insertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-PreExecute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert %d", it.ID()))
	defer tr.Elapse("insert execute done")

	collectionName := it.insertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		return err
	}
	it.insertMsg.CollectionID = collID
	var partitionID UniqueID
	if len(it.insertMsg.PartitionName) > 0 {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, it.insertMsg.PartitionName)
		if err != nil {
			return err
		}
	} else {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, Params.CommonCfg.DefaultPartitionName.GetValue())
		if err != nil {
			return err
		}
	}
	it.insertMsg.PartitionID = partitionID
	tr.Record("get collection id & partition id from cache")

	stream, err := it.chMgr.getOrCreateDmlStream(collID)
	if err != nil {
		return err
	}
	tr.Record("get used message stream")

	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Ctx(ctx).Error("get vChannels failed",
			zap.Int64("collectionID", collID),
			zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}

	log.Ctx(ctx).Debug("send insert request to virtual channels",
		zap.String("collection", it.insertMsg.GetCollectionName()),
		zap.String("partition", it.insertMsg.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Int64("partition_id", partitionID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()))

	// assign segmentID for insert data and repack data by segmentID
	var msgPack *msgstream.MsgPack
	msgPack, err = assignSegmentID(it.TraceCtx(), it.insertMsg, it.result, channelNames, it.idAllocator, it.segIDAssigner)
	if err != nil {
		log.Error("assign segmentID and repack insert data failed",
			zap.Int64("collectionID", collID),
			zap.Error(err))
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	log.Debug("assign segmentID for insert data success",
		zap.Int64("collectionID", collID),
		zap.String("collectionName", it.insertMsg.CollectionName))
	tr.Record("assign segment id")
	err = stream.Produce(msgPack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}
	sendMsgDur := tr.Record("send insert request to dml channel")
	metrics.ProxySendMutationReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.InsertLabel).Observe(float64(sendMsgDur.Milliseconds()))

	log.Debug("Proxy Insert Execute done",
		zap.String("collectionName", collectionName))

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}
