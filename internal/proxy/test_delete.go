package proxy

import (
	"context"
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

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
	dt.DeleteRequest.Base = commonpbutil.NewMsgBase()
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
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithMsgID(dt.Base.MsgID),
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
