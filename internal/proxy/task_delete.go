package proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type BaseDeleteTask = msgstream.DeleteMsg

type deleteTask struct {
	Condition
	ctx context.Context
	tr  *timerecord.TimeRecorder

	req    *milvuspb.DeleteRequest
	result *milvuspb.MutationResult

	// channel
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
	pChannels []pChan
	vChannels []vChan

	idAllocator *allocator.IDAllocator
	lb          LBPolicy

	// delete info
	schema       *schemapb.CollectionSchema
	ts           Timestamp
	msgID        UniqueID
	collectionID UniqueID
	partitionID  UniqueID
	count        int
}

func (dt *deleteTask) TraceCtx() context.Context {
	return dt.ctx
}

func (dt *deleteTask) ID() UniqueID {
	return dt.msgID
}

func (dt *deleteTask) SetID(uid UniqueID) {
	dt.msgID = uid
}

func (dt *deleteTask) Type() commonpb.MsgType {
	return commonpb.MsgType_Delete
}

func (dt *deleteTask) Name() string {
	return DeleteTaskName
}

func (dt *deleteTask) BeginTs() Timestamp {
	return dt.ts
}

func (dt *deleteTask) EndTs() Timestamp {
	return dt.ts
}

func (dt *deleteTask) SetTs(ts Timestamp) {
	dt.ts = ts
}

func (dt *deleteTask) OnEnqueue() error {
	return nil
}

func (dt *deleteTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(dt.ctx, dt.req.GetDbName(), dt.req.GetCollectionName())
	if err != nil {
		return err
	}
	channels, err := dt.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	dt.pChannels = channels
	return nil
}

func (dt *deleteTask) getChannels() []pChan {
	return dt.pChannels
}

func getExpr(plan *planpb.PlanNode) (bool, *planpb.Expr_TermExpr) {
	// simple delete request need expr with "pk in [a, b]"
	termExpr, ok := plan.Node.(*planpb.PlanNode_Query).Query.Predicates.Expr.(*planpb.Expr_TermExpr)
	if !ok {
		return false, nil
	}

	if !termExpr.TermExpr.GetColumnInfo().GetIsPrimaryKey() {
		return false, nil
	}
	return true, termExpr
}

func getPrimaryKeysFromExpr(schema *schemapb.CollectionSchema, termExpr *planpb.Expr_TermExpr) (res *schemapb.IDs, rowNum int64, err error) {
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
	dt.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: dt.BeginTs(),
	}

	log := log.Ctx(ctx)
	collName := dt.req.GetCollectionName()
	if err := validateCollectionName(collName); err != nil {
		return ErrWithLog(log, "Invalid collection name", err)
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, dt.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection id", err)
	}
	dt.collectionID = collID

	partitionKeyMode, err := isPartitionKeyMode(ctx, dt.req.GetDbName(), dt.req.GetCollectionName())
	if err != nil {
		return ErrWithLog(log, "Failed to get partition key mode", err)
	}
	if partitionKeyMode && len(dt.req.PartitionName) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
	}

	// If partitionName is not empty, partitionID will be set.
	if len(dt.req.PartitionName) > 0 {
		partName := dt.req.GetPartitionName()
		if err := validatePartitionTag(partName, true); err != nil {
			return ErrWithLog(log, "Invalid partition name", err)
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, dt.req.GetDbName(), collName, partName)
		if err != nil {
			return ErrWithLog(log, "Failed to get partition id", err)
		}
		dt.partitionID = partID
	} else {
		dt.partitionID = common.InvalidPartitionID
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dt.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection schema", err)
	}
	dt.schema = schema

	// hash primary keys to channels
	channelNames, err := dt.chMgr.getVChannels(dt.collectionID)
	if err != nil {
		return ErrWithLog(log, "Failed to get primary keys from expr", err)
	}
	dt.vChannels = channelNames

	log.Debug("pre delete done", zap.Int64("collection_id", dt.collectionID))

	return nil
}

func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))
	stream, err := dt.chMgr.getOrCreateDmlStream(dt.collectionID)
	if err != nil {
		return err
	}

	plan, err := planparserv2.CreateRetrievePlan(dt.schema, dt.req.Expr)
	if err != nil {
		return fmt.Errorf("failed to create expr plan, expr = %s", dt.req.GetExpr())
	}

	isSimple, termExp := getExpr(plan)
	if isSimple {
		// if could get delete.primaryKeys from delete expr
		err := dt.simpleDelete(ctx, termExp, stream)
		if err != nil {
			return err
		}
	} else {
		// if get complex delete expr
		// need query from querynode before delete
		err = dt.complexDelete(ctx, plan, stream)
		if err != nil {
			log.Warn("complex delete failed,but delete some data", zap.Int("count", dt.count), zap.String("expr", dt.req.GetExpr()))
			return err
		}
	}

	return nil
}

func (dt *deleteTask) PostExecute(ctx context.Context) error {
	return nil
}

func (dt *deleteTask) getStreamingQueryAndDelteFunc(stream msgstream.MsgStream, plan *planpb.PlanNode) executeFunc {
	return func(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channelIDs ...string) error {
		partationIDs := []int64{}
		if dt.partitionID != common.InvalidFieldID {
			partationIDs = append(partationIDs, dt.partitionID)
		}

		log := log.Ctx(ctx).With(
			zap.Int64("collectionID", dt.collectionID),
			zap.Int64s("partationIDs", partationIDs),
			zap.Strings("channels", channelIDs),
			zap.Int64("nodeID", nodeID))
		// set plan
		_, outputFieldIDs := translatePkOutputFields(dt.schema)
		outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
		plan.OutputFieldIds = outputFieldIDs
		log.Debug("start query for delete")

		serializedPlan, err := proto.Marshal(plan)
		if err != nil {
			return err
		}

		queryReq := &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
					commonpbutil.WithMsgID(dt.msgID),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
					commonpbutil.WithTargetID(nodeID),
				),
				MvccTimestamp:      dt.ts,
				ReqID:              paramtable.GetNodeID(),
				DbID:               0, // TODO
				CollectionID:       dt.collectionID,
				PartitionIDs:       partationIDs,
				SerializedExprPlan: serializedPlan,
				OutputFieldsId:     outputFieldIDs,
				GuaranteeTimestamp: parseGuaranteeTsFromConsistency(dt.ts, dt.ts, commonpb.ConsistencyLevel_Bounded),
			},
			DmlChannels: channelIDs,
			Scope:       querypb.DataScope_All,
		}

		rc := timerecord.NewTimeRecorder("QueryStreamDelete")
		client, err := qn.QueryStream(ctx, queryReq)
		if err != nil {
			log.Warn("query stream for delete create failed", zap.Error(err))
			return err
		}

		for {
			result, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					log.Debug("query stream for delete finished", zap.Int64("msgID", dt.msgID), zap.Duration("duration", rc.ElapseSpan()))
					return nil
				}
				return err
			}

			err = merr.Error(result.GetStatus())
			if err != nil {
				log.Warn("query stream for delete get error status", zap.Int64("msgID", dt.msgID), zap.Error(err))
				return err
			}

			err = dt.produce(ctx, stream, result.GetIds())
			if err != nil {
				log.Warn("query stream for delete produce result failed", zap.Int64("msgID", dt.msgID), zap.Error(err))
				return err
			}
		}
	}
}

func (dt *deleteTask) complexDelete(ctx context.Context, plan *planpb.PlanNode, stream msgstream.MsgStream) error {
	err := dt.lb.Execute(ctx, CollectionWorkLoad{
		db:             dt.req.GetDbName(),
		collectionName: dt.req.GetCollectionName(),
		collectionID:   dt.collectionID,
		nq:             1,
		exec:           dt.getStreamingQueryAndDelteFunc(stream, plan),
	})
	if err != nil {
		log.Warn("fail to get or create dml stream", zap.Error(err))
		return err
	}

	return nil
}

func (dt *deleteTask) simpleDelete(ctx context.Context, termExp *planpb.Expr_TermExpr, stream msgstream.MsgStream) error {
	primaryKeys, numRow, err := getPrimaryKeysFromExpr(dt.schema, termExp)
	if err != nil {
		log.Info("Failed to get primary keys from expr", zap.Error(err))
		return err
	}
	log.Debug("get primary keys from expr",
		zap.Int64("len of primary keys", numRow),
		zap.Int64("collectionID", dt.collectionID),
		zap.Int64("partationID", dt.partitionID))
	err = dt.produce(ctx, stream, primaryKeys)
	if err != nil {
		return err
	}
	return nil
}

func (dt *deleteTask) produce(ctx context.Context, stream msgstream.MsgStream, primaryKeys *schemapb.IDs) error {
	hashValues := typeutil.HashPK2Channels(primaryKeys, dt.vChannels)
	// repack delete msg by dmChannel
	result := make(map[uint32]msgstream.TsMsg)
	numRows := int64(0)
	for index, key := range hashValues {
		vchannel := dt.vChannels[key]
		_, ok := result[key]
		if !ok {
			deleteMsg, err := dt.newDeleteMsg(ctx)
			if err != nil {
				return err
			}
			deleteMsg.ShardName = vchannel
			result[key] = deleteMsg
		}
		curMsg := result[key].(*msgstream.DeleteMsg)
		curMsg.HashValues = append(curMsg.HashValues, hashValues[index])
		curMsg.Timestamps = append(curMsg.Timestamps, dt.ts)

		typeutil.AppendIDs(curMsg.PrimaryKeys, primaryKeys, index)
		curMsg.NumRows++
		numRows++
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

	log.Debug("send delete request to virtual channels",
		zap.String("collectionName", dt.req.GetCollectionName()),
		zap.Int64("collectionID", dt.collectionID),
		zap.Strings("virtual_channels", dt.vChannels),
		zap.Int64("taskID", dt.ID()),
		zap.Duration("prepare duration", dt.tr.RecordSpan()))

	err := stream.Produce(msgPack)
	if err != nil {
		return err
	}
	dt.result.DeleteCnt += numRows
	return nil
}

func (dt *deleteTask) newDeleteMsg(ctx context.Context) (*msgstream.DeleteMsg, error) {
	msgid, err := dt.idAllocator.AllocOne()
	if err != nil {
		return nil, errors.Wrap(err, "failed to allocate MsgID of delete")
	}
	sliceRequest := msgpb.DeleteRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Delete),
			// msgid of delete msg must be set
			// or it will be seen as duplicated msg in mq
			commonpbutil.WithMsgID(msgid),
			commonpbutil.WithTimeStamp(dt.ts),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID:   dt.collectionID,
		PartitionID:    dt.partitionID,
		CollectionName: dt.req.GetCollectionName(),
		PartitionName:  dt.req.GetPartitionName(),
		PrimaryKeys:    &schemapb.IDs{},
	}
	return &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx: ctx,
		},
		DeleteRequest: sliceRequest,
	}, nil
}
