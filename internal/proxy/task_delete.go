package proxy

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type BaseDeleteTask = msgstream.DeleteMsg

type deleteTask struct {
	baseTask
	Condition
	ctx context.Context
	tr  *timerecord.TimeRecorder

	req *milvuspb.DeleteRequest

	// channel
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
	pChannels []pChan
	vChannels []vChan

	idAllocator allocator.Interface

	// delete info
	primaryKeys  *schemapb.IDs
	collectionID UniqueID
	partitionID  UniqueID
	dbID         UniqueID

	// set by scheduler
	ts    Timestamp
	msgID UniqueID

	// result
	count       int64
	allQueryCnt int64

	sessionTS Timestamp
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
	if dt.req.Base == nil {
		dt.req.Base = commonpbutil.NewMsgBase()
	}
	dt.req.Base.MsgType = commonpb.MsgType_Delete
	dt.req.Base.SourceID = paramtable.GetNodeID()
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

func (dt *deleteTask) PreExecute(ctx context.Context) error {
	return nil
}

func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))
	stream, err := dt.chMgr.getOrCreateDmlStream(ctx, dt.collectionID)
	if err != nil {
		return err
	}

	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		dt.primaryKeys, dt.vChannels,
		dt.idAllocator, dt.ts,
		dt.collectionID, dt.req.GetCollectionName(),
		dt.partitionID, dt.req.GetPartitionName(),
		dt.req.GetDbName(),
	)
	if err != nil {
		return err
	}

	// send delete request to log broker
	msgPack := &msgstream.MsgPack{
		BeginTs: dt.BeginTs(),
		EndTs:   dt.EndTs(),
	}

	for _, msgs := range result {
		for _, msg := range msgs {
			msgPack.Msgs = append(msgPack.Msgs, msg)
		}
	}

	log.Ctx(ctx).Debug("send delete request to virtual channels",
		zap.String("collectionName", dt.req.GetCollectionName()),
		zap.Int64("collectionID", dt.collectionID),
		zap.Strings("virtual_channels", dt.vChannels),
		zap.Int64("taskID", dt.ID()),
		zap.Duration("prepare duration", dt.tr.RecordSpan()))

	err = stream.Produce(ctx, msgPack)
	if err != nil {
		return err
	}
	dt.sessionTS = dt.ts
	dt.count += numRows
	return nil
}

func (dt *deleteTask) PostExecute(ctx context.Context) error {
	metrics.ProxyDeleteVectors.WithLabelValues(
		paramtable.GetStringNodeID(),
		dt.req.GetDbName(),
		dt.req.GetCollectionName(),
	).Add(float64(dt.count))
	return nil
}

func repackDeleteMsgByHash(
	ctx context.Context,
	primaryKeys *schemapb.IDs,
	vChannels []string,
	idAllocator allocator.Interface,
	ts uint64,
	collectionID int64,
	collectionName string,
	partitionID int64,
	partitionName string,
	dbName string,
) (map[uint32][]*msgstream.DeleteMsg, int64, error) {
	maxSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	hashValues := typeutil.HashPK2Channels(primaryKeys, vChannels)
	// repack delete msg by dmChannel
	result := make(map[uint32][]*msgstream.DeleteMsg)
	lastMessageSize := map[uint32]int{}

	numRows := int64(0)
	numMessage := 0

	createMessage := func(key uint32, vchannel string) *msgstream.DeleteMsg {
		numMessage++
		lastMessageSize[key] = 0
		return &msgstream.DeleteMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx: ctx,
			},
			DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					// msgid of delete msg must be set later
					// or it will be seen as duplicated msg in mq
					commonpbutil.WithTimeStamp(ts),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				ShardName:      vchannel,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				CollectionID:   collectionID,
				PartitionID:    partitionID,
				PrimaryKeys:    &schemapb.IDs{},
			},
		}
	}

	for index, key := range hashValues {
		vchannel := vChannels[key]
		msgs, ok := result[key]
		if !ok {
			result[key] = make([]*msgstream.DeleteMsg, 1)
			msgs = result[key]
			result[key][0] = createMessage(key, vchannel)
		}
		curMsg := msgs[len(msgs)-1]
		size, id := typeutil.GetId(primaryKeys, index)
		if lastMessageSize[key]+16+size > maxSize {
			curMsg = createMessage(key, vchannel)
			result[key] = append(result[key], curMsg)
		}
		curMsg.HashValues = append(curMsg.HashValues, hashValues[index])
		curMsg.Timestamps = append(curMsg.Timestamps, ts)

		typeutil.AppendID(curMsg.PrimaryKeys, id)
		lastMessageSize[key] += 16 + size
		curMsg.NumRows++
		numRows++
	}

	// alloc messageID
	start, _, err := idAllocator.Alloc(uint32(numMessage))
	if err != nil {
		return nil, 0, err
	}

	cnt := int64(0)
	for _, msgs := range result {
		for _, msg := range msgs {
			msg.Base.MsgID = start + cnt
			cnt++
		}
	}
	return result, numRows, nil
}

type deleteRunner struct {
	req    *milvuspb.DeleteRequest
	result *milvuspb.MutationResult

	// channel
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
	vChannels []vChan

	idAllocator     allocator.Interface
	tsoAllocatorIns tsoAllocator
	limiter         types.Limiter

	// delete info
	schema       *schemaInfo
	dbID         UniqueID
	collectionID UniqueID
	partitionIDs []UniqueID
	plan         *planpb.PlanNode

	// for query
	msgID int64
	ts    uint64
	lb    LBPolicy
	count atomic.Int64

	// task queue
	queue *dmTaskQueue

	allQueryCnt atomic.Int64
	sessionTS   atomic.Uint64
}

func (dr *deleteRunner) Init(ctx context.Context) error {
	log := log.Ctx(ctx)
	var err error

	collName := dr.req.GetCollectionName()
	if err := validateCollectionName(collName); err != nil {
		return ErrWithLog(log, "Invalid collection name", err)
	}

	db, err := globalMetaCache.GetDatabaseInfo(ctx, dr.req.GetDbName())
	if err != nil {
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrDatabaseNotFound)
	}
	dr.dbID = db.dbID

	dr.collectionID, err = globalMetaCache.GetCollectionID(ctx, dr.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection id", merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound))
	}

	replicateID, err := GetReplicateID(ctx, dr.req.GetDbName(), collName)
	if err != nil {
		log.Warn("get replicate info failed", zap.String("collectionName", collName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	if replicateID != "" {
		return merr.WrapErrCollectionReplicateMode("delete")
	}

	dr.schema, err = globalMetaCache.GetCollectionSchema(ctx, dr.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection schema", err)
	}

	start := time.Now()
	dr.plan, err = planparserv2.CreateRetrievePlan(dr.schema.schemaHelper, dr.req.GetExpr(), dr.req.GetExprTemplateValues())
	if err != nil {
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "delete", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create delete plan: %v", err))
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "delete", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))

	if planparserv2.IsAlwaysTruePlan(dr.plan) {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("delete plan can't be empty or always true : %s", dr.req.GetExpr()))
	}

	// Set partitionIDs, could be empty if no partition name specified and no partition key
	partName := dr.req.GetPartitionName()
	if dr.schema.IsPartitionKeyCollection() {
		if len(partName) > 0 {
			return errors.New("not support manually specifying the partition names if partition key mode is used")
		}
		expr, err := exprutil.ParseExprFromPlan(dr.plan)
		if err != nil {
			return err
		}
		partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
		hashedPartitionNames, err := assignPartitionKeys(ctx, dr.req.GetDbName(), dr.req.GetCollectionName(), partitionKeys)
		if err != nil {
			return err
		}
		dr.partitionIDs, err = getPartitionIDs(ctx, dr.req.GetDbName(), dr.req.GetCollectionName(), hashedPartitionNames)
		if err != nil {
			return err
		}
	} else if len(partName) > 0 {
		// static validation
		if err := validatePartitionTag(partName, true); err != nil {
			return ErrWithLog(log, "Invalid partition name", err)
		}

		// dynamic validation
		partID, err := globalMetaCache.GetPartitionID(ctx, dr.req.GetDbName(), collName, partName)
		if err != nil {
			return ErrWithLog(log, "Failed to get partition id", err)
		}
		dr.partitionIDs = []UniqueID{partID} // only one partID
	}

	// set vchannels
	channelNames, err := dr.chMgr.getVChannels(dr.collectionID)
	if err != nil {
		return ErrWithLog(log, "Failed to get vchannels from collection", err)
	}
	dr.vChannels = channelNames

	dr.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
	}
	return nil
}

func (dr *deleteRunner) Run(ctx context.Context) error {
	isSimple, pk, numRow := getPrimaryKeysFromPlan(dr.schema.CollectionSchema, dr.plan)
	if isSimple {
		// if could get delete.primaryKeys from delete expr
		err := dr.simpleDelete(ctx, pk, numRow)
		if err != nil {
			return err
		}
	} else {
		// if get complex delete expr
		// need query from querynode before delete
		err := dr.complexDelete(ctx, dr.plan)
		if err != nil {
			log.Ctx(ctx).Warn("complex delete failed,but delete some data", zap.Int64("count", dr.result.DeleteCnt), zap.String("expr", dr.req.GetExpr()))
			return err
		}
	}
	return nil
}

func (dr *deleteRunner) produce(ctx context.Context, primaryKeys *schemapb.IDs, partitionID UniqueID) (*deleteTask, error) {
	dt := &deleteTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		req:          dr.req,
		idAllocator:  dr.idAllocator,
		chMgr:        dr.chMgr,
		chTicker:     dr.chTicker,
		collectionID: dr.collectionID,
		partitionID:  partitionID,
		vChannels:    dr.vChannels,
		primaryKeys:  primaryKeys,
		dbID:         dr.dbID,
	}

	var enqueuedTask task = dt
	if streamingutil.IsStreamingServiceEnabled() {
		enqueuedTask = &deleteTaskByStreamingService{deleteTask: dt}
	}

	if err := dr.queue.Enqueue(enqueuedTask); err != nil {
		log.Ctx(ctx).Error("Failed to enqueue delete task: " + err.Error())
		return nil, err
	}

	return dt, nil
}

// getStreamingQueryAndDelteFunc return query function used by LBPolicy
// make sure it concurrent safe
func (dr *deleteRunner) getStreamingQueryAndDelteFunc(plan *planpb.PlanNode) executeFunc {
	return func(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
		log := log.Ctx(ctx).With(
			zap.Int64("collectionID", dr.collectionID),
			zap.Int64s("partitionIDs", dr.partitionIDs),
			zap.String("channel", channel),
			zap.Int64("nodeID", nodeID))

		// set plan
		_, outputFieldIDs := translatePkOutputFields(dr.schema.CollectionSchema)
		outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
		plan.OutputFieldIds = outputFieldIDs

		serializedPlan, err := proto.Marshal(plan)
		if err != nil {
			return err
		}

		queryReq := &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
					commonpbutil.WithMsgID(dr.msgID),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
					commonpbutil.WithTargetID(nodeID),
				),
				MvccTimestamp:      dr.ts,
				ReqID:              paramtable.GetNodeID(),
				DbID:               0, // TODO
				CollectionID:       dr.collectionID,
				PartitionIDs:       dr.partitionIDs,
				SerializedExprPlan: serializedPlan,
				OutputFieldsId:     outputFieldIDs,
				GuaranteeTimestamp: parseGuaranteeTsFromConsistency(dr.ts, dr.ts, dr.req.GetConsistencyLevel()),
			},
			DmlChannels: []string{channel},
			Scope:       querypb.DataScope_All,
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		log.Debug("start query for delete", zap.Int64("msgID", dr.msgID))
		client, err := qn.QueryStream(ctx, queryReq)
		if err != nil {
			log.Warn("query stream for delete create failed", zap.Error(err))
			return err
		}

		taskCh := make(chan *deleteTask, 256)
		var receiveErr error
		go func() {
			receiveErr = dr.receiveQueryResult(ctx, client, taskCh)
			close(taskCh)
		}()
		var allQueryCnt int64
		// wait all task finish
		var sessionTS uint64
		for task := range taskCh {
			err := task.WaitToFinish()
			if err != nil {
				return err
			}
			dr.count.Add(task.count)
			allQueryCnt += task.allQueryCnt
			if sessionTS < task.sessionTS {
				sessionTS = task.sessionTS
			}
		}

		// query or produce task failed
		if receiveErr != nil {
			return receiveErr
		}
		dr.allQueryCnt.Add(allQueryCnt)
		dr.sessionTS.Store(sessionTS)
		return nil
	}
}

func (dr *deleteRunner) receiveQueryResult(ctx context.Context, client querypb.QueryNode_QueryStreamClient, taskCh chan *deleteTask) error {
	// If a complex delete tries to delete multiple partitions in the filter, use AllPartitionID
	// otherwise use the target partitionID, which can come from partition name(UDF) or a partition key expression
	// TODO: Get partitionID from Query results
	msgPartitionID := common.AllPartitionsID
	if len(dr.partitionIDs) == 1 {
		msgPartitionID = dr.partitionIDs[0]
	}

	for {
		result, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				log.Ctx(ctx).Debug("query stream for delete finished", zap.Int64("msgID", dr.msgID))
				return nil
			}
			return err
		}

		err = merr.Error(result.GetStatus())
		if err != nil {
			log.Ctx(ctx).Warn("query stream for delete get error status", zap.Int64("msgID", dr.msgID), zap.Error(err))
			return err
		}

		if dr.limiter != nil {
			err := dr.limiter.Alloc(ctx, dr.dbID, map[int64][]int64{dr.collectionID: dr.partitionIDs}, internalpb.RateType_DMLDelete, proto.Size(result.GetIds()))
			if err != nil {
				log.Ctx(ctx).Warn("query stream for delete failed because rate limiter", zap.Int64("msgID", dr.msgID), zap.Error(err))
				return err
			}
		}

		task, err := dr.produce(ctx, result.GetIds(), msgPartitionID)
		if err != nil {
			log.Ctx(ctx).Warn("produce delete task failed", zap.Error(err))
			return err
		}
		task.allQueryCnt = result.GetAllRetrieveCount()

		taskCh <- task
	}
}

func (dr *deleteRunner) complexDelete(ctx context.Context, plan *planpb.PlanNode) error {
	rc := timerecord.NewTimeRecorder("QueryStreamDelete")
	var err error

	dr.msgID, err = dr.idAllocator.AllocOne()
	if err != nil {
		return err
	}

	dr.ts, err = dr.tsoAllocatorIns.AllocOne(ctx)
	if err != nil {
		return err
	}

	err = dr.lb.Execute(ctx, CollectionWorkLoad{
		db:             dr.req.GetDbName(),
		collectionName: dr.req.GetCollectionName(),
		collectionID:   dr.collectionID,
		nq:             1,
		exec:           dr.getStreamingQueryAndDelteFunc(plan),
	})
	dr.result.DeleteCnt = dr.count.Load()
	dr.result.Timestamp = dr.sessionTS.Load()
	if err != nil {
		log.Ctx(ctx).Warn("fail to execute complex delete",
			zap.Int64("deleteCnt", dr.result.GetDeleteCnt()),
			zap.Duration("interval", rc.ElapseSpan()),
			zap.Error(err))
		return err
	}

	log.Ctx(ctx).Info("complex delete finished", zap.Int64("deleteCnt", dr.result.GetDeleteCnt()), zap.Duration("interval", rc.ElapseSpan()))
	return nil
}

func (dr *deleteRunner) simpleDelete(ctx context.Context, pk *schemapb.IDs, numRow int64) error {
	partitionID := common.AllPartitionsID
	if len(dr.partitionIDs) == 1 {
		partitionID = dr.partitionIDs[0]
	}
	log.Ctx(ctx).Debug("get primary keys from expr",
		zap.Int64("len of primary keys", numRow),
		zap.Int64("collectionID", dr.collectionID),
		zap.Int64("partitionID", partitionID))

	task, err := dr.produce(ctx, pk, partitionID)
	if err != nil {
		log.Ctx(ctx).Warn("produce delete task failed")
		return err
	}

	err = task.WaitToFinish()
	if err == nil {
		dr.result.DeleteCnt = task.count
		dr.result.Timestamp = task.sessionTS
	}
	return err
}

func getPrimaryKeysFromPlan(schema *schemapb.CollectionSchema, plan *planpb.PlanNode) (isSimpleDelete bool, pks *schemapb.IDs, pkCount int64) {
	var err error
	// simple delete request with "pk in [a, b]"
	termExpr, ok := plan.Node.(*planpb.PlanNode_Query).Query.Predicates.Expr.(*planpb.Expr_TermExpr)
	if ok {
		if !termExpr.TermExpr.GetColumnInfo().GetIsPrimaryKey() {
			return false, nil, 0
		}

		pks, pkCount, err = getPrimaryKeysFromTermExpr(schema, termExpr)
		if err != nil {
			return false, nil, 0
		}
		return true, pks, pkCount
	}

	// simple delete with "pk == a"
	unaryRangeExpr, ok := plan.Node.(*planpb.PlanNode_Query).Query.Predicates.Expr.(*planpb.Expr_UnaryRangeExpr)
	if ok {
		if unaryRangeExpr.UnaryRangeExpr.GetOp() != planpb.OpType_Equal || !unaryRangeExpr.UnaryRangeExpr.GetColumnInfo().GetIsPrimaryKey() {
			return false, nil, 0
		}

		pks, err = getPrimaryKeysFromUnaryRangeExpr(schema, unaryRangeExpr)
		if err != nil {
			return false, nil, 0
		}
		return true, pks, 1
	}

	return false, nil, 0
}

func getPrimaryKeysFromUnaryRangeExpr(schema *schemapb.CollectionSchema, unaryRangeExpr *planpb.Expr_UnaryRangeExpr) (pks *schemapb.IDs, err error) {
	pks = &schemapb.IDs{}
	switch unaryRangeExpr.UnaryRangeExpr.GetColumnInfo().GetDataType() {
	case schemapb.DataType_Int64:
		pks.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{unaryRangeExpr.UnaryRangeExpr.GetValue().GetInt64Val()},
			},
		}
	case schemapb.DataType_VarChar:
		pks.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{unaryRangeExpr.UnaryRangeExpr.GetValue().GetStringVal()},
			},
		}
	default:
		return pks, fmt.Errorf("invalid field data type specifyed in simple delete expr")
	}

	return pks, nil
}

func getPrimaryKeysFromTermExpr(schema *schemapb.CollectionSchema, termExpr *planpb.Expr_TermExpr) (pks *schemapb.IDs, pkCount int64, err error) {
	pks = &schemapb.IDs{}
	pkCount = int64(len(termExpr.TermExpr.Values))
	switch termExpr.TermExpr.ColumnInfo.GetDataType() {
	case schemapb.DataType_Int64:
		ids := make([]int64, 0)
		for _, v := range termExpr.TermExpr.Values {
			ids = append(ids, v.GetInt64Val())
		}
		pks.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: ids,
			},
		}
	case schemapb.DataType_VarChar:
		ids := make([]string, 0)
		for _, v := range termExpr.TermExpr.Values {
			ids = append(ids, v.GetStringVal())
		}
		pks.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: ids,
			},
		}
	default:
		return pks, 0, fmt.Errorf("invalid field data type specifyed in simple delete expr")
	}

	return pks, pkCount, nil
}
