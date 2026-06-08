package proxy

import (
	"context"
	"io"
	"strconv"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	storageCost segcore.StorageCost

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
	if dt.req.Namespace == nil {
		return nil
	}
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dt.req.GetDbName(), dt.req.GetCollectionName())
	if err != nil {
		return err
	}
	return common.CheckNamespace(schema.CollectionSchema, dt.req.Namespace)
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
	namespace *string,
	schema *schemapb.CollectionSchema,
) (map[uint32][]*msgstream.DeleteMsg, int64, error) {
	maxSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	var hashValues []uint32
	// Delete tombstones are PK+timestamp based. Namespace can narrow routing,
	// but it is not part of the tombstone identity; PKs must stay unique across
	// namespaces in the same collection.
	channelID, ok, err := namespaceShardingChannelID(schema, namespace, vChannels)
	if err != nil {
		return nil, 0, err
	}
	if ok {
		size := typeutil.GetSizeOfIDs(primaryKeys)
		hashValues = make([]uint32, size)
		for i := 0; i < size; i++ {
			hashValues[i] = channelID
		}
	} else {
		hashValues, err = typeutil.HashPK2Channels(primaryKeys, vChannels)
		if err != nil {
			return nil, 0, err
		}
	}
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
	lb    shardclient.LBPolicy
	count atomic.Int64

	// task queue
	queue *dmTaskQueue

	allQueryCnt atomic.Int64
	sessionTS   atomic.Uint64

	scannedRemoteBytes atomic.Int64
	scannedTotalBytes  atomic.Int64
}

func (dr *deleteRunner) Init(ctx context.Context) error {
	var err error

	collName := dr.req.GetCollectionName()
	log := mlog.With(
		mlog.FieldDbName(dr.req.GetDbName()),
		mlog.FieldCollectionName(collName),
	)
	if err := validateCollectionName(collName); err != nil {
		return ErrWithLog(log, "Invalid collection name", err)
	}

	db, err := globalMetaCache.GetDatabaseInfo(ctx, dr.req.GetDbName())
	if err != nil {
		return err
	}
	dr.dbID = db.dbID

	dr.collectionID, err = globalMetaCache.GetCollectionID(ctx, dr.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection id", err)
	}

	dr.schema, err = globalMetaCache.GetCollectionSchema(ctx, dr.req.GetDbName(), collName)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection schema", err)
	}
	if err := validateTextStorageV3Enabled(dr.schema.CollectionSchema); err != nil {
		return ErrWithLog(log, "TEXT field requires StorageV3", err)
	}
	partitionName, namespaceAsPartition, err := resolveNamespacePartitionName(dr.schema.CollectionSchema, dr.req.Namespace, dr.req.GetPartitionName())
	if err != nil {
		return err
	}
	if namespaceAsPartition {
		dr.req.PartitionName = partitionName
	}

	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, dr.req.GetDbName(), collName, dr.collectionID)
	if err != nil {
		return ErrWithLog(log, "Failed to get collection info", err)
	}
	colTimezone := getColTimezone(colInfo)
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: colTimezone}

	start := time.Now()
	dr.plan, err = planparserv2.CreateRetrievePlanArgs(dr.schema.schemaHelper, dr.req.GetExpr(), dr.req.GetExprTemplateValues(), visitorArgs)
	if err != nil {
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "delete", metrics.FailLabel).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create delete plan: %v", err))
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "delete", metrics.SuccessLabel).Observe(float64(time.Since(start).Microseconds()) / 1000.0)

	if planparserv2.IsAlwaysTruePlan(dr.plan) {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("delete plan can't be empty or always true : %s", dr.req.GetExpr()))
	}

	dr.plan.Namespace = namespaceForPlan(dr.schema.CollectionSchema, dr.req.Namespace)
	// Set partitionIDs, could be empty if no partition name specified and no partition key
	partName := dr.req.GetPartitionName()
	if namespacePartitionKeyMode(dr.schema.CollectionSchema) && dr.req.Namespace != nil {
		if len(partName) > 0 {
			return merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if namespace is used")
		}
		hashedPartitionNames, err := assignNamespacePartitionKey(ctx, dr.req.GetDbName(), dr.req.GetCollectionName(), dr.req.Namespace)
		if err != nil {
			return err
		}
		dr.partitionIDs, err = getPartitionIDs(ctx, dr.req.GetDbName(), dr.req.GetCollectionName(), hashedPartitionNames)
		if err != nil {
			return err
		}
	} else if dr.schema.IsPartitionKeyCollection() {
		if len(partName) > 0 {
			return merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if partition key mode is used")
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
		return dr.simpleDelete(ctx, pk, numRow)
	}

	return dr.nonSimpleDelete(ctx, dr.plan)
}

func (dr *deleteRunner) produce(ctx context.Context, primaryKeys *schemapb.IDs, partitionID UniqueID) (*deleteTask, error) {
	dt := &deleteTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		req:          dr.req,
		idAllocator:  dr.idAllocator,
		chMgr:        dr.chMgr,
		collectionID: dr.collectionID,
		partitionID:  partitionID,
		vChannels:    dr.vChannels,
		primaryKeys:  primaryKeys,
		dbID:         dr.dbID,
	}
	if err := dr.queue.Enqueue(dt); err != nil {
		mlog.Error(ctx, "Failed to enqueue delete task: "+err.Error())
		return nil, err
	}

	return dt, nil
}

// getStreamingQueryAndDelteFunc return query function used by LBPolicy
// make sure it concurrent safe
func (dr *deleteRunner) getStreamingQueryAndDelteFunc(plan *planpb.PlanNode) shardclient.ExecuteFunc {
	return func(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
		log := mlog.With(
			mlog.FieldCollectionID(dr.collectionID),
			mlog.Int64s("partitionIDs", dr.partitionIDs),
			mlog.String("channel", channel),
			mlog.FieldNodeID(nodeID))

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
				QueryLabel:         metrics.DeleteQueryLabel,
			},
			DmlChannels: []string{channel},
			Scope:       querypb.DataScope_All,
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		log.Debug(ctx, "start query for delete", mlog.Int64("msgID", dr.msgID))
		client, err := qn.QueryStream(ctx, queryReq)
		if err != nil {
			log.Warn(ctx, "query stream for delete create failed", mlog.Err(err))
			return err
		}

		taskCh := make(chan *deleteTask, 256)
		var receiveErr error
		go func() {
			receiveErr = dr.receiveQueryResult(ctx, client, taskCh)
			close(taskCh)
		}()
		var allQueryCnt int64
		var scannedRemoteBytes int64
		var scannedTotalBytes int64
		// wait all task finish
		var sessionTS uint64
		for task := range taskCh {
			err := task.WaitToFinish()
			if err != nil {
				return err
			}
			dr.count.Add(task.count)
			allQueryCnt += task.allQueryCnt
			scannedRemoteBytes += task.storageCost.ScannedRemoteBytes
			scannedTotalBytes += task.storageCost.ScannedTotalBytes
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
		dr.scannedRemoteBytes.Add(scannedRemoteBytes)
		dr.scannedTotalBytes.Add(scannedTotalBytes)
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
				mlog.Debug(ctx, "query stream for delete finished", mlog.Int64("msgID", dr.msgID))
				return nil
			}
			return err
		}

		err = merr.Error(result.GetStatus())
		if err != nil {
			mlog.Warn(ctx, "query stream for delete get error status", mlog.Int64("msgID", dr.msgID), mlog.Err(err))
			return err
		}

		if dr.limiter != nil {
			err := dr.limiter.Alloc(ctx, dr.dbID, map[int64][]int64{dr.collectionID: dr.partitionIDs}, internalpb.RateType_DMLDelete, proto.Size(result.GetIds()))
			if err != nil {
				mlog.Warn(ctx, "query stream for delete failed because rate limiter", mlog.Int64("msgID", dr.msgID), mlog.Err(err))
				return err
			}
		}

		task, err := dr.produce(ctx, result.GetIds(), msgPartitionID)
		if err != nil {
			mlog.Warn(ctx, "produce delete task failed", mlog.Err(err))
			return err
		}
		task.allQueryCnt = result.GetAllRetrieveCount()
		task.storageCost = segcore.StorageCost{
			ScannedRemoteBytes: result.GetScannedRemoteBytes(),
			ScannedTotalBytes:  result.GetScannedTotalBytes(),
		}

		taskCh <- task
	}
}

// canUsePredicateDelete is a local conservative allowlist for predicate-delete producer safety.
// Existing Milvus expression validators cover the broader query engine, while predicate delete storage apply supports a narrower subset.
func canUsePredicateDelete(plan *planpb.PlanNode, schema *schemapb.CollectionSchema) bool {
	if plan == nil || plan.GetQuery() == nil {
		return false
	}
	return canUsePredicateDeleteExpr(plan.GetQuery().GetPredicates(), schema)
}

// predicateDeleteAllowedCompareOps and predicateDeleteAllowedBinaryOps are the single,
// greppable source of truth for the expression operators predicate delete accepts today.
// canUsePredicateDeleteExpr default-denies anything not enumerated here, so newly added
// planpb operators stay unsupported until they are deliberately allowed.
var predicateDeleteAllowedCompareOps = map[planpb.OpType]bool{
	planpb.OpType_Equal:        true,
	planpb.OpType_NotEqual:     true,
	planpb.OpType_LessThan:     true,
	planpb.OpType_LessEqual:    true,
	planpb.OpType_GreaterThan:  true,
	planpb.OpType_GreaterEqual: true,
}

var predicateDeleteAllowedBinaryOps = map[planpb.BinaryExpr_BinaryOp]bool{
	planpb.BinaryExpr_LogicalAnd: true,
	planpb.BinaryExpr_LogicalOr:  true,
}

func canUsePredicateDeleteExpr(expr *planpb.Expr, schema *schemapb.CollectionSchema) bool {
	if expr == nil || expr.GetIsTemplate() {
		return false
	}

	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		binaryExpr := e.BinaryExpr
		return predicateDeleteAllowedBinaryOps[binaryExpr.GetOp()] &&
			canUsePredicateDeleteExpr(binaryExpr.GetLeft(), schema) &&
			canUsePredicateDeleteExpr(binaryExpr.GetRight(), schema)
	case *planpb.Expr_UnaryExpr:
		unaryExpr := e.UnaryExpr
		return unaryExpr.GetOp() == planpb.UnaryExpr_Not &&
			canUsePredicateDeleteExpr(unaryExpr.GetChild(), schema)
	case *planpb.Expr_UnaryRangeExpr:
		unaryRangeExpr := e.UnaryRangeExpr
		return predicateDeleteAllowedCompareOps[unaryRangeExpr.GetOp()] &&
			unaryRangeExpr.GetTemplateVariableName() == "" &&
			len(unaryRangeExpr.GetExtraValues()) == 0 &&
			isPredicateDeleteColumnSupported(schema, unaryRangeExpr.GetColumnInfo()) &&
			isPredicateDeleteLiteralSupported(unaryRangeExpr.GetValue())
	case *planpb.Expr_TermExpr:
		termExpr := e.TermExpr
		// Plain field IN literal-list uses ColumnInfo + Values; IsInField is for container membership.
		if termExpr.GetIsInField() || termExpr.GetTemplateVariableName() != "" || len(termExpr.GetValues()) == 0 {
			return false
		}
		if !isPredicateDeleteColumnSupported(schema, termExpr.GetColumnInfo()) {
			return false
		}
		for _, value := range termExpr.GetValues() {
			if !isPredicateDeleteLiteralSupported(value) {
				return false
			}
		}
		return true
	case *planpb.Expr_NullExpr:
		nullExpr := e.NullExpr
		return nullExpr.GetOp() == planpb.NullExpr_IsNull &&
			isPredicateDeleteColumnSupported(schema, nullExpr.GetColumnInfo())
	default:
		// default-deny: new planpb.Expr_* variants must be explicitly enumerated above.
		return false
	}
}

func isPredicateDeleteColumnSupported(schema *schemapb.CollectionSchema, columnInfo *planpb.ColumnInfo) bool {
	if schema == nil || columnInfo == nil {
		return false
	}
	if len(columnInfo.GetNestedPath()) > 0 || columnInfo.GetIsElementLevel() {
		return false
	}
	if !isPredicateDeleteScalarType(columnInfo.GetDataType()) {
		return false
	}

	for _, field := range schema.GetFields() {
		if field.GetFieldID() != columnInfo.GetFieldId() {
			continue
		}
		if field.GetIsDynamic() || field.GetDataType() != columnInfo.GetDataType() {
			return false
		}
		return isPredicateDeleteScalarType(field.GetDataType())
	}
	return false
}

func isPredicateDeleteScalarType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_String,
		schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}

func isPredicateDeleteLiteralSupported(value *planpb.GenericValue) bool {
	if value == nil {
		return false
	}
	switch value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal,
		*planpb.GenericValue_Int64Val,
		*planpb.GenericValue_FloatVal,
		*planpb.GenericValue_StringVal:
		return true
	default:
		return false
	}
}

// nonSimpleDelete uses predicate delete only when it is enabled, supported, and the matched row count is above the threshold.
// Otherwise it keeps the existing PK-delete path for non-simple expressions.
func (dr *deleteRunner) nonSimpleDelete(ctx context.Context, plan *planpb.PlanNode) error {
	// Allocate a single MVCC delete timestamp for the whole non-simple delete so the
	// count probe and the chosen path (complexDelete or predicateDelete) all use the
	// same ts ("count at T, delete at T").
	deleteTs, err := dr.tsoAllocatorIns.AllocOne(ctx)
	if err != nil {
		return err
	}
	// Predicate delete is only applied under the StorageV3 format (common.storage.useLoonFFI)
	// and behind the feature flag; fall back to the PK-delete path when either is off.
	if !paramtable.Get().CommonCfg.EnablePredicateDelete.GetAsBool() ||
		!paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		if err = dr.complexDelete(ctx, plan, deleteTs); err != nil {
			mlog.Warn(ctx, "complex delete failed,but delete some data", mlog.Int64("count", dr.result.DeleteCnt), mlog.String("expr", dr.req.GetExpr()))
			return err
		}
		return nil
	}
	if !canUsePredicateDelete(plan, dr.schema.CollectionSchema) {
		mlog.Info(ctx, "predicate delete falls back to pk delete for unsupported expression", mlog.String("expr", dr.req.GetExpr()))
		if err = dr.complexDelete(ctx, plan, deleteTs); err != nil {
			mlog.Warn(ctx, "complex delete failed,but delete some data", mlog.Int64("count", dr.result.DeleteCnt), mlog.String("expr", dr.req.GetExpr()))
			return err
		}
		return nil
	}

	matchedCount, err := dr.countPredicateDeleteHits(ctx, plan, deleteTs)
	if err != nil {
		// The count probe is only a path-selection signal; the request is still fully
		// serviceable via complexDelete. A transient QueryNode or partial-shard error
		// must not turn a working delete into a user-visible failure, so fall back to
		// the PK-delete path. Real context cancellation/timeout is still propagated.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		mlog.Warn(ctx, "predicate delete count probe failed, falling back to pk delete",
			mlog.String("expr", dr.req.GetExpr()), mlog.Err(err))
		if err = dr.complexDelete(ctx, plan, deleteTs); err != nil {
			mlog.Warn(ctx, "complex delete failed,but delete some data", mlog.Int64("count", dr.result.DeleteCnt), mlog.String("expr", dr.req.GetExpr()))
			return err
		}
		return nil
	}
	threshold := paramtable.Get().CommonCfg.PredicateDeleteHitCountThreshold.GetAsInt64()
	if matchedCount <= threshold {
		mlog.Info(ctx, "predicate delete falls back to pk delete",
			mlog.Int64("matchedCount", matchedCount),
			mlog.Int64("threshold", threshold),
			mlog.String("expr", dr.req.GetExpr()))
		if err = dr.complexDelete(ctx, plan, deleteTs); err != nil {
			mlog.Warn(ctx, "complex delete failed,but delete some data", mlog.Int64("count", dr.result.DeleteCnt), mlog.String("expr", dr.req.GetExpr()))
			return err
		}
		return nil
	}

	if err = dr.predicateDelete(ctx, plan, matchedCount, deleteTs); err != nil {
		mlog.Warn(ctx, "predicate delete failed", mlog.String("expr", dr.req.GetExpr()), mlog.Err(err))
		return err
	}
	return nil
}

func (dr *deleteRunner) complexDelete(ctx context.Context, plan *planpb.PlanNode, ts uint64) error {
	rc := timerecord.NewTimeRecorder("QueryStreamDelete")
	var err error

	dr.ts = ts

	dr.msgID, err = dr.idAllocator.AllocOne()
	if err != nil {
		return err
	}

	channelName, useNamespaceChannel, err := namespaceShardingChannel(dr.schema.CollectionSchema, dr.req.Namespace, dr.vChannels)
	if err != nil {
		return err
	}
	if useNamespaceChannel {
		err = dr.lb.ExecuteWithRetry(ctx, shardclient.ChannelWorkload{
			Db:             dr.req.GetDbName(),
			CollectionName: dr.req.GetCollectionName(),
			CollectionID:   dr.collectionID,
			Channel:        channelName,
			Nq:             1,
			Exec:           dr.getStreamingQueryAndDelteFunc(plan),
		})
	} else {
		err = dr.lb.Execute(ctx, shardclient.CollectionWorkLoad{
			Db:             dr.req.GetDbName(),
			CollectionName: dr.req.GetCollectionName(),
			CollectionID:   dr.collectionID,
			Nq:             1,
			Exec:           dr.getStreamingQueryAndDelteFunc(plan),
		})
	}
	dr.result.DeleteCnt = dr.count.Load()
	dr.result.Timestamp = dr.sessionTS.Load()
	if err != nil {
		mlog.Warn(ctx, "fail to execute complex delete",
			mlog.Int64("deleteCnt", dr.result.GetDeleteCnt()),
			mlog.Duration("interval", rc.ElapseSpan()),
			mlog.Err(err))
		return err
	}
	mlog.Info(ctx, "complex delete finished", mlog.Int64("deleteCnt", dr.result.GetDeleteCnt()), mlog.Duration("interval", rc.ElapseSpan()))
	return nil
}

// predicateDelete appends predicate-delete WAL messages. deleteTs is the shared MVCC delete
// timestamp allocated by nonSimpleDelete (same ts used by the count probe). matchedCount is the
// probe-time hit count and is reported as the affected-rows estimate; the physical delete is
// applied asynchronously by the consumer side.
func (dr *deleteRunner) predicateDelete(ctx context.Context, plan *planpb.PlanNode, matchedCount int64, deleteTs uint64) error {
	serializedPlan, err := proto.Marshal(plan)
	if err != nil {
		return err
	}

	dr.ts = deleteTs

	// Allocate a msgID for parity with complexDelete/simpleDelete so any post-run
	// consumer of dr.msgID (logs, metrics, audit) observes a real value on the
	// predicate path too, instead of a zero value only for predicate deletes.
	dr.msgID, err = dr.idAllocator.AllocOne()
	if err != nil {
		return err
	}

	partitionID := common.AllPartitionsID
	if len(dr.partitionIDs) == 1 {
		partitionID = dr.partitionIDs[0]
	}

	sessionTS, err := appendPredicateDeleteMessages(
		ctx,
		dr.collectionID,
		dr.req.GetCollectionName(),
		partitionID,
		dr.req.GetPartitionName(),
		dr.req.GetDbName(),
		dr.vChannels,
		dr.idAllocator,
		dr.ts,
		serializedPlan,
	)
	if err != nil {
		return err
	}

	dr.result.DeleteCnt = matchedCount
	dr.result.Timestamp = sessionTS
	return nil
}

func (dr *deleteRunner) countPredicateDeleteHits(ctx context.Context, plan *planpb.PlanNode, queryTs uint64) (int64, error) {
	countPlan, ok := proto.Clone(plan).(*planpb.PlanNode)
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg("failed to clone delete plan for predicate delete count")
	}
	queryPlan := countPlan.GetQuery()
	if queryPlan == nil {
		return 0, merr.WrapErrServiceInternalMsg("predicate delete count requires a query plan")
	}
	queryPlan.IsCount = false
	queryPlan.Aggregates = []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 0}}
	countPlan.OutputFieldIds = nil

	serializedPlan, err := proto.Marshal(countPlan)
	if err != nil {
		return 0, merr.WrapErrServiceInternalErr(err, "failed to serialize predicate delete count plan")
	}

	msgID, err := dr.idAllocator.AllocOne()
	if err != nil {
		return 0, err
	}

	var matchedCount atomic.Int64
	err = dr.lb.Execute(ctx, shardclient.CollectionWorkLoad{
		Db:             dr.req.GetDbName(),
		CollectionName: dr.req.GetCollectionName(),
		CollectionID:   dr.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
			result, err := qn.Query(ctx, &querypb.QueryRequest{
				Req: &internalpb.RetrieveRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
						commonpbutil.WithMsgID(msgID),
						commonpbutil.WithSourceID(paramtable.GetNodeID()),
						commonpbutil.WithTargetID(nodeID),
					),
					MvccTimestamp:      queryTs,
					ReqID:              paramtable.GetNodeID(),
					DbID:               0, // TODO
					CollectionID:       dr.collectionID,
					PartitionIDs:       dr.partitionIDs,
					SerializedExprPlan: serializedPlan,
					GuaranteeTimestamp: parseGuaranteeTsFromConsistency(queryTs, queryTs, dr.req.GetConsistencyLevel()),
					QueryLabel:         metrics.DeleteQueryLabel,
					Aggregates:         queryPlan.GetAggregates(),
				},
				DmlChannels: []string{channel},
				Scope:       querypb.DataScope_All,
			})
			if err != nil {
				return err
			}
			if result == nil {
				return merr.WrapErrServiceInternalMsg("predicate delete count query returned nil result")
			}
			if err := merr.Error(result.GetStatus()); err != nil {
				return err
			}
			count, err := funcutil.CntOfInternalResult(result)
			if err != nil {
				return merr.Wrap(err, "failed to parse predicate delete count result")
			}
			matchedCount.Add(count)
			return nil
		},
	})
	if err != nil {
		return 0, err
	}
	return matchedCount.Load(), nil
}

func (dr *deleteRunner) simpleDelete(ctx context.Context, pk *schemapb.IDs, numRow int64) error {
	partitionID := common.AllPartitionsID
	if len(dr.partitionIDs) == 1 {
		partitionID = dr.partitionIDs[0]
	}
	mlog.Debug(ctx, "get primary keys from expr",
		mlog.Int64("len of primary keys", numRow),
		mlog.FieldCollectionID(dr.collectionID),
		mlog.FieldPartitionID(partitionID))

	task, err := dr.produce(ctx, pk, partitionID)
	if err != nil {
		mlog.Warn(ctx, "produce delete task failed")
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
		return pks, merr.WrapErrParameterInvalidMsg("invalid field data type specifyed in simple delete expr")
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
		return pks, 0, merr.WrapErrParameterInvalidMsg("invalid field data type specifyed in simple delete expr")
	}

	return pks, pkCount, nil
}
