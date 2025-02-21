package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	WithCache    = true
	WithoutCache = false
)

const (
	RetrieveTaskName = "RetrieveTask"
	QueryTaskName    = "QueryTask"
)

type queryTask struct {
	baseTask
	Condition
	*internalpb.RetrieveRequest

	ctx            context.Context
	result         *milvuspb.QueryResults
	request        *milvuspb.QueryRequest
	qc             types.QueryCoordClient
	ids            *schemapb.IDs
	collectionName string
	queryParams    *queryParams
	schema         *schemaInfo

	userOutputFields  []string
	userDynamicFields []string

	resultBuf *typeutil.ConcurrentSet[*internalpb.RetrieveResults]

	plan             *planpb.PlanNode
	partitionKeyMode bool
	lb               LBPolicy
	channelsMvcc     map[string]Timestamp
	fastSkip         bool

	reQuery              bool
	allQueryCnt          int64
	totalRelatedDataSize int64
	mustUsePartitionKey  bool
}

type queryParams struct {
	limit        int64
	offset       int64
	reduceType   reduce.IReduceType
	isIterator   bool
	collectionID int64
}

// translateToOutputFieldIDs translates output fields name to output fields id.
func translateToOutputFieldIDs(outputFields []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	if len(outputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID && !typeutil.IsVectorType(field.DataType) {
				outputFieldIDs = append(outputFieldIDs, field.FieldID)
			}
		}
	} else {
		var pkFieldID UniqueID
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkFieldID = field.FieldID
			}
		}
		for _, reqField := range outputFields {
			var fieldFound bool
			for _, field := range schema.Fields {
				if reqField == field.Name {
					outputFieldIDs = append(outputFieldIDs, field.FieldID)
					fieldFound = true
					break
				}
			}
			if !fieldFound {
				return nil, fmt.Errorf("field %s not exist", reqField)
			}
		}

		// pk field needs to be in output field list
		var pkFound bool
		for _, outputField := range outputFieldIDs {
			if outputField == pkFieldID {
				pkFound = true
				break
			}
		}

		if !pkFound {
			outputFieldIDs = append(outputFieldIDs, pkFieldID)
		}
	}
	return outputFieldIDs, nil
}

func filterSystemFields(outputFieldIDs []UniqueID) []UniqueID {
	filtered := make([]UniqueID, 0, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if !common.IsSystemField(outputFieldID) {
			filtered = append(filtered, outputFieldID)
		}
	}
	return filtered
}

// parseQueryParams get limit and offset from queryParamsPair, both are optional.
func parseQueryParams(queryParamsPair []*commonpb.KeyValuePair) (*queryParams, error) {
	var (
		limit             int64
		offset            int64
		reduceStopForBest bool
		isIterator        bool
		err               error
		collectionID      int64
	)
	reduceStopForBestStr, err := funcutil.GetAttrByKeyFromRepeatedKV(ReduceStopForBestKey, queryParamsPair)
	// if reduce_stop_for_best is provided
	if err == nil {
		reduceStopForBest, err = strconv.ParseBool(reduceStopForBestStr)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("true or false", reduceStopForBestStr,
				"value for reduce_stop_for_best is invalid")
		}
	}

	isIteratorStr, err := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, queryParamsPair)
	// if reduce_stop_for_best is provided
	if err == nil {
		isIterator, err = strconv.ParseBool(isIteratorStr)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("true or false", isIteratorStr,
				"value for iterator field is invalid")
		}
	}

	collectionIdStr, err := funcutil.GetAttrByKeyFromRepeatedKV(CollectionID, queryParamsPair)
	if err == nil {
		collectionID, err = strconv.ParseInt(collectionIdStr, 0, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("int value for collection_id", CollectionID,
				"value for collection id is invalid")
		}
	}

	reduceType := reduce.IReduceNoOrder
	if isIterator {
		if reduceStopForBest {
			reduceType = reduce.IReduceInOrderForBest
		} else {
			reduceType = reduce.IReduceInOrder
		}
	}

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	// if limit is not provided
	if err != nil {
		return &queryParams{limit: typeutil.Unlimited, reduceType: reduceType, isIterator: isIterator}, nil
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair)
	// if offset is provided
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	// validate max result window.
	if err = validateMaxQueryResultWindow(offset, limit); err != nil {
		return nil, fmt.Errorf("invalid max query result window, %w", err)
	}

	return &queryParams{
		limit:        limit,
		offset:       offset,
		reduceType:   reduceType,
		isIterator:   isIterator,
		collectionID: collectionID,
	}, nil
}

func matchCountRule(outputs []string) bool {
	return len(outputs) == 1 && strings.ToLower(strings.TrimSpace(outputs[0])) == "count(*)"
}

func createCntPlan(expr string, schemaHelper *typeutil.SchemaHelper, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, error) {
	if expr == "" {
		return &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: nil,
					IsCount:    true,
				},
			},
		}, nil
	}
	start := time.Now()
	plan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, exprTemplateValues)
	if err != nil {
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
		return nil, merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	plan.Node.(*planpb.PlanNode_Query).Query.IsCount = true

	return plan, nil
}

func (t *queryTask) createPlan(ctx context.Context) error {
	schema := t.schema

	cntMatch := matchCountRule(t.request.GetOutputFields())
	if cntMatch {
		var err error
		t.plan, err = createCntPlan(t.request.GetExpr(), schema.schemaHelper, t.request.GetExprTemplateValues())
		t.userOutputFields = []string{"count(*)"}
		return err
	}

	var err error
	if t.plan == nil {
		start := time.Now()
		t.plan, err = planparserv2.CreateRetrievePlan(schema.schemaHelper, t.request.Expr, t.request.GetExprTemplateValues())
		if err != nil {
			metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
		}
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	}

	t.request.OutputFields, t.userOutputFields, t.userDynamicFields, err = translateOutputFields(t.request.OutputFields, t.schema, true)
	if err != nil {
		return err
	}

	outputFieldIDs, err := translateToOutputFieldIDs(t.request.GetOutputFields(), schema.CollectionSchema)
	if err != nil {
		return err
	}
	outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
	t.RetrieveRequest.OutputFieldsId = outputFieldIDs
	t.plan.OutputFieldIds = outputFieldIDs
	t.plan.DynamicFields = t.userDynamicFields
	log.Ctx(ctx).Debug("translate output fields to field ids",
		zap.Int64s("OutputFieldsID", t.OutputFieldsId),
		zap.String("requestType", "query"))

	return nil
}

func (t *queryTask) CanSkipAllocTimestamp() bool {
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	if !useDefaultConsistency {
		// legacy SDK & resultful behavior
		if t.request.GetConsistencyLevel() == commonpb.ConsistencyLevel_Strong && t.request.GetGuaranteeTimestamp() > 0 {
			return true
		}
		consistencyLevel = t.request.GetConsistencyLevel()
	} else {
		collID, err := globalMetaCache.GetCollectionID(context.Background(), t.request.GetDbName(), t.request.GetCollectionName())
		if err != nil { // err is not nil if collection not exists
			log.Ctx(t.ctx).Warn("query task get collectionID failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}

		collectionInfo, err2 := globalMetaCache.GetCollectionInfo(context.Background(), t.request.GetDbName(), t.request.GetCollectionName(), collID)
		if err2 != nil {
			log.Ctx(t.ctx).Warn("query task get collection info failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}
		consistencyLevel = collectionInfo.consistencyLevel
	}
	return consistencyLevel != commonpb.ConsistencyLevel_Strong
}

func (t *queryTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName

	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName),
		zap.Strings("partitionNames", t.request.GetPartitionNames()),
		zap.String("requestType", "query"))

	if err := validateCollectionName(collectionName); err != nil {
		log.Warn("Invalid collectionName.")
		return err
	}
	log.Debug("Validate collectionName.")

	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("Failed to get collection id.", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	t.CollectionID = collID
	log.Debug("Get collection ID by name", zap.Int64("collectionID", t.CollectionID))

	t.partitionKeyMode, err = isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed", zap.Int64("collectionID", t.CollectionID), zap.Error(err))
		return err
	}
	if t.partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if partition key mode is used"))
	}
	if t.mustUsePartitionKey && !t.partitionKeyMode {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("must use partition key in the query request " +
			"because the mustUsePartitionKey config is true"))
	}

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Warn("invalid partition name", zap.String("partition name", tag))
			return err
		}
	}
	log.Debug("Validate partition names.")

	// fetch search_growing from query param
	if t.RetrieveRequest.IgnoreGrowing, err = isIgnoreGrowing(t.request.GetQueryParams()); err != nil {
		return err
	}

	queryParams, err := parseQueryParams(t.request.GetQueryParams())
	if err != nil {
		return err
	}
	if queryParams.collectionID > 0 && queryParams.collectionID != t.GetCollectionID() {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("Input collection id is not consistent to collectionID in the context," +
			"alias or database may have changed"))
	}
	if queryParams.reduceType == reduce.IReduceInOrderForBest {
		t.RetrieveRequest.ReduceStopForBest = true
	}
	t.RetrieveRequest.ReduceType = int32(queryParams.reduceType)

	t.queryParams = queryParams
	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), t.collectionName)
	if err != nil {
		log.Warn("get collection schema failed", zap.Error(err))
		return err
	}
	t.schema = schema

	if t.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		t.request.Expr = IDs2Expr(pkField, t.ids)
	}

	if err := t.createPlan(ctx); err != nil {
		return err
	}
	t.plan.Node.(*planpb.PlanNode_Query).Query.Limit = t.RetrieveRequest.Limit

	if planparserv2.IsAlwaysTruePlan(t.plan) && t.RetrieveRequest.Limit == typeutil.Unlimited {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("empty expression should be used with limit"))
	}

	// convert partition names only when requery is false
	if !t.reQuery {
		partitionNames := t.request.GetPartitionNames()
		if t.partitionKeyMode {
			expr, err := exprutil.ParseExprFromPlan(t.plan)
			if err != nil {
				return err
			}
			partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
			hashedPartitionNames, err := assignPartitionKeys(ctx, t.request.GetDbName(), t.request.CollectionName, partitionKeys)
			if err != nil {
				return err
			}

			partitionNames = append(partitionNames, hashedPartitionNames...)
		}
		t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, t.request.GetDbName(), t.request.CollectionName, partitionNames)
		if err != nil {
			return err
		}
	}

	// count with pagination
	if t.plan.GetQuery().GetIsCount() && t.queryParams.limit != typeutil.Unlimited {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("count entities with pagination is not allowed"))
	}

	t.RetrieveRequest.IsCount = t.plan.GetQuery().GetIsCount()
	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(t.plan)
	if err != nil {
		return err
	}

	// Set username for this query request,
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		t.RetrieveRequest.Username = username
	}

	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err2 != nil {
		log.Warn("Proxy::queryTask::PreExecute failed to GetCollectionInfo from cache",
			zap.String("collectionName", collectionName), zap.Int64("collectionID", t.CollectionID),
			zap.Error(err2))
		return err2
	}

	guaranteeTs := t.request.GetGuaranteeTimestamp()
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	if useDefaultConsistency {
		consistencyLevel = collectionInfo.consistencyLevel
		guaranteeTs = parseGuaranteeTsFromConsistency(guaranteeTs, t.BeginTs(), consistencyLevel)
	} else {
		consistencyLevel = t.request.GetConsistencyLevel()
		// Compatibility logic, parse guarantee timestamp
		if consistencyLevel == 0 && guaranteeTs > 0 {
			guaranteeTs = parseGuaranteeTs(guaranteeTs, t.BeginTs())
		} else {
			// parse from guarantee timestamp and user input consistency level
			guaranteeTs = parseGuaranteeTsFromConsistency(guaranteeTs, t.BeginTs(), consistencyLevel)
		}
	}
	t.GuaranteeTimestamp = guaranteeTs
	// need modify mvccTs and guaranteeTs for iterator specially
	if t.queryParams.isIterator && t.request.GetGuaranteeTimestamp() > 0 {
		t.MvccTimestamp = t.request.GetGuaranteeTimestamp()
		t.GuaranteeTimestamp = t.request.GetGuaranteeTimestamp()
	}

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // TODO
	log.Debug("Query PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("mvcc_ts", t.GetMvccTimestamp()),
		zap.Uint64("timeout_ts", t.GetTimeoutTimestamp()))
	return nil
}

func (t *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")
	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", "query"))

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.RetrieveResults]()
	err := t.lb.Execute(ctx, CollectionWorkLoad{
		db:             t.request.GetDbName(),
		collectionID:   t.CollectionID,
		collectionName: t.collectionName,
		nq:             1,
		exec:           t.queryShard,
	})
	if err != nil {
		log.Warn("fail to execute query", zap.Error(err))
		return errors.Wrap(err, "failed to query")
	}

	log.Debug("Query Execute done.")
	return nil
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", "query"))

	var err error

	toReduceResults := make([]*internalpb.RetrieveResults, 0)
	t.allQueryCnt = 0
	t.totalRelatedDataSize = 0
	select {
	case <-t.TraceCtx().Done():
		log.Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Debug("all queries are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.RetrieveResults) bool {
			toReduceResults = append(toReduceResults, res)
			t.allQueryCnt += res.GetAllRetrieveCount()
			t.totalRelatedDataSize += res.GetCostAggregation().GetTotalRelatedDataSize()
			log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")

	reducer := createMilvusReducer(ctx, t.queryParams, t.RetrieveRequest, t.schema.CollectionSchema, t.plan, t.collectionName)

	t.result, err = reducer.Reduce(toReduceResults)
	if err != nil {
		log.Warn("fail to reduce query result", zap.Error(err))
		return err
	}
	t.result.OutputFields = t.userOutputFields
	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}
	t.result.PrimaryFieldName = primaryFieldSchema.GetName()
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	if t.queryParams.isIterator && t.request.GetGuaranteeTimestamp() == 0 {
		// first page for iteration, need to set up sessionTs for iterator
		t.result.SessionTs = getMaxMvccTsFromChannels(t.channelsMvcc, t.BeginTs())
	}
	log.Debug("Query PostExecute done")
	return nil
}

func (t *queryTask) IsSubTask() bool {
	return t.reQuery
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	needOverrideMvcc := false
	mvccTs := t.MvccTimestamp
	if len(t.channelsMvcc) > 0 {
		mvccTs, needOverrideMvcc = t.channelsMvcc[channel]
		// In fast mode, if there is no corresponding channel in channelsMvcc, quickly skip this query.
		if !needOverrideMvcc && t.fastSkip {
			return nil
		}
	}

	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	if needOverrideMvcc && mvccTs > 0 {
		retrieveReq.MvccTimestamp = mvccTs
		retrieveReq.GuaranteeTimestamp = mvccTs
	}
	retrieveReq.ConsistencyLevel = t.ConsistencyLevel
	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: []string{channel},
		Scope:       querypb.DataScope_All,
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channel))

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Warn("QueryNode query return error", zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode query result error", zap.Any("errorCode", result.GetStatus().GetErrorCode()), zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to Query on QueryNode %d", nodeID)
	}

	log.Debug("get query result")
	t.resultBuf.Insert(result)
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)
	return nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids *schemapb.IDs) string {
	var idsStr string
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		idsStr = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids.GetIntId().GetData())), ", "), "[]")
	case *schemapb.IDs_StrId:
		strs := lo.Map(ids.GetStrId().GetData(), func(str string, _ int) string {
			return fmt.Sprintf("\"%s\"", str)
		})
		idsStr = strings.Trim(strings.Join(strs, ", "), "[]")
	}

	return fieldName + " in [ " + idsStr + " ]"
}

func reduceRetrieveResults(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams) (*milvuspb.QueryResults, error) {
	log.Ctx(ctx).Debug("reduceInternalRetrieveResults", zap.Int("len(retrieveResults)", len(retrieveResults)))
	var (
		ret     = &milvuspb.QueryResults{}
		loopEnd int
	)

	validRetrieveResults := []*internalpb.RetrieveResults{}
	for _, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	cursors := make([]int64, len(validRetrieveResults))

	if queryParams != nil && queryParams.limit != typeutil.Unlimited {
		// IReduceInOrderForBest will try to get as many results as possible
		// so loopEnd in this case will be set to the sum of all results' size
		// to get as many qualified results as possible
		if reduce.ShouldUseInputLimit(queryParams.reduceType) {
			loopEnd = int(queryParams.limit)
		}
	}

	// handle offset
	if queryParams != nil && queryParams.offset > 0 {
		for i := int64(0); i < queryParams.offset; i++ {
			sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
			if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
				return ret, nil
			}
			cursors[sel]++
		}
	}

	ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].GetFieldsData(), int64(loopEnd))
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd; j++ {
		sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
			break
		}
		retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])

		// limit retrieve result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
		}

		cursors[sel]++
	}

	return ret, nil
}

func reduceRetrieveResultsAndFillIfEmpty(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams, outputFieldsID []int64, schema *schemapb.CollectionSchema) (*milvuspb.QueryResults, error) {
	result, err := reduceRetrieveResults(ctx, retrieveResults, queryParams)
	if err != nil {
		return nil, err
	}

	// filter system fields.
	filtered := filterSystemFields(outputFieldsID)
	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewMilvusResult(result), filtered, schema); err != nil {
		return nil, fmt.Errorf("failed to fill retrieve results: %s", err.Error())
	}

	return result, nil
}

func (t *queryTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *queryTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *queryTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *queryTask) Name() string {
	return RetrieveTaskName
}

func (t *queryTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *queryTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) SetTs(ts Timestamp) {
	if t.reQuery && t.Base.Timestamp != 0 {
		return
	}
	t.Base.Timestamp = ts
}

func (t *queryTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}
