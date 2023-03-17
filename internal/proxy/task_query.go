package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/planpb"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	Condition
	*internalpb.RetrieveRequest

	ctx            context.Context
	result         *milvuspb.QueryResults
	request        *milvuspb.QueryRequest
	qc             types.QueryCoord
	ids            *schemapb.IDs
	collectionName string
	queryParams    *queryParams
	schema         *schemapb.CollectionSchema

	resultBuf       chan *internalpb.RetrieveResults
	toReduceResults []*internalpb.RetrieveResults

	queryShardPolicy pickShardPolicy
	shardMgr         *shardClientMgr

	plan *planpb.PlanNode
}

type queryParams struct {
	limit  int64
	offset int64
}

// translateOutputFields translates output fields name to output fields id.
func translateToOutputFieldIDs(outputFields []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	if len(outputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID && field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
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
		limit  int64
		offset int64
		err    error
	)

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	// if limit is not provided
	if err != nil {
		return &queryParams{limit: typeutil.Unlimited}, nil
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}
	if limit != 0 {
		if err := validateLimit(limit); err != nil {
			return nil, fmt.Errorf("%s [%d] is invalid, %w", LimitKey, limit, err)
		}
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair)
	// if offset is provided
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}

		if offset != 0 {
			if err := validateLimit(offset); err != nil {
				return nil, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
			}
		}
	}

	if err = validateLimit(limit + offset); err != nil {
		return nil, fmt.Errorf("invalid limit[%d] + offset[%d], %w", limit, offset, err)
	}

	return &queryParams{
		limit:  limit,
		offset: offset,
	}, nil
}

func matchCountRule(outputs []string) bool {
	return len(outputs) == 1 && strings.ToLower(strings.TrimSpace(outputs[0])) == "count(*)"
}

func createCntPlan(expr string, schema *schemapb.CollectionSchema) (*planpb.PlanNode, error) {
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

	plan, err := planparserv2.CreateRetrievePlan(schema, expr)
	if err != nil {
		return nil, err
	}

	plan.Node.(*planpb.PlanNode_Query).Query.IsCount = true

	return plan, nil
}

func (t *queryTask) createPlan(ctx context.Context) error {
	schema := t.schema

	cntMatch := matchCountRule(t.request.GetOutputFields())
	if cntMatch {
		var err error
		t.plan, err = createCntPlan(t.request.GetExpr(), schema)
		return err
	}

	if t.request.Expr == "" {
		return fmt.Errorf("query expression is empty")
	}

	plan, err := planparserv2.CreateRetrievePlan(schema, t.request.Expr)
	if err != nil {
		return err
	}

	t.request.OutputFields, err = translateOutputFields(t.request.OutputFields, schema, true)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Debug("translate output fields",
		zap.Strings("OutputFields", t.request.OutputFields),
		zap.String("requestType", "query"))

	outputFieldIDs, err := translateToOutputFieldIDs(t.request.GetOutputFields(), schema)
	if err != nil {
		return err
	}
	outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
	t.RetrieveRequest.OutputFieldsId = outputFieldIDs
	plan.OutputFieldIds = outputFieldIDs
	t.plan = plan
	log.Ctx(ctx).Debug("translate output fields to field ids",
		zap.Int64s("OutputFieldsID", t.OutputFieldsId),
		zap.String("requestType", "query"))

	return nil
}

func (t *queryTask) PreExecute(ctx context.Context) error {
	if t.queryShardPolicy == nil {
		t.queryShardPolicy = mergeRoundRobinPolicy
	}

	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Ctx(ctx).Warn("Invalid collectionName.",
			zap.String("collectionName", collectionName),
			zap.String("requestType", "query"))
		return err
	}

	log.Ctx(ctx).Debug("Validate collectionName.",
		zap.Any("collectionName", collectionName),
		zap.Any("requestType", "query"))

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("Failed to get collection id.",
			zap.Any("collectionName", collectionName),
			zap.Any("requestType", "query"))
		return err
	}

	t.CollectionID = collID
	log.Ctx(ctx).Debug("Get collection ID by name",
		zap.Int64("collectionID", t.CollectionID),
		zap.String("collectionName", collectionName),
		zap.Any("requestType", "query"))

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Ctx(ctx).Warn("invalid partition name",
				zap.String("partition name", tag),
				zap.Any("requestType", "query"))
			return err
		}
	}
	log.Ctx(ctx).Debug("Validate partition names.",
		zap.Any("requestType", "query"))

	t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, t.request.GetPartitionNames())
	if err != nil {
		log.Ctx(ctx).Warn("failed to get partitions in collection.", zap.String("collectionName", collectionName),
			zap.Error(err),
			zap.Any("requestType", "query"))
		return err
	}
	log.Ctx(ctx).Debug("Get partitions in collection.",
		zap.Any("collectionName", collectionName),
		zap.Any("requestType", "query"))

	//fetch search_growing from search param
	var ignoreGrowing bool
	for i, kv := range t.request.GetQueryParams() {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err = strconv.ParseBool(kv.Value)
			if err != nil {
				return errors.New("parse search growing failed")
			}
			t.request.QueryParams = append(t.request.GetQueryParams()[:i], t.request.GetQueryParams()[i+1:]...)
			break
		}
	}
	t.RetrieveRequest.IgnoreGrowing = ignoreGrowing

	queryParams, err := parseQueryParams(t.request.GetQueryParams())
	if err != nil {
		return err
	}
	t.queryParams = queryParams
	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

	loaded, err := checkIfLoaded(ctx, t.qc, collectionName, t.RetrieveRequest.GetPartitionIDs())
	if err != nil {
		return fmt.Errorf("checkIfLoaded failed when query, collection:%v, partitions:%v, err = %s", collectionName, t.request.GetPartitionNames(), err)
	}
	if !loaded {
		return fmt.Errorf("collection:%v or partition:%v not loaded into memory when query", collectionName, t.request.GetPartitionNames())
	}

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, collectionName)
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

	if t.plan.GetQuery().GetIsCount() {
		t.RetrieveRequest.IsCount = true
	}

	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(t.plan)
	if err != nil {
		return err
	}

	if t.request.TravelTimestamp == 0 {
		t.TravelTimestamp = t.BeginTs()
	} else {
		t.TravelTimestamp = t.request.TravelTimestamp
	}

	err = validateTravelTimestamp(t.TravelTimestamp, t.BeginTs())
	if err != nil {
		return err
	}

	guaranteeTs := t.request.GetGuaranteeTimestamp()
	t.GuaranteeTimestamp = parseGuaranteeTs(guaranteeTs, t.BeginTs())

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // TODO
	log.Ctx(ctx).Debug("Query PreExecute done.",
		zap.Any("requestType", "query"),
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("travel_ts", t.GetTravelTimestamp()),
		zap.Uint64("timeout_ts", t.GetTimeoutTimestamp()))
	return nil
}

func (t *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")
	log := log.Ctx(ctx)

	executeQuery := func(withCache bool) error {
		shards, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName)
		if err != nil {
			return err
		}
		t.resultBuf = make(chan *internalpb.RetrieveResults, len(shards))
		t.toReduceResults = make([]*internalpb.RetrieveResults, 0, len(shards))

		if err := t.queryShardPolicy(ctx, t.shardMgr, t.queryShard, shards); err != nil {
			return err
		}
		return nil
	}

	err := executeQuery(WithCache)
	if err != nil {
		log.Warn("invalid shard leaders cache, updating shardleader caches and retry query",
			zap.Error(err))
		// invalidate cache first, since ctx may be canceled or timeout here
		globalMetaCache.DeprecateShardCache(t.collectionName)
		err = executeQuery(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to query on all shard leaders, err=%s", err.Error())
	}

	log.Debug("Query Execute done.",
		zap.String("requestType", "query"))
	return nil
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	var err error

	select {
	case <-t.TraceCtx().Done():
		log.Ctx(ctx).Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Ctx(ctx).Debug("all queries are finished or canceled")
		close(t.resultBuf)
		for res := range t.resultBuf {
			t.toReduceResults = append(t.toReduceResults, res)
			log.Ctx(ctx).Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()))
		}
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")

	reducer := createMilvusReducer(ctx, t.queryParams, t.RetrieveRequest, t.schema, t.plan, t.collectionName)

	t.result, err = reducer.Reduce(t.toReduceResults)
	if err != nil {
		return err
	}
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	log.Ctx(ctx).Debug("Query PostExecute done",
		zap.String("requestType", "query"))
	return nil
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs []string) error {
	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: channelIDs,
		Scope:       querypb.DataScope_All,
	}

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode query return error",
			zap.Int64("nodeID", nodeID),
			zap.Strings("channels", channelIDs), zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader", zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs))
		globalMetaCache.DeprecateShardCache(t.collectionName)
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode query result error",
			zap.Int64("nodeID", nodeID),
			zap.String("reason", result.GetStatus().GetReason()))
		return fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason())
	}

	log.Ctx(ctx).Debug("get query result",
		zap.Int64("nodeID", nodeID),
		zap.Strings("channelIDs", channelIDs))
	t.resultBuf <- result
	return nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids *schemapb.IDs) string {
	var idsStr string
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		idsStr = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids.GetIntId().GetData())), ", "), "[]")
	case *schemapb.IDs_StrId:
		idsStr = strings.Trim(strings.Join(ids.GetStrId().GetData(), ", "), "[]")
	}

	return fieldName + " in [ " + idsStr + " ]"
}

func reduceRetrieveResults(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams) (*milvuspb.QueryResults, error) {
	log.Ctx(ctx).Debug("reduceInternelRetrieveResults", zap.Int("len(retrieveResults)", len(retrieveResults)))
	var (
		ret = &milvuspb.QueryResults{}

		skipDupCnt int64
		loopEnd    int
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

	ret.FieldsData = make([]*schemapb.FieldData, len(validRetrieveResults[0].GetFieldsData()))
	idSet := make(map[interface{}]struct{})
	cursors := make([]int64, len(validRetrieveResults))

	if queryParams != nil && queryParams.limit != typeutil.Unlimited {
		loopEnd = int(queryParams.limit)

		if queryParams.offset > 0 {
			for i := int64(0); i < queryParams.offset; i++ {
				sel := typeutil.SelectMinPK(validRetrieveResults, cursors)
				if sel == -1 {
					return ret, nil
				}
				cursors[sel]++
			}
		}
	}

	for j := 0; j < loopEnd; j++ {
		sel := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		if _, ok := idSet[pk]; !ok {
			typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])
			idSet[pk] = struct{}{}
		} else {
			// primary keys duplicate
			skipDupCnt++
		}
		cursors[sel]++
	}

	if skipDupCnt > 0 {
		log.Ctx(ctx).Debug("skip duplicated query result while reducing QueryResults", zap.Int64("count", skipDupCnt))
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
	if err := typeutil.FillRetrieveResultIfEmpty(typeutil.NewMilvusResult(result), filtered, schema); err != nil {
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
	t.Base.Timestamp = ts
}

func (t *queryTask) OnEnqueue() error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}
