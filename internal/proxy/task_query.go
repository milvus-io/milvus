package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/samber/lo"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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

	userOutputFields []string

	resultBuf *typeutil.ConcurrentSet[*internalpb.RetrieveResults]

	plan             *planpb.PlanNode
	partitionKeyMode bool
	lb               LBPolicy
}

type queryParams struct {
	limit  int64
	offset int64
}

// translateToOutputFieldIDs translates output fields name to output fields id.
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
		if err := validateTopKLimit(limit); err != nil {
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
			if err := validateTopKLimit(offset); err != nil {
				return nil, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
			}
		}
	}

	if err = validateTopKLimit(limit + offset); err != nil {
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

	t.request.OutputFields, t.userOutputFields, err = translateOutputFields(t.request.OutputFields, schema, true)
	if err != nil {
		return err
	}

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

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Warn("Failed to get collection id.")
		return err
	}
	t.CollectionID = collID
	log.Debug("Get collection ID by name", zap.Int64("collectionID", t.CollectionID))

	t.partitionKeyMode, err = isPartitionKeyMode(ctx, collectionName)
	if err != nil {
		log.Warn("check partition key mode failed", zap.Int64("collectionID", t.CollectionID))
		return err
	}
	if t.partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
	}

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Warn("invalid partition name", zap.String("partition name", tag))
			return err
		}
	}
	log.Debug("Validate partition names.")

	//fetch search_growing from query param
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

	//fetch iteration_extension_reduce_rate from query param
	var iterationExtensionReduceRate int64
	for i, kv := range t.request.GetQueryParams() {
		if kv.GetKey() == IterationExtensionReduceRateKey {
			iterationExtensionReduceRate, err = strconv.ParseInt(kv.Value, 0, 64)
			if err != nil {
				return errors.New("parse query iteration_extension_reduce_rate failed")
			}
			t.request.QueryParams = append(t.request.GetQueryParams()[:i], t.request.GetQueryParams()[i+1:]...)
			break
		}
	}
	t.RetrieveRequest.IterationExtensionReduceRate = iterationExtensionReduceRate

	queryParams, err := parseQueryParams(t.request.GetQueryParams())
	if err != nil {
		return err
	}
	t.queryParams = queryParams
	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

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

	partitionNames := t.request.GetPartitionNames()
	if t.partitionKeyMode {
		expr, err := ParseExprFromPlan(t.plan)
		if err != nil {
			return err
		}
		partitionKeys := ParsePartitionKeys(expr)
		hashedPartitionNames, err := assignPartitionKeys(ctx, t.request.CollectionName, partitionKeys)
		if err != nil {
			return err
		}

		partitionNames = append(partitionNames, hashedPartitionNames...)
	}
	t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, t.request.CollectionName, partitionNames)
	if err != nil {
		return err
	}

	// count with pagination
	if t.plan.GetQuery().GetIsCount() && t.queryParams.limit != typeutil.Unlimited {
		return fmt.Errorf("count entities with pagination is not allowed")
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
	log.Debug("Query PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("travel_ts", t.GetTravelTimestamp()),
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
		collection: t.collectionName,
		nq:         1,
		exec:       t.queryShard,
	})

	if err != nil {
		return merr.WrapErrShardDelegatorQueryFailed(err.Error())
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
	select {
	case <-t.TraceCtx().Done():
		log.Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Debug("all queries are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.RetrieveResults) bool {
			toReduceResults = append(toReduceResults, res)
			log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")

	reducer := createMilvusReducer(ctx, t.queryParams, t.RetrieveRequest, t.schema, t.plan, t.collectionName)

	t.result, err = reducer.Reduce(toReduceResults)
	if err != nil {
		return err
	}
	t.result.OutputFields = t.userOutputFields
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	log.Debug("Query PostExecute done")
	return nil
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs ...string) error {
	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: channelIDs,
		Scope:       querypb.DataScope_All,
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.Strings("channels", channelIDs))

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Warn("QueryNode query return error", zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		globalMetaCache.DeprecateShardCache(t.collectionName)
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode query result error")
		return fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason())
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
	t.Base.Timestamp = ts
}

func (t *queryTask) OnEnqueue() error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}
