package proxy

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	SearchTaskName = "SearchTask"
	SearchLevelKey = "level"

	// requeryThreshold is the estimated threshold for the size of the search results.
	// If the number of estimated search results exceeds this threshold,
	// a second query request will be initiated to retrieve output fields data.
	// In this case, the first search will not return any output field from QueryNodes.
	requeryThreshold = 0.5 * 1024 * 1024
	radiusKey        = "radius"
	rangeFilterKey   = "range_filter"
)

// type requery func(span trace.Span, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, error)

type searchTask struct {
	baseTask
	Condition
	ctx context.Context
	*internalpb.SearchRequest

	result  *milvuspb.SearchResults
	request *milvuspb.SearchRequest

	tr                     *timerecord.TimeRecorder
	collectionName         string
	schema                 *schemaInfo
	needRequery            bool
	partitionKeyMode       bool
	enableMaterializedView bool
	mustUsePartitionKey    bool
	resultSizeInsufficient bool
	isTopkReduce           bool
	isRecallEvaluation     bool

	translatedOutputFields []string
	userOutputFields       []string
	userDynamicFields      []string

	resultBuf *typeutil.ConcurrentSet[*internalpb.SearchResults]

	partitionIDsSet *typeutil.ConcurrentSet[UniqueID]

	mixCoord        types.MixCoordClient
	node            types.ProxyComponent
	lb              LBPolicy
	queryChannelsTs map[string]Timestamp
	queryInfos      []*planpb.QueryInfo
	relatedDataSize int64

	// Will be deprecated, use functionScore after milvus 2.6
	reScorers   []reScorer
	groupScorer func(group *Group) error

	// New reranker functions
	functionScore *rerank.FunctionScore
	rankParams    *rankParams

	isIterator bool
	// we always remove pk field from output fields, as search result already contains pk field.
	// if the user explicitly set pk field in output fields, we add it back to the result.
	userRequestedPkFieldExplicitly bool

	// To facilitate writing unit tests
	requeryFunc func(t *searchTask, span trace.Span, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, error)
}

func (t *searchTask) CanSkipAllocTimestamp() bool {
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	if !useDefaultConsistency {
		// legacy SDK & restful behavior
		if t.request.GetConsistencyLevel() == commonpb.ConsistencyLevel_Strong && t.request.GetGuaranteeTimestamp() > 0 {
			return true
		}
		consistencyLevel = t.request.GetConsistencyLevel()
	} else {
		collID, err := globalMetaCache.GetCollectionID(context.Background(), t.request.GetDbName(), t.request.GetCollectionName())
		if err != nil { // err is not nil if collection not exists
			log.Ctx(t.ctx).Warn("search task get collectionID failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}

		collectionInfo, err2 := globalMetaCache.GetCollectionInfo(context.Background(), t.request.GetDbName(), t.request.GetCollectionName(), collID)
		if err2 != nil {
			log.Ctx(t.ctx).Warn("search task get collection info failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}
		consistencyLevel = collectionInfo.consistencyLevel
	}
	return consistencyLevel != commonpb.ConsistencyLevel_Strong
}

func (t *searchTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PreExecute")
	defer sp.End()

	t.SearchRequest.IsAdvanced = len(t.request.GetSubReqs()) > 0
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil { // err is not nil if collection not exists
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}

	t.SearchRequest.DbID = 0 // todo
	t.SearchRequest.CollectionID = collID
	log := log.Ctx(ctx).With(zap.Int64("collID", collID), zap.String("collName", collectionName))
	t.schema, err = globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get collection schema failed", zap.Error(err))
		return err
	}

	t.partitionKeyMode, err = isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("is partition key mode failed", zap.Error(err))
		return err
	}
	if t.partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
	}
	if t.mustUsePartitionKey && !t.partitionKeyMode {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("must use partition key in the search request " +
			"because the mustUsePartitionKey config is true"))
	}

	if !t.partitionKeyMode && len(t.request.GetPartitionNames()) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		t.SearchRequest.PartitionIDs, err = getPartitionIDs(ctx, t.request.GetDbName(), collectionName, t.request.GetPartitionNames())
		if err != nil {
			log.Warn("failed to get partition ids", zap.Error(err))
			return err
		}
	}

	t.translatedOutputFields, t.userOutputFields, t.userDynamicFields, t.userRequestedPkFieldExplicitly, err = translateOutputFields(t.request.OutputFields, t.schema, true)
	if err != nil {
		log.Warn("translate output fields failed", zap.Error(err), zap.Any("schema", t.schema))
		return err
	}
	log.Debug("translate output fields",
		zap.Strings("output fields", t.translatedOutputFields))

	if t.SearchRequest.GetIsAdvanced() {
		if len(t.request.GetSubReqs()) > defaultMaxSearchRequest {
			return errors.New(fmt.Sprintf("maximum of ann search requests is %d", defaultMaxSearchRequest))
		}
	}

	nq, err := t.checkNq(ctx)
	if err != nil {
		log.Info("failed to check nq", zap.Error(err))
		return err
	}
	t.SearchRequest.Nq = nq

	if t.SearchRequest.IgnoreGrowing, err = isIgnoreGrowing(t.request.SearchParams); err != nil {
		return err
	}

	outputFieldIDs, err := getOutputFieldIDs(t.schema, t.translatedOutputFields)
	if err != nil {
		log.Info("fail to get output field ids", zap.Error(err))
		return err
	}
	t.SearchRequest.OutputFieldsId = outputFieldIDs

	// Currently, we get vectors by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	if t.SearchRequest.GetIsAdvanced() {
		err = t.initAdvancedSearchRequest(ctx)
	} else {
		err = t.initSearchRequest(ctx)
	}
	if err != nil {
		log.Debug("init search request failed", zap.Error(err))
		return err
	}

	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err2 != nil {
		log.Warn("Proxy::searchTask::PreExecute failed to GetCollectionInfo from cache",
			zap.String("collectionName", collectionName), zap.Int64("collectionID", t.CollectionID), zap.Error(err2))
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

	// use collection schema updated timestamp if it's greater than calculate guarantee timestamp
	// this make query view updated happens before new read request happens
	// see also schema change design
	if collectionInfo.updateTimestamp > guaranteeTs {
		guaranteeTs = collectionInfo.updateTimestamp
	}

	t.SearchRequest.GuaranteeTimestamp = guaranteeTs
	t.SearchRequest.ConsistencyLevel = consistencyLevel
	if t.isIterator && t.request.GetGuaranteeTimestamp() > 0 {
		t.MvccTimestamp = t.request.GetGuaranteeTimestamp()
		t.GuaranteeTimestamp = t.request.GetGuaranteeTimestamp()
	}
	t.SearchRequest.IsIterator = t.isIterator

	if deadline, ok := t.TraceCtx().Deadline(); ok {
		t.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	// Set username of this search request for feature like task scheduling.
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		t.SearchRequest.Username = username
	}

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.SearchResults]()

	log.Debug("search PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Bool("use_default_consistency", useDefaultConsistency),
		zap.Any("consistency level", consistencyLevel),
		zap.Uint64("timeout_ts", t.SearchRequest.GetTimeoutTimestamp()))
	return nil
}

func (t *searchTask) checkNq(ctx context.Context) (int64, error) {
	var nq int64
	if t.SearchRequest.GetIsAdvanced() {
		// In the context of Advanced Search, it is essential to verify that the number of vectors
		// for each individual search, denoted as nq, remains consistent.
		nq = t.request.GetNq()
		for _, req := range t.request.GetSubReqs() {
			subNq, err := getNqFromSubSearch(req)
			if err != nil {
				return 0, err
			}
			req.Nq = subNq
			if nq == 0 {
				nq = subNq
				continue
			}
			if subNq != nq {
				err = merr.WrapErrParameterInvalid(nq, subNq, "sub search request nq should be the same")
				return 0, err
			}
		}
		t.request.Nq = nq
	} else {
		var err error
		nq, err = getNq(t.request)
		if err != nil {
			return 0, err
		}
		t.request.Nq = nq
	}

	// Check if nq is valid:
	// https://milvus.io/docs/limitations.md
	if err := validateNQLimit(nq); err != nil {
		return 0, fmt.Errorf("%s [%d] is invalid, %w", NQKey, nq, err)
	}
	return nq, nil
}

func setQueryInfoIfMvEnable(queryInfo *planpb.QueryInfo, t *searchTask, plan *planpb.PlanNode) error {
	if t.enableMaterializedView {
		partitionKeyFieldSchema, err := typeutil.GetPartitionKeyFieldSchema(t.schema.CollectionSchema)
		if err != nil {
			log.Ctx(t.ctx).Warn("failed to get partition key field schema", zap.Error(err))
			return err
		}
		if typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyFieldSchema) {
			collInfo, colErr := globalMetaCache.GetCollectionInfo(t.ctx, t.request.GetDbName(), t.collectionName, t.CollectionID)
			if colErr != nil {
				log.Ctx(t.ctx).Warn("failed to get collection info", zap.Error(colErr))
				return err
			}

			if collInfo.partitionKeyIsolation {
				expr, err := exprutil.ParseExprFromPlan(plan)
				if err != nil {
					log.Ctx(t.ctx).Warn("failed to parse expr from plan during MV", zap.Error(err))
					return err
				}
				err = exprutil.ValidatePartitionKeyIsolation(expr)
				if err != nil {
					return err
				}
				// force set hints to disable
				queryInfo.Hints = "disable"
			}
			queryInfo.MaterializedViewInvolved = true
		} else {
			return errors.New("partition key field data type is not supported in materialized view")
		}
	}
	return nil
}

func (t *searchTask) initAdvancedSearchRequest(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "init advanced search request")
	defer sp.End()
	t.partitionIDsSet = typeutil.NewConcurrentSet[UniqueID]()
	log := log.Ctx(ctx).With(zap.Int64("collID", t.GetCollectionID()), zap.String("collName", t.collectionName))
	var err error
	// TODO: Use function score uniformly to implement related logic
	if t.request.FunctionScore != nil {
		if t.functionScore, err = rerank.NewFunctionScore(t.schema.CollectionSchema, t.request.FunctionScore); err != nil {
			log.Warn("Failed to create function score", zap.Error(err))
			return err
		}
	} else {
		t.reScorers, err = NewReScorers(ctx, len(t.request.GetSubReqs()), t.request.GetSearchParams())
		if err != nil {
			log.Info("generate reScorer failed", zap.Any("params", t.request.GetSearchParams()), zap.Error(err))
			return err
		}

		// set up groupScorer for hybridsearch+groupBy
		groupScorerStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RankGroupScorer, t.request.GetSearchParams())
		if err != nil {
			groupScorerStr = MaxScorer
		}
		groupScorer, err := GetGroupScorer(groupScorerStr)
		if err != nil {
			return err
		}
		t.groupScorer = groupScorer
	}

	t.needRequery = len(t.request.OutputFields) > 0 || len(t.functionScore.GetAllInputFieldNames()) > 0

	if t.rankParams, err = parseRankParams(t.request.GetSearchParams(), t.schema.CollectionSchema); err != nil {
		log.Error("parseRankParams failed", zap.Error(err))
		return err
	}

	if !t.functionScore.IsSupportGroup() && t.rankParams.GetGroupByFieldId() >= 0 {
		return merr.WrapErrParameterInvalidMsg("Current rerank does not support grouping search")
	}

	t.SearchRequest.SubReqs = make([]*internalpb.SubSearchRequest, len(t.request.GetSubReqs()))
	t.queryInfos = make([]*planpb.QueryInfo, len(t.request.GetSubReqs()))
	queryFieldIDs := []int64{}
	for index, subReq := range t.request.GetSubReqs() {
		plan, queryInfo, offset, _, err := t.tryGeneratePlan(subReq.GetSearchParams(), subReq.GetDsl(), subReq.GetExprTemplateValues())
		if err != nil {
			return err
		}

		ignoreGrowing := t.SearchRequest.IgnoreGrowing
		if !ignoreGrowing {
			// fetch ignore_growing from sub search param if not set in search request
			if ignoreGrowing, err = isIgnoreGrowing(subReq.GetSearchParams()); err != nil {
				return err
			}
		}

		internalSubReq := &internalpb.SubSearchRequest{
			Dsl:                subReq.GetDsl(),
			PlaceholderGroup:   subReq.GetPlaceholderGroup(),
			DslType:            subReq.GetDslType(),
			SerializedExprPlan: nil,
			Nq:                 subReq.GetNq(),
			PartitionIDs:       nil,
			Topk:               queryInfo.GetTopk(),
			Offset:             offset,
			MetricType:         queryInfo.GetMetricType(),
			GroupByFieldId:     t.rankParams.GetGroupByFieldId(),
			GroupSize:          t.rankParams.GetGroupSize(),
			IgnoreGrowing:      ignoreGrowing,
		}

		internalSubReq.FieldId = queryInfo.GetQueryFieldId()
		queryFieldIDs = append(queryFieldIDs, internalSubReq.FieldId)
		// set PartitionIDs for sub search
		if t.partitionKeyMode {
			// isolation has tighter constraint, check first
			mvErr := setQueryInfoIfMvEnable(queryInfo, t, plan)
			if mvErr != nil {
				return mvErr
			}
			partitionIDs, err2 := t.tryParsePartitionIDsFromPlan(plan)
			if err2 != nil {
				return err2
			}
			if len(partitionIDs) > 0 {
				internalSubReq.PartitionIDs = partitionIDs
				t.partitionIDsSet.Upsert(partitionIDs...)
			}
		} else {
			internalSubReq.PartitionIDs = t.SearchRequest.GetPartitionIDs()
		}

		plan.OutputFieldIds = nil
		plan.DynamicFields = nil

		internalSubReq.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		if typeutil.IsFieldSparseFloatVector(t.schema.CollectionSchema, internalSubReq.FieldId) {
			metrics.ProxySearchSparseNumNonZeros.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.collectionName, metrics.HybridSearchLabel, strconv.FormatInt(internalSubReq.FieldId, 10)).Observe(float64(typeutil.EstimateSparseVectorNNZFromPlaceholderGroup(internalSubReq.PlaceholderGroup, int(internalSubReq.GetNq()))))
		}
		t.SearchRequest.SubReqs[index] = internalSubReq
		t.queryInfos[index] = queryInfo
		log.Debug("proxy init search request",
			zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
			zap.Stringer("plan", plan)) // may be very large if large term passed.
	}

	if function.HasNonBM25Functions(t.schema.CollectionSchema.Functions, queryFieldIDs) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AdvancedSearch-call-function-udf")
		defer sp.End()
		exec, err := function.NewFunctionExecutor(t.schema.CollectionSchema)
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessSearch(ctx, t.SearchRequest); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}

	t.SearchRequest.GroupByFieldId = t.rankParams.GetGroupByFieldId()
	t.SearchRequest.GroupSize = t.rankParams.GetGroupSize()

	// used for requery
	if t.partitionKeyMode {
		t.SearchRequest.PartitionIDs = t.partitionIDsSet.Collect()
	}

	return nil
}

func (t *searchTask) advancedPostProcess(ctx context.Context, span trace.Span, toReduceResults []*internalpb.SearchResults) error {
	// Collecting the results of a subsearch
	// [[shard1, shard2, ...],[shard1, shard2, ...]]
	multipleInternalResults := make([][]*internalpb.SearchResults, len(t.SearchRequest.GetSubReqs()))
	for _, searchResult := range toReduceResults {
		// if get a non-advanced result, skip all
		if !searchResult.GetIsAdvanced() {
			continue
		}
		for _, subResult := range searchResult.GetSubResults() {
			// swallow copy
			internalResults := &internalpb.SearchResults{
				MetricType:     subResult.GetMetricType(),
				NumQueries:     subResult.GetNumQueries(),
				TopK:           subResult.GetTopK(),
				SlicedBlob:     subResult.GetSlicedBlob(),
				SlicedNumCount: subResult.GetSlicedNumCount(),
				SlicedOffset:   subResult.GetSlicedOffset(),
				IsAdvanced:     false,
			}
			reqIndex := subResult.GetReqIndex()
			multipleInternalResults[reqIndex] = append(multipleInternalResults[reqIndex], internalResults)
		}
	}

	multipleMilvusResults := make([]*milvuspb.SearchResults, len(t.SearchRequest.GetSubReqs()))
	for index, internalResults := range multipleInternalResults {
		subReq := t.SearchRequest.GetSubReqs()[index]
		// Since the metrictype in the request may be empty, it can only be obtained from the result
		subMetricType := getMetricType(internalResults)
		result, err := t.reduceResults(t.ctx, internalResults, subReq.GetNq(), subReq.GetTopk(), subReq.GetOffset(), subMetricType, t.queryInfos[index], true)
		if err != nil {
			return err
		}

		if t.functionScore == nil {
			t.reScorers[index].setMetricType(subMetricType)
			t.reScorers[index].reScore(result)
		}
		multipleMilvusResults[index] = result
	}

	if t.functionScore == nil {
		if err := t.rank(ctx, span, multipleMilvusResults); err != nil {
			return err
		}
	} else {
		if err := t.hybridSearchRank(ctx, span, multipleMilvusResults); err != nil {
			return err
		}
	}

	t.result.Results.FieldsData = lo.Filter(t.result.Results.FieldsData, func(fieldData *schemapb.FieldData, i int) bool {
		return lo.Contains(t.translatedOutputFields, fieldData.GetFieldName())
	})
	t.fillResult()
	return nil
}

func (t *searchTask) fillResult() {
	limit := t.SearchRequest.GetTopk() - t.SearchRequest.GetOffset()
	resultSizeInsufficient := false
	for _, topk := range t.result.Results.Topks {
		if topk < limit {
			resultSizeInsufficient = true
			break
		}
	}
	t.resultSizeInsufficient = resultSizeInsufficient
	t.result.CollectionName = t.collectionName
	t.fillInFieldInfo()
}

// TODO: Old version rerank: rrf/weighted, subsequent unified rerank implementation
func (t *searchTask) rank(ctx context.Context, span trace.Span, multipleMilvusResults []*milvuspb.SearchResults) error {
	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}
	if t.result, err = rankSearchResultData(ctx, t.SearchRequest.GetNq(),
		t.rankParams,
		primaryFieldSchema.GetDataType(),
		multipleMilvusResults,
		t.SearchRequest.GetGroupByFieldId(),
		t.SearchRequest.GetGroupSize(),
		t.groupScorer); err != nil {
		log.Warn("rank search result failed", zap.Error(err))
		return err
	}

	if t.needRequery {
		if t.requeryFunc == nil {
			t.requeryFunc = requeryImpl
		}
		queryResult, err := t.requeryFunc(t, span, t.result.Results.Ids, t.translatedOutputFields)
		if err != nil {
			log.Warn("failed to requery", zap.Error(err))
			return err
		}
		fields, err := t.reorganizeRequeryResults(ctx, queryResult.GetFieldsData(), []*schemapb.IDs{t.result.Results.Ids})
		if err != nil {
			return err
		}
		t.result.Results.FieldsData = fields[0]
	}

	return nil
}

func mergeIDs(idsList []*schemapb.IDs) (*schemapb.IDs, int) {
	uniqueIDs := &schemapb.IDs{}
	count := 0
	switch idsList[0].GetIdField().(type) {
	case *schemapb.IDs_IntId:
		idsSet := typeutil.NewSet[int64]()
		for _, ids := range idsList {
			if data := ids.GetIntId().GetData(); data != nil {
				idsSet.Insert(data...)
			}
		}
		count = idsSet.Len()
		uniqueIDs.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: idsSet.Collect(),
			},
		}
	case *schemapb.IDs_StrId:
		idsSet := typeutil.NewSet[string]()
		for _, ids := range idsList {
			if data := ids.GetStrId().GetData(); data != nil {
				idsSet.Insert(data...)
			}
		}
		count = idsSet.Len()
		uniqueIDs.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: idsSet.Collect(),
			},
		}
	}
	return uniqueIDs, count
}

func (t *searchTask) hybridSearchRank(ctx context.Context, span trace.Span, multipleMilvusResults []*milvuspb.SearchResults) error {
	var err error
	// The first step of hybrid search is without meta information. If rerank requires meta data, we need to do requery.
	// At this time, outputFields and rerank input_fields will be recalled.
	// If we want to save memory, we can only recall the rerank input_fields in this step, and recall the output_fields in the third step
	if t.needRequery {
		idsList := lo.FilterMap(multipleMilvusResults, func(m *milvuspb.SearchResults, _ int) (*schemapb.IDs, bool) {
			return m.Results.Ids, true
		})
		allIDs, count := mergeIDs(idsList)
		if count == 0 {
			t.result = &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: t.Nq,
					TopK:       t.rankParams.limit,
					FieldsData: make([]*schemapb.FieldData, 0),
					Scores:     []float32{},
					Ids:        &schemapb.IDs{},
					Topks:      []int64{},
				},
			}
			return nil
		}
		allNames := typeutil.NewSet[string](t.translatedOutputFields...)
		allNames.Insert(t.functionScore.GetAllInputFieldNames()...)
		queryResult, err := t.requeryFunc(t, span, allIDs, allNames.Collect())
		if err != nil {
			log.Warn("failed to requery", zap.Error(err))
			return err
		}
		fields, err := t.reorganizeRequeryResults(ctx, queryResult.GetFieldsData(), idsList)
		if err != nil {
			return err
		}
		for i := 0; i < len(multipleMilvusResults); i++ {
			multipleMilvusResults[i].Results.FieldsData = fields[i]
		}
		params := rerank.NewSearchParams(
			t.Nq, t.rankParams.limit, t.rankParams.offset, t.rankParams.roundDecimal,
			t.rankParams.groupByFieldId, t.rankParams.groupSize, t.rankParams.strictGroupSize,
		)

		if t.result, err = t.functionScore.Process(ctx, params, multipleMilvusResults); err != nil {
			return err
		}
		if fields, err := t.reorganizeRequeryResults(ctx, queryResult.GetFieldsData(), []*schemapb.IDs{t.result.Results.Ids}); err != nil {
			return err
		} else {
			t.result.Results.FieldsData = fields[0]
		}
	} else {
		params := rerank.NewSearchParams(
			t.Nq, t.rankParams.limit, t.rankParams.offset, t.rankParams.roundDecimal,
			t.rankParams.groupByFieldId, t.rankParams.groupSize, t.rankParams.strictGroupSize,
		)
		if t.result, err = t.functionScore.Process(ctx, params, multipleMilvusResults); err != nil {
			return err
		}
	}
	return nil
}

func (t *searchTask) initSearchRequest(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "init search request")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.GetCollectionID()), zap.String("collName", t.collectionName))

	plan, queryInfo, offset, isIterator, err := t.tryGeneratePlan(t.request.GetSearchParams(), t.request.GetDsl(), t.request.GetExprTemplateValues())
	if err != nil {
		return err
	}

	if t.request.FunctionScore != nil {
		// TODO: When rerank is configured, range search is also supported
		if isIterator {
			return merr.WrapErrParameterInvalidMsg("Range search do not support rerank")
		}

		if t.functionScore, err = rerank.NewFunctionScore(t.schema.CollectionSchema, t.request.FunctionScore); err != nil {
			log.Warn("Failed to create function score", zap.Error(err))
			return err
		}

		// TODO: When rerank is configured, grouping search is also supported
		if !t.functionScore.IsSupportGroup() && queryInfo.GetGroupByFieldId() > 0 {
			return merr.WrapErrParameterInvalidMsg("Current rerank does not support grouping search")
		}
	}

	t.isIterator = isIterator
	t.SearchRequest.Offset = offset
	t.SearchRequest.FieldId = queryInfo.GetQueryFieldId()

	if t.partitionKeyMode {
		// isolation has tighter constraint, check first
		mvErr := setQueryInfoIfMvEnable(queryInfo, t, plan)
		if mvErr != nil {
			return mvErr
		}
		partitionIDs, err2 := t.tryParsePartitionIDsFromPlan(plan)
		if err2 != nil {
			return err2
		}
		if len(partitionIDs) > 0 {
			t.SearchRequest.PartitionIDs = partitionIDs
		}
	}

	vectorOutputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})
	t.needRequery = len(vectorOutputFields) > 0
	if t.needRequery {
		plan.OutputFieldIds = t.functionScore.GetAllInputFieldIDs()
	} else {
		primaryFieldSchema, err := t.schema.GetPkField()
		if err != nil {
			return err
		}
		allFieldIDs := typeutil.NewSet[int64](t.SearchRequest.OutputFieldsId...)
		allFieldIDs.Insert(t.functionScore.GetAllInputFieldIDs()...)
		allFieldIDs.Insert(primaryFieldSchema.FieldID)
		plan.OutputFieldIds = allFieldIDs.Collect()
		plan.DynamicFields = t.userDynamicFields
	}

	t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}
	if typeutil.IsFieldSparseFloatVector(t.schema.CollectionSchema, t.SearchRequest.FieldId) {
		metrics.ProxySearchSparseNumNonZeros.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.collectionName, metrics.SearchLabel, strconv.FormatInt(t.SearchRequest.FieldId, 10)).Observe(float64(typeutil.EstimateSparseVectorNNZFromPlaceholderGroup(t.request.PlaceholderGroup, int(t.request.GetNq()))))
	}
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup
	t.SearchRequest.Topk = queryInfo.GetTopk()
	t.SearchRequest.MetricType = queryInfo.GetMetricType()
	t.queryInfos = append(t.queryInfos, queryInfo)
	t.SearchRequest.DslType = commonpb.DslType_BoolExprV1
	t.SearchRequest.GroupByFieldId = queryInfo.GroupByFieldId
	t.SearchRequest.GroupSize = queryInfo.GroupSize

	if t.SearchRequest.MetricType == metric.BM25 {
		analyzer, err := funcutil.GetAttrByKeyFromRepeatedKV("analyzer_name", t.request.GetSearchParams())
		if err == nil {
			t.SearchRequest.AnalyzerName = analyzer
		}
	}

	if function.HasNonBM25Functions(t.schema.CollectionSchema.Functions, []int64{queryInfo.GetQueryFieldId()}) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-call-function-udf")
		defer sp.End()
		exec, err := function.NewFunctionExecutor(t.schema.CollectionSchema)
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessSearch(ctx, t.SearchRequest); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}

	log.Debug("proxy init search request",
		zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
		zap.Stringer("plan", plan)) // may be very large if large term passed.

	return nil
}

func (t *searchTask) searchPostProcess(ctx context.Context, span trace.Span, toReduceResults []*internalpb.SearchResults) error {
	metricType := getMetricType(toReduceResults)
	result, err := t.reduceResults(t.ctx, toReduceResults, t.SearchRequest.GetNq(), t.SearchRequest.GetTopk(), t.SearchRequest.GetOffset(), metricType, t.queryInfos[0], false)
	if err != nil {
		return err
	}

	if t.functionScore != nil && len(result.Results.FieldsData) != 0 {
		params := rerank.NewSearchParams(t.Nq, t.SearchRequest.GetTopk(), t.SearchRequest.GetOffset(),
			t.queryInfos[0].RoundDecimal, t.queryInfos[0].GroupByFieldId, t.queryInfos[0].GroupSize, t.queryInfos[0].StrictGroupSize)
		// rank only returns id and score
		if t.result, err = t.functionScore.Process(ctx, params, []*milvuspb.SearchResults{result}); err != nil {
			return err
		}
		if !t.needRequery {
			fields, err := t.reorganizeRequeryResults(ctx, result.Results.FieldsData, []*schemapb.IDs{t.result.Results.Ids})
			if err != nil {
				return err
			}
			t.result.Results.FieldsData = fields[0]
		}
	} else {
		t.result = result
	}
	t.fillResult()
	if t.needRequery {
		if t.requeryFunc == nil {
			t.requeryFunc = requeryImpl
		}
		queryResult, err := t.requeryFunc(t, span, t.result.Results.Ids, t.translatedOutputFields)
		if err != nil {
			log.Warn("failed to requery", zap.Error(err))
			return err
		}
		fields, err := t.reorganizeRequeryResults(ctx, queryResult.GetFieldsData(), []*schemapb.IDs{t.result.Results.Ids})
		if err != nil {
			return err
		}
		t.result.Results.FieldsData = fields[0]
	}
	t.result.Results.FieldsData = lo.Filter(t.result.Results.FieldsData, func(fieldData *schemapb.FieldData, i int) bool {
		return lo.Contains(t.translatedOutputFields, fieldData.GetFieldName())
	})
	return nil
}

func (t *searchTask) tryGeneratePlan(params []*commonpb.KeyValuePair, dsl string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, *planpb.QueryInfo, int64, bool, error) {
	annsFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, params)
	if err != nil || len(annsFieldName) == 0 {
		vecFields := typeutil.GetVectorFieldSchemas(t.schema.CollectionSchema)
		if len(vecFields) == 0 {
			return nil, nil, 0, false, errors.New(AnnsFieldKey + " not found in schema")
		}

		if enableMultipleVectorFields && len(vecFields) > 1 {
			return nil, nil, 0, false, errors.New("multiple anns_fields exist, please specify a anns_field in search_params")
		}
		annsFieldName = vecFields[0].Name
	}
	searchInfo := parseSearchInfo(params, t.schema.CollectionSchema, t.rankParams)
	if searchInfo.parseError != nil {
		return nil, nil, 0, false, searchInfo.parseError
	}
	if searchInfo.collectionID > 0 && searchInfo.collectionID != t.GetCollectionID() {
		return nil, nil, 0, false, merr.WrapErrParameterInvalidMsg("collection id:%d in the request is not consistent to that in the search context,"+
			"alias or database may have been changed: %d", searchInfo.collectionID, t.GetCollectionID())
	}

	annField := typeutil.GetFieldByName(t.schema.CollectionSchema, annsFieldName)
	if searchInfo.planInfo.GetGroupByFieldId() != -1 && annField.GetDataType() == schemapb.DataType_BinaryVector {
		return nil, nil, 0, false, errors.New("not support search_group_by operation based on binary vector column")
	}

	searchInfo.planInfo.QueryFieldId = annField.GetFieldID()
	start := time.Now()
	plan, planErr := planparserv2.CreateSearchPlan(t.schema.schemaHelper, dsl, annsFieldName, searchInfo.planInfo, exprTemplateValues)
	if planErr != nil {
		log.Ctx(t.ctx).Warn("failed to create query plan", zap.Error(planErr),
			zap.String("dsl", dsl), // may be very large if large term passed.
			zap.String("anns field", annsFieldName), zap.Any("query info", searchInfo.planInfo))
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "search", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
		return nil, nil, 0, false, merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", planErr)
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "search", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	log.Ctx(t.ctx).Debug("create query plan",
		zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
		zap.String("anns field", annsFieldName), zap.Any("query info", searchInfo.planInfo))
	return plan, searchInfo.planInfo, searchInfo.offset, searchInfo.isIterator, nil
}

func (t *searchTask) tryParsePartitionIDsFromPlan(plan *planpb.PlanNode) ([]int64, error) {
	expr, err := exprutil.ParseExprFromPlan(plan)
	if err != nil {
		log.Ctx(t.ctx).Warn("failed to parse expr", zap.Error(err))
		return nil, err
	}
	partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
	hashedPartitionNames, err := assignPartitionKeys(t.ctx, t.request.GetDbName(), t.collectionName, partitionKeys)
	if err != nil {
		log.Ctx(t.ctx).Warn("failed to assign partition keys", zap.Error(err))
		return nil, err
	}

	if len(hashedPartitionNames) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		PartitionIDs, err2 := getPartitionIDs(t.ctx, t.request.GetDbName(), t.collectionName, hashedPartitionNames)
		if err2 != nil {
			log.Ctx(t.ctx).Warn("failed to get partition ids", zap.Error(err2))
			return nil, err2
		}
		return PartitionIDs, nil
	}
	return nil, nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-Execute")
	defer sp.End()
	log := log.Ctx(ctx).WithLazy(zap.Int64("nq", t.SearchRequest.GetNq()))

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	err := t.lb.Execute(ctx, CollectionWorkLoad{
		db:             t.request.GetDbName(),
		collectionID:   t.SearchRequest.CollectionID,
		collectionName: t.collectionName,
		nq:             t.Nq,
		exec:           t.searchShard,
	})
	if err != nil {
		log.Warn("search execute failed", zap.Error(err))
		return errors.Wrap(err, "failed to search")
	}

	log.Debug("Search Execute done.",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
}

func getMetricType(toReduceResults []*internalpb.SearchResults) string {
	metricType := ""
	if len(toReduceResults) >= 1 {
		metricType = toReduceResults[0].GetMetricType()
	}
	return metricType
}

func (t *searchTask) reduceResults(ctx context.Context, toReduceResults []*internalpb.SearchResults, nq, topK int64, offset int64, metricType string, queryInfo *planpb.QueryInfo, isAdvance bool) (*milvuspb.SearchResults, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "reduceResults")
	defer sp.End()

	log := log.Ctx(ctx)
	// Decode all search results
	validSearchResults, err := decodeSearchResults(ctx, toReduceResults)
	if err != nil {
		log.Warn("failed to decode search results", zap.Error(err))
		return nil, err
	}

	if len(validSearchResults) <= 0 {
		return fillInEmptyResult(nq), nil
	}

	// Reduce all search results
	log.Debug("proxy search post execute reduce",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int("number of valid search results", len(validSearchResults)))
	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return nil, err
	}
	var result *milvuspb.SearchResults
	result, err = reduceSearchResult(ctx, validSearchResults, reduce.NewReduceSearchResultInfo(nq, topK).WithMetricType(metricType).WithPkType(primaryFieldSchema.GetDataType()).
		WithOffset(offset).WithGroupByField(queryInfo.GetGroupByFieldId()).WithGroupSize(queryInfo.GetGroupSize()).WithAdvance(isAdvance))
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return nil, err
	}
	return result, nil
}

// find the last bound based on reduced results and metric type
// only support nq == 1, for search iterator v2
func getLastBound(result *milvuspb.SearchResults, incomingLastBound *float32, metricType string) float32 {
	len := len(result.Results.Scores)
	if len > 0 && result.GetResults().GetNumQueries() == 1 {
		return result.Results.Scores[len-1]
	}
	// if no results found and incoming last bound is not nil, return it
	if incomingLastBound != nil {
		return *incomingLastBound
	}
	// if no results found and it is the first call, return the closest bound
	if metric.PositivelyRelated(metricType) {
		return math.MaxFloat32
	}
	return -math.MaxFloat32
}

func (t *searchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PostExecute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()
	log := log.Ctx(ctx).With(zap.Int64("nq", t.SearchRequest.GetNq()))

	toReduceResults, err := t.collectSearchResults(ctx)
	if err != nil {
		log.Warn("failed to collect search results", zap.Error(err))
		return err
	}

	t.queryChannelsTs = make(map[string]uint64)
	t.relatedDataSize = 0
	isTopkReduce := false
	isRecallEvaluation := false
	for _, r := range toReduceResults {
		if r.GetIsTopkReduce() {
			isTopkReduce = true
		}
		if r.GetIsRecallEvaluation() {
			isRecallEvaluation = true
		}
		t.relatedDataSize += r.GetCostAggregation().GetTotalRelatedDataSize()
		for ch, ts := range r.GetChannelsMvcc() {
			t.queryChannelsTs[ch] = ts
		}
	}

	t.isTopkReduce = isTopkReduce
	t.isRecallEvaluation = isRecallEvaluation

	if t.SearchRequest.GetIsAdvanced() {
		err = t.advancedPostProcess(ctx, sp, toReduceResults)
	} else {
		err = t.searchPostProcess(ctx, sp, toReduceResults)
	}

	if err != nil {
		return err
	}
	t.result.Results.OutputFields = t.userOutputFields
	t.result.CollectionName = t.request.GetCollectionName()

	primaryFieldSchema, _ := t.schema.GetPkField()
	if t.userRequestedPkFieldExplicitly {
		t.result.Results.OutputFields = append(t.result.Results.OutputFields, primaryFieldSchema.GetName())
		var scalars *schemapb.ScalarField
		if primaryFieldSchema.GetDataType() == schemapb.DataType_Int64 {
			scalars = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: t.result.Results.Ids.GetIntId(),
				},
			}
		} else {
			scalars = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: t.result.Results.Ids.GetStrId(),
				},
			}
		}
		pkFieldData := &schemapb.FieldData{
			FieldName: primaryFieldSchema.GetName(),
			FieldId:   primaryFieldSchema.GetFieldID(),
			Type:      primaryFieldSchema.GetDataType(),
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: scalars,
			},
		}
		t.result.Results.FieldsData = append(t.result.Results.FieldsData, pkFieldData)
	}
	t.result.Results.PrimaryFieldName = primaryFieldSchema.GetName()
	if t.isIterator && len(t.queryInfos) == 1 && t.queryInfos[0] != nil {
		if iterInfo := t.queryInfos[0].GetSearchIteratorV2Info(); iterInfo != nil {
			t.result.Results.SearchIteratorV2Results = &schemapb.SearchIteratorV2Results{
				Token:     iterInfo.GetToken(),
				LastBound: getLastBound(t.result, iterInfo.LastBound, getMetricType(toReduceResults)),
			}
		}
	}
	if t.isIterator && t.request.GetGuaranteeTimestamp() == 0 {
		// first page for iteration, need to set up sessionTs for iterator
		t.result.SessionTs = getMaxMvccTsFromChannels(t.queryChannelsTs, t.BeginTs())
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	log.Debug("Search post execute done",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	searchReq := typeutil.Clone(t.SearchRequest)
	searchReq.GetBase().TargetID = nodeID
	req := &querypb.SearchRequest{
		Req:             searchReq,
		DmlChannels:     []string{channel},
		Scope:           querypb.DataScope_All,
		TotalChannelNum: int32(1),
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channel))

	var result *internalpb.SearchResults
	var err error

	result, err = qn.Search(ctx, req)
	if err != nil {
		log.Warn("QueryNode search return error", zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode search result error",
			zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to search on QueryNode %d", nodeID)
	}
	if t.resultBuf != nil {
		t.resultBuf.Insert(result)
	}
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)

	return nil
}

func (t *searchTask) estimateResultSize(nq int64, topK int64) (int64, error) {
	vectorOutputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})
	// Currently, we get vectors by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	if len(vectorOutputFields) > 0 {
		return math.MaxInt64, nil
	}
	// If no vector field as output, no need to requery.
	return 0, nil

	//outputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
	//	return lo.Contains(t.translatedOutputFields, field.GetName())
	//})
	//sizePerRecord, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{Fields: outputFields})
	//if err != nil {
	//	return 0, err
	//}
	//return int64(sizePerRecord) * nq * topK, nil
}

func requeryImpl(t *searchTask, span trace.Span, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, error) {
	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Retrieve,
			Timestamp: t.BeginTs(),
		},
		DbName:                t.request.GetDbName(),
		CollectionName:        t.request.GetCollectionName(),
		ConsistencyLevel:      t.SearchRequest.GetConsistencyLevel(),
		NotReturnAllMeta:      t.request.GetNotReturnAllMeta(),
		Expr:                  "",
		OutputFields:          outputFields,
		PartitionNames:        t.request.GetPartitionNames(),
		UseDefaultConsistency: false,
		GuaranteeTimestamp:    t.SearchRequest.GuaranteeTimestamp,
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(t.schema.CollectionSchema)
	if err != nil {
		return nil, err
	}

	plan := planparserv2.CreateRequeryPlan(pkField, ids)
	channelsMvcc := make(map[string]Timestamp)
	for k, v := range t.queryChannelsTs {
		channelsMvcc[k] = v
	}
	qt := &queryTask{
		ctx:       t.ctx,
		Condition: NewTaskCondition(t.ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:            paramtable.GetNodeID(),
			PartitionIDs:     t.GetPartitionIDs(), // use search partitionIDs
			ConsistencyLevel: t.ConsistencyLevel,
		},
		request:      queryReq,
		plan:         plan,
		mixCoord:     t.node.(*Proxy).mixCoord,
		lb:           t.node.(*Proxy).lbPolicy,
		channelsMvcc: channelsMvcc,
		fastSkip:     true,
		reQuery:      true,
	}
	queryResult, err := t.node.(*Proxy).query(t.ctx, qt, span)
	if err != nil {
		return nil, err
	}
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, merr.Error(queryResult.GetStatus())
	}
	return queryResult, err
}

func (t *searchTask) reorganizeRequeryResults(ctx context.Context, fields []*schemapb.FieldData, idsList []*schemapb.IDs) ([][]*schemapb.FieldData, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(t.ctx, "reorganizeRequeryResults")
	defer sp.End()

	pkField, err := typeutil.GetPrimaryFieldSchema(t.schema.CollectionSchema)
	if err != nil {
		return nil, err
	}
	pkFieldData, err := typeutil.GetPrimaryFieldData(fields, pkField)
	if err != nil {
		return nil, err
	}
	offsets := make(map[any]int)
	for i := 0; i < typeutil.GetPKSize(pkFieldData); i++ {
		pk := typeutil.GetData(pkFieldData, i)
		offsets[pk] = i
	}

	allFieldData := make([][]*schemapb.FieldData, len(idsList))
	for idx, ids := range idsList {
		if ids == nil {
			allFieldData[idx] = []*schemapb.FieldData{}
			continue
		}
		if fieldData, err := t.pickFieldData(ids, offsets, fields); err != nil {
			return nil, err
		} else {
			allFieldData[idx] = fieldData
		}
	}
	return allFieldData, nil
}

// pick field data from query results
func (t *searchTask) pickFieldData(ids *schemapb.IDs, pkOffset map[any]int, fields []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	// Reorganize Results. The order of query result ids will be altered and differ from queried ids.
	// We should reorganize query results to keep the order of original queried ids. For example:
	// ===========================================
	//  3  2  5  4  1  (query ids)
	//       ||
	//       || (query)
	//       \/
	//  4  3  5  1  2  (result ids)
	// v4 v3 v5 v1 v2  (result vectors)
	//       ||
	//       || (reorganize)
	//       \/
	//  3  2  5  4  1  (result ids)
	// v3 v2 v5 v4 v1  (result vectors)
	// ===========================================
	fieldsData := make([]*schemapb.FieldData, len(fields))
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := pkOffset[id]; !ok {
			return nil, merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(pkOffset), t.GetCollectionID()))
		}
		typeutil.AppendFieldData(fieldsData, fields, int64(pkOffset[id]))
	}

	return fieldsData, nil
}

func (t *searchTask) fillInFieldInfo() {
	for _, retField := range t.result.Results.FieldsData {
		for _, schemaField := range t.schema.Fields {
			if retField != nil && retField.FieldId == schemaField.FieldID {
				retField.FieldName = schemaField.Name
				retField.Type = schemaField.DataType
				retField.IsDynamic = schemaField.IsDynamic
			}
		}
	}
}

func (t *searchTask) collectSearchResults(ctx context.Context) ([]*internalpb.SearchResults, error) {
	select {
	case <-t.TraceCtx().Done():
		log.Ctx(ctx).Warn("search task wait to finish timeout!")
		return nil, fmt.Errorf("search task wait to finish timeout, msgID=%d", t.ID())
	default:
		toReduceResults := make([]*internalpb.SearchResults, 0)
		log.Ctx(ctx).Debug("all searches are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.SearchResults) bool {
			toReduceResults = append(toReduceResults, res)
			log.Ctx(ctx).Debug("proxy receives one search result",
				zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
		return toReduceResults, nil
	}
}

func decodeSearchResults(ctx context.Context, searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "decodeSearchResults")
	defer sp.End()
	tr := timerecord.NewTimeRecorder("decodeSearchResults")
	results := make([]*schemapb.SearchResultData, 0)
	for _, partialSearchResult := range searchResults {
		if partialSearchResult.SlicedBlob == nil {
			continue
		}

		var partialResultData schemapb.SearchResultData
		err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
		if err != nil {
			return nil, err
		}

		results = append(results, &partialResultData)
	}
	tr.CtxElapse(ctx, "decodeSearchResults done")
	return results, nil
}

func checkSearchResultData(data *schemapb.SearchResultData, nq int64, topk int64, pkHitNum int) error {
	if data.NumQueries != nq {
		return fmt.Errorf("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
	}
	if data.TopK != topk {
		return fmt.Errorf("search result's topk(%d) mis-match with %d", data.TopK, topk)
	}

	if len(data.Scores) != pkHitNum {
		return fmt.Errorf("search result's score length invalid, score length=%d, expectedLength=%d",
			len(data.Scores), pkHitNum)
	}
	return nil
}

func selectHighestScoreIndex(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, subSearchNqOffset [][]int64, cursors []int64, qi int64) (int, int64) {
	var (
		subSearchIdx        = -1
		resultDataIdx int64 = -1
	)
	maxScore := minFloat32
	for i := range cursors {
		if cursors[i] >= subSearchResultData[i].Topks[qi] {
			continue
		}
		sIdx := subSearchNqOffset[i][qi] + cursors[i]
		sScore := subSearchResultData[i].Scores[sIdx]

		// Choose the larger score idx or the smaller pk idx with the same score
		if subSearchIdx == -1 || sScore > maxScore {
			subSearchIdx = i
			resultDataIdx = sIdx
			maxScore = sScore
		} else if sScore == maxScore {
			if subSearchIdx == -1 {
				// A bad case happens where Knowhere returns distance/score == +/-maxFloat32
				// by mistake.
				log.Ctx(ctx).Error("a bad score is returned, something is wrong here!", zap.Float32("score", sScore))
			} else if typeutil.ComparePK(
				typeutil.GetPK(subSearchResultData[i].GetIds(), sIdx),
				typeutil.GetPK(subSearchResultData[subSearchIdx].GetIds(), resultDataIdx)) {
				subSearchIdx = i
				resultDataIdx = sIdx
				maxScore = sScore
			}
		}
	}
	return subSearchIdx, resultDataIdx
}

func (t *searchTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *searchTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *searchTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *searchTask) Name() string {
	return SearchTaskName
}

func (t *searchTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *searchTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *searchTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *searchTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *searchTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}
