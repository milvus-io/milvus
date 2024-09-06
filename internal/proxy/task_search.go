package proxy

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	requery                bool
	partitionKeyMode       bool
	enableMaterializedView bool
	mustUsePartitionKey    bool

	userOutputFields  []string
	userDynamicFields []string

	resultBuf *typeutil.ConcurrentSet[*internalpb.SearchResults]

	partitionIDsSet *typeutil.ConcurrentSet[UniqueID]

	qc              types.QueryCoordClient
	node            types.ProxyComponent
	lb              LBPolicy
	queryChannelsTs map[string]Timestamp
	queryInfos      []*planpb.QueryInfo
	relatedDataSize int64

	reScorers  []reScorer
	rankParams *rankParams
}

func (t *searchTask) CanSkipAllocTimestamp() bool {
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
			log.Warn("search task get collectionID failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}

		collectionInfo, err2 := globalMetaCache.GetCollectionInfo(context.Background(), t.request.GetDbName(), t.request.GetCollectionName(), collID)
		if err2 != nil {
			log.Warn("search task get collection info failed, can't skip alloc timestamp",
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

	t.request.OutputFields, t.userOutputFields, t.userDynamicFields, err = translateOutputFields(t.request.OutputFields, t.schema, false)
	if err != nil {
		log.Warn("translate output fields failed", zap.Error(err))
		return err
	}
	log.Debug("translate output fields",
		zap.Strings("output fields", t.request.GetOutputFields()))

	if t.SearchRequest.GetIsAdvanced() {
		if len(t.request.GetSubReqs()) > defaultMaxSearchRequest {
			return errors.New(fmt.Sprintf("maximum of ann search requests is %d", defaultMaxSearchRequest))
		}
	}
	if t.SearchRequest.GetIsAdvanced() {
		t.rankParams, err = parseRankParams(t.request.GetSearchParams())
		if err != nil {
			return err
		}
	}
	// Manually update nq if not set.
	nq, err := t.checkNq(ctx)
	if err != nil {
		log.Info("failed to check nq", zap.Error(err))
		return err
	}
	t.SearchRequest.Nq = nq

	var ignoreGrowing bool
	// parse common search params
	for i, kv := range t.request.GetSearchParams() {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err = strconv.ParseBool(kv.GetValue())
			if err != nil {
				return errors.New("parse search growing failed")
			}
			t.request.SearchParams = append(t.request.GetSearchParams()[:i], t.request.GetSearchParams()[i+1:]...)
			break
		}
	}
	t.SearchRequest.IgnoreGrowing = ignoreGrowing

	outputFieldIDs, err := getOutputFieldIDs(t.schema, t.request.GetOutputFields())
	if err != nil {
		log.Info("fail to get output field ids", zap.Error(err))
		return err
	}
	t.SearchRequest.OutputFieldsId = outputFieldIDs

	// Currently, we get vectors by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	vectorOutputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.request.GetOutputFields(), field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})

	if t.SearchRequest.GetIsAdvanced() {
		t.requery = len(t.request.OutputFields) > 0
		err = t.initAdvancedSearchRequest(ctx)
	} else {
		t.requery = len(vectorOutputFields) > 0
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
	t.SearchRequest.GuaranteeTimestamp = guaranteeTs
	t.SearchRequest.ConsistencyLevel = consistencyLevel

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
			log.Warn("failed to get partition key field schema", zap.Error(err))
			return err
		}
		if typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyFieldSchema) {
			collInfo, colErr := globalMetaCache.GetCollectionInfo(t.ctx, t.request.GetDbName(), t.collectionName, t.CollectionID)
			if colErr != nil {
				log.Warn("failed to get collection info", zap.Error(colErr))
				return err
			}

			if collInfo.partitionKeyIsolation {
				expr, err := exprutil.ParseExprFromPlan(plan)
				if err != nil {
					log.Warn("failed to parse expr from plan during MV", zap.Error(err))
					return err
				}
				err = exprutil.ValidatePartitionKeyIsolation(expr)
				if err != nil {
					return err
				}
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
	// fetch search_growing from search param
	t.SearchRequest.SubReqs = make([]*internalpb.SubSearchRequest, len(t.request.GetSubReqs()))
	t.queryInfos = make([]*planpb.QueryInfo, len(t.request.GetSubReqs()))
	for index, subReq := range t.request.GetSubReqs() {
		plan, queryInfo, offset, err := t.tryGeneratePlan(subReq.GetSearchParams(), subReq.GetDsl(), true)
		if err != nil {
			return err
		}
		if queryInfo.GetGroupByFieldId() != -1 {
			return errors.New("not support search_group_by operation in the hybrid search")
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
		}

		// set PartitionIDs for sub search
		if t.partitionKeyMode {
			// isolatioin has tighter constraint, check first
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

		if t.requery {
			plan.OutputFieldIds = nil
			plan.DynamicFields = nil
		} else {
			plan.OutputFieldIds = t.SearchRequest.OutputFieldsId
			plan.DynamicFields = t.userDynamicFields
		}

		internalSubReq.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		t.SearchRequest.SubReqs[index] = internalSubReq
		t.queryInfos[index] = queryInfo
		log.Debug("proxy init search request",
			zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
			zap.Stringer("plan", plan)) // may be very large if large term passed.
	}
	// used for requery
	if t.partitionKeyMode {
		t.SearchRequest.PartitionIDs = t.partitionIDsSet.Collect()
	}
	var err error
	t.reScorers, err = NewReScorers(len(t.request.GetSubReqs()), t.request.GetSearchParams())
	if err != nil {
		log.Info("generate reScorer failed", zap.Any("params", t.request.GetSearchParams()), zap.Error(err))
		return err
	}
	return nil
}

func (t *searchTask) initSearchRequest(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "init search request")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.GetCollectionID()), zap.String("collName", t.collectionName))
	// fetch search_growing from search param

	plan, queryInfo, offset, err := t.tryGeneratePlan(t.request.GetSearchParams(), t.request.GetDsl(), false)
	if err != nil {
		return err
	}

	t.SearchRequest.Offset = offset

	if t.partitionKeyMode {
		// isolatioin has tighter constraint, check first
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

	if t.requery {
		plan.OutputFieldIds = nil
	} else {
		plan.OutputFieldIds = t.SearchRequest.OutputFieldsId
		plan.DynamicFields = t.userDynamicFields
	}

	t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup
	t.SearchRequest.Topk = queryInfo.GetTopk()
	t.SearchRequest.MetricType = queryInfo.GetMetricType()
	t.queryInfos = append(t.queryInfos, queryInfo)
	t.SearchRequest.DslType = commonpb.DslType_BoolExprV1
	t.SearchRequest.ExtraSearchParam = &internalpb.ExtraSearchParam{GroupByFieldId: queryInfo.GroupByFieldId, GroupSize: queryInfo.GroupSize}
	log.Debug("proxy init search request",
		zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
		zap.Stringer("plan", plan)) // may be very large if large term passed.

	return nil
}

func (t *searchTask) tryGeneratePlan(params []*commonpb.KeyValuePair, dsl string, ignoreOffset bool) (*planpb.PlanNode, *planpb.QueryInfo, int64, error) {
	annsFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, params)
	if err != nil || len(annsFieldName) == 0 {
		vecFields := typeutil.GetVectorFieldSchemas(t.schema.CollectionSchema)
		if len(vecFields) == 0 {
			return nil, nil, 0, errors.New(AnnsFieldKey + " not found in schema")
		}

		if enableMultipleVectorFields && len(vecFields) > 1 {
			return nil, nil, 0, errors.New("multiple anns_fields exist, please specify a anns_field in search_params")
		}
		annsFieldName = vecFields[0].Name
	}
	queryInfo, offset, parseErr := parseSearchInfo(params, t.schema.CollectionSchema, ignoreOffset)
	if parseErr != nil {
		return nil, nil, 0, parseErr
	}
	annField := typeutil.GetFieldByName(t.schema.CollectionSchema, annsFieldName)
	if queryInfo.GetGroupByFieldId() != -1 && annField.GetDataType() == schemapb.DataType_BinaryVector {
		return nil, nil, 0, errors.New("not support search_group_by operation based on binary vector column")
	}
	plan, planErr := planparserv2.CreateSearchPlan(t.schema.schemaHelper, dsl, annsFieldName, queryInfo)
	if planErr != nil {
		log.Warn("failed to create query plan", zap.Error(planErr),
			zap.String("dsl", dsl), // may be very large if large term passed.
			zap.String("anns field", annsFieldName), zap.Any("query info", queryInfo))
		return nil, nil, 0, merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", planErr)
	}
	log.Debug("create query plan",
		zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
		zap.String("anns field", annsFieldName), zap.Any("query info", queryInfo))
	return plan, queryInfo, offset, nil
}

func (t *searchTask) tryParsePartitionIDsFromPlan(plan *planpb.PlanNode) ([]int64, error) {
	expr, err := exprutil.ParseExprFromPlan(plan)
	if err != nil {
		log.Warn("failed to parse expr", zap.Error(err))
		return nil, err
	}
	partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
	hashedPartitionNames, err := assignPartitionKeys(t.ctx, t.request.GetDbName(), t.collectionName, partitionKeys)
	if err != nil {
		log.Warn("failed to assign partition keys", zap.Error(err))
		return nil, err
	}

	if len(hashedPartitionNames) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		PartitionIDs, err2 := getPartitionIDs(t.ctx, t.request.GetDbName(), t.collectionName, hashedPartitionNames)
		if err2 != nil {
			log.Warn("failed to get partition ids", zap.Error(err2))
			return nil, err2
		}
		return PartitionIDs, nil
	}
	return nil, nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.Int64("nq", t.SearchRequest.GetNq()))

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

func (t *searchTask) reduceResults(ctx context.Context, toReduceResults []*internalpb.SearchResults, nq, topK int64, offset int64, queryInfo *planpb.QueryInfo) (*milvuspb.SearchResults, error) {
	metricType := ""
	if len(toReduceResults) >= 1 {
		metricType = toReduceResults[0].GetMetricType()
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "reduceResults")
	defer sp.End()

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
	result, err = reduceSearchResult(ctx, NewReduceSearchResultInfo(validSearchResults, nq, topK,
		metricType, primaryFieldSchema.DataType, offset, queryInfo))
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return nil, err
	}
	return result, nil
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
	for _, r := range toReduceResults {
		t.relatedDataSize += r.GetCostAggregation().GetTotalRelatedDataSize()
		for ch, ts := range r.GetChannelsMvcc() {
			t.queryChannelsTs[ch] = ts
		}
	}

	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}

	if t.SearchRequest.GetIsAdvanced() {
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

			metricType := ""
			if len(internalResults) >= 1 {
				metricType = internalResults[0].GetMetricType()
			}
			result, err := t.reduceResults(t.ctx, internalResults, subReq.GetNq(), subReq.GetTopk(), subReq.GetOffset(), t.queryInfos[index])
			if err != nil {
				return err
			}
			t.reScorers[index].setMetricType(metricType)
			t.reScorers[index].reScore(result)
			multipleMilvusResults[index] = result
		}
		t.result, err = rankSearchResultData(ctx, t.SearchRequest.GetNq(),
			t.rankParams,
			primaryFieldSchema.GetDataType(),
			multipleMilvusResults)
		if err != nil {
			log.Warn("rank search result failed", zap.Error(err))
			return err
		}
	} else {
		t.result, err = t.reduceResults(t.ctx, toReduceResults, t.SearchRequest.Nq, t.SearchRequest.GetTopk(), t.SearchRequest.GetOffset(), t.queryInfos[0])
		if err != nil {
			return err
		}
	}

	t.result.CollectionName = t.collectionName
	t.fillInFieldInfo()

	if t.requery {
		err = t.Requery()
		if err != nil {
			log.Warn("failed to requery", zap.Error(err))
			return err
		}
	}
	t.result.Results.OutputFields = t.userOutputFields
	t.result.CollectionName = t.request.GetCollectionName()

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
		return lo.Contains(t.request.GetOutputFields(), field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})
	// Currently, we get vectors by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	if len(vectorOutputFields) > 0 {
		return math.MaxInt64, nil
	}
	// If no vector field as output, no need to requery.
	return 0, nil

	//outputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
	//	return lo.Contains(t.request.GetOutputFields(), field.GetName())
	//})
	//sizePerRecord, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{Fields: outputFields})
	//if err != nil {
	//	return 0, err
	//}
	//return int64(sizePerRecord) * nq * topK, nil
}

func (t *searchTask) Requery() error {
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
		OutputFields:          t.request.GetOutputFields(),
		PartitionNames:        t.request.GetPartitionNames(),
		UseDefaultConsistency: false,
		GuaranteeTimestamp:    t.SearchRequest.GuaranteeTimestamp,
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(t.schema.CollectionSchema)
	if err != nil {
		return err
	}
	ids := t.result.GetResults().GetIds()
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
			ReqID:        paramtable.GetNodeID(),
			PartitionIDs: t.GetPartitionIDs(), // use search partitionIDs
		},
		request:      queryReq,
		plan:         plan,
		qc:           t.node.(*Proxy).queryCoord,
		lb:           t.node.(*Proxy).lbPolicy,
		channelsMvcc: channelsMvcc,
		fastSkip:     true,
		reQuery:      true,
	}
	queryResult, err := t.node.(*Proxy).query(t.ctx, qt)
	if err != nil {
		return err
	}
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(queryResult.GetStatus())
	}
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
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(t.ctx, "reorganizeRequeryResults")
	defer sp.End()
	pkFieldData, err := typeutil.GetPrimaryFieldData(queryResult.GetFieldsData(), pkField)
	if err != nil {
		return err
	}
	offsets := make(map[any]int)
	for i := 0; i < typeutil.GetPKSize(pkFieldData); i++ {
		pk := typeutil.GetData(pkFieldData, i)
		offsets[pk] = i
	}

	t.result.Results.FieldsData = make([]*schemapb.FieldData, len(queryResult.GetFieldsData()))
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := offsets[id]; !ok {
			return merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(offsets), t.GetCollectionID()))
		}
		typeutil.AppendFieldData(t.result.Results.FieldsData, queryResult.GetFieldsData(), int64(offsets[id]))
	}

	t.result.Results.FieldsData = lo.Filter(t.result.Results.FieldsData, func(fieldData *schemapb.FieldData, i int) bool {
		return lo.Contains(t.request.GetOutputFields(), fieldData.GetFieldName())
	})
	return nil
}

func (t *searchTask) fillInFieldInfo() {
	if len(t.request.OutputFields) != 0 && len(t.result.Results.FieldsData) != 0 {
		for i, name := range t.request.OutputFields {
			for _, field := range t.schema.Fields {
				if t.result.Results.FieldsData[i] != nil && field.Name == name {
					t.result.Results.FieldsData[i].FieldName = field.Name
					t.result.Results.FieldsData[i].FieldId = field.FieldID
					t.result.Results.FieldsData[i].Type = field.DataType
					t.result.Results.FieldsData[i].IsDynamic = field.IsDynamic
				}
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

func checkSearchResultData(data *schemapb.SearchResultData, nq int64, topk int64) error {
	if data.NumQueries != nq {
		return fmt.Errorf("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
	}
	if data.TopK != topk {
		return fmt.Errorf("search result's topk(%d) mis-match with %d", data.TopK, topk)
	}

	pkHitNum := typeutil.GetSizeOfIDs(data.GetIds())
	if len(data.Scores) != pkHitNum {
		return fmt.Errorf("search result's score length invalid, score length=%d, expectedLength=%d",
			len(data.Scores), pkHitNum)
	}
	return nil
}

func selectHighestScoreIndex(subSearchResultData []*schemapb.SearchResultData, subSearchNqOffset [][]int64, cursors []int64, qi int64) (int, int64) {
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
				log.Error("a bad score is returned, something is wrong here!", zap.Float32("score", sScore))
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
