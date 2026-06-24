package proxy

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/shallowcopy"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	SearchTaskName = "SearchTask"
	SearchLevelKey = "level"

	// requeryThreshold is the estimated threshold for the size of the search results.
	// If the number of estimated search results exceeds this threshold,
	// a second query request will be initiated to retrieve output fields data.
	// In this case, the first search will not return any output field from QueryNodes.
	requeryThreshold   = 0.5 * 1024 * 1024
	radiusKey          = "radius"
	rangeFilterKey     = "range_filter"
	iterativeFilterKey = "iterative_filter"
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
	partitionKeyIsolation  bool
	largeTopKEnabled       bool
	enableMaterializedView bool
	mustUsePartitionKey    bool
	resultSizeInsufficient bool
	isTopkReduce           bool
	isRecallEvaluation     bool

	translatedOutputFields []string
	userOutputFields       []string
	userDynamicFields      []string
	highlighter            Highlighter
	resultBuf              *typeutil.ConcurrentSet[*internalpb.SearchResults]

	partitionIDsSet *typeutil.ConcurrentSet[UniqueID]

	mixCoord          types.MixCoordClient
	node              types.ProxyComponent
	lb                shardclient.LBPolicy
	shardClientMgr    shardclient.ShardClientMgr
	queryChannelsTs   map[string]Timestamp
	queryChannelsNode *typeutil.ConcurrentMap[string, int64]
	queryInfos        []*planpb.QueryInfo
	relatedDataSize   int64

	// Rerank configuration metadata (nil means no rerank)
	rerankMeta rerankMeta
	rankParams *rankParams

	// Order by fields for sorting results
	orderByFields []OrderByField

	resolvedTimezoneStr string

	isIterator bool
	// we always remove pk field from output fields, as search result already contains pk field.
	// if the user explicitly set pk field in output fields, we add it back to the result.
	userRequestedPkFieldExplicitly bool

	storageCost  segcore.StorageCost
	traceEnabled bool

	aggCtx *search_agg.SearchAggregationContext

	// Old SDK sent only singular group_by_field; output must downgrade plural→singular.
	legacyGroupByWire bool

	hybridSubSearchInfos []hybridSubSearchInfo
	hybridElementLevel   bool
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
			mlog.Warn(t.ctx, "search task get collectionID failed, can't skip alloc timestamp",
				mlog.String("collectionName", t.request.GetCollectionName()), mlog.Err(err))
			return false
		}

		collectionInfo, err2 := globalMetaCache.GetCollectionInfo(context.Background(), t.request.GetDbName(), t.request.GetCollectionName(), collID)
		if err2 != nil {
			mlog.Warn(t.ctx, "search task get collection info failed, can't skip alloc timestamp",
				mlog.String("collectionName", t.request.GetCollectionName()), mlog.Err(err))
			return false
		}
		consistencyLevel = collectionInfo.consistencyLevel
	}
	return consistencyLevel != commonpb.ConsistencyLevel_Strong
}

func (t *searchTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PreExecute")
	defer sp.End()

	t.IsAdvanced = len(t.request.GetSubReqs()) > 0
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	t.DbID = 0 // todo
	t.CollectionID = collID
	log := mlog.With(mlog.Int64("collID", collID), mlog.String("collName", collectionName))
	t.schema, err = globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn(ctx, "get collection schema failed", mlog.Err(err))
		return err
	}
	if err := validateTextStorageV3Enabled(t.schema.CollectionSchema); err != nil {
		return err
	}

	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err2 != nil {
		log.Warn(ctx, "Proxy::searchTask::PreExecute failed to GetCollectionInfo from cache",
			mlog.String("collectionName", collectionName), mlog.Int64("collectionID", t.CollectionID), mlog.Err(err2))
		return err2
	}
	t.largeTopKEnabled = collectionInfo.queryMode == common.QueryModeLargeTopK
	t.partitionKeyIsolation = collectionInfo.partitionKeyIsolation

	t.partitionKeyMode, err = isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn(ctx, "is partition key mode failed", mlog.Err(err))
		return err
	}
	if t.partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if partition key mode is used")
	}
	if t.mustUsePartitionKey && !t.partitionKeyMode {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("must use partition key in the search request " +
			"because the mustUsePartitionKey config is true"))
	}

	if !t.partitionKeyMode && len(t.request.GetPartitionNames()) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		t.PartitionIDs, err = getPartitionIDs(ctx, t.request.GetDbName(), collectionName, t.request.GetPartitionNames())
		if err != nil {
			log.Warn(ctx, "failed to get partition ids", mlog.Err(err))
			return err
		}
	}
	err = common.CheckNamespace(t.schema.CollectionSchema, t.request.Namespace)
	if err != nil {
		return err
	}

	var aggs []agg.AggregateBase
	t.translatedOutputFields, t.userOutputFields, t.userDynamicFields, aggs, t.userRequestedPkFieldExplicitly, err = translateOutputFields(t.request.OutputFields, t.schema, true)
	if err != nil {
		log.Warn(ctx, "translate output fields failed", mlog.Err(err), mlog.Any("schema", t.schema))
		return err
	}
	if len(aggs) > 0 {
		log.Warn(ctx, "aggregates are not supported in search request", mlog.Strings("aggregates", lo.Map(aggs, func(agg agg.AggregateBase, _ int) string { return agg.OriginalName() })))
		return merr.WrapErrParameterInvalidMsg("aggregates are not supported in search request")
	}
	log.Debug(ctx, "translate output fields",
		mlog.Strings("output fields", t.translatedOutputFields))

	if t.GetIsAdvanced() {
		if len(t.request.GetSubReqs()) > defaultMaxSearchRequest {
			return merr.WrapErrParameterInvalidMsg("maximum of ann search requests is %d", defaultMaxSearchRequest)
		}
	}

	nq, err := t.checkNq(ctx)
	if err != nil {
		log.Info(ctx, "failed to check nq", mlog.Err(err))
		return err
	}
	t.Nq = nq

	if t.IgnoreGrowing, err = isIgnoreGrowing(t.request.SearchParams); err != nil {
		return err
	}

	outputFieldIDs, err := getOutputFieldIDs(t.schema, t.translatedOutputFields)
	if err != nil {
		log.Info(ctx, "fail to get output field ids", mlog.Err(err))
		return err
	}
	t.OutputFieldsId = outputFieldIDs

	// Currently, we get vectors by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	traceVal, _ := funcutil.GetAttrByKeyFromRepeatedKV(PipelineTraceKey, t.request.GetSearchParams())
	t.traceEnabled = strings.EqualFold(traceVal, "true")
	// initSearchAggregation must run before init{,Advanced}SearchRequest so
	// that t.SearchRequest.GroupByFieldIds and t.aggCtx are populated before
	// queryInfo is built and captured — otherwise queryInfo.GroupByFieldIds
	// stays empty at reduce stage and DerivedTopK/DerivedGroupSize overrides
	// are skipped.
	if err = t.initSearchAggregation(); err != nil {
		log.Debug(ctx, "init search aggregation failed", mlog.Err(err))
		return err
	}

	if t.GetIsAdvanced() {
		err = t.initAdvancedSearchRequest(ctx)
	} else {
		err = t.initSearchRequest(ctx)
	}
	if err != nil {
		log.Debug(ctx, "init search request failed", mlog.Err(err))
		return err
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

	// update actual consistency level
	accesslog.SetActualConsistencyLevel(ctx, consistencyLevel)

	// use collection schema updated timestamp if it's greater than calculate guarantee timestamp
	// this make query view updated happens before new read request happens
	// see also schema change design
	if collectionInfo.updateTimestamp > guaranteeTs {
		guaranteeTs = collectionInfo.updateTimestamp
	}

	t.GuaranteeTimestamp = guaranteeTs
	// Extract physical time for entity-level TTL (issue #47413)
	physicalTimeMs, _ := tsoutil.ParseHybridTs(guaranteeTs)
	t.EntityTtlPhysicalTime = uint64(physicalTimeMs * 1000)
	t.ConsistencyLevel = consistencyLevel
	if t.isIterator && t.request.GetGuaranteeTimestamp() > 0 {
		t.MvccTimestamp = t.request.GetGuaranteeTimestamp()
		t.GuaranteeTimestamp = t.request.GetGuaranteeTimestamp()
	}
	t.IsIterator = t.isIterator

	if deadline, ok := t.TraceCtx().Deadline(); ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	// Set username of this search request for feature like task scheduling.
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		t.Username = username
	}

	if collectionInfo.collectionTTL != 0 {
		physicalTime := tsoutil.PhysicalTime(t.GetBase().GetTimestamp())
		expireTime := physicalTime.Add(-time.Duration(collectionInfo.collectionTTL))
		t.CollectionTtlTimestamps = tsoutil.ComposeTSByTime(expireTime, 0)
		// preventing overflow, abort
		if t.CollectionTtlTimestamps > t.GetBase().GetTimestamp() {
			return merr.WrapErrServiceInternalMsg("ttl timestamp overflow, base timestamp: %d, ttl duration %v", t.GetBase().GetTimestamp(), collectionInfo.collectionTTL)
		}
	}

	timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.request.SearchParams)
	if exist {
		if !timestamptz.IsTimezoneValid(timezone) {
			log.Info(ctx, "get invalid timezone from request", mlog.String("timezone", timezone))
			return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", timezone)
		}
		log.Debug(ctx, "determine timezone from request", mlog.String("user defined timezone", timezone))
	} else {
		timezone = getColTimezone(collectionInfo)
		log.Debug(ctx, "determine timezone from collection", mlog.Any("collection timezone", timezone))
	}
	t.resolvedTimezoneStr = timezone

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.SearchResults]()

	if err = ValidateTask(t); err != nil {
		return err
	}

	log.Debug(ctx, "search PreExecute done.",
		mlog.Uint64("guarantee_ts", guaranteeTs),
		mlog.Bool("use_default_consistency", useDefaultConsistency),
		mlog.Any("consistency level", consistencyLevel),
		mlog.Uint64("timeout_ts", t.GetTimeoutTimestamp()),
		mlog.Uint64("collection_ttl_timestamps", t.CollectionTtlTimestamps))
	return nil
}

func (t *searchTask) checkNq(ctx context.Context) (int64, error) {
	var nq int64
	if t.GetIsAdvanced() {
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
		return 0, merr.WrapErrParameterInvalidMsg("%s [%d] is invalid, %v", NQKey, nq, err)
	}
	return nq, nil
}

func (t *searchTask) initSearchAggregation() error {
	spec := t.request.GetSearchAggregation()
	if spec == nil {
		t.aggCtx = nil
		t.GroupByFieldIds = nil
		return nil
	}

	if t.GetIsAdvanced() {
		return merr.WrapErrParameterInvalidMsg("search aggregation is not supported for hybrid search")
	}

	if t.request.GetHighlighter() != nil {
		return merr.WrapErrParameterInvalidMsg("highlighter and search_aggregation cannot be used simultaneously")
	}

	groupByField, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, t.request.GetSearchParams())
	if err == nil && strings.TrimSpace(groupByField) != "" {
		return merr.WrapErrParameterInvalidMsg("group_by_field and search_aggregation cannot be used simultaneously")
	}
	groupByFields, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldsKey, t.request.GetSearchParams())
	if err == nil && strings.TrimSpace(groupByFields) != "" {
		return merr.WrapErrParameterInvalidMsg("group_by_fields and search_aggregation cannot be used simultaneously")
	}

	aggCtx, err := search_agg.BuildSearchAggregationContext(spec, t.schema.CollectionSchema, t.GetNq())
	if err != nil {
		return err
	}

	t.GroupByFieldIds = aggCtx.AllGroupByFieldIDs()
	for _, fieldID := range t.GetOutputFieldsId() {
		aggCtx.UserOutputFieldIDs[fieldID] = struct{}{}
	}

	// Append metric source / top_hits sort fields to OutputFieldsId so segcore
	// writes them into fields_data. Group-by fields are delivered separately
	// via SearchResultData.group_by_field_values and must NOT be appended here.
	if extra := aggCtx.ExtraOutputFieldIDs(); len(extra) > 0 {
		outputSet := typeutil.NewSet[int64](t.GetOutputFieldsId()...)
		outputSet.Insert(extra...)
		t.OutputFieldsId = outputSet.Collect()
	}

	t.aggCtx = aggCtx
	return nil
}

func (t *searchTask) validatePartitionKeyIsolation(plan *planpb.PlanNode) error {
	if !t.partitionKeyIsolation {
		return nil
	}
	expr, err := exprutil.ParseExprFromPlan(plan)
	if err != nil {
		mlog.Warn(t.ctx, "failed to parse expr from plan when validating partition key isolation", mlog.Err(err))
		return err
	}
	return exprutil.ValidatePartitionKeyIsolation(expr)
}

func setQueryInfoIfMvEnable(queryInfo *planpb.QueryInfo, t *searchTask) error {
	if t.enableMaterializedView {
		partitionKeyFieldSchema, err := typeutil.GetPartitionKeyFieldSchema(t.schema.CollectionSchema)
		if err != nil {
			mlog.Warn(t.ctx, "failed to get partition key field schema", mlog.Err(err))
			return err
		}
		if typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyFieldSchema) {
			if t.partitionKeyIsolation {
				queryInfo.Hints = "disable"
			}
			queryInfo.MaterializedViewInvolved = true
		} else {
			return merr.WrapErrParameterInvalidMsg("partition key field data type is not supported in materialized view")
		}
	}
	return nil
}

func (t *searchTask) initAdvancedSearchRequest(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "init advanced search request")
	defer sp.End()
	t.partitionIDsSet = typeutil.NewConcurrentSet[UniqueID]()
	log := mlog.With(mlog.Int64("collID", t.GetCollectionID()), mlog.String("collName", t.collectionName))

	// Old SDK hybrid callers pass only singular group_by_field; output must
	// downgrade plural→singular the same way the non-advanced path does.
	_, errGroupByField := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, t.request.GetSearchParams())
	_, errGroupByFields := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldsKey, t.request.GetSearchParams())
	t.legacyGroupByWire = errGroupByField == nil && errGroupByFields != nil && t.request.GetSearchAggregation() == nil

	var err error
	if t.request.FunctionScore != nil {
		t.rerankMeta = newRerankMeta(t.schema.CollectionSchema, t.request.FunctionScore)
	} else {
		t.rerankMeta = newRerankMetaFromLegacy(t.request.GetSearchParams())
	}

	allFields := typeutil.GetAllFieldSchemas(t.schema.CollectionSchema)
	vectorOutputFields := lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})
	// TEXT type output fields need requery since TEXT data is stored as LOB references
	textOutputFields := lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsTextType(field.GetDataType())
	})

	if t.rankParams, err = parseRankParams(t.request.GetSearchParams(), t.schema.CollectionSchema, t.largeTopKEnabled); err != nil {
		log.Error(ctx, "parseRankParams failed", mlog.Err(err))
		return err
	}

	// Parse order_by_fields from main search params
	if t.orderByFields, err = parseOrderByFields(t.request.GetSearchParams(), t.schema.CollectionSchema); err != nil {
		log.Error(ctx, "parseOrderByFields failed", mlog.Err(err))
		return err
	}

	// order_by is not supported for hybrid search
	if len(t.orderByFields) > 0 {
		return merr.WrapErrParameterInvalidMsg("order_by is not supported for hybrid search")
	}

	switch strings.ToLower(paramtable.Get().CommonCfg.HybridSearchRequeryPolicy.GetValue()) {
	case "always":
		t.needRequery = true
	case "outputvector":
		// hybrid group by not support non-requery due to pk-group by field binding not guaranteed
		// TEXT fields also need requery since data is stored as LOB references
		t.needRequery = len(vectorOutputFields) > 0 || len(textOutputFields) > 0 || t.rankParams.GetGroupByFieldId() >= 0
	case "outputfields":
		fallthrough
	default:
		t.needRequery = len(t.request.GetOutputFields()) > 0
	}
	t.needRequery = t.needRequery || (t.rerankMeta != nil && len(t.rerankMeta.GetInputFieldNames()) > 0)
	if t.skipRequeryByNamespacePartitionMode() {
		t.needRequery = false
	}

	t.SubReqs = make([]*internalpb.SubSearchRequest, len(t.request.GetSubReqs()))
	t.queryInfos = make([]*planpb.QueryInfo, len(t.request.GetSubReqs()))
	t.hybridSubSearchInfos = make([]hybridSubSearchInfo, len(t.request.GetSubReqs()))
	t.hybridElementLevel = false
	queryFieldIDs := []int64{}
	for index, subReq := range t.request.GetSubReqs() {
		// For hybrid search, order_by_fields comes from main search params, not sub-search params
		plan, queryInfo, offset, subIsIterator, _, searchType, err := t.tryGeneratePlan(subReq.GetSearchParams(), subReq.GetDsl(), subReq.GetExprTemplateValues())
		if err != nil {
			return err
		}

		convertedPlaceholder, placeholderType, err := t.convertPlaceholderIfNeeded(subReq.GetPlaceholderGroup(), queryInfo.GetQueryFieldId())
		if err != nil {
			return err
		}
		if err := validateElementFilterVectorSearch(plan, t.schema.CollectionSchema, queryInfo.GetQueryFieldId(), placeholderType); err != nil {
			return err
		}

		subSearchInfo := classifyHybridSubSearch(t.schema.CollectionSchema, queryInfo.GetQueryFieldId(), placeholderType)
		annsField := typeutil.GetField(t.schema.CollectionSchema, queryInfo.GetQueryFieldId())
		collapseConfig, elementScopeProvided, sanitizedSearchParams, err := parseAndRemoveElementScope(queryInfo.GetSearchParams())
		if err != nil {
			return err
		}
		if elementScopeProvided {
			if subSearchInfo.Kind != hybridSubSearchStructElement {
				return merr.WrapErrParameterInvalidMsg("%s is only supported for element-level search on struct array vector sub-fields", elementScopeKey)
			}
			if err := validateElementCollapseMetricType(collapseConfig, resolveElementCollapseMetricType(queryInfo.GetMetricType(), annsField)); err != nil {
				return err
			}
			queryInfo.SearchParams = sanitizedSearchParams
			subSearchInfo.ElementScopeProvided = true
			subSearchInfo.Collapse = collapseConfig
		}

		// ArrayOfVector hybrid validation is kind-specific: embedding-list
		// rejects range/iterator here, element-level rejects legacy iterator
		// here, and group-by is validated after same-struct inference below.

		// Hybrid search only supports plain top-K on ArrayOfVector fields. Both
		// element-level and embedding-list searches reject advanced controls here.
		if annsField != nil && annsField.GetDataType() == schemapb.DataType_ArrayOfVector {
			isStructElementSubSearch := subSearchInfo.Kind == hybridSubSearchStructElement
			isStructEmbListSubSearch := subSearchInfo.Kind == hybridSubSearchStructEmbList
			if isStructElementSubSearch || isStructEmbListSubSearch {
				searchKind := "element-level"
				if isStructEmbListSubSearch {
					searchKind = "embedding-list"
				}
				if isStructEmbListSubSearch && gjson.Get(queryInfo.GetSearchParams(), radiusKey).Exists() {
					return merr.WrapErrParameterInvalid("", "",
						"range search is not supported for vector array ("+searchKind+") fields in hybrid search, fieldName:"+annsField.GetName())
				}
				if subIsIterator {
					return merr.WrapErrParameterInvalid("", "",
						"search iterator is not supported for vector array ("+searchKind+") fields in hybrid search, fieldName:"+annsField.GetName())
				}
			}
		}
		t.hybridSubSearchInfos[index] = subSearchInfo

		ignoreGrowing := t.IgnoreGrowing
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
			SearchType:         searchType,
		}

		// set analyzer name for sub search
		analyzer, err := funcutil.GetAttrByKeyFromRepeatedKV(AnalyzerKey, subReq.GetSearchParams())
		if err == nil {
			internalSubReq.AnalyzerName = analyzer
		}

		internalSubReq.FieldId = queryInfo.GetQueryFieldId()

		queryFieldIDs = append(queryFieldIDs, internalSubReq.FieldId)
		// set PartitionIDs for sub search
		if t.partitionKeyMode {
			// isolation has tighter constraint, check first
			if err := t.validatePartitionKeyIsolation(plan); err != nil {
				return err
			}
			mvErr := setQueryInfoIfMvEnable(queryInfo, t)
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
			internalSubReq.PartitionIDs = t.GetPartitionIDs()
		}

		var rerankInputFieldIDs []int64
		if t.rerankMeta != nil {
			rerankInputFieldIDs = t.rerankMeta.GetInputFieldIDs()
		}
		if t.needRequery {
			plan.OutputFieldIds = rerankInputFieldIDs
		} else {
			primaryFieldSchema, err := t.schema.GetPkField()
			if err != nil {
				return err
			}
			allFieldIDs := typeutil.NewSet(t.OutputFieldsId...)
			allFieldIDs.Insert(rerankInputFieldIDs...)
			allFieldIDs.Insert(primaryFieldSchema.FieldID)
			plan.OutputFieldIds = allFieldIDs.Collect()
			plan.DynamicFields = t.userDynamicFields
		}
		plan.Namespace = t.request.Namespace

		internalSubReq.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		if typeutil.IsFieldSparseFloatVector(t.schema.CollectionSchema, internalSubReq.FieldId) {
			metrics.ProxySearchSparseNumNonZeros.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.collectionName, metrics.HybridSearchLabel, strconv.FormatInt(internalSubReq.FieldId, 10)).Observe(float64(typeutil.EstimateSparseVectorNNZFromPlaceholderGroup(internalSubReq.PlaceholderGroup, int(internalSubReq.GetNq()))))
		}
		internalSubReq.PlaceholderGroup = convertedPlaceholder
		t.SubReqs[index] = internalSubReq
		t.queryInfos[index] = queryInfo
		log.Debug(ctx, "proxy init search request",
			mlog.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
			mlog.Stringer("plan", plan)) // may be very large if large term passed.
	}

	t.hybridElementLevel = inferElementLevelHybrid(t.hybridSubSearchInfos)
	if err := t.validateHybridArrayOfVectorGroupBy(); err != nil {
		return err
	}
	for index, info := range t.hybridSubSearchInfos {
		if t.hybridElementLevel && info.ElementScopeProvided {
			return merr.WrapErrParameterInvalidMsg("%s is not allowed for same-struct element-level hybrid search", elementScopeKey)
		}
		if !t.hybridElementLevel && info.Kind == hybridSubSearchStructElement && !info.ElementScopeProvided {
			t.hybridSubSearchInfos[index].Collapse = defaultElementCollapseConfig()
		}
	}

	if embedding.HasNonBM25AndMinHashFunctions(t.schema.Functions, queryFieldIDs) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AdvancedSearch-call-function-udf")
		defer sp.End()
		exec, err := embedding.NewFunctionExecutor(t.schema.CollectionSchema, nil, &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: t.request.GetDbName()})
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessSearch(ctx, t.SearchRequest); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}

	t.GroupByFieldId = t.rankParams.GetGroupByFieldId()
	t.GroupSize = t.rankParams.GetGroupSize()

	if t.partitionKeyMode {
		t.PartitionIDs = t.partitionIDsSet.Collect()
	}

	return nil
}

func (t *searchTask) validateHybridArrayOfVectorGroupBy() error {
	groupByFieldIDs := t.rankParams.GetGroupByFieldIds()
	if len(groupByFieldIDs) == 0 {
		return nil
	}

	fieldName := func(index int) string {
		if index >= 0 && index < len(t.queryInfos) {
			if field := typeutil.GetField(t.schema.CollectionSchema, t.queryInfos[index].GetQueryFieldId()); field != nil {
				return field.GetName()
			}
		}
		return ""
	}

	hasStructElementSubSearch := false
	for index, info := range t.hybridSubSearchInfos {
		switch info.Kind {
		case hybridSubSearchStructEmbList:
			return merr.WrapErrParameterInvalid("", "",
				"group by search is not supported for vector array (embedding-list) fields in hybrid search, fieldName:"+fieldName(index))
		case hybridSubSearchStructElement:
			hasStructElementSubSearch = true
			if !t.hybridElementLevel {
				return merr.WrapErrParameterInvalid("", "",
					"group by search is only supported for same-struct element-level vector array fields in hybrid search, fieldName:"+fieldName(index))
			}
		}
	}
	if !hasStructElementSubSearch {
		return nil
	}

	pkField, err := t.schema.GetPkField()
	if err != nil {
		return err
	}
	if len(groupByFieldIDs) != 1 || pkField == nil || groupByFieldIDs[0] != pkField.GetFieldID() {
		return merr.WrapErrParameterInvalid("", "",
			"only group by primary key is supported for same-struct element-level vector array fields in hybrid search")
	}
	return nil
}

func (t *searchTask) fillResult() {
	limit := t.GetTopk() - t.GetOffset()
	resultSizeInsufficient := false
	if t.aggCtx == nil {
		for _, topk := range t.result.Results.Topks {
			if topk < limit {
				resultSizeInsufficient = true
				break
			}
		}
	}
	t.resultSizeInsufficient = resultSizeInsufficient
	t.result.CollectionName = t.collectionName
}

func (t *searchTask) getBM25SearchTexts(placeholder []byte) ([]string, error) {
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(placeholder, pb); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to unmarshal BM25 search placeholder group: %v", err)
	}

	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search")
	}

	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search, got %s", holder.Type.String())
	}

	texts := funcutil.GetVarCharFromPlaceholder(holder)
	return texts, nil
}

func (t *searchTask) createLexicalHighlighter(highlighter *commonpb.Highlighter, metricType string, annsField int64, placeholder []byte, analyzerName string) error {
	h, err := NewLexicalHighlighter(highlighter)
	if err != nil {
		return err
	}
	t.highlighter = h
	if h.highlightSearch {
		if metricType != metric.BM25 {
			return merr.WrapErrParameterInvalidMsg(`Search highlight only support with metric type "BM25" but was: %s`, t.GetMetricType())
		}
		function, ok := getBM25FunctionOfAnnsField(annsField, t.schema.GetFunctions())
		if !ok {
			return merr.WrapErrServiceInternal(`Search with highlight failed, input field of BM25 annsField not found`)
		}
		fieldId := function.InputFieldIds[0]
		fieldName := function.InputFieldNames[0]
		// set bm25 search text as highlight search texts
		texts, err := t.getBM25SearchTexts(placeholder)
		if err != nil {
			return err
		}
		err = h.addTaskWithSearchText(t.schema, fieldId, fieldName, analyzerName, texts)
		if err != nil {
			return err
		}
	}
	return h.initHighlightQueries(t)
}

func (t *searchTask) addHighlightTask(highlighter *commonpb.Highlighter, metricType string, annsField int64, placeholder []byte, analyzerName string) error {
	if highlighter == nil {
		return nil
	}

	switch highlighter.GetType() {
	case commonpb.HighlightType_Lexical:
		return t.createLexicalHighlighter(highlighter, metricType, annsField, placeholder, analyzerName)
	case commonpb.HighlightType_Semantic:
		h, err := newSemanticHighlighter(t, &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: t.request.GetDbName()})
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("Create SemanticHighlight failed: %v ", err)
		}
		t.highlighter = h
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported highlight type: %v", highlighter.GetType())
	}
}

func (t *searchTask) initSearchRequest(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "init search request")
	defer sp.End()

	log := mlog.With(mlog.Int64("collID", t.GetCollectionID()), mlog.String("collName", t.collectionName))

	// Old SDK: group_by_field only, no group_by_fields, no agg → output as singular.
	_, errGroupByField := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, t.request.GetSearchParams())
	_, errGroupByFields := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldsKey, t.request.GetSearchParams())
	t.legacyGroupByWire = errGroupByField == nil && errGroupByFields != nil && t.request.GetSearchAggregation() == nil

	plan, queryInfo, offset, isIterator, orderByFields, searchType, err := t.tryGeneratePlan(t.request.GetSearchParams(), t.request.GetDsl(), t.request.GetExprTemplateValues())
	if err != nil {
		return err
	}
	t.orderByFields = orderByFields

	t.SearchType = searchType

	if t.request.FunctionScore != nil {
		t.rerankMeta = newRerankMeta(t.schema.CollectionSchema, t.request.FunctionScore)
	}

	// order_by and function_score cannot be used together
	if len(t.orderByFields) > 0 && t.rerankMeta != nil {
		return merr.WrapErrParameterInvalidMsg("order_by and function_score cannot be used together: they specify conflicting sort criteria")
	}

	analyzer, err := funcutil.GetAttrByKeyFromRepeatedKV(AnalyzerKey, t.request.GetSearchParams())
	if err == nil {
		t.AnalyzerName = analyzer
	}

	t.isIterator = isIterator
	t.Offset = offset
	if t.aggCtx != nil {
		if t.isIterator {
			return merr.WrapErrParameterInvalidMsg("search iterator is not supported with search_aggregation")
		}
		if t.Offset > 0 {
			return merr.WrapErrParameterInvalidMsg("offset is not supported with search_aggregation")
		}
	}
	t.FieldId = queryInfo.GetQueryFieldId()

	if err := t.addHighlightTask(t.request.GetHighlighter(), queryInfo.GetMetricType(), queryInfo.GetQueryFieldId(), t.request.GetPlaceholderGroup(), t.GetAnalyzerName()); err != nil {
		return err
	}

	// add highlight field ids to output fields id
	if t.highlighter != nil {
		t.OutputFieldsId = append(t.OutputFieldsId, t.highlighter.RequiredFieldIDs()...)
	}

	if t.partitionKeyMode {
		// isolation has tighter constraint, check first
		if err := t.validatePartitionKeyIsolation(plan); err != nil {
			return err
		}
		mvErr := setQueryInfoIfMvEnable(queryInfo, t)
		if mvErr != nil {
			return mvErr
		}
		partitionIDs, err2 := t.tryParsePartitionIDsFromPlan(plan)
		if err2 != nil {
			return err2
		}
		if len(partitionIDs) > 0 {
			t.PartitionIDs = partitionIDs
		}
	}

	if t.aggCtx != nil {
		t.needRequery = false
	} else {
		allFields := typeutil.GetAllFieldSchemas(t.schema.CollectionSchema)
		vectorOutputFields := lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
			return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType())
		})
		// TEXT type output fields need requery since TEXT data is stored as LOB references
		textOutputFields := lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
			return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsTextType(field.GetDataType())
		})
		switch strings.ToLower(paramtable.Get().CommonCfg.SearchRequeryPolicy.GetValue()) {
		case "always":
			t.needRequery = true
		case "outputfields":
			t.needRequery = len(t.request.GetOutputFields()) > 0
		case "outputvector":
			fallthrough
		default:
			t.needRequery = len(vectorOutputFields) > 0 || len(textOutputFields) > 0
		}
	}
	if t.skipRequeryByNamespacePartitionMode() {
		t.needRequery = false
	}
	var rerankInputFieldIDs []int64
	if t.rerankMeta != nil {
		rerankInputFieldIDs = t.rerankMeta.GetInputFieldIDs()
	}
	if t.needRequery {
		plan.OutputFieldIds = rerankInputFieldIDs
	} else {
		primaryFieldSchema, err := t.schema.GetPkField()
		if err != nil {
			return err
		}
		allFieldIDs := typeutil.NewSet[int64](t.OutputFieldsId...)
		allFieldIDs.Insert(rerankInputFieldIDs...)
		allFieldIDs.Insert(primaryFieldSchema.FieldID)
		plan.OutputFieldIds = allFieldIDs.Collect()
		plan.DynamicFields = t.userDynamicFields
		// Merge highlight dynamic fields into plan.DynamicFields
		if t.highlighter != nil {
			highlightDynFields := t.highlighter.DynamicFieldNames()
			if len(highlightDynFields) > 0 {
				dynFieldSet := typeutil.NewSet[string](plan.DynamicFields...)
				dynFieldSet.Insert(highlightDynFields...)
				plan.DynamicFields = dynFieldSet.Collect()
			}
		}
	}
	plan.Namespace = t.request.Namespace

	// Propagate agg-path overrides into queryInfo BEFORE plan serialization so
	// segcore sees the derived topK / groupSize and plural GroupByFieldIds.
	// strict_group_size is forced true: aggregation metrics need each top-K
	// group filled to groupSize for meaningful sample sizes.
	// Non-destructive: only overwrite queryInfo.GroupByFieldIds when aggCtx
	// populated t.SearchRequest.GroupByFieldIds. Non-agg plural group_by path
	// relies on parseSearchInfo having stamped it from the search_params
	// keyword; blind copy of an empty slice would clobber that.
	if ids := t.GetGroupByFieldIds(); len(ids) > 0 {
		queryInfo.GroupByFieldIds = ids
	}
	if t.aggCtx != nil {
		t.Topk = t.aggCtx.DerivedTopK
		t.GroupSize = t.aggCtx.DerivedGroupSize
		queryInfo.Topk = t.aggCtx.DerivedTopK
		queryInfo.GroupSize = t.aggCtx.DerivedGroupSize
		queryInfo.StrictGroupSize = true
	}

	t.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}
	t.PkFilter = checkSegmentFilter(plan)
	if typeutil.IsFieldSparseFloatVector(t.schema.CollectionSchema, t.FieldId) {
		metrics.ProxySearchSparseNumNonZeros.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.collectionName, metrics.SearchLabel, strconv.FormatInt(t.FieldId, 10)).Observe(float64(typeutil.EstimateSparseVectorNNZFromPlaceholderGroup(t.request.GetPlaceholderGroup(), int(t.request.GetNq()))))
	}
	// Convert placeholder group vector type if needed (e.g., fp32 -> fp16/bf16)
	var placeholderType commonpb.PlaceholderType
	t.PlaceholderGroup, placeholderType, err = t.convertPlaceholderIfNeeded(t.request.GetPlaceholderGroup(), t.FieldId)
	if err != nil {
		return err
	}
	if err := validateElementFilterVectorSearch(plan, t.schema.CollectionSchema, t.FieldId, placeholderType); err != nil {
		return err
	}

	// For ArrayOfVector fields, the placeholder type decides the search semantics:
	// - Element-level (plain vector placeholder): behaves like a normal single-vector
	//   search; supports range search, search iterator v2, and group by primary key.
	// - Embedding-list-level (multi-search-multi): does not support range search,
	//   iterator, or group by (other than the PK case above).
	annsField := typeutil.GetField(t.schema.CollectionSchema, t.FieldId)
	if annsField != nil && annsField.GetDataType() == schemapb.DataType_ArrayOfVector {
		isEmbList := isEmbeddingListPlaceholderType(placeholderType)

		if isEmbList {
			if gjson.Get(queryInfo.GetSearchParams(), radiusKey).Exists() {
				return merr.WrapErrParameterInvalid("", "",
					"range search is not supported for multi-search-multi on embedding list fields")
			}
			if t.isIterator {
				return merr.WrapErrParameterInvalid("", "",
					"search iterator is not supported for multi-search-multi on embedding list fields")
			}
		} else if t.isIterator && queryInfo.GetSearchIteratorV2Info() == nil {
			return merr.WrapErrParameterInvalid("", "",
				"legacy search iterator is not supported for element-level search; use search iterator v2")
		}

		groupByFieldIDs := queryInfo.GetGroupByFieldIds()
		if len(groupByFieldIDs) == 0 && queryInfo.GetGroupByFieldId() > 0 {
			groupByFieldIDs = []int64{queryInfo.GetGroupByFieldId()}
		}
		if len(groupByFieldIDs) > 0 {
			if isEmbList {
				return merr.WrapErrParameterInvalid("", "",
					"group by is not supported for multi-search-multi on embedding list fields")
			}
			pkField, _ := t.schema.GetPkField()
			for _, groupByFieldID := range groupByFieldIDs {
				if pkField == nil || groupByFieldID != pkField.GetFieldID() {
					return merr.WrapErrParameterInvalid("", "",
						"only group by primary key is supported for element-level search")
				}
			}
		}
	}

	t.Topk = queryInfo.GetTopk()
	t.MetricType = queryInfo.GetMetricType()

	t.queryInfos = append(t.queryInfos, queryInfo)
	t.DslType = commonpb.DslType_BoolExprV1
	t.GroupByFieldId = queryInfo.GroupByFieldId
	t.GroupSize = queryInfo.GroupSize
	if embedding.HasNonBM25AndMinHashFunctions(t.schema.Functions, []int64{queryInfo.GetQueryFieldId()}) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-call-function-udf")
		defer sp.End()
		exec, err := embedding.NewFunctionExecutor(t.schema.CollectionSchema, nil, &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: t.request.GetDbName()})
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessSearch(ctx, t.SearchRequest); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}

	log.Debug(ctx, "proxy init search request",
		mlog.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
		mlog.Stringer("plan", plan)) // may be very large if large term passed.

	return nil
}

func (t *searchTask) skipRequeryByNamespacePartitionMode() bool {
	return t.schema != nil &&
		t.schema.CollectionSchema != nil &&
		common.IsNamespaceModePartition(t.schema.GetProperties()...)
}

// convertPlaceholderIfNeeded converts fp32 vectors to fp16/bf16 if the target field uses lower precision.
// Returns converted bytes and the original placeholder type (before any conversion).
func (t *searchTask) convertPlaceholderIfNeeded(phgBytes []byte, fieldID int64) ([]byte, commonpb.PlaceholderType, error) {
	field := typeutil.GetFieldByID(t.schema.CollectionSchema, fieldID)
	if field == nil {
		return phgBytes, 0, nil
	}
	return ConvertPlaceholderGroup(phgBytes, field)
}

func (t *searchTask) tryGeneratePlan(params []*commonpb.KeyValuePair, dsl string, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, *planpb.QueryInfo, int64, bool, []OrderByField, internalpb.SearchType, error) {
	annsFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, params)
	if err != nil || len(annsFieldName) == 0 {
		vecFields := typeutil.GetVectorFieldSchemas(t.schema.CollectionSchema)
		if len(vecFields) == 0 {
			return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, merr.WrapErrParameterInvalidMsg(AnnsFieldKey + " not found in schema")
		}

		if enableMultipleVectorFields && len(vecFields) > 1 {
			return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, merr.WrapErrParameterInvalidMsg("multiple anns_fields exist, please specify a anns_field in search_params")
		}
		annsFieldName = vecFields[0].Name
	}
	searchInfo, err := parseSearchInfo(params, t.schema.CollectionSchema, t.rankParams, t.largeTopKEnabled)
	if err != nil {
		return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, err
	}
	if searchInfo.collectionID > 0 && searchInfo.collectionID != t.GetCollectionID() {
		return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, merr.WrapErrParameterInvalidMsg("collection id:%d in the request is not consistent to that in the search context,"+
			"alias or database may have been changed: %d", searchInfo.collectionID, t.GetCollectionID())
	}

	annField := typeutil.GetFieldByName(t.schema.CollectionSchema, annsFieldName)
	if searchInfo.planInfo.GetGroupByFieldId() != -1 && annField.GetDataType() == schemapb.DataType_BinaryVector {
		return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, merr.WrapErrParameterInvalidMsg("not support search_group_by operation based on binary vector column")
	}

	searchInfo.planInfo.QueryFieldId = annField.GetFieldID()

	hasFilter := dsl != "" || len(exprTemplateValues) > 0
	searchType := internalpb.SearchType_DEFAULT
	// if function score is not nil, set searchType to DEFAULT, optimizations will be disabled in queryhook
	if t.request.GetFunctionScore() == nil {
		searchType = searchInfo.DetermineSearchType(hasFilter)
	}

	start := time.Now()
	plan, planErr := planparserv2.CreateSearchPlanArgs(t.schema.schemaHelper, dsl, annsFieldName, searchInfo.planInfo, exprTemplateValues, t.request.GetFunctionScore(), &planparserv2.ParserVisitorArgs{Timezone: t.resolvedTimezoneStr})
	if planErr != nil {
		mlog.Warn(t.ctx, "failed to create query plan", mlog.Err(planErr),
			mlog.String("dsl", dsl), // may be very large if large term passed.
			mlog.String("anns field", annsFieldName), mlog.Any("query info", searchInfo.planInfo))
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "search", metrics.FailLabel).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
		return nil, nil, 0, false, nil, internalpb.SearchType_DEFAULT, merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", planErr)
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "search", metrics.SuccessLabel).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
	mlog.Debug(t.ctx, "create query plan",
		mlog.String("dsl", t.request.Dsl), // may be very large if large term passed.
		mlog.String("anns field", annsFieldName), mlog.Any("query info", searchInfo.planInfo))
	return plan, searchInfo.planInfo, searchInfo.offset, searchInfo.isIterator, searchInfo.orderByFields, searchType, nil
}

func (t *searchTask) tryParsePartitionIDsFromPlan(plan *planpb.PlanNode) ([]int64, error) {
	expr, err := exprutil.ParseExprFromPlan(plan)
	if err != nil {
		mlog.Warn(t.ctx, "failed to parse expr", mlog.Err(err))
		return nil, err
	}
	partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
	hashedPartitionNames, err := assignPartitionKeys(t.ctx, t.request.GetDbName(), t.collectionName, partitionKeys)
	if err != nil {
		mlog.Warn(t.ctx, "failed to assign partition keys", mlog.Err(err))
		return nil, err
	}

	if len(hashedPartitionNames) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		PartitionIDs, err2 := getPartitionIDs(t.ctx, t.request.GetDbName(), t.collectionName, hashedPartitionNames)
		if err2 != nil {
			mlog.Warn(t.ctx, "failed to get partition ids", mlog.Err(err2))
			return nil, err2
		}
		return PartitionIDs, nil
	}
	return nil, nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-Execute")
	defer sp.End()
	log := mlog.WithLazy(mlog.Int64("nq", t.GetNq()))

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	t.queryChannelsNode = typeutil.NewConcurrentMap[string, int64]()
	err := t.lb.Execute(ctx, shardclient.CollectionWorkLoad{
		Db:             t.request.GetDbName(),
		CollectionID:   t.CollectionID,
		CollectionName: t.collectionName,
		Nq:             t.Nq,
		Exec:           t.searchShard,
	})
	if err != nil {
		log.Warn(ctx, "search execute failed", mlog.Err(err))
		return errors.Wrap(err, "failed to search")
	}

	log.Debug(ctx, "Search Execute done.",
		mlog.Int64("collection", t.GetCollectionID()),
		mlog.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
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

func isEmbeddingListPlaceholderType(pt commonpb.PlaceholderType) bool {
	switch pt {
	case commonpb.PlaceholderType_EmbListFloatVector,
		commonpb.PlaceholderType_EmbListFloat16Vector,
		commonpb.PlaceholderType_EmbListBFloat16Vector,
		commonpb.PlaceholderType_EmbListBinaryVector,
		commonpb.PlaceholderType_EmbListInt8Vector:
		return true
	default:
		return false
	}
}

func validateElementFilterVectorSearch(plan *planpb.PlanNode, schema *schemapb.CollectionSchema, fieldID int64, placeholderType commonpb.PlaceholderType) error {
	anns := plan.GetVectorAnns()
	if anns == nil {
		return nil
	}
	elementFilter := anns.GetPredicates().GetElementFilterExpr()
	if elementFilter == nil {
		return nil
	}

	field := typeutil.GetField(schema, fieldID)
	parentStructName, isStructSubField := getStructParentFieldName(schema, fieldID)
	_, isPlainVectorPlaceholderType := placeholderTypeToDataType[placeholderType]
	if field != nil &&
		field.GetDataType() == schemapb.DataType_ArrayOfVector &&
		isStructSubField &&
		parentStructName == elementFilter.GetStructName() &&
		isPlainVectorPlaceholderType {
		return nil
	}

	return merr.WrapErrParameterInvalidMsg(
		"element_filter is only supported for element-level search on vector sub-fields of the same struct array; use MATCH_ANY/MATCH_* for row-level vector search")
}

func (t *searchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PostExecute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()
	log := mlog.With(mlog.Int64("nq", t.GetNq()))

	toReduceResults, err := t.collectSearchResults(ctx)
	if err != nil {
		log.Warn(ctx, "failed to collect search results", mlog.Err(err))
		return err
	}

	t.queryChannelsTs = make(map[string]uint64)
	t.relatedDataSize = 0
	isTopkReduce := false
	isRecallEvaluation := false
	storageCost := segcore.StorageCost{}
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
		storageCost.ScannedRemoteBytes += r.GetScannedRemoteBytes()
		storageCost.ScannedTotalBytes += r.GetScannedTotalBytes()
	}

	t.isTopkReduce = isTopkReduce
	t.isRecallEvaluation = isRecallEvaluation

	// call pipeline
	pipeline, err := newSearchPipeline(t)
	if err != nil {
		log.Warn(ctx, "Faild to create post process pipeline")
		return err
	}
	if t.result, t.storageCost, err = pipeline.Run(ctx, sp, toReduceResults, storageCost); err != nil {
		return err
	}
	t.fillResult()
	t.result.Results.OutputFields = t.userOutputFields
	reconstructStructFieldDataForSearch(t.result, t.schema.CollectionSchema)
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

	fieldsData := t.result.GetResults().GetFieldsData()
	for i, fieldData := range fieldsData {
		if fieldData.Type == schemapb.DataType_Geometry {
			if err := validateGeometryFieldSearchResult(&fieldsData[i]); err != nil {
				log.Warn(ctx, "fail to validate geometry field search result", mlog.Err(err))
				return err
			}
		}
	}
	// Validate Geometry on all group-by columns. All internal pipeline
	// stages emit plural; runs before the legacy-wire downgrade below.
	for i, gbv := range t.result.GetResults().GetGroupByFieldValues() {
		if gbv != nil && gbv.GetType() == schemapb.DataType_Geometry {
			if err := validateGeometryFieldSearchResult(&t.result.Results.GroupByFieldValues[i]); err != nil {
				log.Warn(ctx, "fail to validate geometry field search result", mlog.Err(err))
				return err
			}
		}
	}

	if t.isIterator && t.request.GetGuaranteeTimestamp() == 0 {
		// first page for iteration, need to set up sessionTs for iterator
		t.result.SessionTs = getMaxMvccTsFromChannels(t.queryChannelsTs, t.BeginTs())
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Microseconds()) / 1000.0)

	timeFields := parseTimeFields(t.request.SearchParams)
	if timeFields != nil {
		log.Debug(ctx, "extracting fields for timestamptz", mlog.Strings("fields", timeFields))
		err = extractFieldsFromResults(t.result.GetResults().GetFieldsData(), t.resolvedTimezoneStr, timeFields)
		if err != nil {
			log.Warn(ctx, "fail to extract fields for timestamptz", mlog.Err(err))
			return err
		}
	} else {
		err = timestamptzUTC2IsoStr(t.result.GetResults().GetFieldsData(), t.resolvedTimezoneStr)
		if err != nil {
			log.Warn(ctx, "fail to translate timestamp", mlog.Err(err))
			return err
		}
	}

	// Legacy-wire downgrade: the old SDK only reads the singular channel
	// (GroupByFieldValue). All internal pipeline stages emit to the plural
	// channel; this is the single boundary that moves the 1-element plural
	// column back into the singular channel so old clients see the group-by
	// result. New-SDK requests leave the plural channel as-is.
	if t.legacyGroupByWire {
		if rd := t.result.GetResults(); rd != nil && len(rd.GetGroupByFieldValues()) == 1 {
			rd.GroupByFieldValue = rd.GroupByFieldValues[0]
			rd.GroupByFieldValues = nil
		}
	}

	log.Debug(ctx, "Search post execute done",
		mlog.Int64("collection", t.GetCollectionID()),
		mlog.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	ctx = retry.WithMaxAttemptsContext(ctx, 1)
	searchReq := shallowcopy.ShallowCopySearchRequest(t.SearchRequest, nodeID)
	req := &querypb.SearchRequest{
		Req:             searchReq,
		DmlChannels:     []string{channel},
		Scope:           querypb.DataScope_All,
		TotalChannelNum: int32(1),
	}

	log := mlog.With(mlog.Int64("collection", t.GetCollectionID()),
		mlog.Int64s("partitionIDs", t.GetPartitionIDs()),
		mlog.Int64("nodeID", nodeID),
		mlog.String("channel", channel))

	var result *internalpb.SearchResults
	var err error

	result, err = qn.Search(ctx, req)
	if err != nil {
		log.Warn(ctx, "QueryNode search return error", mlog.Err(err))
		// globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		t.shardClientMgr.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn(ctx, "QueryNode is not shardLeader")
		t.shardClientMgr.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return merr.Error(result.GetStatus())
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn(ctx, "QueryNode search result error",
			mlog.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to search on QueryNode %d", nodeID)
	}
	if t.resultBuf != nil {
		t.resultBuf.Insert(result)
	}
	if t.queryChannelsNode != nil {
		t.queryChannelsNode.Insert(channel, nodeID)
	}
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)

	return nil
}

func (t *searchTask) estimateResultSize(nq int64, topK int64) (int64, error) {
	vectorOutputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType())
	})
	for _, structArrayField := range t.schema.GetStructArrayFields() {
		for _, field := range structArrayField.GetFields() {
			if lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsVectorType(field.GetDataType()) {
				vectorOutputFields = append(vectorOutputFields, field)
			}
		}
	}
	// TEXT type output fields also need requery since TEXT data is stored as LOB references
	textOutputFields := lo.Filter(t.schema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return lo.Contains(t.translatedOutputFields, field.GetName()) && typeutil.IsTextType(field.GetDataType())
	})
	// Currently, we get vectors and TEXT by requery. Once we support getting vectors from search,
	// searches with small result size could no longer need requery.
	if len(vectorOutputFields) > 0 || len(textOutputFields) > 0 {
		return math.MaxInt64, nil
	}
	// If no vector or TEXT field as output, no need to requery.
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

func (t *searchTask) collectSearchResults(ctx context.Context) ([]*internalpb.SearchResults, error) {
	select {
	case <-t.TraceCtx().Done():
		mlog.Warn(ctx, "search task wait to finish timeout!")
		return nil, merr.Wrapf(t.TraceCtx().Err(), "search task wait to finish timeout, msgID=%d", t.ID())
	default:
		toReduceResults := make([]*internalpb.SearchResults, 0)
		mlog.Debug(ctx, "all searches are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.SearchResults) bool {
			toReduceResults = append(toReduceResults, res)
			mlog.Debug(ctx, "proxy receives one search result",
				mlog.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
		return toReduceResults, nil
	}
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
