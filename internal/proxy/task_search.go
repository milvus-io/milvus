package proxy

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/distance"
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
)

type searchTask struct {
	Condition
	*internalpb.SearchRequest
	ctx context.Context

	result  *milvuspb.SearchResults
	request *milvuspb.SearchRequest

	tr             *timerecord.TimeRecorder
	collectionName string
	schema         *schemapb.CollectionSchema
	requery        bool

	userOutputFields []string

	offset    int64
	resultBuf *typeutil.ConcurrentSet[*internalpb.SearchResults]

	qc   types.QueryCoord
	node types.ProxyComponent
	lb   LBPolicy
}

func getPartitionIDs(ctx context.Context, collectionName string, partitionNames []string) (partitionIDs []UniqueID, err error) {
	for _, tag := range partitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return nil, err
		}
	}

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	partitionsRecord := make(map[UniqueID]bool)
	partitionIDs = make([]UniqueID, 0, len(partitionNames))
	for _, partitionName := range partitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid partition: %s", partitionName)
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					partitionIDs = append(partitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("partition name %s not found", partitionName)
		}
	}
	return partitionIDs, nil
}

// parseSearchInfo returns QueryInfo and offset
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair) (*planpb.QueryInfo, int64, error) {
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(TopKKey + " not found in search_params")
	}
	topK, err := strconv.ParseInt(topKStr, 0, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("%s [%s] is invalid", TopKKey, topKStr)
	}
	if err := validateTopKLimit(topK); err != nil {
		return nil, 0, fmt.Errorf("%s [%d] is invalid, %w", TopKKey, topK, err)
	}

	var offset int64
	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, searchParamsPair)
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}

		if offset != 0 {
			if err := validateTopKLimit(offset); err != nil {
				return nil, 0, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
			}
		}
	}

	queryTopK := topK + offset
	if err := validateTopKLimit(queryTopK); err != nil {
		return nil, 0, fmt.Errorf("%s+%s [%d] is invalid, %w", OffsetKey, TopKKey, queryTopK, err)
	}

	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(common.MetricTypeKey + " not found in search_params")
	}

	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, searchParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}

	roundDecimal, err := strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, 0, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		return nil, 0, err
	}
	return &planpb.QueryInfo{
		Topk:         queryTopK,
		MetricType:   metricType,
		SearchParams: searchParamStr,
		RoundDecimal: roundDecimal,
	}, offset, nil
}

func getOutputFieldIDs(schema *schemapb.CollectionSchema, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		hitField := false
		for _, field := range schema.GetFields() {
			if field.Name == name {
				outputFieldIDs = append(outputFieldIDs, field.GetFieldID())
				hitField = true
				break
			}
		}
		if !hitField {
			return nil, fmt.Errorf("Field %s not exist", name)
		}
	}
	return outputFieldIDs, nil
}

func getNq(req *milvuspb.SearchRequest) (int64, error) {
	if req.GetNq() == 0 {
		// keep compatible with older client version.
		x := &commonpb.PlaceholderGroup{}
		err := proto.Unmarshal(req.GetPlaceholderGroup(), x)
		if err != nil {
			return 0, err
		}
		total := int64(0)
		for _, h := range x.GetPlaceholders() {
			total += int64(len(h.Values))
		}
		return total, nil
	}
	return req.GetNq(), nil
}

func (t *searchTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PreExecute")
	defer sp.End()

	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	t.SearchRequest.DbID = 0 // todo
	t.SearchRequest.CollectionID = collID
	t.schema, _ = globalMetaCache.GetCollectionSchema(ctx, collectionName)

	partitionKeyMode, err := isPartitionKeyMode(ctx, collectionName)
	if err != nil {
		return err
	}
	if partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
	}

	t.request.OutputFields, t.userOutputFields, err = translateOutputFields(t.request.OutputFields, t.schema, false)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Debug("translate output fields",
		zap.Strings("output fields", t.request.GetOutputFields()))

	// fetch search_growing from search param
	var ignoreGrowing bool
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

	// Manually update nq if not set.
	nq, err := getNq(t.request)
	if err != nil {
		return err
	}
	// Check if nq is valid:
	// https://milvus.io/docs/limitations.md
	if err := validateNQLimit(nq); err != nil {
		return fmt.Errorf("%s [%d] is invalid, %w", NQKey, nq, err)
	}
	t.SearchRequest.Nq = nq

	outputFieldIDs, err := getOutputFieldIDs(t.schema, t.request.GetOutputFields())
	if err != nil {
		return err
	}
	t.SearchRequest.OutputFieldsId = outputFieldIDs

	partitionNames := t.request.GetPartitionNames()
	if t.request.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, t.request.GetSearchParams())
		if err != nil || len(annsField) == 0 {
			if enableMultipleVectorFields {
				return errors.New(AnnsFieldKey + " not found in search_params")
			}
			vecFieldSchema, err2 := typeutil.GetVectorFieldSchema(t.schema)
			if err2 != nil {
				return errors.New(AnnsFieldKey + " not found in schema")
			}
			annsField = vecFieldSchema.Name
		}
		queryInfo, offset, err := parseSearchInfo(t.request.GetSearchParams())
		if err != nil {
			return err
		}
		t.offset = offset

		plan, err := planparserv2.CreateSearchPlan(t.schema, t.request.Dsl, annsField, queryInfo)
		if err != nil {
			log.Ctx(ctx).Warn("failed to create query plan", zap.Error(err),
				zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
				zap.String("anns field", annsField), zap.Any("query info", queryInfo))
			return fmt.Errorf("failed to create query plan: %v", err)
		}
		log.Ctx(ctx).Debug("create query plan",
			zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
			zap.String("anns field", annsField), zap.Any("query info", queryInfo))

		if partitionKeyMode {
			expr, err := ParseExprFromPlan(plan)
			if err != nil {
				return err
			}
			partitionKeys := ParsePartitionKeys(expr)
			hashedPartitionNames, err := assignPartitionKeys(ctx, collectionName, partitionKeys)
			if err != nil {
				return err
			}

			partitionNames = append(partitionNames, hashedPartitionNames...)
		}

		plan.OutputFieldIds = outputFieldIDs

		t.SearchRequest.Topk = queryInfo.GetTopk()
		t.SearchRequest.MetricType = queryInfo.GetMetricType()
		t.SearchRequest.DslType = commonpb.DslType_BoolExprV1

		estimateSize, err := t.estimateResultSize(nq, t.SearchRequest.Topk)
		if err != nil {
			return err
		}
		if estimateSize >= requeryThreshold {
			t.requery = true
			plan.OutputFieldIds = nil
		}

		t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}

		log.Ctx(ctx).Debug("Proxy::searchTask::PreExecute",
			zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
			zap.String("plan", plan.String())) // may be very large if large term passed.
	}

	// translate partition name to partition ids. Use regex-pattern to match partition name.
	t.SearchRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, partitionNames)
	if err != nil {
		return err
	}

	travelTimestamp := t.request.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = typeutil.MaxTimestamp
	}
	err = validateTravelTimestamp(travelTimestamp, t.BeginTs())
	if err != nil {
		return err
	}
	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, collectionName)
	if err2 != nil {
		log.Ctx(ctx).Debug("Proxy::searchTask::PreExecute failed to GetCollectionInfo from cache",
			zap.Any("collectionName", collectionName), zap.Error(err2))
		return err2
	}
	guaranteeTs := t.request.GetGuaranteeTimestamp()
	t.SearchRequest.TravelTimestamp = travelTimestamp
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

	if deadline, ok := t.TraceCtx().Deadline(); ok {
		t.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.SearchRequest.Dsl = t.request.Dsl
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup

	log.Ctx(ctx).Debug("search PreExecute done.",
		zap.Uint64("travel_ts", travelTimestamp), zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Bool("use_default_consistency", useDefaultConsistency),
		zap.Any("consistency level", consistencyLevel),
		zap.Uint64("timeout_ts", t.SearchRequest.GetTimeoutTimestamp()))

	return nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-Execute")
	defer sp.End()
	log := log.Ctx(ctx)

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.SearchResults]()

	err := t.lb.Execute(ctx, CollectionWorkLoad{
		collection: t.collectionName,
		nq:         t.Nq,
		exec:       t.searchShard,
	})
	if err != nil {
		return merr.WrapErrShardDelegatorSearchFailed(err.Error())
	}

	log.Debug("Search Execute done.",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
}

func (t *searchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PostExecute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	var (
		Nq         = t.SearchRequest.GetNq()
		Topk       = t.SearchRequest.GetTopk()
		MetricType = t.SearchRequest.GetMetricType()
	)
	toReduceResults, err := t.collectSearchResults(ctx)
	if err != nil {
		return err
	}

	// Decode all search results
	tr.CtxRecord(ctx, "decodeResultStart")
	validSearchResults, err := decodeSearchResults(ctx, toReduceResults)
	if err != nil {
		return err
	}
	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	if len(validSearchResults) <= 0 {
		log.Ctx(ctx).Warn("search result is empty")

		t.fillInEmptyResult(Nq)
		return nil
	}

	// Reduce all search results
	log.Ctx(ctx).Debug("proxy search post execute reduce",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int("number of valid search results", len(validSearchResults)))
	tr.CtxRecord(ctx, "reduceResultStart")
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(t.schema)
	if err != nil {
		return err
	}

	t.result, err = reduceSearchResultData(ctx, validSearchResults, Nq, Topk, MetricType, primaryFieldSchema.DataType, t.offset)
	if err != nil {
		return err
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	t.result.CollectionName = t.collectionName
	t.fillInFieldInfo()

	if t.requery {
		err = t.Requery()
		if err != nil {
			return err
		}
	}
	t.result.Results.OutputFields = t.userOutputFields

	log.Ctx(ctx).Debug("Search post execute done",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs ...string) error {
	searchReq := typeutil.Clone(t.SearchRequest)
	searchReq.GetBase().TargetID = nodeID
	req := &querypb.SearchRequest{
		Req:             searchReq,
		DmlChannels:     channelIDs,
		Scope:           querypb.DataScope_All,
		TotalChannelNum: int32(len(channelIDs)),
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.Strings("channels", channelIDs))

	var result *internalpb.SearchResults
	var err error

	result, err = qn.Search(ctx, req)
	if err != nil {
		log.Warn("QueryNode search return error", zap.Error(err))
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode search result error",
			zap.String("reason", result.GetStatus().GetReason()))
		return fmt.Errorf("fail to Search, QueryNode ID=%d, reason=%s", nodeID, result.GetStatus().GetReason())
	}
	t.resultBuf.Insert(result)

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
	pkField, err := typeutil.GetPrimaryFieldSchema(t.schema)
	if err != nil {
		return err
	}
	ids := t.result.GetResults().GetIds()
	expr := IDs2Expr(pkField.GetName(), ids)

	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Retrieve,
		},
		CollectionName:     t.request.GetCollectionName(),
		Expr:               expr,
		OutputFields:       t.request.GetOutputFields(),
		PartitionNames:     t.request.GetPartitionNames(),
		TravelTimestamp:    t.request.GetTravelTimestamp(),
		GuaranteeTimestamp: t.request.GetGuaranteeTimestamp(),
		QueryParams:        t.request.GetSearchParams(),
	}
	queryResult, err := t.node.Query(t.ctx, queryReq)
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
	pkFieldData, err := typeutil.GetPrimaryFieldData(queryResult.GetFieldsData(), pkField)
	if err != nil {
		return err
	}
	offsets := make(map[any]int)
	for i := 0; i < typeutil.GetDataSize(pkFieldData); i++ {
		pk := typeutil.GetData(pkFieldData, i)
		offsets[pk] = i
	}

	t.result.Results.FieldsData = make([]*schemapb.FieldData, len(queryResult.GetFieldsData()))
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := offsets[id]; !ok {
			return fmt.Errorf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(offsets), t.GetCollectionID())
		}
		typeutil.AppendFieldData(t.result.Results.FieldsData, queryResult.GetFieldsData(), int64(offsets[id]))
	}

	// filter id field out if it is not specified as output
	t.result.Results.FieldsData = lo.Filter(t.result.Results.FieldsData, func(fieldData *schemapb.FieldData, i int) bool {
		return lo.Contains(t.request.GetOutputFields(), fieldData.GetFieldName())
	})

	return nil
}

func (t *searchTask) fillInEmptyResult(numQueries int64) {
	t.result = &milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "search result is empty",
		},
		CollectionName: t.collectionName,
		Results: &schemapb.SearchResultData{
			NumQueries: numQueries,
			Topks:      make([]int64, numQueries),
		},
	}
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

func reduceSearchResultData(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, nq int64, topk int64, metricType string, pkType schemapb.DataType, offset int64) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultData")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultData",
		zap.Int("len(subSearchResultData)", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: make([]*schemapb.FieldData, len(subSearchResultData[0].FieldsData)),
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	switch pkType {
	case schemapb.DataType_Int64:
		ret.GetResults().Ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: make([]int64, 0),
			},
		}
	case schemapb.DataType_VarChar:
		ret.GetResults().Ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: make([]string, 0),
			},
		}
	default:
		return nil, errors.New("unsupported pk type")
	}

	for i, sData := range subSearchResultData {
		log.Ctx(ctx).Debug("subSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Any("length of FieldsData", len(sData.FieldsData)))
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
			return ret, err
		}
		// printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
	}

	var (
		subSearchNum = len(subSearchResultData)
		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset = make([][]int64, subSearchNum)
	)
	for i := 0; i < subSearchNum; i++ {
		subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
		for j := int64(1); j < nq; j++ {
			subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
		}
	}

	var (
		skipDupCnt int64
		realTopK   int64 = -1
	)

	// reducing nq * topk results
	for i := int64(0); i < nq; i++ {

		var (
			// cursor of current data of each subSearch for merging the j-th data of TopK.
			// sum(cursors) == j
			cursors = make([]int64, subSearchNum)

			j     int64
			idSet = make(map[interface{}]struct{})
		)

		// skip offset results
		for k := int64(0); k < offset; k++ {
			subSearchIdx, _ := selectHighestScoreIndex(subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}

			cursors[subSearchIdx]++
		}

		// keep limit results
		for j = 0; j < limit; {
			// From all the sub-query result sets of the i-th query vector,
			//   find the sub-query result set index of the score j-th data,
			//   and the index of the data in schemapb.SearchResultData
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}

			id := typeutil.GetPK(subSearchResultData[subSearchIdx].GetIds(), resultDataIdx)
			score := subSearchResultData[subSearchIdx].Scores[resultDataIdx]

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.Results.FieldsData, subSearchResultData[subSearchIdx].FieldsData, resultDataIdx)
				typeutil.AppendPKs(ret.Results.Ids, id)
				ret.Results.Scores = append(ret.Results.Scores, score)
				idSet[id] = struct{}{}
				j++
			} else {
				// skip entity with same id
				skipDupCnt++
			}
			cursors[subSearchIdx]++
		}
		if realTopK != -1 && realTopK != j {
			log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
			// return nil, errors.New("the length (topk) between all result of query is different")
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)
	}
	log.Ctx(ctx).Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))

	if skipDupCnt > 0 {
		log.Info("skip duplicated search result", zap.Int64("count", skipDupCnt))
	}

	ret.Results.TopK = realTopK // realTopK is the topK of the nq-th query
	if !distance.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	// printSearchResultData(ret.Results, "proxy reduce result")
	return ret, nil
}

// func printSearchResultData(data *schemapb.SearchResultData, header string) {
//     size := len(data.GetIds().GetIntId().GetData())
//     if size != len(data.Scores) {
//         log.Error("SearchResultData length mis-match")
//     }
//     log.Debug("==== SearchResultData ====",
//         zap.String("header", header), zap.Int64("nq", data.NumQueries), zap.Int64("topk", data.TopK))
//     for i := 0; i < size; i++ {
//         log.Debug("", zap.Int("i", i), zap.Int64("id", data.GetIds().GetIntId().Data[i]), zap.Float32("score", data.Scores[i]))
//     }
// }

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
