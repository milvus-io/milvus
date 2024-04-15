package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

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
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
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
	Condition
	*internalpb.SearchRequest
	ctx context.Context

	result  *milvuspb.SearchResults
	request *milvuspb.SearchRequest

	tr                     *timerecord.TimeRecorder
	collectionName         string
	schema                 *schemaInfo
	requery                bool
	partitionKeyMode       bool
	enableMaterializedView bool

	userOutputFields []string

	offset    int64
	dimension int64
	resultBuf *typeutil.ConcurrentSet[*internalpb.SearchResults]

	qc              types.QueryCoordClient
	node            types.ProxyComponent
	lb              LBPolicy
	queryChannelsTs map[string]Timestamp
	queryInfo       *planpb.QueryInfo
	relatedDataSize int64
}

func getPartitionIDs(ctx context.Context, dbName string, collectionName string, partitionNames []string) (partitionIDs []UniqueID, err error) {
	for _, tag := range partitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return nil, err
		}
	}

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}

	useRegexp := Params.ProxyCfg.PartitionNameRegexp.GetAsBool()

	partitionsSet := typeutil.NewSet[int64]()
	for _, partitionName := range partitionNames {
		if useRegexp {
			// Legacy feature, use partition name as regexp
			pattern := fmt.Sprintf("^%s$", partitionName)
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid partition: %s", partitionName)
			}
			var found bool
			for name, pID := range partitionsMap {
				if re.MatchString(name) {
					partitionsSet.Insert(pID)
					found = true
				}
			}
			if !found {
				return nil, fmt.Errorf("partition name %s not found", partitionName)
			}
		} else {
			partitionID, found := partitionsMap[partitionName]
			if !found {
				// TODO change after testcase updated: return nil, merr.WrapErrPartitionNotFound(partitionName)
				return nil, fmt.Errorf("partition name %s not found", partitionName)
			}
			if !partitionsSet.Contain(partitionID) {
				partitionsSet.Insert(partitionID)
			}
		}
	}
	return partitionsSet.Collect(), nil
}

// parseSearchInfo returns QueryInfo and offset
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema) (*planpb.QueryInfo, int64, error) {
	// 1. parse offset and real topk
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

	// 2. parse metrics type
	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, searchParamsPair)
	if err != nil {
		metricType = ""
	}

	// 3. parse round decimal
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

	// 4. parse search param str
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		searchParamStr = ""
	}

	err = checkRangeSearchParams(searchParamStr, metricType)
	if err != nil {
		return nil, 0, err
	}

	// 5. parse group by field
	groupByFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, searchParamsPair)
	if err != nil {
		groupByFieldName = ""
	}
	var groupByFieldId int64 = -1
	if groupByFieldName != "" {
		fields := schema.GetFields()
		for _, field := range fields {
			if field.Name == groupByFieldName {
				groupByFieldId = field.FieldID
				break
			}
		}
		if groupByFieldId == -1 {
			return nil, 0, merr.WrapErrFieldNotFound(groupByFieldName, "groupBy field not found in schema")
		}
	}

	// 6. parse iterator tag, prevent trying to groupBy when doing iteration or doing range-search
	isIterator, _ := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, searchParamsPair)
	if isIterator == "True" && groupByFieldId > 0 {
		return nil, 0, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do groupBy when doing iteration")
	}
	if strings.Contains(searchParamStr, radiusKey) && groupByFieldId > 0 {
		return nil, 0, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do range-search when doing search-group-by")
	}

	return &planpb.QueryInfo{
		Topk:           queryTopK,
		MetricType:     metricType,
		SearchParams:   searchParamStr,
		RoundDecimal:   roundDecimal,
		GroupByFieldId: groupByFieldId,
	}, offset, nil
}

func getOutputFieldIDs(schema *schemaInfo, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		id, ok := schema.MapFieldID(name)
		if !ok {
			return nil, fmt.Errorf("Field %s not exist", name)
		}
		outputFieldIDs = append(outputFieldIDs, id)
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

func (t *searchTask) CanSkipAllocTimestamp() bool {
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	if !useDefaultConsistency {
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

	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
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

	if !t.partitionKeyMode && len(t.request.GetPartitionNames()) > 0 {
		// translate partition name to partition ids. Use regex-pattern to match partition name.
		t.PartitionIDs, err = getPartitionIDs(ctx, t.request.GetDbName(), collectionName, t.request.GetPartitionNames())
		if err != nil {
			log.Warn("failed to get partition ids", zap.Error(err))
			return err
		}
	}

	t.request.OutputFields, t.userOutputFields, err = translateOutputFields(t.request.OutputFields, t.schema, false)
	if err != nil {
		log.Warn("translate output fields failed", zap.Error(err))
		return err
	}
	log.Debug("translate output fields",
		zap.Strings("output fields", t.request.GetOutputFields()))

	err = initSearchRequest(ctx, t, false)
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

	log.Debug("search PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Bool("use_default_consistency", useDefaultConsistency),
		zap.Any("consistency level", consistencyLevel),
		zap.Uint64("timeout_ts", t.SearchRequest.GetTimeoutTimestamp()))
	return nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.Int64("nq", t.SearchRequest.GetNq()))

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.SearchResults]()

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

func (t *searchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search-PostExecute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()
	log := log.Ctx(ctx).With(zap.Int64("nq", t.SearchRequest.GetNq()))

	var (
		Nq         = t.SearchRequest.GetNq()
		Topk       = t.SearchRequest.GetTopk()
		MetricType = t.SearchRequest.GetMetricType()
	)
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

	if len(toReduceResults) >= 1 {
		MetricType = toReduceResults[0].GetMetricType()
	}

	// Decode all search results
	tr.CtxRecord(ctx, "decodeResultStart")
	validSearchResults, err := decodeSearchResults(ctx, toReduceResults)
	if err != nil {
		log.Warn("failed to decode search results", zap.Error(err))
		return err
	}
	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	if len(validSearchResults) <= 0 {
		t.fillInEmptyResult(Nq)
		return nil
	}

	// Reduce all search results
	log.Debug("proxy search post execute reduce",
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int("number of valid search results", len(validSearchResults)))
	tr.CtxRecord(ctx, "reduceResultStart")
	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}

	t.result, err = reduceSearchResult(ctx, NewReduceSearchResultInfo(validSearchResults, Nq, Topk,
		MetricType, primaryFieldSchema.DataType, t.offset, t.queryInfo))
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return err
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

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
	t.resultBuf.Insert(result)
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
		DbName:             t.request.GetDbName(),
		CollectionName:     t.request.GetCollectionName(),
		Expr:               "",
		OutputFields:       t.request.GetOutputFields(),
		PartitionNames:     t.request.GetPartitionNames(),
		GuaranteeTimestamp: t.request.GetGuaranteeTimestamp(),
		QueryParams:        t.request.GetSearchParams(),
	}

	return doRequery(t.ctx, t.GetCollectionID(), t.node, t.schema.CollectionSchema, queryReq, t.result, t.queryChannelsTs, t.GetPartitionIDs())
}

func (t *searchTask) fillInEmptyResult(numQueries int64) {
	t.result = &milvuspb.SearchResults{
		Status:         merr.Success("search result is empty"),
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

func doRequery(ctx context.Context,
	collectionID int64,
	node types.ProxyComponent,
	schema *schemapb.CollectionSchema,
	request *milvuspb.QueryRequest,
	result *milvuspb.SearchResults,
	queryChannelsTs map[string]Timestamp,
	partitionIDs []int64,
) error {
	outputFields := request.GetOutputFields()
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	ids := result.GetResults().GetIds()
	plan := planparserv2.CreateRequeryPlan(pkField, ids)
	channelsMvcc := make(map[string]Timestamp)
	for k, v := range queryChannelsTs {
		channelsMvcc[k] = v
	}
	qt := &queryTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:        paramtable.GetNodeID(),
			PartitionIDs: partitionIDs, // use search partitionIDs
		},
		request:      request,
		plan:         plan,
		qc:           node.(*Proxy).queryCoord,
		lb:           node.(*Proxy).lbPolicy,
		channelsMvcc: channelsMvcc,
		fastSkip:     true,
		reQuery:      true,
	}
	queryResult, err := node.(*Proxy).query(ctx, qt)
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
	for i := 0; i < typeutil.GetPKSize(pkFieldData); i++ {
		pk := typeutil.GetData(pkFieldData, i)
		offsets[pk] = i
	}

	result.Results.FieldsData = make([]*schemapb.FieldData, len(queryResult.GetFieldsData()))
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := offsets[id]; !ok {
			return merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(offsets), collectionID))
		}
		typeutil.AppendFieldData(result.Results.FieldsData, queryResult.GetFieldsData(), int64(offsets[id]))
	}

	// filter id field out if it is not specified as output
	result.Results.FieldsData = lo.Filter(result.Results.FieldsData, func(fieldData *schemapb.FieldData, i int) bool {
		return lo.Contains(outputFields, fieldData.GetFieldName())
	})

	return nil
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

type rangeSearchParams struct {
	radius      float64
	rangeFilter float64
}

func checkRangeSearchParams(str string, metricType string) error {
	if len(str) == 0 {
		// no search params, no need to check
		return nil
	}
	var data map[string]*json.RawMessage
	err := json.Unmarshal([]byte(str), &data)
	if err != nil {
		log.Info("json Unmarshal fail when checkRangeSearchParams")
		return err
	}
	radius, ok := data[radiusKey]
	// will not do range search, no need to check
	if !ok {
		return nil
	}
	if radius == nil {
		return merr.WrapErrParameterInvalidMsg("pass invalid type for radius")
	}
	var params rangeSearchParams
	err = json.Unmarshal(*radius, &params.radius)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("must pass numpy type for radius")
	}

	rangeFilter, ok := data[rangeFilterKey]
	// not pass range_filter, no need to check
	if !ok {
		return nil
	}

	if rangeFilter == nil {
		return merr.WrapErrParameterInvalidMsg("pass invalid type for range_filter")
	}
	err = json.Unmarshal(*rangeFilter, &params.rangeFilter)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("must pass numpy type for range_filter")
	}

	if metric.PositivelyRelated(metricType) {
		if params.radius >= params.rangeFilter {
			msg := fmt.Sprintf("range_filter must be greater than radius for IP/COSINE, range_filter:%f, radius:%f", params.rangeFilter, params.radius)
			return merr.WrapErrParameterInvalidMsg(msg)
		}
	} else {
		if params.radius <= params.rangeFilter {
			msg := fmt.Sprintf("range_filter must be less than radius for L2/HAMMING/JACCARD, range_filter:%f, radius:%f", params.rangeFilter, params.radius)
			return merr.WrapErrParameterInvalidMsg(msg)
		}
	}
	return nil
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
