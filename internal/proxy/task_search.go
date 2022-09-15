package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type searchTask struct {
	Condition
	*internalpb.SearchRequest
	ctx context.Context

	result         *milvuspb.SearchResults
	request        *milvuspb.SearchRequest
	qc             types.QueryCoord
	tr             *timerecord.TimeRecorder
	collectionName string
	schema         *schemapb.CollectionSchema

	offset          int64
	resultBuf       chan *internalpb.SearchResults
	toReduceResults []*internalpb.SearchResults

	searchShardPolicy pickShardPolicy
	shardMgr          *shardClientMgr
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

// parseQueryInfo returns QueryInfo and offset
func parseQueryInfo(searchParamsPair []*commonpb.KeyValuePair) (*planpb.QueryInfo, int64, error) {
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(TopKKey + " not found in search_params")
	}
	topK, err := strconv.ParseInt(topKStr, 0, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("%s [%s] is invalid", TopKKey, topKStr)
	}
	if err := validateTopK(topK); err != nil {
		return nil, 0, fmt.Errorf("invalid limit, %w", err)
	}

	var offset int64
	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, searchParamsPair)
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	queryTopK := topK + offset
	if err := validateTopK(queryTopK); err != nil {
		return nil, 0, err
	}

	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(MetricTypeKey + " not found in search_params")
	}

	searchParams, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(SearchParamsKey + " not found in search_params")
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

	return &planpb.QueryInfo{
		Topk:         queryTopK,
		MetricType:   metricType,
		SearchParams: searchParams,
		RoundDecimal: roundDecimal,
	}, offset, nil
}

func getOutputFieldIDs(schema *schemapb.CollectionSchema, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		hitField := false
		for _, field := range schema.GetFields() {
			if field.Name == name {
				if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
					return nil, errors.New("search doesn't support vector field as output_fields")
				}
				outputFieldIDs = append(outputFieldIDs, field.GetFieldID())

				hitField = true
				break
			}
		}
		if !hitField {
			errMsg := "Field " + name + " not exist"
			return nil, errors.New(errMsg)
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
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-PreExecute")
	defer sp.Finish()

	if t.searchShardPolicy == nil {
		t.searchShardPolicy = mergeRoundRobinPolicy
	}

	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	t.SearchRequest.DbID = 0 // todo
	t.SearchRequest.CollectionID = collID
	t.schema, _ = globalMetaCache.GetCollectionSchema(ctx, collectionName)

	// translate partition name to partition ids. Use regex-pattern to match partition name.
	t.SearchRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, t.request.GetPartitionNames())
	if err != nil {
		return err
	}

	// check if collection/partitions are loaded into query node
	loaded, err := checkIfLoaded(ctx, t.qc, collectionName, t.SearchRequest.GetPartitionIDs())
	if err != nil {
		return fmt.Errorf("checkIfLoaded failed when search, collection:%v, partitions:%v, err = %s", collectionName, t.request.GetPartitionNames(), err)
	}
	if !loaded {
		return fmt.Errorf("collection:%v or partition:%v not loaded into memory when search", collectionName, t.request.GetPartitionNames())
	}

	t.request.OutputFields, err = translateOutputFields(t.request.OutputFields, t.schema, false)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Debug("translate output fields", zap.Int64("msgID", t.ID()),
		zap.Strings("output fields", t.request.GetOutputFields()))

	if t.request.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, t.request.GetSearchParams())
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		queryInfo, offset, err := parseQueryInfo(t.request.GetSearchParams())
		if err != nil {
			return err
		}
		t.offset = offset

		plan, err := planparserv2.CreateSearchPlan(t.schema, t.request.Dsl, annsField, queryInfo)
		if err != nil {
			log.Ctx(ctx).Warn("failed to create query plan", zap.Error(err), zap.Int64("msgID", t.ID()),
				zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
				zap.String("anns field", annsField), zap.Any("query info", queryInfo))
			return fmt.Errorf("failed to create query plan: %v", err)
		}
		log.Ctx(ctx).Debug("create query plan", zap.Int64("msgID", t.ID()),
			zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
			zap.String("anns field", annsField), zap.Any("query info", queryInfo))

		outputFieldIDs, err := getOutputFieldIDs(t.schema, t.request.GetOutputFields())
		if err != nil {
			return err
		}

		t.SearchRequest.OutputFieldsId = outputFieldIDs
		plan.OutputFieldIds = outputFieldIDs

		t.SearchRequest.Topk = queryInfo.GetTopk()
		t.SearchRequest.MetricType = queryInfo.GetMetricType()
		t.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}

		log.Ctx(ctx).Debug("Proxy::searchTask::PreExecute", zap.Int64("msgID", t.ID()),
			zap.Int64s("plan.OutputFieldIds", plan.GetOutputFieldIds()),
			zap.String("plan", plan.String())) // may be very large if large term passed.
	}

	travelTimestamp := t.request.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = typeutil.MaxTimestamp
	}
	err = validateTravelTimestamp(travelTimestamp, t.BeginTs())
	if err != nil {
		return err
	}
	t.SearchRequest.TravelTimestamp = travelTimestamp

	guaranteeTs := t.request.GetGuaranteeTimestamp()
	guaranteeTs = parseGuaranteeTs(guaranteeTs, t.BeginTs())
	t.SearchRequest.GuaranteeTimestamp = guaranteeTs

	if deadline, ok := t.TraceCtx().Deadline(); ok {
		t.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.SearchRequest.Dsl = t.request.Dsl
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup
	nq, err := getNq(t.request)
	if err != nil {
		return err
	}
	t.SearchRequest.Nq = nq

	log.Ctx(ctx).Debug("search PreExecute done.", zap.Int64("msgID", t.ID()),
		zap.Uint64("travel_ts", travelTimestamp), zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("timeout_ts", t.SearchRequest.GetTimeoutTimestamp()))

	return nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	executeSearch := func(withCache bool) error {
		shard2Leaders, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName)
		if err != nil {
			return err
		}
		t.resultBuf = make(chan *internalpb.SearchResults, len(shard2Leaders))
		t.toReduceResults = make([]*internalpb.SearchResults, 0, len(shard2Leaders))
		if err := t.searchShardPolicy(ctx, t.shardMgr, t.searchShard, shard2Leaders); err != nil {
			log.Ctx(ctx).Warn("failed to do search", zap.Error(err), zap.String("Shards", fmt.Sprintf("%v", shard2Leaders)))
			return err
		}
		return nil
	}

	err := executeSearch(WithCache)
	if errors.Is(err, errInvalidShardLeaders) || funcutil.IsGrpcErr(err) || errors.Is(err, grpcclient.ErrConnect) {
		log.Ctx(ctx).Warn("first search failed, updating shardleader caches and retry search",
			zap.Int64("msgID", t.ID()), zap.Error(err))
		return executeSearch(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to search on all shard leaders, err=%v", err)
	}

	log.Ctx(ctx).Debug("Search Execute done.", zap.Int64("msgID", t.ID()))
	return nil
}

func (t *searchTask) PostExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-PostExecute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	var (
		Nq         = t.SearchRequest.GetNq()
		Topk       = t.SearchRequest.GetTopk()
		MetricType = t.SearchRequest.GetMetricType()
	)

	if err := t.collectSearchResults(ctx); err != nil {
		return err
	}

	// Decode all search results
	tr.CtxRecord(ctx, "decodeResultStart")
	validSearchResults, err := decodeSearchResults(ctx, t.toReduceResults)
	if err != nil {
		return err
	}
	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10),
		metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	if len(validSearchResults) <= 0 {
		log.Ctx(ctx).Warn("search result is empty", zap.Int64("msgID", t.ID()))

		t.fillInEmptyResult(Nq)
		return nil
	}

	// Reduce all search results
	log.Ctx(ctx).Debug("proxy search post execute reduce", zap.Int64("msgID", t.ID()), zap.Int("number of valid search results", len(validSearchResults)))
	tr.CtxRecord(ctx, "reduceResultStart")
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(t.schema)
	if err != nil {
		return err
	}

	t.result, err = reduceSearchResultData(ctx, validSearchResults, Nq, Topk, MetricType, primaryFieldSchema.DataType, t.offset)
	if err != nil {
		return err
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	t.result.CollectionName = t.collectionName
	t.fillInFieldInfo()

	log.Ctx(ctx).Debug("Search post execute done", zap.Int64("msgID", t.ID()))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs []string) error {
	req := &querypb.SearchRequest{
		Req:         t.SearchRequest,
		DmlChannels: channelIDs,
		Scope:       querypb.DataScope_All,
	}
	result, err := qn.Search(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode search return error", zap.Int64("msgID", t.ID()),
			zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs), zap.Error(err))
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader", zap.Int64("msgID", t.ID()),
			zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs))
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode search result error", zap.Int64("msgID", t.ID()), zap.Int64("nodeID", nodeID),
			zap.String("reason", result.GetStatus().GetReason()))
		return fmt.Errorf("fail to Search, QueryNode ID=%d, reason=%s", nodeID, result.GetStatus().GetReason())
	}
	t.resultBuf <- result

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
				}
			}
		}
	}
}

func (t *searchTask) collectSearchResults(ctx context.Context) error {
	select {
	case <-t.TraceCtx().Done():
		log.Ctx(ctx).Debug("wait to finish timeout!", zap.Int64("msgID", t.ID()))
		return fmt.Errorf("search task wait to finish timeout, msgID=%d", t.ID())
	default:
		log.Ctx(ctx).Debug("all searches are finished or canceled", zap.Int64("msgID", t.ID()))
		close(t.resultBuf)
		for res := range t.resultBuf {
			t.toReduceResults = append(t.toReduceResults, res)
			log.Ctx(ctx).Debug("proxy receives one search result", zap.Int64("sourceID", res.GetBase().GetSourceID()), zap.Int64("msgID", t.ID()))
		}
	}
	return nil
}

// checkIfLoaded check if collection was loaded into QueryNode
func checkIfLoaded(ctx context.Context, qc types.QueryCoord, collectionName string, searchPartitionIDs []UniqueID) (bool, error) {
	info, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
	if err != nil {
		return false, fmt.Errorf("GetCollectionInfo failed, collection = %s, err = %s", collectionName, err)
	}
	if info.isLoaded {
		return true, nil
	}
	if len(searchPartitionIDs) == 0 {
		return false, nil
	}

	// If request to search partitions
	resp, err := qc.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_ShowPartitions,
			SourceID: Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: info.collID,
		PartitionIDs: searchPartitionIDs,
	})
	if err != nil {
		return false, fmt.Errorf("showPartitions failed, collection = %s, partitionIDs = %v, err = %s", collectionName, searchPartitionIDs, err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return false, fmt.Errorf("showPartitions failed, collection = %s, partitionIDs = %v, reason = %s", collectionName, searchPartitionIDs, resp.GetStatus().GetReason())
	}

	for _, persent := range resp.InMemoryPercentages {
		if persent < 100 {
			return false, nil
		}
	}
	return true, nil
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
		if sScore > maxScore {
			subSearchIdx = i
			resultDataIdx = sIdx

			maxScore = sScore
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
		//printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
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
	t.Base = &commonpb.MsgBase{}
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}
