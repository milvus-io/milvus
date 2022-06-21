package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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

	resultBuf       chan *internalpb.SearchResults
	toReduceResults []*internalpb.SearchResults
	runningGroup    *errgroup.Group
	runningGroupCtx context.Context

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

func parseQueryInfo(searchParamsPair []*commonpb.KeyValuePair) (*planpb.QueryInfo, error) {
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		return nil, errors.New(TopKKey + " not found in search_params")
	}
	topK, err := strconv.Atoi(topKStr)
	if err != nil {
		return nil, errors.New(TopKKey + " " + topKStr + " is not invalid")
	}

	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, searchParamsPair)
	if err != nil {
		return nil, errors.New(MetricTypeKey + " not found in search_params")
	}

	searchParams, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		return nil, errors.New(SearchParamsKey + " not found in search_params")
	}

	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, searchParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}
	roundDecimal, err := strconv.Atoi(roundDecimalStr)
	if err != nil {
		return nil, errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
	}

	return &planpb.QueryInfo{
		Topk:         int64(topK),
		MetricType:   metricType,
		SearchParams: searchParams,
		RoundDecimal: int64(roundDecimal),
	}, nil
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
		t.searchShardPolicy = roundRobinPolicy
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
	log.Debug("translate output fields", zap.Int64("msgID", t.ID()),
		zap.Strings("output fields", t.request.GetOutputFields()))

	if t.request.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, t.request.GetSearchParams())
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		queryInfo, err := parseQueryInfo(t.request.GetSearchParams())
		if err != nil {
			return err
		}

		plan, err := planparserv2.CreateSearchPlan(t.schema, t.request.Dsl, annsField, queryInfo)
		if err != nil {
			log.Debug("failed to create query plan", zap.Error(err), zap.Int64("msgID", t.ID()),
				zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
				zap.String("anns field", annsField), zap.Any("query info", queryInfo))
			return fmt.Errorf("failed to create query plan: %v", err)
		}
		log.Debug("create query plan", zap.Int64("msgID", t.ID()),
			zap.String("dsl", t.request.Dsl), // may be very large if large term passed.
			zap.String("anns field", annsField), zap.Any("query info", queryInfo))

		outputFieldIDs, err := getOutputFieldIDs(t.schema, t.request.GetOutputFields())
		if err != nil {
			return err
		}
		t.SearchRequest.OutputFieldsId = outputFieldIDs
		plan.OutputFieldIds = outputFieldIDs

		t.SearchRequest.MetricType = queryInfo.GetMetricType()
		t.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}

		t.SearchRequest.Topk = queryInfo.GetTopk()
		if err := validateTopK(queryInfo.GetTopk()); err != nil {
			return err
		}
		log.Debug("Proxy::searchTask::PreExecute", zap.Int64("msgID", t.ID()),
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

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.SearchRequest.Dsl = t.request.Dsl
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup
	if t.SearchRequest.Nq, err = getNq(t.request); err != nil {
		return err
	}
	log.Info("search PreExecute done.", zap.Int64("msgID", t.ID()),
		zap.Uint64("travel_ts", travelTimestamp), zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("timeout_ts", t.SearchRequest.GetTimeoutTimestamp()))

	return nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.Elapse("done")

	executeSearch := func(withCache bool) error {
		shard2Leaders, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName)
		if err != nil {
			return err
		}
		t.resultBuf = make(chan *internalpb.SearchResults, len(shard2Leaders))
		t.toReduceResults = make([]*internalpb.SearchResults, 0, len(shard2Leaders))
		t.runningGroup, t.runningGroupCtx = errgroup.WithContext(ctx)

		// TODO: try to merge rpc send to different shard leaders.
		// If two shard leader is on the same querynode maybe we should merge request to save rpc
		for channelID, leaders := range shard2Leaders {
			channelID := channelID
			leaders := leaders
			t.runningGroup.Go(func() error {
				log.Debug("proxy starting to query one shard", zap.Int64("msgID", t.ID()),
					zap.Int64("collectionID", t.CollectionID),
					zap.String("collection name", t.collectionName),
					zap.String("shard channel", channelID),
					zap.Uint64("timeoutTs", t.TimeoutTimestamp))

				return t.searchShard(t.runningGroupCtx, leaders, channelID)
			})
		}
		err = t.runningGroup.Wait()
		return err
	}

	err := executeSearch(WithCache)
	if errors.Is(err, errInvalidShardLeaders) || funcutil.IsGrpcErr(err) || errors.Is(err, grpcclient.ErrConnect) {
		log.Warn("first search failed, updating shardleader caches and retry search",
			zap.Int64("msgID", t.ID()), zap.Error(err))
		return executeSearch(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to search on all shard leaders, err=%w", err)
	}

	log.Debug("Search Execute done.", zap.Int64("msgID", t.ID()))
	return nil
}

func (t *searchTask) PostExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-PostExecute")
	defer sp.Finish()
	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case <-t.TraceCtx().Done():
				log.Debug("wait to finish timeout!", zap.Int64("msgID", t.ID()))
				return
			case <-t.runningGroupCtx.Done():
				log.Debug("all searches are finished or canceled", zap.Int64("msgID", t.ID()))
				close(t.resultBuf)
				for res := range t.resultBuf {
					t.toReduceResults = append(t.toReduceResults, res)
					log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()), zap.Int64("msgID", t.ID()))
				}
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()
	tr.Record("decodeResultStart")
	validSearchResults, err := decodeSearchResults(t.toReduceResults)
	if err != nil {
		return err
	}
	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10),
		metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	log.Debug("proxy search post execute stage 2", zap.Int64("msgID", t.ID()),
		zap.Int("len(validSearchResults)", len(validSearchResults)))
	if len(validSearchResults) <= 0 {
		log.Warn("search result is empty", zap.Int64("msgID", t.ID()))

		t.result = &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "search result is empty",
			},
			CollectionName: t.collectionName,
		}
		// add information if any
		if len(t.toReduceResults) > 0 {
			t.result.Results = &schemapb.SearchResultData{
				NumQueries: t.toReduceResults[0].NumQueries,
				Topks:      make([]int64, t.toReduceResults[0].NumQueries),
			}
		}
		return nil
	}

	tr.Record("reduceResultStart")
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(t.schema)
	if err != nil {
		return err
	}
	t.result, err = reduceSearchResultData(validSearchResults, t.toReduceResults[0].NumQueries, t.toReduceResults[0].TopK, t.toReduceResults[0].MetricType, primaryFieldSchema.DataType)
	if err != nil {
		return err
	}

	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
	t.result.CollectionName = t.collectionName

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.CollectionName)
	if err != nil {
		return err
	}
	if len(t.request.OutputFields) != 0 && len(t.result.Results.FieldsData) != 0 {
		for k, fieldName := range t.request.OutputFields {
			for _, field := range schema.Fields {
				if t.result.Results.FieldsData[k] != nil && field.Name == fieldName {
					t.result.Results.FieldsData[k].FieldName = field.Name
					t.result.Results.FieldsData[k].FieldId = field.FieldID
					t.result.Results.FieldsData[k].Type = field.DataType
				}
			}
		}
	}
	log.Info("Search post execute done", zap.Int64("msgID", t.ID()))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, leaders []nodeInfo, channelID string) error {

	search := func(nodeID UniqueID, qn types.QueryNode) error {
		req := &querypb.SearchRequest{
			Req:        t.SearchRequest,
			DmlChannel: channelID,
			Scope:      querypb.DataScope_All,
		}
		result, err := qn.Search(ctx, req)
		if err != nil {
			log.Warn("QueryNode search return error", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("channel", channelID), zap.Error(err))
			return err
		}
		if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
			log.Warn("QueryNode is not shardLeader", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("channel", channelID))
			return errInvalidShardLeaders
		}
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("QueryNode search result error", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("reason", result.GetStatus().GetReason()))
			return fmt.Errorf("fail to Search, QueryNode ID=%d, reason=%s", nodeID, result.GetStatus().GetReason())
		}
		t.resultBuf <- result
		return nil
	}

	err := t.searchShardPolicy(t.TraceCtx(), t.shardMgr, search, leaders)
	if err != nil {
		log.Warn("fail to search to all shard leaders", zap.Int64("msgID", t.ID()),
			zap.Any("shard leaders", leaders))
		return err
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

	// If request to search partitions
	if len(searchPartitionIDs) > 0 {
		resp, err := qc.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_ShowCollections,
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
		// Current logic: show partitions won't return error if the given partitions are all loaded
		return true, nil
	}

	// If request to search collection and collection is not fully loaded
	resp, err := qc.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_ShowCollections,
			SourceID: Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: info.collID,
	})
	if err != nil {
		return false, fmt.Errorf("showPartitions failed, collection = %s, partitionIDs = %v, err = %s", collectionName, searchPartitionIDs, err)
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return false, fmt.Errorf("showPartitions failed, collection = %s, partitionIDs = %v, reason = %s", collectionName, searchPartitionIDs, resp.GetStatus().GetReason())
	}

	if len(resp.GetPartitionIDs()) > 0 {
		log.Warn("collection not fully loaded, search on these partitions",
			zap.String("collection", collectionName),
			zap.Int64("collectionID", info.collID), zap.Int64s("partitionIDs", resp.GetPartitionIDs()))
		return true, nil
	}

	return false, nil
}

func decodeSearchResults(searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
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
	tr.Elapse("decodeSearchResults done")
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

func selectSearchResultData(dataArray []*schemapb.SearchResultData, resultOffsets [][]int64, offsets []int64, qi int64) int {
	sel := -1
	maxDistance := minFloat32        // distance here means score :)
	for i, offset := range offsets { // query num, the number of ways to merge
		if offset >= dataArray[i].Topks[qi] {
			continue
		}
		idx := resultOffsets[i][qi] + offset
		distance := dataArray[i].Scores[idx]
		if distance > maxDistance {
			sel = i
			maxDistance = distance
		}
	}
	return sel
}

func reduceSearchResultData(searchResultData []*schemapb.SearchResultData, nq int64, topk int64, metricType string, pkType schemapb.DataType) (*milvuspb.SearchResults, error) {

	tr := timerecord.NewTimeRecorder("reduceSearchResultData")
	defer func() {
		tr.Elapse("done")
	}()

	log.Debug("reduceSearchResultData", zap.Int("len(searchResultData)", len(searchResultData)),
		zap.Int64("nq", nq), zap.Int64("topk", topk), zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: 0,
		},
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
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

	for i, sData := range searchResultData {
		log.Debug("reduceSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Int64s("topks", sData.Topks),
			zap.Any("len(FieldsData)", len(sData.FieldsData)))
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			log.Warn("invalid search results", zap.Error(err))
			return ret, err
		}
		//printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < nq; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
	}

	var skipDupCnt int64
	var realTopK int64 = -1
	for i := int64(0); i < nq; i++ {
		offsets := make([]int64, len(searchResultData))

		var idSet = make(map[interface{}]struct{})
		var j int64
		for j = 0; j < topk; {
			sel := selectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			score := searchResultData[sel].Scores[idx]

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.Results.FieldsData, searchResultData[sel].FieldsData, idx)
				typeutil.AppendPKs(ret.Results.Ids, id)
				ret.Results.Scores = append(ret.Results.Scores, score)
				idSet[id] = struct{}{}
				j++
			} else {
				// skip entity with same id
				skipDupCnt++
			}
			offsets[sel]++
		}
		if realTopK != -1 && realTopK != j {
			log.Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
			// return nil, errors.New("the length (topk) between all result of query is different")
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)
	}
	log.Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))
	ret.Results.TopK = realTopK

	if !distance.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	// printSearchResultData(ret.Results, "proxy reduce result")
	return ret, nil
}

//func printSearchResultData(data *schemapb.SearchResultData, header string) {
//	size := len(data.Ids.GetIntId().Data)
//	if size != len(data.Scores) {
//		log.Error("SearchResultData length mis-match")
//	}
//	log.Debug("==== SearchResultData ====",
//		zap.String("header", header), zap.Int64("nq", data.NumQueries), zap.Int64("topk", data.TopK))
//	for i := 0; i < size; i++ {
//		log.Debug("", zap.Int("i", i), zap.Int64("id", data.Ids.GetIntId().Data[i]), zap.Float32("score", data.Scores[i]))
//	}
//}

// func printSearchResult(partialSearchResult *internalpb.SearchResults) {
//     for i := 0; i < len(partialSearchResult.Hits); i++ {
//         testHits := milvuspb.Hits{}
//         err := proto.Unmarshal(partialSearchResult.Hits[i], &testHits)
//         if err != nil {
//             panic(err)
//         }
//         fmt.Println(testHits.IDs)
//         fmt.Println(testHits.Scores)
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
