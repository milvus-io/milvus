package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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

	resultBuf       chan *internalpb.SearchResults
	toReduceResults []*internalpb.SearchResults
	runningGroup    *errgroup.Group
	runningGroupCtx context.Context

	getQueryNodePolicy getQueryNodePolicy
	searchShardPolicy  pickShardPolicy
}

func (t *searchTask) PreExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-PreExecute")

	if t.getQueryNodePolicy == nil {
		t.getQueryNodePolicy = defaultGetQueryNodePolicy
	}

	if t.searchShardPolicy == nil {
		t.searchShardPolicy = roundRobinPolicy
	}

	defer sp.Finish()
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collectionName := t.request.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	t.CollectionID = collID
	t.collectionName = collectionName
	t.PartitionIDs = []UniqueID{}

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return err
		}
	}

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return err
	}

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range t.request.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					t.PartitionIDs = append(t.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			return fmt.Errorf("partition name %s not found", partitionName)
		}
	}

	// check if collection/partitions are loaded into query node
	if !t.checkIfLoaded(collID, t.PartitionIDs) {
		return fmt.Errorf("collection:%v or partitions:%v not loaded into memory", collectionName, t.request.GetPartitionNames())
	}

	// TODO(dragondriver): necessary to check if partition was loaded into query node?
	t.Base.MsgType = commonpb.MsgType_Search

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, collectionName)

	outputFields, err := translateOutputFields(t.request.OutputFields, schema, false)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", outputFields))
	t.request.OutputFields = outputFields

	if t.request.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, t.request.SearchParams)
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, t.request.SearchParams)
		if err != nil {
			return errors.New(TopKKey + " not found in search_params")
		}
		topK, err := strconv.Atoi(topKStr)
		if err != nil {
			return errors.New(TopKKey + " " + topKStr + " is not invalid")
		}

		metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, t.request.SearchParams)
		if err != nil {
			return errors.New(MetricTypeKey + " not found in search_params")
		}

		searchParams, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, t.request.SearchParams)
		if err != nil {
			return errors.New(SearchParamsKey + " not found in search_params")
		}
		roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, t.request.SearchParams)
		if err != nil {
			roundDecimalStr = "-1"
		}
		roundDecimal, err := strconv.Atoi(roundDecimalStr)
		if err != nil {
			return errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
		}

		if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
			return errors.New(RoundDecimalKey + " " + roundDecimalStr + " is not invalid")
		}

		queryInfo := &planpb.QueryInfo{
			Topk:         int64(topK),
			MetricType:   metricType,
			SearchParams: searchParams,
			RoundDecimal: int64(roundDecimal),
		}

		log.Debug("create query plan",
			//zap.Any("schema", schema),
			zap.String("dsl", t.request.Dsl),
			zap.String("anns field", annsField),
			zap.Any("query info", queryInfo))

		plan, err := createQueryPlan(schema, t.request.Dsl, annsField, queryInfo)
		if err != nil {
			log.Debug("failed to create query plan",
				zap.Error(err),
				//zap.Any("schema", schema),
				zap.String("dsl", t.request.Dsl),
				zap.String("anns field", annsField),
				zap.Any("query info", queryInfo))

			return fmt.Errorf("failed to create query plan: %v", err)
		}
		for _, name := range t.request.OutputFields {
			hitField := false
			for _, field := range schema.Fields {
				if field.Name == name {
					if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
						return errors.New("search doesn't support vector field as output_fields")
					}

					t.SearchRequest.OutputFieldsId = append(t.SearchRequest.OutputFieldsId, field.FieldID)
					plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
					hitField = true
					break
				}
			}
			if !hitField {
				errMsg := "Field " + name + " not exist"
				return errors.New(errMsg)
			}
		}

		t.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		t.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		log.Debug("Proxy::searchTask::PreExecute", zap.Any("plan.OutputFieldIds", plan.OutputFieldIds),
			zap.Any("plan", plan.String()))
	}
	travelTimestamp := t.request.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = t.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(t.BeginTs(), travelTimestamp) / 1000
		if durationSeconds > Params.CommonCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
	}
	guaranteeTimestamp := t.request.GuaranteeTimestamp
	if guaranteeTimestamp == 0 {
		guaranteeTimestamp = t.BeginTs()
	}
	t.TravelTimestamp = travelTimestamp
	t.GuaranteeTimestamp = guaranteeTimestamp
	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // todo
	t.SearchRequest.Dsl = t.request.Dsl
	t.SearchRequest.PlaceholderGroup = t.request.PlaceholderGroup

	log.Info("search PreExecute done.",
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "search"))
	return nil
}

func (t *searchTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(t.TraceCtx(), "Proxy-Search-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", t.ID()))
	defer tr.Elapse("done")

	executeSearch := func(withCache bool) error {
		shards, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName, t.qc)
		if err != nil {
			return err
		}

		t.resultBuf = make(chan *internalpb.SearchResults, len(shards))
		t.toReduceResults = make([]*internalpb.SearchResults, 0, len(shards))
		t.runningGroup, t.runningGroupCtx = errgroup.WithContext(ctx)

		// TODO: try to merge rpc send to different shard leaders.
		// If two shard leader is on the same querynode maybe we should merge request to save rpc
		for _, shard := range shards {
			s := shard
			t.runningGroup.Go(func() error {
				log.Debug("proxy starting to query one shard",
					zap.Int64("collectionID", t.CollectionID),
					zap.String("collection name", t.collectionName),
					zap.String("shard channel", s.GetChannelName()),
					zap.Uint64("timeoutTs", t.TimeoutTimestamp))

				err := t.searchShard(t.runningGroupCtx, s)
				if err != nil {
					return err
				}
				return nil
			})
		}

		err = t.runningGroup.Wait()
		return err
	}

	err := executeSearch(WithCache)
	if err == errInvalidShardLeaders {
		log.Warn("invalid shard leaders from cache, updating shardleader caches and retry search")
		return executeSearch(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to search on all shard leaders, err=%s", err.Error())
	}

	log.Info("Search Execute done.",
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "search"))
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
				log.Debug("wait to finish timeout!", zap.Int64("taskID", t.ID()))
				return
			case <-t.runningGroupCtx.Done():
				log.Debug("all searches are finished or canceled", zap.Any("taskID", t.ID()))
				close(t.resultBuf)
				for res := range t.resultBuf {
					t.toReduceResults = append(t.toReduceResults, res)
					log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()), zap.Any("taskID", t.ID()))
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
	metrics.ProxyDecodeSearchResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
	log.Debug("proxy search post execute stage 2", zap.Any("len(validSearchResults)", len(validSearchResults)))
	if len(validSearchResults) <= 0 {
		log.Warn("search result is empty", zap.Any("requestID", t.Base.MsgID), zap.String("requestType", "search"))

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
	t.result, err = reduceSearchResultData(validSearchResults, t.toReduceResults[0].NumQueries, t.toReduceResults[0].TopK, t.toReduceResults[0].MetricType)
	if err != nil {
		return err
	}
	metrics.ProxyReduceSearchResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.SuccessLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
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
	log.Info("Search post execute done", zap.Any("requestID", t.Base.MsgID), zap.String("requestType", "search"))
	return nil
}

func (t *searchTask) searchShard(ctx context.Context, leaders *querypb.ShardLeadersList) error {

	search := func(nodeID UniqueID, qn types.QueryNode) error {
		req := &querypb.SearchRequest{
			Req:        t.SearchRequest,
			DmlChannel: leaders.GetChannelName(),
		}

		result, err := qn.Search(ctx, req)
		if err != nil {
			log.Warn("QueryNode search returns error", zap.Int64("nodeID", nodeID),
				zap.Error(err))
			return errInvalidShardLeaders
		}
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("QueryNode search result error", zap.Int64("nodeID", nodeID),
				zap.String("reason", result.GetStatus().GetReason()))
			return fmt.Errorf("fail to Search, QueryNode ID=%d, reason=%s", nodeID, result.GetStatus().GetReason())
		}

		t.resultBuf <- result
		return nil
	}

	err := t.searchShardPolicy(t.TraceCtx(), t.getQueryNodePolicy, search, leaders)
	if err != nil {
		log.Warn("fail to search to all shard leaders", zap.Any("shard leaders", leaders.GetNodeIds()))
		return err
	}

	return nil
}

func (t *searchTask) checkIfLoaded(collectionID UniqueID, searchPartitionIDs []UniqueID) bool {
	// If request to search partitions
	if len(searchPartitionIDs) > 0 {
		resp, err := t.qc.ShowPartitions(t.ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     t.Base.MsgID,
				Timestamp: t.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
			PartitionIDs: searchPartitionIDs,
		})
		if err != nil {
			log.Warn("fail to show partitions by QueryCoord",
				zap.Int64("requestID", t.Base.MsgID),
				zap.Int64("collectionID", collectionID),
				zap.Int64s("partitionIDs", searchPartitionIDs),
				zap.String("requestType", "search"),
				zap.Error(err))
			return false
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("fail to show partitions by QueryCoord",
				zap.Int64("collectionID", collectionID),
				zap.Int64s("partitionIDs", searchPartitionIDs),
				zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "search"),
				zap.String("reason", resp.GetStatus().GetReason()))
			return false
		}
		// Current logic: show partitions won't return error if the given partitions are all loaded
		return true
	}

	// If request to search collection
	resp, err := t.qc.ShowCollections(t.ctx, &querypb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     t.Base.MsgID,
			Timestamp: t.Base.Timestamp,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
	})
	if err != nil {
		log.Warn("fail to show collections by QueryCoord",
			zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "search"),
			zap.Error(err))
		return false
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("fail to show collections by QueryCoord",
			zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "search"),
			zap.String("reason", resp.GetStatus().GetReason()))
		return false
	}

	loaded := false
	for index, collID := range resp.CollectionIDs {
		if collID == collectionID && resp.GetInMemoryPercentages()[index] >= int64(100) {
			loaded = true
			break
		}
	}

	if !loaded {
		resp, err := t.qc.ShowPartitions(t.ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     t.Base.MsgID,
				Timestamp: t.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
		})
		if err != nil {
			log.Warn("fail to show partitions by QueryCoord",
				zap.Int64("requestID", t.Base.MsgID),
				zap.Int64("collectionID", collectionID),
				zap.String("requestType", "search"),
				zap.Error(err))
			return false
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("fail to show partitions by QueryCoord",
				zap.Int64("collectionID", collectionID),
				zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "search"),
				zap.String("reason", resp.GetStatus().GetReason()))
			return false
		}

		if len(resp.GetPartitionIDs()) > 0 {
			log.Warn("collection not fully loaded, search on these partitions", zap.Int64s("partitionIDs", resp.GetPartitionIDs()))
			return true
		}
	}

	return loaded
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

	expectedLength := (int)(nq * topk)
	if len(data.Ids.GetIntId().Data) != expectedLength {
		return fmt.Errorf("search result's ID length invalid, ID length=%d, expectd length=%d",
			len(data.Ids.GetIntId().Data), expectedLength)
	}
	if len(data.Scores) != expectedLength {
		return fmt.Errorf("search result's score length invalid, score length=%d, expectedLength=%d",
			len(data.Scores), expectedLength)
	}
	return nil
}

func selectSearchResultData(dataArray []*schemapb.SearchResultData, offsets []int64, topk int64, qi int64) int {
	sel := -1
	maxDistance := minFloat32        // distance here means score :)
	for i, offset := range offsets { // query num, the number of ways to merge
		if offset >= topk {
			continue
		}
		idx := qi*topk + offset
		id := dataArray[i].Ids.GetIntId().Data[idx]
		if id != -1 {
			distance := dataArray[i].Scores[idx]
			if distance > maxDistance {
				sel = i
				maxDistance = distance
			}
		}
	}
	return sel
}

func reduceSearchResultData(searchResultData []*schemapb.SearchResultData, nq int64, topk int64, metricType string) (*milvuspb.SearchResults, error) {

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
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: make([]int64, 0),
					},
				},
			},
			Topks: make([]int64, 0),
		},
	}

	for i, sData := range searchResultData {
		log.Debug("reduceSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Any("len(FieldsData)", len(sData.FieldsData)))
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			log.Warn("invalid search results", zap.Error(err))
			return ret, err
		}
		//printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
	}

	var skipDupCnt int64
	var realTopK int64 = -1
	for i := int64(0); i < nq; i++ {
		offsets := make([]int64, len(searchResultData))

		var idSet = make(map[int64]struct{})
		var j int64
		for j = 0; j < topk; {
			sel := selectSearchResultData(searchResultData, offsets, topk, i)
			if sel == -1 {
				break
			}
			idx := i*topk + offsets[sel]

			id := searchResultData[sel].Ids.GetIntId().Data[idx]
			score := searchResultData[sel].Scores[idx]
			// ignore invalid search result
			if id == -1 {
				continue
			}

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.Results.FieldsData, searchResultData[sel].FieldsData, idx)
				ret.Results.Ids.GetIntId().Data = append(ret.Results.Ids.GetIntId().Data, id)
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

// func (t *searchTaskV2) getChannels() ([]pChan, error) {
//     collID, err := globalMetaCache.GetCollectionID(t.ctx, t.request.CollectionName)
//     if err != nil {
//         return nil, err
//     }
//
//     var channels []pChan
//     channels, err = t.chMgr.getChannels(collID)
//     if err != nil {
//         err := t.chMgr.createDMLMsgStream(collID)
//         if err != nil {
//             return nil, err
//         }
//         return t.chMgr.getChannels(collID)
//     }
//
//     return channels, nil
// }

// func (t *searchTaskV2) getVChannels() ([]vChan, error) {
//     collID, err := globalMetaCache.GetCollectionID(t.ctx, t.request.CollectionName)
//     if err != nil {
//         return nil, err
//     }
//
//     var channels []vChan
//     channels, err = t.chMgr.getVChannels(collID)
//     if err != nil {
//         err := t.chMgr.createDMLMsgStream(collID)
//         if err != nil {
//             return nil, err
//         }
//         return t.chMgr.getVChannels(collID)
//     }
//
//     return channels, nil
// }
