package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
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
	ctx            context.Context
	resultBuf      chan []*internalpb.SearchResults
	result         *milvuspb.SearchResults
	query          *milvuspb.SearchRequest
	chMgr          channelsMgr
	qc             types.QueryCoord
	collectionName string

	tr           *timerecord.TimeRecorder
	collectionID UniqueID
}

func (st *searchTask) PreExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-PreExecute")
	defer sp.Finish()
	st.Base.MsgType = commonpb.MsgType_Search
	st.Base.SourceID = Params.ProxyCfg.ProxyID

	collectionName := st.query.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	st.collectionID = collID

	if err := validateCollectionName(st.query.CollectionName); err != nil {
		return err
	}

	for _, tag := range st.query.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return err
		}
	}

	// check if collection was already loaded into query node
	showResp, err := st.qc.ShowCollections(st.ctx, &querypb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     st.Base.MsgID,
			Timestamp: st.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID: 0, // TODO(dragondriver)
	})
	if err != nil {
		return err
	}
	if showResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(showResp.Status.Reason)
	}
	log.Debug("successfully get collections from QueryCoord",
		zap.String("target collection name", collectionName),
		zap.Int64("target collection ID", collID),
		zap.Any("collections", showResp.CollectionIDs),
	)
	collectionLoaded := false

	for _, collectionID := range showResp.CollectionIDs {
		if collectionID == collID {
			collectionLoaded = true
			break
		}
	}
	if !collectionLoaded {
		return fmt.Errorf("collection %v was not loaded into memory", collectionName)
	}

	// TODO(dragondriver): necessary to check if partition was loaded into query node?

	st.Base.MsgType = commonpb.MsgType_Search

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, collectionName)

	outputFields, err := translateOutputFields(st.query.OutputFields, schema, false)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", outputFields))
	st.query.OutputFields = outputFields

	if st.query.GetDslType() == commonpb.DslType_BoolExprV1 {
		annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, st.query.SearchParams)
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, st.query.SearchParams)
		if err != nil {
			return errors.New(TopKKey + " not found in search_params")
		}
		topK, err := strconv.Atoi(topKStr)
		if err != nil {
			return errors.New(TopKKey + " " + topKStr + " is not invalid")
		}

		metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(MetricTypeKey, st.query.SearchParams)
		if err != nil {
			return errors.New(MetricTypeKey + " not found in search_params")
		}

		searchParams, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, st.query.SearchParams)
		if err != nil {
			return errors.New(SearchParamsKey + " not found in search_params")
		}
		roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, st.query.SearchParams)
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
			zap.String("dsl", st.query.Dsl),
			zap.String("anns field", annsField),
			zap.Any("query info", queryInfo))

		plan, err := createQueryPlan(schema, st.query.Dsl, annsField, queryInfo)
		if err != nil {
			log.Debug("failed to create query plan",
				zap.Error(err),
				//zap.Any("schema", schema),
				zap.String("dsl", st.query.Dsl),
				zap.String("anns field", annsField),
				zap.Any("query info", queryInfo))

			return fmt.Errorf("failed to create query plan: %v", err)
		}
		for _, name := range st.query.OutputFields {
			hitField := false
			for _, field := range schema.Fields {
				if field.Name == name {
					if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
						return errors.New("search doesn't support vector field as output_fields")
					}

					st.SearchRequest.OutputFieldsId = append(st.SearchRequest.OutputFieldsId, field.FieldID)
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

		st.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		st.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
		log.Debug("Proxy::searchTask::PreExecute", zap.Any("plan.OutputFieldIds", plan.OutputFieldIds),
			zap.Any("plan", plan.String()))
	}
	travelTimestamp := st.query.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = st.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(st.BeginTs(), travelTimestamp) / 1000
		if durationSeconds > Params.CommonCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
	}
	guaranteeTimestamp := st.query.GuaranteeTimestamp
	if guaranteeTimestamp == 0 {
		guaranteeTimestamp = st.BeginTs()
	}
	st.SearchRequest.TravelTimestamp = travelTimestamp
	st.SearchRequest.GuaranteeTimestamp = guaranteeTimestamp
	deadline, ok := st.TraceCtx().Deadline()
	if ok {
		st.SearchRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	st.SearchRequest.ResultChannelID = Params.ProxyCfg.SearchResultChannelNames[0]
	st.SearchRequest.DbID = 0 // todo
	st.SearchRequest.CollectionID = collID
	st.SearchRequest.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return err
	}

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range st.query.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					st.PartitionIDs = append(st.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	st.SearchRequest.Dsl = st.query.Dsl
	st.SearchRequest.PlaceholderGroup = st.query.PlaceholderGroup

	return nil
}

func (st *searchTask) Execute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-Execute")
	defer sp.Finish()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute search %d", st.ID()))
	defer tr.Elapse("done")

	var tsMsg msgstream.TsMsg = &msgstream.SearchMsg{
		SearchRequest: *st.SearchRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyCfg.ProxyID)},
			BeginTimestamp: st.Base.Timestamp,
			EndTimestamp:   st.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: st.Base.Timestamp,
		EndTs:   st.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg

	collectionName := st.query.CollectionName
	info, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	st.collectionName = info.schema.Name

	stream, err := st.chMgr.getDQLStream(info.collID)
	if err != nil {
		err = st.chMgr.createDQLStream(info.collID)
		if err != nil {
			st.result = &milvuspb.SearchResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}
			return err
		}
		stream, err = st.chMgr.getDQLStream(info.collID)
		if err != nil {
			st.result = &milvuspb.SearchResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}
			return err
		}
	}
	tr.Record("get used message stream")
	err = stream.Produce(&msgPack)
	if err != nil {
		log.Debug("proxy", zap.String("send search request failed", err.Error()))
	}
	st.tr.Record("send message done")
	log.Debug("proxy sent one searchMsg",
		zap.Int64("collectionID", st.CollectionID),
		zap.Int64("msgID", tsMsg.ID()),
		zap.Int("length of search msg", len(msgPack.Msgs)),
		zap.Uint64("timeoutTs", st.SearchRequest.TimeoutTimestamp))
	sendMsgDur := tr.Record("send search msg to message stream")
	metrics.ProxySendMessageLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		metrics.SearchLabel).Observe(float64(sendMsgDur.Milliseconds()))

	return err
}

func (st *searchTask) PostExecute(ctx context.Context) error {
	sp, ctx := trace.StartSpanFromContextWithOperationName(st.TraceCtx(), "Proxy-Search-PostExecute")
	defer sp.Finish()
	tr := timerecord.NewTimeRecorder("searchTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()
	for {
		select {
		case <-st.TraceCtx().Done():
			log.Debug("Proxy searchTask PostExecute Loop exit caused by ctx.Done", zap.Int64("taskID", st.ID()))
			return fmt.Errorf("searchTask:wait to finish failed, timeout: %d", st.ID())
		case searchResults := <-st.resultBuf:
			// fmt.Println("searchResults: ", searchResults)
			filterSearchResults := make([]*internalpb.SearchResults, 0)
			var filterReason string
			errNum := 0
			for _, partialSearchResult := range searchResults {
				if partialSearchResult.Status.ErrorCode == commonpb.ErrorCode_Success {
					filterSearchResults = append(filterSearchResults, partialSearchResult)
					// For debugging, please don't delete.
					// printSearchResult(partialSearchResult)
				} else {
					errNum++
					filterReason += partialSearchResult.Status.Reason + "\n"
				}
			}

			log.Debug("Proxy Search PostExecute stage1",
				zap.Any("len(filterSearchResults)", len(filterSearchResults)))
			metrics.ProxyWaitForSearchResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), metrics.SearchLabel).Observe(float64(st.tr.RecordSpan().Milliseconds()))
			tr.Record("Proxy Search PostExecute stage1 done")
			if len(filterSearchResults) <= 0 || errNum > 0 {
				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    filterReason,
					},
					CollectionName: st.collectionName,
				}
				return fmt.Errorf("QueryNode search fail, reason %s: id %d", filterReason, st.ID())
			}
			tr.Record("decodeResultStart")
			validSearchResults, err := decodeSearchResults(filterSearchResults)
			if err != nil {
				return err
			}
			metrics.ProxyDecodeSearchResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
			log.Debug("Proxy Search PostExecute stage2", zap.Any("len(validSearchResults)", len(validSearchResults)))
			if len(validSearchResults) <= 0 {
				filterReason += "empty search result\n"
				log.Debug("Proxy Search PostExecute stage2 failed", zap.Any("filterReason", filterReason))

				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
						Reason:    filterReason,
					},
					Results: &schemapb.SearchResultData{
						NumQueries: searchResults[0].NumQueries,
						Topks:      make([]int64, searchResults[0].NumQueries),
					},
					CollectionName: st.collectionName,
				}
				return nil
			}

			tr.Record("reduceResultStart")
			st.result, err = reduceSearchResultData(validSearchResults, searchResults[0].NumQueries, searchResults[0].TopK, searchResults[0].MetricType)
			if err != nil {
				return err
			}
			metrics.ProxyReduceSearchResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), metrics.SuccessLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
			st.result.CollectionName = st.collectionName

			schema, err := globalMetaCache.GetCollectionSchema(ctx, st.query.CollectionName)
			if err != nil {
				return err
			}
			if len(st.query.OutputFields) != 0 && len(st.result.Results.FieldsData) != 0 {
				for k, fieldName := range st.query.OutputFields {
					for _, field := range schema.Fields {
						if st.result.Results.FieldsData[k] != nil && field.Name == fieldName {
							st.result.Results.FieldsData[k].FieldName = field.Name
							st.result.Results.FieldsData[k].FieldId = field.FieldID
							st.result.Results.FieldsData[k].Type = field.DataType
						}
					}
				}
			}
			return nil
		}
	}
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
	if len(data.Ids.GetIntId().Data) != (int)(nq*topk) {
		return fmt.Errorf("search result's id length %d invalid", len(data.Ids.GetIntId().Data))
	}
	if len(data.Scores) != (int)(nq*topk) {
		return fmt.Errorf("search result's score length %d invalid", len(data.Scores))
	}
	return nil
}

func selectSearchResultData(dataArray []*schemapb.SearchResultData, offsets []int64, topk int64, qi int64) int {
	sel := -1
	maxDistance := minFloat32
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
			zap.Int("i", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Any("len(FieldsData)", len(sData.FieldsData)))
		if err := checkSearchResultData(sData, nq, topk); err != nil {
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

func (st *searchTask) TraceCtx() context.Context {
	return st.ctx
}

func (st *searchTask) ID() UniqueID {
	return st.Base.MsgID
}

func (st *searchTask) SetID(uid UniqueID) {
	st.Base.MsgID = uid
}

func (st *searchTask) Name() string {
	return SearchTaskName
}

func (st *searchTask) Type() commonpb.MsgType {
	return st.Base.MsgType
}

func (st *searchTask) BeginTs() Timestamp {
	return st.Base.Timestamp
}

func (st *searchTask) EndTs() Timestamp {
	return st.Base.Timestamp
}

func (st *searchTask) SetTs(ts Timestamp) {
	st.Base.Timestamp = ts
}

func (st *searchTask) OnEnqueue() error {
	st.Base = &commonpb.MsgBase{}
	st.Base.MsgType = commonpb.MsgType_Search
	st.Base.SourceID = Params.ProxyCfg.ProxyID
	return nil
}

func (st *searchTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []pChan
	channels, err = st.chMgr.getChannels(collID)
	if err != nil {
		err := st.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return st.chMgr.getChannels(collID)
	}

	return channels, nil
}

func (st *searchTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []vChan
	channels, err = st.chMgr.getVChannels(collID)
	if err != nil {
		err := st.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return st.chMgr.getVChannels(collID)
	}

	return channels, nil
}
