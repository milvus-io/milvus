package proxy

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	HybridSearchTaskName = "HybridSearchTask"
)

type hybridSearchTask struct {
	baseTask
	Condition
	ctx context.Context
	*internalpb.HybridSearchRequest

	result      *milvuspb.SearchResults
	request     *milvuspb.HybridSearchRequest
	searchTasks []*searchTask

	tr               *timerecord.TimeRecorder
	schema           *schemaInfo
	requery          bool
	partitionKeyMode bool

	userOutputFields []string

	qc   types.QueryCoordClient
	node types.ProxyComponent
	lb   LBPolicy

	resultBuf             *typeutil.ConcurrentSet[*querypb.HybridSearchResult]
	multipleRecallResults *typeutil.ConcurrentSet[*milvuspb.SearchResults]
	partitionIDsSet       *typeutil.ConcurrentSet[UniqueID]

	reScorers       []reScorer
	queryChannelsTs map[string]Timestamp
	rankParams      *rankParams
}

func (t *hybridSearchTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-PreExecute")
	defer sp.End()

	if len(t.request.Requests) <= 0 {
		return errors.New("minimum of ann search requests is 1")
	}

	if len(t.request.Requests) > defaultMaxSearchRequest {
		return errors.New(fmt.Sprintf("maximum of ann search requests is %d", defaultMaxSearchRequest))
	}
	for _, req := range t.request.GetRequests() {
		nq, err := getNq(req)
		if err != nil {
			log.Debug("failed to get nq", zap.Error(err))
			return err
		}
		if nq != 1 {
			err = merr.WrapErrParameterInvalid("1", fmt.Sprint(nq), "nq should be equal to 1")
			log.Debug(err.Error())
			return err
		}
	}

	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		return err
	}
	t.CollectionID = collID

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
	if t.partitionKeyMode {
		if len(t.request.GetPartitionNames()) != 0 {
			return errors.New("not support manually specifying the partition names if partition key mode is used")
		}
		t.partitionIDsSet = typeutil.NewConcurrentSet[UniqueID]()
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

	if len(t.request.OutputFields) > 0 {
		t.requery = true
	}

	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err2 != nil {
		log.Warn("Proxy::hybridSearchTask::PreExecute failed to GetCollectionInfo from cache",
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

	t.reScorers, err = NewReScorer(t.request.GetRequests(), t.request.GetRankParams())
	if err != nil {
		log.Info("generate reScorer failed", zap.Any("rank params", t.request.GetRankParams()), zap.Error(err))
		return err
	}
	t.HybridSearchRequest.GuaranteeTimestamp = guaranteeTs
	t.searchTasks = make([]*searchTask, len(t.request.GetRequests()))
	for index := range t.request.Requests {
		searchReq := t.request.Requests[index]

		if len(searchReq.GetCollectionName()) == 0 {
			searchReq.CollectionName = t.request.GetCollectionName()
		} else if searchReq.GetCollectionName() != t.request.GetCollectionName() {
			return errors.New(fmt.Sprintf("inconsistent collection name in hybrid search request, "+
				"expect %s, actual %s", searchReq.GetCollectionName(), t.request.GetCollectionName()))
		}

		searchReq.PartitionNames = t.request.GetPartitionNames()
		searchReq.ConsistencyLevel = consistencyLevel
		searchReq.GuaranteeTimestamp = guaranteeTs
		searchReq.UseDefaultConsistency = useDefaultConsistency
		searchReq.OutputFields = nil

		t.searchTasks[index] = &searchTask{
			ctx:            ctx,
			Condition:      NewTaskCondition(ctx),
			collectionName: collectionName,
			SearchRequest: &internalpb.SearchRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Search),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				ReqID:        paramtable.GetNodeID(),
				DbID:         0, // todo
				CollectionID: collID,
				PartitionIDs: t.GetPartitionIDs(),
			},
			request: searchReq,
			schema:  t.schema,
			tr:      timerecord.NewTimeRecorder("hybrid search"),
			qc:      t.qc,
			node:    t.node,
			lb:      t.lb,

			partitionKeyMode: t.partitionKeyMode,
			resultBuf:        typeutil.NewConcurrentSet[*internalpb.SearchResults](),
		}
		err := initSearchRequest(ctx, t.searchTasks[index], true)
		if err != nil {
			log.Debug("init hybrid search request failed", zap.Error(err))
			return err
		}
		if t.partitionKeyMode {
			t.partitionIDsSet.Upsert(t.searchTasks[index].GetPartitionIDs()...)
		}
	}

	log.Debug("hybrid search preExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Bool("use_default_consistency", t.request.GetUseDefaultConsistency()),
		zap.Any("consistency level", t.request.GetConsistencyLevel()))

	return nil
}

func (t *hybridSearchTask) hybridSearchShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	hybridSearchReq := typeutil.Clone(t.HybridSearchRequest)
	hybridSearchReq.GetBase().TargetID = nodeID
	if t.partitionKeyMode {
		t.PartitionIDs = t.partitionIDsSet.Collect()
	}
	req := &querypb.HybridSearchRequest{
		Req:             hybridSearchReq,
		DmlChannels:     []string{channel},
		TotalChannelNum: int32(1),
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channel))

	var result *querypb.HybridSearchResult
	var err error

	result, err = qn.HybridSearch(ctx, req)
	if err != nil {
		log.Warn("QueryNode hybrid search return error", zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.request.GetCollectionName())
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.request.GetCollectionName())
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode hybrid search result error",
			zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to hybrid search on QueryNode %d", nodeID)
	}
	t.resultBuf.Insert(result)
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)

	return nil
}

func (t *hybridSearchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-Execute")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.CollectionID), zap.String("collName", t.request.GetCollectionName()))
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute hybrid search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	for _, searchTask := range t.searchTasks {
		t.HybridSearchRequest.Reqs = append(t.HybridSearchRequest.Reqs, searchTask.SearchRequest)
	}

	t.resultBuf = typeutil.NewConcurrentSet[*querypb.HybridSearchResult]()
	err := t.lb.Execute(ctx, CollectionWorkLoad{
		db:             t.request.GetDbName(),
		collectionID:   t.CollectionID,
		collectionName: t.request.GetCollectionName(),
		nq:             1,
		exec:           t.hybridSearchShard,
	})
	if err != nil {
		log.Warn("hybrid search execute failed", zap.Error(err))
		return errors.Wrap(err, "failed to hybrid search")
	}

	log.Debug("hybrid search execute done.")
	return nil
}

type rankParams struct {
	limit        int64
	offset       int64
	roundDecimal int64
}

// parseRankParams get limit and offset from rankParams, both are optional.
func parseRankParams(rankParamsPair []*commonpb.KeyValuePair) (*rankParams, error) {
	var (
		limit        int64
		offset       int64
		roundDecimal int64
		err          error
	)

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, rankParamsPair)
	if err != nil {
		return nil, errors.New(LimitKey + " not found in rank_params")
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, rankParamsPair)
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	// validate max result window.
	if err = validateMaxQueryResultWindow(offset, limit); err != nil {
		return nil, fmt.Errorf("invalid max query result window, %w", err)
	}

	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, rankParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}

	roundDecimal, err = strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	return &rankParams{
		limit:        limit,
		offset:       offset,
		roundDecimal: roundDecimal,
	}, nil
}

func (t *hybridSearchTask) collectHybridSearchResults(ctx context.Context) error {
	select {
	case <-t.TraceCtx().Done():
		log.Ctx(ctx).Warn("hybrid search task wait to finish timeout!")
		return fmt.Errorf("hybrid search task wait to finish timeout, msgID=%d", t.ID())
	default:
		log.Ctx(ctx).Debug("all hybrid searches are finished or canceled")
		t.resultBuf.Range(func(res *querypb.HybridSearchResult) bool {
			for index, searchResult := range res.GetResults() {
				t.searchTasks[index].resultBuf.Insert(searchResult)
			}
			log.Ctx(ctx).Debug("proxy receives one hybrid search result",
				zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})

		t.multipleRecallResults = typeutil.NewConcurrentSet[*milvuspb.SearchResults]()
		for i, searchTask := range t.searchTasks {
			err := searchTask.PostExecute(ctx)
			if err != nil {
				return err
			}
			t.reScorers[i].reScore(searchTask.result)
			t.multipleRecallResults.Insert(searchTask.result)
		}

		return nil
	}
}

func (t *hybridSearchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-PostExecute")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.CollectionID), zap.String("collName", t.request.GetCollectionName()))
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy postExecute hybrid search %d", t.ID()))
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	err := t.collectHybridSearchResults(ctx)
	if err != nil {
		log.Warn("failed to collect hybrid search results", zap.Error(err))
		return err
	}

	metricType := ""
	t.queryChannelsTs = make(map[string]uint64)
	for _, r := range t.resultBuf.Collect() {
		metricType = r.GetResults()[0].GetMetricType()
		for ch, ts := range r.GetChannelsMvcc() {
			t.queryChannelsTs[ch] = ts
		}
	}

	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}

	t.rankParams, err = parseRankParams(t.request.GetRankParams())
	if err != nil {
		return err
	}

	t.result, err = rankSearchResultData(ctx, 1,
		t.rankParams,
		primaryFieldSchema.GetDataType(),
		metricType,
		t.multipleRecallResults.Collect())
	if err != nil {
		log.Warn("rank search result failed", zap.Error(err))
		return err
	}

	t.result.CollectionName = t.request.GetCollectionName()
	t.fillInFieldInfo()

	if t.requery {
		err := t.Requery()
		if err != nil {
			log.Warn("failed to requery", zap.Error(err))
			return err
		}
	}
	t.result.Results.OutputFields = t.userOutputFields

	log.Debug("hybrid search post execute done")
	return nil
}

func (t *hybridSearchTask) Requery() error {
	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Retrieve,
		},
		DbName:                t.request.GetDbName(),
		CollectionName:        t.request.GetCollectionName(),
		Expr:                  "",
		OutputFields:          t.request.GetOutputFields(),
		PartitionNames:        t.request.GetPartitionNames(),
		GuaranteeTimestamp:    t.request.GetGuaranteeTimestamp(),
		TravelTimestamp:       t.request.GetTravelTimestamp(),
		NotReturnAllMeta:      t.request.GetNotReturnAllMeta(),
		ConsistencyLevel:      t.request.GetConsistencyLevel(),
		UseDefaultConsistency: t.request.GetUseDefaultConsistency(),
		QueryParams: []*commonpb.KeyValuePair{
			{
				Key:   LimitKey,
				Value: strconv.FormatInt(t.rankParams.limit, 10),
			},
		},
	}

	return doRequery(t.ctx, t.CollectionID, t.node, t.schema.CollectionSchema, queryReq, t.result, t.queryChannelsTs, t.GetPartitionIDs())
}

func rankSearchResultData(ctx context.Context,
	nq int64,
	params *rankParams,
	pkType schemapb.DataType,
	metricType string,
	searchResults []*milvuspb.SearchResults,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("rankSearchResultData")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	offset := params.offset
	limit := params.limit
	topk := limit + offset
	roundDecimal := params.roundDecimal
	log.Ctx(ctx).Debug("rankSearchResultData",
		zap.Int("len(searchResults)", len(searchResults)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metric type", metricType))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       limit,
			FieldsData: make([]*schemapb.FieldData, 0),
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

	// []map[id]score
	accumulatedScores := make([]map[interface{}]float32, nq)
	for i := int64(0); i < nq; i++ {
		accumulatedScores[i] = make(map[interface{}]float32)
	}

	for _, result := range searchResults {
		ret.Results.AllSearchCount += result.GetResults().GetAllSearchCount()
		scores := result.GetResults().GetScores()
		start := int64(0)
		for i := int64(0); i < nq; i++ {
			realTopk := result.GetResults().Topks[i]
			for j := start; j < start+realTopk; j++ {
				id := typeutil.GetPK(result.GetResults().GetIds(), j)
				accumulatedScores[i][id] += scores[j]
			}
			start += realTopk
		}
	}

	for i := int64(0); i < nq; i++ {
		idSet := accumulatedScores[i]
		keys := make([]interface{}, 0)
		for key := range idSet {
			keys = append(keys, key)
		}

		if int64(len(keys)) <= offset {
			ret.Results.Topks = append(ret.Results.Topks, 0)
			continue
		}

		compareKeys := func(keyI, keyJ interface{}) bool {
			switch keyI.(type) {
			case int64:
				return keyI.(int64) < keyJ.(int64)
			case string:
				return keyI.(string) < keyJ.(string)
			}
			return false
		}

		// sort id by score
		var less func(i, j int) bool
		if metric.PositivelyRelated(metricType) {
			less = func(i, j int) bool {
				if idSet[keys[i]] == idSet[keys[j]] {
					return compareKeys(keys[i], keys[j])
				}
				return idSet[keys[i]] > idSet[keys[j]]
			}
		} else {
			less = func(i, j int) bool {
				if idSet[keys[i]] == idSet[keys[j]] {
					return compareKeys(keys[i], keys[j])
				}
				return idSet[keys[i]] < idSet[keys[j]]
			}
		}

		sort.Slice(keys, less)

		if int64(len(keys)) > topk {
			keys = keys[:topk]
		}

		// set real topk
		ret.Results.Topks = append(ret.Results.Topks, int64(len(keys))-offset)
		// append id and score
		for index := offset; index < int64(len(keys)); index++ {
			typeutil.AppendPKs(ret.Results.Ids, keys[index])
			score := idSet[keys[index]]
			if roundDecimal != -1 {
				multiplier := math.Pow(10.0, float64(roundDecimal))
				score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
			}
			ret.Results.Scores = append(ret.Results.Scores, score)
		}
	}

	return ret, nil
}

func (t *hybridSearchTask) fillInFieldInfo() {
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

func (t *hybridSearchTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *hybridSearchTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *hybridSearchTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *hybridSearchTask) Name() string {
	return HybridSearchTaskName
}

func (t *hybridSearchTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *hybridSearchTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hybridSearchTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *hybridSearchTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *hybridSearchTask) OnEnqueue() error {
	t.Base = commonpbutil.NewMsgBase()
	t.Base.MsgType = commonpb.MsgType_Search
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}
