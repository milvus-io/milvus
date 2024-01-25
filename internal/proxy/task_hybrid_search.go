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
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	HybridSearchTaskName = "HybridSearchTask"
)

type hybridSearchTask struct {
	Condition
	ctx context.Context

	result  *milvuspb.SearchResults
	request *milvuspb.HybridSearchRequest

	tr      *timerecord.TimeRecorder
	schema  *schemaInfo
	requery bool

	userOutputFields []string

	qc              types.QueryCoordClient
	node            types.ProxyComponent
	lb              LBPolicy
	queryChannelsTs map[string]Timestamp

	collectionID UniqueID

	multipleRecallResults *typeutil.ConcurrentSet[*milvuspb.SearchResults]
	reScorers             []reScorer
	rankParams            *rankParams
}

func (t *hybridSearchTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-PreExecute")
	defer sp.End()

	if len(t.request.Requests) <= 0 {
		return errors.New("minimum of ann search requests is 1")
	}

	if len(t.request.Requests) > defaultMaxSearchRequest {
		return errors.New("maximum of ann search requests is 1024")
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

	collectionName := t.request.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		return err
	}
	t.collectionID = collID

	log := log.Ctx(ctx).With(zap.Int64("collID", collID), zap.String("collName", collectionName))
	t.schema, err = globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get collection schema failed", zap.Error(err))
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("is partition key mode failed", zap.Error(err))
		return err
	}
	if partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
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

	log.Debug("hybrid search preExecute done.",
		zap.Uint64("guarantee_ts", t.request.GetGuaranteeTimestamp()),
		zap.Bool("use_default_consistency", t.request.GetUseDefaultConsistency()),
		zap.Any("consistency level", t.request.GetConsistencyLevel()))

	return nil
}

func (t *hybridSearchTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-Execute")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.collectionID), zap.String("collName", t.request.GetCollectionName()))
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute hybrid search %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	futures := make([]*conc.Future[*milvuspb.SearchResults], len(t.request.Requests))
	for index := range t.request.Requests {
		searchReq := t.request.Requests[index]
		future := conc.Go(func() (*milvuspb.SearchResults, error) {
			searchReq.TravelTimestamp = t.request.GetTravelTimestamp()
			searchReq.GuaranteeTimestamp = t.request.GetGuaranteeTimestamp()
			searchReq.NotReturnAllMeta = t.request.GetNotReturnAllMeta()
			searchReq.ConsistencyLevel = t.request.GetConsistencyLevel()
			searchReq.UseDefaultConsistency = t.request.GetUseDefaultConsistency()
			searchReq.OutputFields = nil

			return t.node.Search(ctx, searchReq)
		})
		futures[index] = future
	}

	err := conc.AwaitAll(futures...)
	if err != nil {
		return err
	}

	t.reScorers, err = NewReScorer(t.request.GetRequests(), t.request.GetRankParams())
	if err != nil {
		log.Info("generate reScorer failed", zap.Any("rank params", t.request.GetRankParams()), zap.Error(err))
		return err
	}
	t.multipleRecallResults = typeutil.NewConcurrentSet[*milvuspb.SearchResults]()
	for i, future := range futures {
		err = future.Err()
		if err != nil {
			log.Debug("QueryNode search result error", zap.Error(err))
			return err
		}
		result := futures[i].Value()
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Debug("QueryNode search result error",
				zap.String("reason", result.GetStatus().GetReason()))
			return merr.Error(result.GetStatus())
		}

		t.reScorers[i].reScore(result)
		t.multipleRecallResults.Insert(result)
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
		return nil, errors.New(LimitKey + " not found in search_params")
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

func (t *hybridSearchTask) PostExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch-PostExecute")
	defer sp.End()

	log := log.Ctx(ctx).With(zap.Int64("collID", t.collectionID), zap.String("collName", t.request.GetCollectionName()))
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy postExecute hybrid search %d", t.ID()))
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

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

	// TODO:Xige-16 refine the mvcc functionality of hybrid search
	// TODO:silverxia move partitionIDs to hybrid search level
	return doRequery(t.ctx, t.collectionID, t.node, t.schema.CollectionSchema, queryReq, t.result, t.queryChannelsTs, []int64{})
}

func rankSearchResultData(ctx context.Context,
	nq int64,
	params *rankParams,
	pkType schemapb.DataType,
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
		zap.Int64("limit", limit))

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

		// sort id by score
		sort.Slice(keys, func(i, j int) bool {
			return idSet[keys[i]] >= idSet[keys[j]]
		})

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
	return t.request.Base.MsgID
}

func (t *hybridSearchTask) SetID(uid UniqueID) {
	t.request.Base.MsgID = uid
}

func (t *hybridSearchTask) Name() string {
	return HybridSearchTaskName
}

func (t *hybridSearchTask) Type() commonpb.MsgType {
	return t.request.Base.MsgType
}

func (t *hybridSearchTask) BeginTs() Timestamp {
	return t.request.Base.Timestamp
}

func (t *hybridSearchTask) EndTs() Timestamp {
	return t.request.Base.Timestamp
}

func (t *hybridSearchTask) SetTs(ts Timestamp) {
	t.request.Base.Timestamp = ts
}

func (t *hybridSearchTask) OnEnqueue() error {
	t.request.Base = commonpbutil.NewMsgBase()
	t.request.Base.MsgType = commonpb.MsgType_Search
	t.request.Base.SourceID = paramtable.GetNodeID()
	return nil
}
