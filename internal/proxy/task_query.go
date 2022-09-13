package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

const (
	WithCache    = true
	WithoutCache = false
)

type queryTask struct {
	Condition
	*internalpb.RetrieveRequest

	ctx            context.Context
	result         *milvuspb.QueryResults
	request        *milvuspb.QueryRequest
	qc             types.QueryCoord
	ids            *schemapb.IDs
	collectionName string
	queryParams    *queryParams

	resultBuf       chan *internalpb.RetrieveResults
	toReduceResults []*internalpb.RetrieveResults

	queryShardPolicy pickShardPolicy
	shardMgr         *shardClientMgr
}

type queryParams struct {
	limit  int64
	offset int64
}

// translateOutputFields translates output fields name to output fields id.
func translateToOutputFieldIDs(outputFields []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	if len(outputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID && field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
				outputFieldIDs = append(outputFieldIDs, field.FieldID)
			}
		}
	} else {
		var pkFieldID UniqueID
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkFieldID = field.FieldID
			}
		}
		for _, reqField := range outputFields {
			var fieldFound bool
			for _, field := range schema.Fields {
				if reqField == field.Name {
					outputFieldIDs = append(outputFieldIDs, field.FieldID)
					fieldFound = true
					break
				}
			}
			if !fieldFound {
				return nil, fmt.Errorf("field %s not exist", reqField)
			}
		}

		// pk field needs to be in output field list
		var pkFound bool
		for _, outputField := range outputFieldIDs {
			if outputField == pkFieldID {
				pkFound = true
				break
			}
		}

		if !pkFound {
			outputFieldIDs = append(outputFieldIDs, pkFieldID)
		}

	}
	return outputFieldIDs, nil
}

// parseQueryParams get limit and offset from queryParamsPair, both are optional.
func parseQueryParams(queryParamsPair []*commonpb.KeyValuePair) (*queryParams, error) {
	var (
		limit  int64
		offset int64
		err    error
	)

	// if limit is provided
	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	if err != nil {
		return &queryParams{limit: typeutil.Unlimited}, nil
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil || limit <= 0 {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	// if offset is provided
	if offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair); err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	if err = validateTopK(limit + offset); err != nil {
		return nil, fmt.Errorf("invalid limit[%d] + offset[%d], %w", limit, offset, err)
	}

	return &queryParams{
		limit:  limit,
		offset: offset,
	}, nil
}

func (t *queryTask) PreExecute(ctx context.Context) error {
	if t.queryShardPolicy == nil {
		t.queryShardPolicy = mergeRoundRobinPolicy
	}

	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Ctx(ctx).Warn("Invalid collection name.", zap.String("collectionName", collectionName),
			zap.Int64("msgID", t.ID()), zap.String("requestType", "query"))
		return err
	}

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}

	t.CollectionID = collID

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Ctx(ctx).Warn("invalid partition name", zap.String("partition name", tag),
				zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
			return err
		}
	}

	t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, t.request.GetPartitionNames())
	if err != nil {
		log.Ctx(ctx).Warn("failed to get partitions in collection.", zap.String("collection name", collectionName),
			zap.Error(err),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}

	queryParams, err := parseQueryParams(t.request.GetQueryParams())
	if err != nil {
		return err
	}
	t.queryParams = queryParams
	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

	loaded, err := checkIfLoaded(ctx, t.qc, collectionName, t.RetrieveRequest.GetPartitionIDs())
	if err != nil {
		return fmt.Errorf("checkIfLoaded failed when query, collection:%v, partitions:%v, err = %s", collectionName, t.request.GetPartitionNames(), err)
	}
	if !loaded {
		return fmt.Errorf("collection:%v or partition:%v not loaded into memory when query", collectionName, t.request.GetPartitionNames())
	}

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, collectionName)

	if t.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		t.request.Expr = IDs2Expr(pkField, t.ids)
	}

	if t.request.Expr == "" {
		return fmt.Errorf("query expression is empty")
	}

	plan, err := planparserv2.CreateRetrievePlan(schema, t.request.Expr)
	if err != nil {
		return err
	}
	t.request.OutputFields, err = translateOutputFields(t.request.OutputFields, schema, true)
	if err != nil {
		return err
	}

	outputFieldIDs, err := translateToOutputFieldIDs(t.request.GetOutputFields(), schema)
	if err != nil {
		return err
	}
	t.RetrieveRequest.OutputFieldsId = outputFieldIDs
	plan.OutputFieldIds = outputFieldIDs

	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}

	if t.request.TravelTimestamp == 0 {
		t.TravelTimestamp = t.BeginTs()
	} else {
		t.TravelTimestamp = t.request.TravelTimestamp
	}

	err = validateTravelTimestamp(t.TravelTimestamp, t.BeginTs())
	if err != nil {
		return err
	}

	guaranteeTs := t.request.GetGuaranteeTimestamp()
	t.GuaranteeTimestamp = parseGuaranteeTs(guaranteeTs, t.BeginTs())

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // TODO
	log.Ctx(ctx).Debug("Query PreExecute done.",
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"),
		zap.Uint64("guarantee_ts", guaranteeTs), zap.Uint64("travel_ts", t.GetTravelTimestamp()),
		zap.Uint64("timeout_ts", t.GetTimeoutTimestamp()))
	return nil
}

func (t *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	executeQuery := func(withCache bool) error {
		shards, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName)
		if err != nil {
			return err
		}
		t.resultBuf = make(chan *internalpb.RetrieveResults, len(shards))
		t.toReduceResults = make([]*internalpb.RetrieveResults, 0, len(shards))

		if err := t.queryShardPolicy(ctx, t.shardMgr, t.queryShard, shards); err != nil {
			return err
		}
		return nil
	}

	err := executeQuery(WithCache)
	if errors.Is(err, errInvalidShardLeaders) || funcutil.IsGrpcErr(err) || errors.Is(err, grpcclient.ErrConnect) {
		log.Ctx(ctx).Warn("invalid shard leaders cache, updating shardleader caches and retry search",
			zap.Int64("msgID", t.ID()), zap.Error(err))
		return executeQuery(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to search on all shard leaders, err=%s", err.Error())
	}

	log.Ctx(ctx).Debug("Query Execute done.",
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
	return nil
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	var err error

	select {
	case <-t.TraceCtx().Done():
		log.Ctx(ctx).Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Ctx(ctx).Debug("all queries are finished or canceled", zap.Int64("msgID", t.ID()))
		close(t.resultBuf)
		for res := range t.resultBuf {
			t.toReduceResults = append(t.toReduceResults, res)
			log.Ctx(ctx).Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()), zap.Any("msgID", t.ID()))
		}
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")
	t.result, err = reduceRetrieveResults(ctx, t.toReduceResults, t.queryParams)
	if err != nil {
		return err
	}
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.QueryLabel).Observe(float64(tr.RecordSpan().Milliseconds()))
	t.result.CollectionName = t.collectionName

	if len(t.result.FieldsData) > 0 {
		t.result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}
	} else {
		log.Ctx(ctx).Warn("Query result is nil", zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		t.result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_EmptyCollection,
			Reason:    "empty collection", // TODO
		}
		return nil
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.CollectionName)
	if err != nil {
		return err
	}
	for i := 0; i < len(t.result.FieldsData); i++ {
		for _, field := range schema.Fields {
			if field.FieldID == t.OutputFieldsId[i] {
				t.result.FieldsData[i].FieldName = field.Name
				t.result.FieldsData[i].FieldId = field.FieldID
				t.result.FieldsData[i].Type = field.DataType
			}
		}
	}
	log.Ctx(ctx).Debug("Query PostExecute done", zap.Int64("msgID", t.ID()), zap.String("requestType", "query"))
	return nil
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs []string) error {
	req := &querypb.QueryRequest{
		Req:         t.RetrieveRequest,
		DmlChannels: channelIDs,
		Scope:       querypb.DataScope_All,
	}

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode query return error", zap.Int64("msgID", t.ID()),
			zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs), zap.Error(err))
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader", zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs))
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode query result error", zap.Int64("msgID", t.ID()), zap.Int64("nodeID", nodeID),
			zap.String("reason", result.GetStatus().GetReason()))
		return fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason())
	}

	log.Ctx(ctx).Debug("get query result", zap.Int64("msgID", t.ID()), zap.Int64("nodeID", nodeID), zap.Strings("channelIDs", channelIDs))
	t.resultBuf <- result
	return nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids *schemapb.IDs) string {
	var idsStr string
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		idsStr = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids.GetIntId().GetData())), ", "), "[]")
	case *schemapb.IDs_StrId:
		idsStr = strings.Trim(strings.Join(ids.GetStrId().GetData(), ", "), "[]")
	}

	return fieldName + " in [ " + idsStr + " ]"
}

func reduceRetrieveResults(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams) (*milvuspb.QueryResults, error) {
	log.Ctx(ctx).Debug("reduceInternelRetrieveResults", zap.Int("len(retrieveResults)", len(retrieveResults)))
	var (
		ret = &milvuspb.QueryResults{}

		skipDupCnt int64
		loopEnd    int
	)

	validRetrieveResults := []*internalpb.RetrieveResults{}
	for _, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	ret.FieldsData = make([]*schemapb.FieldData, len(validRetrieveResults[0].GetFieldsData()))
	idSet := make(map[interface{}]struct{})
	cursors := make([]int64, len(validRetrieveResults))

	if queryParams != nil && queryParams.limit != typeutil.Unlimited {
		loopEnd = int(queryParams.limit)

		if queryParams.offset > 0 {
			for i := int64(0); i < queryParams.offset; i++ {
				sel := typeutil.SelectMinPK(validRetrieveResults, cursors)
				if sel == -1 {
					return ret, nil
				}
				cursors[sel]++
			}
		}
	}

	for j := 0; j < loopEnd; j++ {
		sel := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		if _, ok := idSet[pk]; !ok {
			typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])
			idSet[pk] = struct{}{}
		} else {
			// primary keys duplicate
			skipDupCnt++
		}
		cursors[sel]++
	}

	if skipDupCnt > 0 {
		log.Ctx(ctx).Debug("skip duplicated query result while reducing QueryResults", zap.Int64("count", skipDupCnt))
	}

	return ret, nil
}

func (t *queryTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *queryTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *queryTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *queryTask) Name() string {
	return RetrieveTaskName
}

func (t *queryTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *queryTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *queryTask) OnEnqueue() error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}
