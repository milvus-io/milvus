package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	WithCache    = true
	WithoutCache = false
)

const (
	RetrieveTaskName = "RetrieveTask"
	QueryTaskName    = "QueryTask"
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
	schema         *schemapb.CollectionSchema

	resultBuf        chan *internalpb.RetrieveResults
	toReduceResults  []*internalpb.RetrieveResults
	userOutputFields []string

	queryShardPolicy pickShardPolicy
	shardMgr         *shardClientMgr
}

type queryParams struct {
	limit  int64
	offset int64
}

// translateToOutputFieldIDs translates output fields name to output fields id.
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

func filterSystemFields(outputFieldIDs []UniqueID) []UniqueID {
	filtered := make([]UniqueID, 0, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if !common.IsSystemField(outputFieldID) {
			filtered = append(filtered, outputFieldID)
		}
	}
	return filtered
}

// parseQueryParams get limit and offset from queryParamsPair, both are optional.
func parseQueryParams(queryParamsPair []*commonpb.KeyValuePair) (*queryParams, error) {
	var (
		limit  int64
		offset int64
		err    error
	)

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	// if limit is not provided
	if err != nil {
		return &queryParams{limit: typeutil.Unlimited}, nil
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair)
	// if offset is provided
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

	return &queryParams{
		limit:  limit,
		offset: offset,
	}, nil
}

func (t *queryTask) assignPartitionKeys(ctx context.Context, keys []*planpb.GenericValue) ([]string, error) {
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, t.request.GetDbName(), t.collectionName)
	if err != nil {
		return nil, err
	}

	partitionKeyFieldSchema, err := typeutil.GetPartitionFieldSchema(t.schema)
	if err != nil {
		return nil, err
	}

	hashedPartitionNames, err := typeutil.HashKey2Partitions(partitionKeyFieldSchema, keys, partitionNames)
	return hashedPartitionNames, err
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

	log.Ctx(ctx).Debug("Validate collection name.", zap.Any("collectionName", collectionName),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}

	t.CollectionID = collID
	log.Ctx(ctx).Debug("Get collection ID by name",
		zap.Int64("collectionID", t.CollectionID), zap.String("collection name", collectionName),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	partitionKeyMode, _ := isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return errors.New("not support manually specifying the partition names if partition key mode is used")
	}

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Ctx(ctx).Warn("invalid partition name", zap.String("partition name", tag),
				zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
			return err
		}
	}
	log.Ctx(ctx).Debug("Validate partition names.",
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	// fetch search_growing from search param
	var ignoreGrowing bool
	for i, kv := range t.request.GetQueryParams() {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err = strconv.ParseBool(kv.Value)
			if err != nil {
				return errors.New("parse search growing failed")
			}
			t.request.QueryParams = append(t.request.GetQueryParams()[:i], t.request.GetQueryParams()[i+1:]...)
			break
		}
	}
	t.RetrieveRequest.IgnoreGrowing = ignoreGrowing

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

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), collectionName)
	t.schema = schema

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

	partitionNames := t.request.GetPartitionNames()
	if partitionKeyMode {
		expr, err := ParseExprFromPlan(plan)
		if err != nil {
			return err
		}
		partitionKeys := ParsePartitionKeys(expr)
		hashedPartitionNames, err := t.assignPartitionKeys(ctx, partitionKeys)
		if err != nil {
			return err
		}

		partitionNames = append(partitionNames, hashedPartitionNames...)
	}
	t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, partitionNames)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get partitions in collection.", zap.String("collection name", collectionName),
			zap.Error(err),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}
	log.Ctx(ctx).Debug("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"), zap.Int64s("partitionIDs", t.RetrieveRequest.GetPartitionIDs()))

	t.request.OutputFields, t.userOutputFields, err = translateOutputFields(t.request.OutputFields, schema, true)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Debug("translate output fields", zap.Any("OutputFields", t.request.OutputFields),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	outputFieldIDs, err := translateToOutputFieldIDs(t.request.GetOutputFields(), schema)
	if err != nil {
		return err
	}
	outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
	t.RetrieveRequest.OutputFieldsId = outputFieldIDs
	plan.OutputFieldIds = outputFieldIDs
	log.Ctx(ctx).Debug("translate output fields to field ids", zap.Any("OutputFieldsID", t.OutputFieldsId),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

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
	log := log.Ctx(ctx)

	// Add user name into context if it's exists.
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		ctx = contextutil.WithUserInGrpcMetadata(ctx, username)
	}

	executeQuery := func() error {
		shards, err := globalMetaCache.GetShards(ctx, true, t.request.GetDbName(), t.collectionName)
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

	retryCtx, cancel := context.WithCancel(ctx)
	err := retry.Do(retryCtx, func() error {
		queryError := executeQuery()
		if !common.IsRetryableError(queryError) {
			cancel()
		}
		if queryError != nil {
			log.Warn("invalid shard leaders cache, updating shardleader caches and retry query",
				zap.Error(queryError))
			globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		}
		return queryError
	}, retry.Attempts(Params.CommonCfg.GrpcRetryTimes))
	cancel()
	if err != nil {
		return fmt.Errorf("fail to query on all shard leaders, err=%s", err.Error())
	}

	log.Debug("Query Execute done.",
		zap.Int64("msgID", t.ID()), zap.String("requestType", "query"))
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
	t.result, err = reduceRetrieveResultsAndFillIfEmpty(ctx, t.toReduceResults, t.queryParams, t.GetOutputFieldsId(), t.schema)
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
		t.result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_EmptyCollection,
			Reason:    "empty collection", // TODO
		}
		return nil
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), t.request.CollectionName)
	if err != nil {
		return err
	}
	for i := 0; i < len(t.result.FieldsData); i++ {
		if t.OutputFieldsId[i] == common.TimeStampField {
			t.result.FieldsData = append(t.result.FieldsData[:i], t.result.FieldsData[(i+1):]...)
			i--
			continue
		}
		for _, field := range schema.Fields {
			if field.FieldID == t.OutputFieldsId[i] {
				// deal with the situation that offset equal to or greater than the number of entities
				if t.result.FieldsData[i] == nil {
					t.result.FieldsData[i], err = typeutil.GenEmptyFieldData(field)
					if err != nil {
						return err
					}
				}
				t.result.FieldsData[i].FieldName = field.Name
				t.result.FieldsData[i].FieldId = field.FieldID
				t.result.FieldsData[i].Type = field.DataType
				t.result.FieldsData[i].IsDynamic = field.IsDynamic
			}
		}
	}
	t.result.OutputFields = t.userOutputFields
	log.Ctx(ctx).Debug("Query PostExecute done", zap.Int64("msgID", t.ID()), zap.String("requestType", "query"))
	return nil
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNode, channelIDs []string, _ int) error {
	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: channelIDs,
		Scope:       querypb.DataScope_All,
	}

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode query return error", zap.Int64("msgID", t.ID()),
			zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs), zap.Error(err))
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return common.NewCodeError(commonpb.ErrorCode_NotReadyServe, err)
	}
	errCode := result.GetStatus().GetErrorCode()
	if errCode == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader", zap.Int64("nodeID", nodeID), zap.Strings("channels", channelIDs))
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return common.NewCodeError(errCode, errInvalidShardLeaders)
	}
	if errCode != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode query result error", zap.Int64("msgID", t.ID()), zap.Int64("nodeID", nodeID),
			zap.String("reason", result.GetStatus().GetReason()))
		return common.NewCodeError(errCode,
			fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason()))
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
		strs := lo.Map(ids.GetStrId().GetData(), func(str string, _ int) string {
			return fmt.Sprintf("\"%s\"", str)
		})
		idsStr = strings.Trim(strings.Join(strs, ", "), "[]")
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

func reduceRetrieveResultsAndFillIfEmpty(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams, outputFieldsID []int64, schema *schemapb.CollectionSchema) (*milvuspb.QueryResults, error) {
	result, err := reduceRetrieveResults(ctx, retrieveResults, queryParams)
	if err != nil {
		return nil, err
	}

	// filter system fields.
	filtered := filterSystemFields(outputFieldsID)
	if err := typeutil.FillRetrieveResultIfEmpty(typeutil.NewMilvusResult(result), filtered, schema); err != nil {
		return nil, fmt.Errorf("failed to fill retrieve results: %s", err.Error())
	}

	return result, nil
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
