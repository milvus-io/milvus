package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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

	resultBuf       chan *internalpb.RetrieveResults
	toReduceResults []*internalpb.RetrieveResults
	runningGroup    *errgroup.Group
	runningGroupCtx context.Context

	queryShardPolicy pickShardPolicy
	shardMgr         *shardClientMgr
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

func (t *queryTask) PreExecute(ctx context.Context) error {
	if t.queryShardPolicy == nil {
		t.queryShardPolicy = roundRobinPolicy
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

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}

	t.CollectionID = collID
	log.Ctx(ctx).Debug("Get collection ID by name",
		zap.Int64("collectionID", t.CollectionID), zap.String("collection name", collectionName),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Ctx(ctx).Warn("invalid partition name", zap.String("partition name", tag),
				zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
			return err
		}
	}
	log.Ctx(ctx).Debug("Validate partition names.",
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, collectionName, t.request.GetPartitionNames())
	if err != nil {
		log.Ctx(ctx).Warn("failed to get partitions in collection.", zap.String("collection name", collectionName),
			zap.Error(err),
			zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))
		return err
	}
	log.Ctx(ctx).Debug("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

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
	log.Ctx(ctx).Debug("translate output fields", zap.Any("OutputFields", t.request.OutputFields),
		zap.Int64("msgID", t.ID()), zap.Any("requestType", "query"))

	outputFieldIDs, err := translateToOutputFieldIDs(t.request.GetOutputFields(), schema)
	if err != nil {
		return err
	}
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

	executeQuery := func(withCache bool) error {
		shards, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName)
		if err != nil {
			return err
		}
		t.resultBuf = make(chan *internalpb.RetrieveResults, len(shards))
		t.toReduceResults = make([]*internalpb.RetrieveResults, 0, len(shards))
		t.runningGroup, t.runningGroupCtx = errgroup.WithContext(ctx)
		for channelID, leaders := range shards {
			channelID := channelID
			leaders := leaders
			t.runningGroup.Go(func() error {
				log.Ctx(ctx).Debug("proxy starting to query one shard",
					zap.Int64("msgID", t.ID()),
					zap.Int64("collectionID", t.CollectionID),
					zap.String("collection name", t.collectionName),
					zap.String("shard channel", channelID),
					zap.Uint64("timeoutTs", t.TimeoutTimestamp))

				err := t.queryShard(t.runningGroupCtx, leaders, channelID)
				if err != nil {
					return err
				}
				return nil
			})
		}

		err = t.runningGroup.Wait()
		return err
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case <-t.TraceCtx().Done():
				log.Ctx(ctx).Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
				return
			case <-t.runningGroupCtx.Done():
				log.Ctx(ctx).Debug("all queries are finished or canceled", zap.Int64("msgID", t.ID()))
				close(t.resultBuf)
				for res := range t.resultBuf {
					t.toReduceResults = append(t.toReduceResults, res)
					log.Ctx(ctx).Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()), zap.Any("msgID", t.ID()))
				}
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")
	t.result, err = mergeRetrieveResults(ctx, t.toReduceResults)
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
			Reason:    "emptly collection", // TODO
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

func (t *queryTask) queryShard(ctx context.Context, leaders []nodeInfo, channelID string) error {
	query := func(nodeID UniqueID, qn types.QueryNode) error {
		req := &querypb.QueryRequest{
			Req:        t.RetrieveRequest,
			DmlChannel: channelID,
			Scope:      querypb.DataScope_All,
		}

		result, err := qn.Query(ctx, req)
		if err != nil {
			log.Ctx(ctx).Warn("QueryNode query return error", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("channel", channelID), zap.Error(err))
			return err
		}
		if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
			log.Ctx(ctx).Warn("QueryNode is not shardLeader", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("channel", channelID))
			return errInvalidShardLeaders
		}
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Ctx(ctx).Warn("QueryNode query result error", zap.Int64("msgID", t.ID()),
				zap.Int64("nodeID", nodeID), zap.String("reason", result.GetStatus().GetReason()))
			return fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason())
		}

		log.Ctx(ctx).Debug("get query result", zap.Int64("msgID", t.ID()),
			zap.Int64("nodeID", nodeID), zap.String("channelID", channelID))
		t.resultBuf <- result
		return nil
	}

	err := t.queryShardPolicy(t.TraceCtx(), t.shardMgr, query, leaders)
	if err != nil {
		log.Ctx(ctx).Warn("fail to Query to all shard leaders", zap.Int64("msgID", t.ID()),
			zap.Int64("taskID", t.ID()), zap.Any("shard leaders", leaders))
		return err
	}

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

func mergeRetrieveResults(ctx context.Context, retrieveResults []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	var ret *milvuspb.QueryResults
	var skipDupCnt int64
	var idSet = make(map[interface{}]struct{})

	// merge results and remove duplicates
	for _, rr := range retrieveResults {
		numPks := typeutil.GetSizeOfIDs(rr.GetIds())
		// skip empty result, it will break merge result
		if rr == nil || rr.Ids == nil || rr.GetIds() == nil || numPks == 0 {
			continue
		}

		if ret == nil {
			ret = &milvuspb.QueryResults{
				FieldsData: make([]*schemapb.FieldData, len(rr.FieldsData)),
			}
		}

		if len(ret.FieldsData) != len(rr.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in proxy RetrieveResults, expect %d get %d", len(ret.FieldsData), len(rr.FieldsData))
		}

		for i := 0; i < numPks; i++ {
			id := typeutil.GetPK(rr.GetIds(), int64(i))
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.FieldsData, rr.FieldsData, int64(i))
				idSet[id] = struct{}{}
			} else {
				// primary keys duplicate
				skipDupCnt++
			}
		}
	}
	log.Ctx(ctx).Debug("skip duplicated query result", zap.Int64("count", skipDupCnt))

	if ret == nil {
		ret = &milvuspb.QueryResults{
			FieldsData: []*schemapb.FieldData{},
		}
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
