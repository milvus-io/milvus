package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
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

	getQueryNodePolicy getQueryNodePolicy
	queryShardPolicy   pickShardPolicy
}

func (t *queryTask) PreExecute(ctx context.Context) error {
	if t.getQueryNodePolicy == nil {
		t.getQueryNodePolicy = defaultGetQueryNodePolicy
	}

	if t.queryShardPolicy == nil {
		t.queryShardPolicy = roundRobinPolicy
	}

	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Warn("Invalid collection name.", zap.String("collectionName", collectionName),
			zap.Int64("requestID", t.Base.MsgID), zap.String("requestType", "query"))
		return err
	}

	log.Info("Validate collection name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}

	t.CollectionID = collID
	log.Info("Get collection ID by name",
		zap.Int64("collectionID", t.CollectionID), zap.String("collection name", collectionName),
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Warn("invalid partition name", zap.String("partition name", tag),
				zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
			return err
		}
	}
	log.Debug("Validate partition names.",
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	t.PartitionIDs = make([]UniqueID, 0)
	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		log.Warn("failed to get partitions in collection.", zap.String("collection name", collectionName),
			zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	log.Debug("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	// Check if partitions are valid partitions in collection
	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range t.request.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Debug("failed to compile partition name regex expression.", zap.Any("partition name", partitionName),
				zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
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
			// FIXME(wxyu): undefined behavior
			errMsg := fmt.Sprintf("partition name: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	loaded, err := t.checkIfLoaded(collID, t.PartitionIDs)
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

	plan, err := createExprPlan(schema, t.request.Expr)
	if err != nil {
		return err
	}
	t.request.OutputFields, err = translateOutputFields(t.request.OutputFields, schema, true)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", t.request.OutputFields),
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	if len(t.request.OutputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= 100 && field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
				t.OutputFieldsId = append(t.OutputFieldsId, field.FieldID)
			}
		}
	} else {
		addPrimaryKey := false
		for _, reqField := range t.request.OutputFields {
			findField := false
			for _, field := range schema.Fields {
				if reqField == field.Name {
					if field.IsPrimaryKey {
						addPrimaryKey = true
					}
					findField = true
					t.OutputFieldsId = append(t.OutputFieldsId, field.FieldID)
					plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
				} else {
					if field.IsPrimaryKey && !addPrimaryKey {
						t.OutputFieldsId = append(t.OutputFieldsId, field.FieldID)
						plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
						addPrimaryKey = true
					}
				}
			}
			if !findField {
				return fmt.Errorf("field %s not exist", reqField)
			}
		}
	}
	log.Debug("translate output fields to field ids", zap.Any("OutputFieldsID", t.OutputFieldsId),
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))

	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}

	if t.request.TravelTimestamp == 0 {
		t.TravelTimestamp = t.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(t.BeginTs(), t.request.TravelTimestamp) / 1000
		if durationSeconds > Params.CommonCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
		t.TravelTimestamp = t.request.TravelTimestamp
	}

	if t.request.GuaranteeTimestamp == 0 {
		t.GuaranteeTimestamp = t.BeginTs()
	} else {
		t.GuaranteeTimestamp = t.request.GuaranteeTimestamp
	}

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // TODO
	log.Info("Query PreExecute done.",
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}

func (t *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", t.ID()))
	defer tr.Elapse("done")

	executeQuery := func(withCache bool) error {
		shards, err := globalMetaCache.GetShards(ctx, withCache, t.collectionName, t.qc)
		if err != nil {
			return err
		}

		t.resultBuf = make(chan *internalpb.RetrieveResults, len(shards))
		t.toReduceResults = make([]*internalpb.RetrieveResults, 0, len(shards))
		t.runningGroup, t.runningGroupCtx = errgroup.WithContext(ctx)
		for _, shard := range shards {
			s := shard
			t.runningGroup.Go(func() error {
				log.Debug("proxy starting to query one shard",
					zap.Int64("collectionID", t.CollectionID),
					zap.String("collection name", t.collectionName),
					zap.String("shard channel", s.GetChannelName()),
					zap.Uint64("timeoutTs", t.TimeoutTimestamp))

				err := t.queryShard(t.runningGroupCtx, s)
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
	if err == errInvalidShardLeaders {
		log.Warn("invalid shard leaders cache, updating shardleader caches and retry search")
		return executeQuery(WithoutCache)
	}
	if err != nil {
		return fmt.Errorf("fail to search on all shard leaders, err=%s", err.Error())
	}

	log.Info("Query Execute done.",
		zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()

	var err error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case <-t.TraceCtx().Done():
				log.Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, taskID:", t.ID()))
				return
			case <-t.runningGroupCtx.Done():
				log.Debug("all queries are finished or canceled", zap.Any("taskID", t.ID()))
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
	t.result, err = mergeRetrieveResults(t.toReduceResults)
	if err != nil {
		return err
	}
	t.result.CollectionName = t.collectionName

	if len(t.result.FieldsData) > 0 {
		t.result.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}
	} else {
		log.Info("Query result is nil", zap.Any("requestID", t.Base.MsgID), zap.Any("requestType", "query"))
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
	log.Info("Query PostExecute done", zap.Any("requestID", t.Base.MsgID), zap.String("requestType", "query"))
	return nil
}

func (t *queryTask) queryShard(ctx context.Context, leaders *querypb.ShardLeadersList) error {
	query := func(nodeID UniqueID, qn types.QueryNode) error {
		req := &querypb.QueryRequest{
			Req:           t.RetrieveRequest,
			IsShardLeader: true,
			DmlChannel:    leaders.GetChannelName(),
		}

		result, err := qn.Query(ctx, req)
		if err != nil || result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
			log.Warn("QueryNode query returns error", zap.Int64("nodeID", nodeID),
				zap.Error(err))
			return errInvalidShardLeaders
		}
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("QueryNode query result error", zap.Int64("nodeID", nodeID),
				zap.String("reason", result.GetStatus().GetReason()))
			return fmt.Errorf("fail to Query, QueryNode ID = %d, reason=%s", nodeID, result.GetStatus().GetReason())
		}

		log.Debug("get query result", zap.Int64("nodeID", nodeID), zap.String("channelID", leaders.GetChannelName()))
		t.resultBuf <- result
		return nil
	}

	err := t.queryShardPolicy(t.TraceCtx(), t.getQueryNodePolicy, query, leaders)
	if err != nil {
		log.Warn("fail to Query to all shard leaders", zap.Int64("taskID", t.ID()), zap.Any("shard leaders", leaders.GetNodeIds()))
		return err
	}

	return nil
}

func (t *queryTask) checkIfLoaded(collectionID UniqueID, queryPartitionIDs []UniqueID) (bool, error) {
	// check if collection was loaded into QueryNode
	info, err := globalMetaCache.GetCollectionInfo(t.ctx, t.collectionName)
	if err != nil {
		return false, fmt.Errorf("GetCollectionInfo failed, collectionID = %d, err = %s", collectionID, err)
	}
	if info.isLoaded {
		return true, nil
	}

	// If request to query partitions
	if len(queryPartitionIDs) > 0 {
		resp, err := t.qc.ShowPartitions(t.ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     t.Base.MsgID,
				Timestamp: t.Base.Timestamp,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			CollectionID: collectionID,
			PartitionIDs: queryPartitionIDs,
		})
		if err != nil {
			return false, fmt.Errorf("showPartitions failed, collectionID = %d, partitionIDs = %v, err = %s", collectionID, queryPartitionIDs, err)
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return false, fmt.Errorf("showPartitions failed, collectionID = %d, partitionIDs = %v, reason = %s", collectionID, queryPartitionIDs, resp.GetStatus().GetReason())
		}
		// Current logic: show partitions won't return error if the given partitions are all loaded
		return true, nil
	}

	// If request to query collection and collection is not fully loaded
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
		return false, fmt.Errorf("showPartitions failed, collectionID = %d, partitionIDs = %v, err = %s", collectionID, queryPartitionIDs, err)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return false, fmt.Errorf("showPartitions failed, collectionID = %d, partitionIDs = %v, reason = %s", collectionID, queryPartitionIDs, resp.GetStatus().GetReason())
	}

	if len(resp.GetPartitionIDs()) > 0 {
		log.Warn("collection not fully loaded, query on these partitions",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", resp.GetPartitionIDs()))
		return true, nil
	}

	return false, nil
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

func mergeRetrieveResults(retrieveResults []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
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
	log.Debug("skip duplicated query result", zap.Int64("count", skipDupCnt))

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
