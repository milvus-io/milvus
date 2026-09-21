package dql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	GetCollectionStatisticsTaskName = "GetCollectionStatisticsTask"
	GetPartitionStatisticsTaskName  = "GetPartitionStatisticsTask"
)

type GetStatisticsTask struct {
	request *milvuspb.GetStatisticsRequest
	result  *milvuspb.GetStatisticsResponse
	baseTask
	Condition
	collectionName string
	partitionNames []string
	// partition ids that are loaded into query node, require get statistics from QueryNode
	loadedPartitionIDs []UniqueID
	// partition ids that are not loaded into query node, require get statistics from DataCoord
	unloadedPartitionIDs []UniqueID

	ctx  context.Context
	mixc types.MixCoordClient
	tr   *timerecord.TimeRecorder

	fromDataCoord bool
	fromQueryNode bool

	// if query from shard
	*internalpb.GetStatisticsRequest
	resultBuf *typeutil.ConcurrentSet[*internalpb.GetStatisticsResponse]

	shardclientMgr shardclient.ShardClientMgr
	lb             shardclient.LBPolicy
}

// NewGetStatisticsTask constructs a statistics task. Host-node dependencies are
// derived from the taskmodel.TaskNode contract.
func NewGetStatisticsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetStatisticsRequest, tr *timerecord.TimeRecorder) *GetStatisticsTask {
	return &GetStatisticsTask{
		request:        request,
		baseTask:       baseTask{MetaCache: node.GetMetaCache()},
		Condition:      NewTaskCondition(ctx),
		ctx:            ctx,
		tr:             tr,
		mixc:           node.MixCoord(),
		lb:             node.LBPolicy(),
		shardclientMgr: node.ShardMgr(),
	}
}

// Result returns the statistics result after execution.
func (g *GetStatisticsTask) Result() *milvuspb.GetStatisticsResponse {
	return g.result
}

func (g *GetStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *GetStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *GetStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *GetStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *GetStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *GetStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *GetStatisticsTask) OnEnqueue() error {
	g.GetStatisticsRequest = &internalpb.GetStatisticsRequest{
		Base: commonpbutil.NewMsgBase(),
	}

	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *GetStatisticsTask) PreExecute(ctx context.Context) error {
	g.DbID = 0
	g.collectionName = g.request.GetCollectionName()
	g.partitionNames = g.request.GetPartitionNames()
	// g.TravelTimestamp = g.request.GetTravelTimestamp()
	g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistics-PreExecute")
	defer sp.End()

	collID, err := g.GetMetaCache().GetCollectionID(ctx, g.request.GetDbName(), g.collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	partIDs, err := GetPartitionIDs(ctx, g.GetMetaCache(), g.request.GetDbName(), g.collectionName, g.partitionNames)
	if err != nil { // err is not nil if partition not exists
		return err
	}

	g.DbID = 0 // todo
	g.CollectionID = collID

	g.TravelTimestamp = g.BeginTs()
	g.GuaranteeTimestamp = parseGuaranteeTs(g.GuaranteeTimestamp, g.BeginTs())

	deadline, ok := g.TraceCtx().Deadline()
	if ok {
		g.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline)
	}

	// check if collection/partitions are loaded into query node
	loaded, unloaded, err := checkFullLoaded(ctx, g.GetMetaCache(), g.mixc, g.request.GetDbName(), g.collectionName, g.CollectionID, partIDs)
	log := mlog.With(
		mlog.String("collectionName", g.collectionName),
		mlog.Int64("collectionID", g.CollectionID),
	)
	if err != nil {
		g.fromDataCoord = true
		g.unloadedPartitionIDs = partIDs
		log.Info(ctx, "checkFullLoaded failed, try get statistics from DataCoord",
			mlog.Err(err))
		return nil
	}
	if len(unloaded) > 0 {
		g.fromDataCoord = true
		g.unloadedPartitionIDs = unloaded
		log.Info(ctx, "some partitions has not been loaded, try get statistics from DataCoord",
			mlog.Int64s("unloaded partitions", unloaded))
	}
	if len(loaded) > 0 {
		g.fromQueryNode = true
		g.loadedPartitionIDs = loaded
		log.Info(ctx, "some partitions has been loaded, try get statistics from QueryNode",
			mlog.Int64s("loaded partitions", loaded))
	}
	return nil
}

func (g *GetStatisticsTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistics-Execute")
	defer sp.End()
	if g.fromQueryNode {
		// if request get statistics of collection which is full loaded into query node
		// then we need not pass partition ids params
		if len(g.request.GetPartitionNames()) == 0 && len(g.unloadedPartitionIDs) == 0 {
			g.loadedPartitionIDs = []UniqueID{}
		}
		err := g.getStatisticsFromQueryNode(ctx)
		if err != nil {
			return err
		}
		mlog.Debug(ctx, "get collection statistics from QueryNode execute done")
	}
	if g.fromDataCoord {
		err := g.getStatisticsFromDataCoord(ctx)
		if err != nil {
			return err
		}
		mlog.Debug(ctx, "get collection statistics from DataCoord execute done")
	}
	return nil
}

func (g *GetStatisticsTask) PostExecute(ctx context.Context) error {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistic-PostExecute")
	defer sp.End()
	tr := timerecord.NewTimeRecorder("getStatisticTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()

	toReduceResults := make([]*internalpb.GetStatisticsResponse, 0)
	select {
	case <-g.TraceCtx().Done():
		mlog.Debug(ctx, "wait to finish timeout!")
		return merr.Wrapf(g.TraceCtx().Err(), "GetStatistics wait to finish timeout, msgID=%d", g.ID())
	default:
		mlog.Debug(ctx, "all get statistics are finished or canceled")
		g.resultBuf.Range(func(res *internalpb.GetStatisticsResponse) bool {
			toReduceResults = append(toReduceResults, res)
			mlog.Debug(ctx, "proxy receives one get statistic response",
				mlog.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
	}

	validResults, err := decodeGetStatisticsResults(toReduceResults)
	if err != nil {
		return err
	}

	result, err := reduceStatisticResponse(validResults)
	if err != nil {
		return err
	}
	g.result = &milvuspb.GetStatisticsResponse{
		Status: merr.Success(),
		Stats:  result,
	}

	mlog.Debug(ctx, "get statistics post execute done", mlog.Any("result", result))
	return nil
}

func (g *GetStatisticsTask) getStatisticsFromDataCoord(ctx context.Context) error {
	collID := g.CollectionID
	partIDs := g.unloadedPartitionIDs

	req := &datapb.GetPartitionStatisticsRequest{
		Base: commonpbutil.UpdateMsgBase(
			g.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_GetPartitionStatistics),
		),
		CollectionID: collID,
		PartitionIDs: partIDs,
	}

	result, err := g.mixc.GetPartitionStatistics(ctx, req)
	if err != nil {
		return err
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(result.GetStatus())
	}
	if g.resultBuf == nil {
		g.resultBuf = typeutil.NewConcurrentSet[*internalpb.GetStatisticsResponse]()
	}
	g.resultBuf.Insert(&internalpb.GetStatisticsResponse{
		Status: merr.Success(),
		Stats:  result.Stats,
	})
	return nil
}

func (g *GetStatisticsTask) getStatisticsFromQueryNode(ctx context.Context) error {
	g.PartitionIDs = g.loadedPartitionIDs
	if g.resultBuf == nil {
		g.resultBuf = typeutil.NewConcurrentSet[*internalpb.GetStatisticsResponse]()
	}
	err := g.lb.Execute(ctx, shardclient.CollectionWorkLoad{
		Db:             g.request.GetDbName(),
		CollectionID:   g.CollectionID,
		CollectionName: g.collectionName,
		Nq:             1,
		Exec:           g.getStatisticsShard,
	})
	if err != nil {
		return errors.Wrap(err, "failed to statistic")
	}

	return nil
}

func (g *GetStatisticsTask) getStatisticsShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	nodeReq := proto.Clone(g.GetStatisticsRequest).(*internalpb.GetStatisticsRequest)
	nodeReq.Base.TargetID = nodeID
	req := &querypb.GetStatisticsRequest{
		Req:         nodeReq,
		DmlChannels: []string{channel},
		Scope:       querypb.DataScope_All,
	}
	result, err := qn.GetStatistics(ctx, req)
	if err != nil {
		mlog.Warn(ctx, "QueryNode statistic return error",
			mlog.Int64("nodeID", nodeID),
			mlog.String("channel", channel),
			mlog.Err(err))
		g.shardclientMgr.InvalidateShardLeaderCache([]int64{g.CollectionID})
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		mlog.Warn(ctx, "QueryNode is not shardLeader",
			mlog.Int64("nodeID", nodeID),
			mlog.String("channel", channel))
		g.shardclientMgr.InvalidateShardLeaderCache([]int64{g.CollectionID})
		return merr.Error(result.GetStatus())
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		mlog.Warn(ctx, "QueryNode statistic result error",
			mlog.Int64("nodeID", nodeID),
			mlog.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to get statistic on QueryNode ID=%d", nodeID)
	}
	g.resultBuf.Insert(result)

	return nil
}

// checkFullLoaded check if collection / partition was fully loaded into QueryNode
// return loaded partitions, unloaded partitions and error
func checkFullLoaded(ctx context.Context, metaCache Cache, qc types.QueryCoordClient, dbName string, collectionName string, collectionID int64, searchPartitionIDs []UniqueID) ([]UniqueID, []UniqueID, error) {
	var loadedPartitionIDs []UniqueID
	var unloadPartitionIDs []UniqueID

	// TODO: Consider to check if partition loaded from cache to save rpc.
	info, err := metaCache.GetCollectionInfo(ctx, dbName, collectionName, collectionID)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "GetCollectionInfo failed, dbName = %s, collectionName = %s, collectionID = %d", dbName, collectionName, collectionID)
	}
	partitionInfos, err := metaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "GetPartitions failed, dbName = %s, collectionName = %s, collectionID = %d", dbName, collectionName, collectionID)
	}

	// If request to search partitions
	if len(searchPartitionIDs) > 0 {
		resp, err := qc.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionID: info.CollID,
			PartitionIDs: searchPartitionIDs,
		})
		if err != nil {
			return nil, nil, merr.Wrapf(err, "showPartitions failed, collection = %d, partitionIDs = %v", collectionID, searchPartitionIDs)
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return nil, nil, merr.Wrapf(merr.Error(resp.GetStatus()), "showPartitions failed, collection = %d, partitionIDs = %v", collectionID, searchPartitionIDs)
		}

		for i, percentage := range resp.GetInMemoryPercentages() {
			if percentage >= 100 {
				loadedPartitionIDs = append(loadedPartitionIDs, resp.GetPartitionIDs()[i])
			} else {
				unloadPartitionIDs = append(unloadPartitionIDs, resp.GetPartitionIDs()[i])
			}
		}
		return loadedPartitionIDs, unloadPartitionIDs, nil
	}

	// If request to search collection
	resp, err := qc.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: info.CollID,
	})
	if err != nil {
		return nil, nil, merr.Wrapf(err, "showPartitions failed, collection = %d, partitionIDs = %v", collectionID, searchPartitionIDs)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, nil, merr.Wrapf(merr.Error(resp.GetStatus()), "showPartitions failed, collection = %d, partitionIDs = %v", collectionID, searchPartitionIDs)
	}

	loadedMap := make(map[UniqueID]bool)

	for i, percentage := range resp.GetInMemoryPercentages() {
		if percentage >= 100 {
			loadedMap[resp.GetPartitionIDs()[i]] = true
			loadedPartitionIDs = append(loadedPartitionIDs, resp.GetPartitionIDs()[i])
		}
	}

	for _, partitionID := range partitionInfos {
		if _, ok := loadedMap[partitionID]; !ok {
			unloadPartitionIDs = append(unloadPartitionIDs, partitionID)
		}
	}

	return loadedPartitionIDs, unloadPartitionIDs, nil
}

func decodeGetStatisticsResults(results []*internalpb.GetStatisticsResponse) ([]map[string]string, error) {
	ret := make([]map[string]string, len(results))
	for i, result := range results {
		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return nil, merr.Wrap(merr.Error(result.GetStatus()), "fail to decode statistics result")
		}
		ret[i] = funcutil.KeyValuePair2Map(result.GetStats())
	}
	return ret, nil
}

func reduceStatisticResponse(results []map[string]string) ([]*commonpb.KeyValuePair, error) {
	mergedResults := map[string]interface{}{
		"row_count": int64(0),
	}
	fieldMethod := map[string]func(string) error{
		"row_count": func(str string) error {
			count, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			mergedResults["row_count"] = mergedResults["row_count"].(int64) + count
			return nil
		},
	}

	err := funcutil.MapReduce(results, fieldMethod)

	stringMap := make(map[string]string)
	for k, v := range mergedResults {
		stringMap[k] = fmt.Sprint(v)
	}

	return funcutil.Map2KeyValuePair(stringMap), err
}

// implement Task
// try to compatible with old API (getCollectionStatistics & getPartitionStatistics)

//type GetPartitionStatisticsTask struct {
//	GetStatisticsTask
//	request *milvuspb.GetPartitionStatisticsRequest
//	result  *milvuspb.GetPartitionStatisticsResponse
//}
//
//func (g *GetPartitionStatisticsTask) PreExecute(ctx context.Context) error {
//	g.GetStatisticsTask.DbID = 0
//	g.GetStatisticsTask.collectionName = g.request.GetCollectionName()
//	g.GetStatisticsTask.partitionNames = []string{g.request.GetPartitionName()}
//	// g.TravelTimestamp = g.request.GetTravelTimestamp()
//	// g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()
//	return g.GetStatisticsTask.PreExecute(ctx)
//}
//
//func (g *GetPartitionStatisticsTask) Execute(ctx context.Context) error {
//	if g.fromQueryNode {
//		err := g.GetStatisticsTask.Execute(ctx)
//		if err != nil {
//			return err
//		}
//		mlog.Debug(context.TODO(), "get partition statistics from QueryNode execute done", mlog.Int64("msgID", g.ID()))
//	}
//	if g.fromDataCoord {
//		collID := g.CollectionID
//		partIDs := g.unloadedPartitionIDs
//
//		req := &datapb.GetPartitionStatisticsRequest{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_GetPartitionStatistics,
//				MsgID:     g.Base.MsgID,
//				Timestamp: g.Base.Timestamp,
//				SourceID:  g.Base.SourceID,
//			},
//			CollectionID: collID,
//			PartitionIDs: partIDs,
//		}
//
//		result, err := g.dc.GetPartitionStatistics(ctx, req)
//		if err != nil {
//			return err
//		}
//		if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
//			return merr.Error(result.GetStatus())
//		}
//		g.toReduceResults = append(g.toReduceResults, &internalpb.GetStatisticsResponse{
//			Status: merr.Success(),
//			Stats:  result.Stats,
//		})
//		mlog.Debug(context.TODO(), "get partition statistics from DataCoord execute done", mlog.Int64("msgID", g.ID()))
//		return nil
//	}
//	return nil
//}
//
//func (g *GetPartitionStatisticsTask) PostExecute(ctx context.Context) error {
//	err := g.GetStatisticsTask.PostExecute(ctx)
//	if err != nil {
//		return err
//	}
//	g.result = &milvuspb.GetPartitionStatisticsResponse{
//		Status: merr.Success(),
//		Stats:  g.innerResult,
//	}
//	return nil
//}
//
//type GetCollectionStatisticsTask struct {
//	GetStatisticsTask
//	request *milvuspb.GetCollectionStatisticsRequest
//	result  *milvuspb.GetCollectionStatisticsResponse
//}
//
//func (g *GetCollectionStatisticsTask) PreExecute(ctx context.Context) error {
//	g.GetStatisticsTask.DbID = 0
//	g.GetStatisticsTask.collectionName = g.request.GetCollectionName()
//	g.GetStatisticsTask.partitionNames = []string{}
//	// g.TravelTimestamp = g.request.GetTravelTimestamp()
//	// g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()
//	return g.GetStatisticsTask.PreExecute(ctx)
//}
//
//func (g *GetCollectionStatisticsTask) Execute(ctx context.Context) error {
//	if g.fromQueryNode {
//		// if you get entire collection, we need to pass partition ids param.
//		if len(g.unloadedPartitionIDs) == 0 {
//			g.GetStatisticsRequest.PartitionIDs = nil
//		}
//		err := g.GetStatisticsTask.Execute(ctx)
//		if err != nil {
//			return err
//		}
//		mlog.Debug(context.TODO(), "get collection statistics from QueryNode execute done", mlog.Int64("msgID", g.ID()))
//	}
//	if g.fromDataCoord {
//		collID := g.CollectionID
//		partIDs := g.unloadedPartitionIDs
//
//		// all collection has not been loaded, get statistics from datacoord
//		if len(g.GetStatisticsRequest.PartitionIDs) == 0 {
//			req := &datapb.GetCollectionStatisticsRequest{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_GetCollectionStatistics,
//					MsgID:     g.Base.MsgID,
//					Timestamp: g.Base.Timestamp,
//					SourceID:  g.Base.SourceID,
//				},
//				CollectionID: collID,
//			}
//
//			result, err := g.dc.GetCollectionStatistics(ctx, req)
//			if err != nil {
//				return err
//			}
//			if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
//				return merr.Error(result.GetStatus())
//			}
//			g.toReduceResults = append(g.toReduceResults, &internalpb.GetStatisticsResponse{
//				Status: merr.Success(),
//				Stats:  result.Stats,
//			})
//		} else { // some partitions have been loaded, get some partition statistics from datacoord
//			req := &datapb.GetPartitionStatisticsRequest{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_GetPartitionStatistics,
//					MsgID:     g.Base.MsgID,
//					Timestamp: g.Base.Timestamp,
//					SourceID:  g.Base.SourceID,
//				},
//				CollectionID: collID,
//				PartitionIDs: partIDs,
//			}
//
//			result, err := g.dc.GetPartitionStatistics(ctx, req)
//			if err != nil {
//				return err
//			}
//			if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
//				return merr.Error(result.GetStatus())
//			}
//			g.toReduceResults = append(g.toReduceResults, &internalpb.GetStatisticsResponse{
//				Status: merr.Success(),
//				Stats:  result.Stats,
//			})
//		}
//		mlog.Debug(context.TODO(), "get collection statistics from DataCoord execute done", mlog.Int64("msgID", g.ID()))
//		return nil
//	}
//	return nil
//}
//
//func (g *GetCollectionStatisticsTask) PostExecute(ctx context.Context) error {
//	err := g.GetStatisticsTask.PostExecute(ctx)
//	if err != nil {
//		return err
//	}
//	g.result = &milvuspb.GetCollectionStatisticsResponse{
//		Status: merr.Success(),
//		Stats:  g.innerResult,
//	}
//	return nil
//}

// old version of get statistics
// please remove it after GetStatisticsTask below is stable
type GetCollectionStatisticsTask struct {
	baseTask
	Condition
	*milvuspb.GetCollectionStatisticsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.GetCollectionStatisticsResponse

	collectionID UniqueID
}

// NewGetCollectionStatisticsTask constructs a collection-statistics task.
// Host-node dependencies are derived from the taskmodel.TaskNode contract.
func NewGetCollectionStatisticsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetCollectionStatisticsRequest) *GetCollectionStatisticsTask {
	return &GetCollectionStatisticsTask{
		baseTask:                       baseTask{MetaCache: node.GetMetaCache()},
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		mixCoord:                       node.MixCoord(),
	}
}

// Result returns the statistics result after execution.
func (g *GetCollectionStatisticsTask) Result() *milvuspb.GetCollectionStatisticsResponse {
	return g.result
}

func (g *GetCollectionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *GetCollectionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *GetCollectionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *GetCollectionStatisticsTask) Name() string {
	return GetCollectionStatisticsTaskName
}

func (g *GetCollectionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *GetCollectionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetCollectionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetCollectionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *GetCollectionStatisticsTask) OnEnqueue() error {
	g.Base = commonpbutil.NewMsgBase()
	g.Base.MsgType = commonpb.MsgType_GetCollectionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *GetCollectionStatisticsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (g *GetCollectionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := g.GetMetaCache().GetCollectionID(ctx, g.GetDbName(), g.CollectionName)
	if err != nil {
		return err
	}
	g.collectionID = collID
	req := &datapb.GetCollectionStatisticsRequest{
		Base: commonpbutil.UpdateMsgBase(
			g.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_GetCollectionStatistics),
		),
		CollectionID: collID,
	}

	result, err := g.mixCoord.GetCollectionStatistics(ctx, req)
	if err = merr.CheckRPCCall(result, err); err != nil {
		return err
	}
	g.result = &milvuspb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
		Stats:  result.Stats,
	}
	return nil
}

func (g *GetCollectionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetPartitionStatisticsTask struct {
	baseTask
	Condition
	*milvuspb.GetPartitionStatisticsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.GetPartitionStatisticsResponse

	collectionID UniqueID
}

// NewGetPartitionStatisticsTask constructs a partition-statistics task.
// Host-node dependencies are derived from the taskmodel.TaskNode contract.
func NewGetPartitionStatisticsTask(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.GetPartitionStatisticsRequest) *GetPartitionStatisticsTask {
	return &GetPartitionStatisticsTask{
		baseTask:                      baseTask{MetaCache: node.GetMetaCache()},
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		mixCoord:                      node.MixCoord(),
	}
}

// Result returns the statistics result after execution.
func (g *GetPartitionStatisticsTask) Result() *milvuspb.GetPartitionStatisticsResponse {
	return g.result
}

func (g *GetPartitionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *GetPartitionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *GetPartitionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *GetPartitionStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *GetPartitionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *GetPartitionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetPartitionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetPartitionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *GetPartitionStatisticsTask) OnEnqueue() error {
	g.Base = commonpbutil.NewMsgBase()
	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *GetPartitionStatisticsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (g *GetPartitionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := g.GetMetaCache().GetCollectionID(ctx, g.GetDbName(), g.CollectionName)
	if err != nil {
		return err
	}
	g.collectionID = collID
	partitionID, err := g.GetMetaCache().GetPartitionID(ctx, g.GetDbName(), g.CollectionName, g.PartitionName)
	if err != nil {
		return err
	}
	req := &datapb.GetPartitionStatisticsRequest{
		Base: commonpbutil.UpdateMsgBase(
			g.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_GetCollectionStatistics),
		),
		CollectionID: collID,
		PartitionIDs: []int64{partitionID},
	}

	result, _ := g.mixCoord.GetPartitionStatistics(ctx, req)
	if result == nil {
		return merr.WrapErrServiceInternalMsg("get partition statistics resp is nil")
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return merr.Error(result.GetStatus())
	}
	g.result = &milvuspb.GetPartitionStatisticsResponse{
		Status: merr.Success(),
		Stats:  result.Stats,
	}
	return nil
}

func (g *GetPartitionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}
