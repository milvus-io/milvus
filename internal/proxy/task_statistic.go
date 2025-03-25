package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	GetCollectionStatisticsTaskName = "GetCollectionStatisticsTask"
	GetPartitionStatisticsTaskName  = "GetPartitionStatisticsTask"
)

type getStatisticsTask struct {
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

	ctx context.Context
	dc  types.DataCoordClient
	tr  *timerecord.TimeRecorder

	fromDataCoord bool
	fromQueryNode bool

	// if query from shard
	*internalpb.GetStatisticsRequest
	qc        types.QueryCoordClient
	resultBuf *typeutil.ConcurrentSet[*internalpb.GetStatisticsResponse]

	lb LBPolicy
}

func (g *getStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *getStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getStatisticsTask) OnEnqueue() error {
	g.GetStatisticsRequest = &internalpb.GetStatisticsRequest{
		Base: commonpbutil.NewMsgBase(),
	}

	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *getStatisticsTask) PreExecute(ctx context.Context) error {
	g.DbID = 0
	g.collectionName = g.request.GetCollectionName()
	g.partitionNames = g.request.GetPartitionNames()
	// g.TravelTimestamp = g.request.GetTravelTimestamp()
	g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistics-PreExecute")
	defer sp.End()

	collID, err := globalMetaCache.GetCollectionID(ctx, g.request.GetDbName(), g.collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	partIDs, err := getPartitionIDs(ctx, g.request.GetDbName(), g.collectionName, g.partitionNames)
	if err != nil { // err is not nil if partition not exists
		return err
	}

	g.GetStatisticsRequest.DbID = 0 // todo
	g.GetStatisticsRequest.CollectionID = collID

	g.TravelTimestamp = g.BeginTs()
	g.GuaranteeTimestamp = parseGuaranteeTs(g.GuaranteeTimestamp, g.BeginTs())

	deadline, ok := g.TraceCtx().Deadline()
	if ok {
		g.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	// check if collection/partitions are loaded into query node
	loaded, unloaded, err := checkFullLoaded(ctx, g.qc, g.request.GetDbName(), g.collectionName, g.GetStatisticsRequest.CollectionID, partIDs)
	log := log.Ctx(ctx).With(
		zap.String("collectionName", g.collectionName),
		zap.Int64("collectionID", g.CollectionID),
	)
	if err != nil {
		g.fromDataCoord = true
		g.unloadedPartitionIDs = partIDs
		log.Info("checkFullLoaded failed, try get statistics from DataCoord",
			zap.Error(err))
		return nil
	}
	if len(unloaded) > 0 {
		g.fromDataCoord = true
		g.unloadedPartitionIDs = unloaded
		log.Info("some partitions has not been loaded, try get statistics from DataCoord",
			zap.Int64s("unloaded partitions", unloaded))
	}
	if len(loaded) > 0 {
		g.fromQueryNode = true
		g.loadedPartitionIDs = loaded
		log.Info("some partitions has been loaded, try get statistics from QueryNode",
			zap.Int64s("loaded partitions", loaded))
	}
	return nil
}

func (g *getStatisticsTask) Execute(ctx context.Context) error {
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
		log.Ctx(ctx).Debug("get collection statistics from QueryNode execute done")
	}
	if g.fromDataCoord {
		err := g.getStatisticsFromDataCoord(ctx)
		if err != nil {
			return err
		}
		log.Ctx(ctx).Debug("get collection statistics from DataCoord execute done")
	}
	return nil
}

func (g *getStatisticsTask) PostExecute(ctx context.Context) error {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistic-PostExecute")
	defer sp.End()
	tr := timerecord.NewTimeRecorder("getStatisticTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()

	toReduceResults := make([]*internalpb.GetStatisticsResponse, 0)
	select {
	case <-g.TraceCtx().Done():
		log.Ctx(ctx).Debug("wait to finish timeout!")
		return nil
	default:
		log.Ctx(ctx).Debug("all get statistics are finished or canceled")
		g.resultBuf.Range(func(res *internalpb.GetStatisticsResponse) bool {
			toReduceResults = append(toReduceResults, res)
			log.Ctx(ctx).Debug("proxy receives one get statistic response",
				zap.Int64("sourceID", res.GetBase().GetSourceID()))
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

	log.Ctx(ctx).Debug("get statistics post execute done", zap.Any("result", result))
	return nil
}

func (g *getStatisticsTask) getStatisticsFromDataCoord(ctx context.Context) error {
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

	result, err := g.dc.GetPartitionStatistics(ctx, req)
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

func (g *getStatisticsTask) getStatisticsFromQueryNode(ctx context.Context) error {
	g.GetStatisticsRequest.PartitionIDs = g.loadedPartitionIDs
	if g.resultBuf == nil {
		g.resultBuf = typeutil.NewConcurrentSet[*internalpb.GetStatisticsResponse]()
	}
	err := g.lb.Execute(ctx, CollectionWorkLoad{
		db:             g.request.GetDbName(),
		collectionID:   g.GetStatisticsRequest.CollectionID,
		collectionName: g.collectionName,
		nq:             1,
		exec:           g.getStatisticsShard,
	})
	if err != nil {
		return errors.Wrap(err, "failed to statistic")
	}

	return nil
}

func (g *getStatisticsTask) getStatisticsShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	nodeReq := proto.Clone(g.GetStatisticsRequest).(*internalpb.GetStatisticsRequest)
	nodeReq.Base.TargetID = nodeID
	req := &querypb.GetStatisticsRequest{
		Req:         nodeReq,
		DmlChannels: []string{channel},
		Scope:       querypb.DataScope_All,
	}
	result, err := qn.GetStatistics(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode statistic return error",
			zap.Int64("nodeID", nodeID),
			zap.String("channel", channel),
			zap.Error(err))
		globalMetaCache.DeprecateShardCache(g.request.GetDbName(), g.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Ctx(ctx).Warn("QueryNode is not shardLeader",
			zap.Int64("nodeID", nodeID),
			zap.String("channel", channel))
		globalMetaCache.DeprecateShardCache(g.request.GetDbName(), g.collectionName)
		return errInvalidShardLeaders
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("QueryNode statistic result error",
			zap.Int64("nodeID", nodeID),
			zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to get statistic on QueryNode ID=%d", nodeID)
	}
	g.resultBuf.Insert(result)

	return nil
}

// checkFullLoaded check if collection / partition was fully loaded into QueryNode
// return loaded partitions, unloaded partitions and error
func checkFullLoaded(ctx context.Context, qc types.QueryCoordClient, dbName string, collectionName string, collectionID int64, searchPartitionIDs []UniqueID) ([]UniqueID, []UniqueID, error) {
	var loadedPartitionIDs []UniqueID
	var unloadPartitionIDs []UniqueID

	// TODO: Consider to check if partition loaded from cache to save rpc.
	info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, collectionName, collectionID)
	if err != nil {
		return nil, nil, fmt.Errorf("GetCollectionInfo failed, dbName = %s, collectionName = %s,collectionID = %d, err = %s", dbName, collectionName, collectionID, err)
	}
	partitionInfos, err := globalMetaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, nil, fmt.Errorf("GetPartitions failed, dbName = %s, collectionName = %s,collectionID = %d, err = %s", dbName, collectionName, collectionID, err)
	}

	// If request to search partitions
	if len(searchPartitionIDs) > 0 {
		resp, err := qc.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionID: info.collID,
			PartitionIDs: searchPartitionIDs,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("showPartitions failed, collection = %d, partitionIDs = %v, err = %s", collectionID, searchPartitionIDs, err)
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return nil, nil, fmt.Errorf("showPartitions failed, collection = %d, partitionIDs = %v, reason = %s", collectionID, searchPartitionIDs, resp.GetStatus().GetReason())
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
		CollectionID: info.collID,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("showPartitions failed, collection = %d, partitionIDs = %v, err = %s", collectionID, searchPartitionIDs, err)
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, nil, fmt.Errorf("showPartitions failed, collection = %d, partitionIDs = %v, reason = %s", collectionID, searchPartitionIDs, resp.GetStatus().GetReason())
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
			return nil, fmt.Errorf("fail to decode result, reason=%s", result.GetStatus().GetReason())
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

//type getPartitionStatisticsTask struct {
//	getStatisticsTask
//	request *milvuspb.GetPartitionStatisticsRequest
//	result  *milvuspb.GetPartitionStatisticsResponse
//}
//
//func (g *getPartitionStatisticsTask) PreExecute(ctx context.Context) error {
//	g.getStatisticsTask.DbID = 0
//	g.getStatisticsTask.collectionName = g.request.GetCollectionName()
//	g.getStatisticsTask.partitionNames = []string{g.request.GetPartitionName()}
//	// g.TravelTimestamp = g.request.GetTravelTimestamp()
//	// g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()
//	return g.getStatisticsTask.PreExecute(ctx)
//}
//
//func (g *getPartitionStatisticsTask) Execute(ctx context.Context) error {
//	if g.fromQueryNode {
//		err := g.getStatisticsTask.Execute(ctx)
//		if err != nil {
//			return err
//		}
//		log.Debug("get partition statistics from QueryNode execute done", zap.Int64("msgID", g.ID()))
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
//		log.Debug("get partition statistics from DataCoord execute done", zap.Int64("msgID", g.ID()))
//		return nil
//	}
//	return nil
//}
//
//func (g *getPartitionStatisticsTask) PostExecute(ctx context.Context) error {
//	err := g.getStatisticsTask.PostExecute(ctx)
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
//type getCollectionStatisticsTask struct {
//	getStatisticsTask
//	request *milvuspb.GetCollectionStatisticsRequest
//	result  *milvuspb.GetCollectionStatisticsResponse
//}
//
//func (g *getCollectionStatisticsTask) PreExecute(ctx context.Context) error {
//	g.getStatisticsTask.DbID = 0
//	g.getStatisticsTask.collectionName = g.request.GetCollectionName()
//	g.getStatisticsTask.partitionNames = []string{}
//	// g.TravelTimestamp = g.request.GetTravelTimestamp()
//	// g.GuaranteeTimestamp = g.request.GetGuaranteeTimestamp()
//	return g.getStatisticsTask.PreExecute(ctx)
//}
//
//func (g *getCollectionStatisticsTask) Execute(ctx context.Context) error {
//	if g.fromQueryNode {
//		// if you get entire collection, we need to pass partition ids param.
//		if len(g.unloadedPartitionIDs) == 0 {
//			g.GetStatisticsRequest.PartitionIDs = nil
//		}
//		err := g.getStatisticsTask.Execute(ctx)
//		if err != nil {
//			return err
//		}
//		log.Debug("get collection statistics from QueryNode execute done", zap.Int64("msgID", g.ID()))
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
//		log.Debug("get collection statistics from DataCoord execute done", zap.Int64("msgID", g.ID()))
//		return nil
//	}
//	return nil
//}
//
//func (g *getCollectionStatisticsTask) PostExecute(ctx context.Context) error {
//	err := g.getStatisticsTask.PostExecute(ctx)
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
// please remove it after getStatisticsTask below is stable
type getCollectionStatisticsTask struct {
	baseTask
	Condition
	*milvuspb.GetCollectionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoordClient
	result    *milvuspb.GetCollectionStatisticsResponse

	collectionID UniqueID
}

func (g *getCollectionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getCollectionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getCollectionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getCollectionStatisticsTask) Name() string {
	return GetCollectionStatisticsTaskName
}

func (g *getCollectionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getCollectionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getCollectionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getCollectionStatisticsTask) OnEnqueue() error {
	g.Base = commonpbutil.NewMsgBase()
	g.Base.MsgType = commonpb.MsgType_GetCollectionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *getCollectionStatisticsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (g *getCollectionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.GetDbName(), g.CollectionName)
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

	result, err := g.dataCoord.GetCollectionStatistics(ctx, req)
	if err = merr.CheckRPCCall(result, err); err != nil {
		return err
	}
	g.result = &milvuspb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
		Stats:  result.Stats,
	}
	return nil
}

func (g *getCollectionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type getPartitionStatisticsTask struct {
	baseTask
	Condition
	*milvuspb.GetPartitionStatisticsRequest
	ctx       context.Context
	dataCoord types.DataCoordClient
	result    *milvuspb.GetPartitionStatisticsResponse

	collectionID UniqueID
}

func (g *getPartitionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *getPartitionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *getPartitionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *getPartitionStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *getPartitionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *getPartitionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *getPartitionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *getPartitionStatisticsTask) OnEnqueue() error {
	g.Base = commonpbutil.NewMsgBase()
	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (g *getPartitionStatisticsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (g *getPartitionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.GetDbName(), g.CollectionName)
	if err != nil {
		return err
	}
	g.collectionID = collID
	partitionID, err := globalMetaCache.GetPartitionID(ctx, g.GetDbName(), g.CollectionName, g.PartitionName)
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

	result, _ := g.dataCoord.GetPartitionStatistics(ctx, req)
	if result == nil {
		return errors.New("get partition statistics resp is nil")
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

func (g *getPartitionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}
