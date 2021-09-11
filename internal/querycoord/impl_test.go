package querycoord

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

func TestGrpcTask(t *testing.T) {
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	t.Run("Test ShowParsOnNotLoadedCol", func(t *testing.T) {
		res, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test LoadEmptyPartition", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowPartitions", func(t *testing.T) {
		res, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowNotLoadedPartitions", func(t *testing.T) {
		res, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{-1},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowAllPartitions", func(t *testing.T) {
		res, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test GetPartitionStates", func(t *testing.T) {
		res, err := queryCoord.GetPartitionStates(ctx, &querypb.GetPartitionStatesRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_GetPartitionStatistics,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadCollection", func(t *testing.T) {
		status, err := queryCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadParAfterLoadCol", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowCollections", func(t *testing.T) {
		res, err := queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionIDs: []UniqueID{defaultCollectionID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowNotLoadedCollections", func(t *testing.T) {
		res, err := queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionIDs: []UniqueID{-1},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowAllCollections", func(t *testing.T) {
		res, err := queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test GetSegmentInfo", func(t *testing.T) {
		res, err := queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs: []UniqueID{defaultSegmentID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseParOfNotLoadedCol", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: -1,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseEmptyPartitions", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ReleaseNotExistPartition", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{-1},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseNotExistCollection", func(t *testing.T) {
		status, err := queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: -1,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		status, err := queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err = queryCoord.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err = queryCoord.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		states, err := queryCoord.GetComponentStates(ctx)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
		assert.Nil(t, err)
	})

	t.Run("Test CreateQueryChannel", func(t *testing.T) {
		res, err := queryCoord.CreateQueryChannel(ctx, &querypb.CreateQueryChannelRequest{
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test GetMetrics", func(t *testing.T) {
		metricReq := make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "system_info"
		req, err := json.Marshal(metricReq)
		assert.Nil(t, err)
		res, err := queryCoord.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: string(req),
		})

		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
	})

	t.Run("Test InvalidMetricType", func(t *testing.T) {
		metricReq := make(map[string]string)
		metricReq["invalidKey"] = "invalidValue"
		req, err := json.Marshal(metricReq)
		assert.Nil(t, err)
		res, err := queryCoord.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: string(req),
		})

		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)

		metricReq = make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "invalid"
		req, err = json.Marshal(metricReq)
		assert.Nil(t, err)
		res, err = queryCoord.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: string(req),
		})

		assert.NotNil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
	})

	//nodes, err := queryCoord.cluster.getOnServiceNodes()
	//assert.Nil(t, err)

	err = node.stop()
	//assert.Nil(t, err)

	//allNodeOffline := waitAllQueryNodeOffline(queryCoord.cluster, nodes)
	//assert.Equal(t, allNodeOffline, true)
	queryCoord.Stop()
}

func TestLoadBalanceTask(t *testing.T) {
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(time.Second)
	res, err := queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	})
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, res.ErrorCode)

	time.Sleep(time.Second)
	for {
		collectionInfo := queryCoord.meta.showCollections()
		if collectionInfo[0].InMemoryPercentage == 100 {
			break
		}
	}
	nodeID := queryNode1.queryNodeID
	queryCoord.cluster.stopNode(nodeID)
	loadBalanceSegment := &querypb.LoadBalanceRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadBalanceSegments,
			SourceID: nodeID,
		},
		SourceNodeIDs: []int64{nodeID},
		BalanceReason: querypb.TriggerCondition_nodeDown,
	}

	loadBalanceTask := &LoadBalanceTask{
		BaseTask: BaseTask{
			ctx:              baseCtx,
			Condition:        NewTaskCondition(baseCtx),
			triggerCondition: querypb.TriggerCondition_nodeDown,
		},
		LoadBalanceRequest: loadBalanceSegment,
		rootCoord:          queryCoord.rootCoordClient,
		dataCoord:          queryCoord.dataCoordClient,
		cluster:            queryCoord.cluster,
		meta:               queryCoord.meta,
	}
	queryCoord.scheduler.Enqueue([]task{loadBalanceTask})

	res, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)

	queryNode1.stop()
	queryNode2.stop()
	queryCoord.Stop()
}

func TestGrpcTaskBeforeHealthy(t *testing.T) {
	ctx := context.Background()
	unHealthyCoord, err := startUnHealthyQueryCoord(ctx)
	assert.Nil(t, err)

	t.Run("Test LoadPartition", func(t *testing.T) {
		status, err := unHealthyCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowPartitions", func(t *testing.T) {
		res, err := unHealthyCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowAllPartitions", func(t *testing.T) {
		res, err := unHealthyCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test GetPartitionStates", func(t *testing.T) {
		res, err := unHealthyCoord.GetPartitionStates(ctx, &querypb.GetPartitionStatesRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_GetPartitionStatistics,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test LoadCollection", func(t *testing.T) {
		status, err := unHealthyCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genCollectionSchema(defaultCollectionID, false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowCollections", func(t *testing.T) {
		res, err := unHealthyCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionIDs: []UniqueID{defaultCollectionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ShowAllCollections", func(t *testing.T) {
		res, err := unHealthyCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test GetSegmentInfo", func(t *testing.T) {
		res, err := unHealthyCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs: []UniqueID{defaultSegmentID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		status, err := unHealthyCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)

	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		status, err := unHealthyCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		states, err := unHealthyCoord.GetComponentStates(ctx)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
		assert.Equal(t, internalpb.StateCode_Abnormal, states.State.StateCode)
		assert.Nil(t, err)
	})

	t.Run("Test CreateQueryChannel", func(t *testing.T) {
		res, err := unHealthyCoord.CreateQueryChannel(ctx, &querypb.CreateQueryChannelRequest{
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.NotNil(t, err)
	})

	t.Run("Test GetMetrics", func(t *testing.T) {
		metricReq := make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "system_info"
		req, err := json.Marshal(metricReq)
		assert.Nil(t, err)
		res, err := unHealthyCoord.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: string(req),
		})

		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
	})

}
