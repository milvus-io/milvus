// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoord

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/sessionutil"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

func waitLoadPartitionDone(ctx context.Context, queryCoord *QueryCoord, collectionID UniqueID, partitionIDs []UniqueID) error {
	for {
		showPartitionReq := &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowPartitions,
			},
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		}

		res, err := queryCoord.ShowPartitions(ctx, showPartitionReq)
		if err != nil || res.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New("showPartitions failed")
		}

		loadDone := true
		for _, percent := range res.InMemoryPercentages {
			if percent < 100 {
				loadDone = false
			}
		}
		if loadDone {
			break
		}
	}

	return nil
}

func waitLoadCollectionDone(ctx context.Context, queryCoord *QueryCoord, collectionID UniqueID) error {
	for {
		showCollectionReq := &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowPartitions,
			},
			CollectionIDs: []UniqueID{collectionID},
		}

		res, err := queryCoord.ShowCollections(ctx, showCollectionReq)
		if err != nil || res.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New("showCollection failed")
		}

		loadDone := true
		for _, percent := range res.InMemoryPercentages {
			if percent < 100 {
				loadDone = false
			}
		}
		if loadDone {
			break
		}
	}

	return nil
}

func TestGrpcTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)

	t.Run("Test ShowParsOnNotLoadedCol", func(t *testing.T) {
		res, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadEmptyPartition", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       genDefaultCollectionSchema(false),
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
		assert.Nil(t, err)
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

	t.Run("Test ReleaseEmptyPartitions", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
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

	t.Run("Test LoadCollection", func(t *testing.T) {
		status, err := queryCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		assert.Nil(t, err)
	})

	//t.Run("Test LoadParAfterLoadCol", func(t *testing.T) {
	//	status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
	//		Base: &commonpb.MsgBase{
	//			MsgType: commonpb.MsgType_LoadPartitions,
	//		},
	//		CollectionID: defaultCollectionID,
	//		PartitionIDs: []UniqueID{defaultPartitionID},
	//		Schema:       genDefaultCollectionSchema(defaultCollectionID, false),
	//	})
	//	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	//	assert.Nil(t, err)
	//})

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
		assert.Nil(t, err)
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

	t.Run("Test LoadBalance", func(t *testing.T) {
		res, err := queryCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadBalanceSegments,
			},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.ErrorCode)
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

		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)

		metricReq = make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "invalid"
		req, err = json.Marshal(metricReq)
		assert.Nil(t, err)
		res, err = queryCoord.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: string(req),
		})

		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
	})

	err = node.stop()
	err = removeNodeSession(node.queryNodeID)
	assert.Nil(t, err)
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestGrpcTaskEnqueueFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	_, err = startQueryNodeServer(ctx)
	assert.Nil(t, err)

	taskIDAllocator := queryCoord.scheduler.taskIDAllocator
	failedAllocator := func() (UniqueID, error) {
		return 0, errors.New("scheduler failed to allocate ID")
	}

	queryCoord.scheduler.taskIDAllocator = failedAllocator

	t.Run("Test LoadPartition", func(t *testing.T) {
		status, err := queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadCollection", func(t *testing.T) {
		status, err := queryCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	queryCoord.scheduler.taskIDAllocator = taskIDAllocator
	status, err := queryCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	})
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	queryCoord.scheduler.taskIDAllocator = failedAllocator

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		status, err := queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	queryCoord.scheduler.taskIDAllocator = taskIDAllocator
	status, err = queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})

	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	status, err = queryCoord.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	})

	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	queryCoord.scheduler.taskIDAllocator = failedAllocator

	t.Run("Test ReleasePartition", func(t *testing.T) {
		status, err := queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadBalance", func(t *testing.T) {
		status, err := queryCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceTask(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	res, err := queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	})
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, res.ErrorCode)

	time.Sleep(100 * time.Millisecond)
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
		BalanceReason: querypb.TriggerCondition_NodeDown,
	}

	loadBalanceTask := &loadBalanceTask{
		baseTask: &baseTask{
			ctx:              baseCtx,
			condition:        newTaskCondition(baseCtx),
			triggerCondition: querypb.TriggerCondition_NodeDown,
		},
		LoadBalanceRequest: loadBalanceSegment,
		rootCoord:          queryCoord.rootCoordClient,
		dataCoord:          queryCoord.dataCoordClient,
		indexCoord:         queryCoord.indexCoordClient,
		cluster:            queryCoord.cluster,
		meta:               queryCoord.meta,
	}
	queryCoord.scheduler.Enqueue(loadBalanceTask)

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
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestGrpcTaskBeforeHealthy(t *testing.T) {
	refreshParams()
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
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
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
		assert.Nil(t, err)
	})

	t.Run("Test ShowAllPartitions", func(t *testing.T) {
		res, err := unHealthyCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.Nil(t, err)
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
		assert.Nil(t, err)
	})

	t.Run("Test LoadCollection", func(t *testing.T) {
		status, err := unHealthyCoord.LoadCollection(ctx, &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genDefaultCollectionSchema(false),
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowCollections", func(t *testing.T) {
		res, err := unHealthyCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
			CollectionIDs: []UniqueID{defaultCollectionID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test ShowAllCollections", func(t *testing.T) {
		res, err := unHealthyCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowCollections,
			},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test GetSegmentInfo", func(t *testing.T) {
		res, err := unHealthyCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs: []UniqueID{defaultSegmentID},
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.Status.ErrorCode)
		assert.Nil(t, err)
	})

	t.Run("Test LoadBalance", func(t *testing.T) {
		res, err := unHealthyCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadBalanceSegments,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, res.ErrorCode)
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
		assert.Nil(t, err)

	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		status, err := unHealthyCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		})
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
		assert.Nil(t, err)
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
		assert.Nil(t, err)
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

	unHealthyCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestQueryCoord_GetComponentStates(t *testing.T) {
	n := &QueryCoord{}
	n.stateCode.Store(internalpb.StateCode_Healthy)
	resp, err := n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	n.session = &sessionutil.Session{}
	n.session.UpdateRegistered(true)
	resp, err = n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func Test_RepeatedLoadSameCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	//first load defaultCollectionID
	status, err := queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadCollectionDone(ctx, queryCoord, defaultCollectionID)

	// second load defaultCollectionID
	status, err = queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadCollectionAndLoadPartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	//first load defaultCollectionID
	status, err := queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadCollectionDone(ctx, queryCoord, defaultCollectionID)

	// second load defaultPartitionID
	status, err = queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatedLoadSamePartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	//first load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// second load defaultPartitionID
	status, err = queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatedLoadDifferentPartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	//first load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// second load defaultPartitionID+1
	loadPartitionReq.PartitionIDs = []UniqueID{defaultPartitionID + 1}
	status, err = queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadPartitionsAndLoadCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	//first load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// second load defaultCollectionID
	status, err = queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadAndReleaseCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	releaseCollectionReq := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}

	//first load defaultCollectionID
	status, err := queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadCollectionDone(ctx, queryCoord, defaultCollectionID)

	// second release defaultCollectionID
	status, err = queryCoord.ReleaseCollection(ctx, releaseCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadAndReleasePartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	releasePartitionReq := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	//first load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// second release defaultPartitionID
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadCollectionAndReleasePartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	releasePartitionReq := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	//first load defaultCollectionID
	status, err := queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadCollectionDone(ctx, queryCoord, defaultCollectionID)

	// second release defaultPartitionID
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadPartitionsAndReleaseCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}
	releaseCollectionReq := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}

	//first load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// second release defaultCollectionID
	status, err = queryCoord.ReleaseCollection(ctx, releaseCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatedReleaseCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	}

	releaseCollectionReq := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}

	// load defaultCollectionID
	status, err := queryCoord.LoadCollection(ctx, loadCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadCollectionDone(ctx, queryCoord, defaultCollectionID)

	// first release defaultCollectionID
	status, err = queryCoord.ReleaseCollection(ctx, releaseCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	// second release defaultCollectionID
	status, err = queryCoord.ReleaseCollection(ctx, releaseCollectionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatedReleaseSamePartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       genDefaultCollectionSchema(false),
	}

	releasePartitionReq := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	// load defaultPartitionID
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID})

	// first release defaultPartitionID
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	// second release defaultPartitionID
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatedReleaseDifferentPartitions(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID, defaultPartitionID + 1},
		Schema:       genDefaultCollectionSchema(false),
	}

	releasePartitionReq := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	// load defaultPartitionID and defaultPartitionID+1
	status, err := queryCoord.LoadPartitions(ctx, loadPartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)
	waitLoadPartitionDone(ctx, queryCoord, defaultCollectionID, []UniqueID{defaultPartitionID, defaultPartitionID + 1})

	// first release defaultPartitionID
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	// second release defaultPartitionID+1
	releasePartitionReq.PartitionIDs = []UniqueID{defaultPartitionID + 1}
	status, err = queryCoord.ReleasePartitions(ctx, releasePartitionReq)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}
