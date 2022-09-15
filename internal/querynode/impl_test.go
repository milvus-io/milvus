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

package querynode

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func TestImpl_GetComponentStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	node.session.UpdateRegistered(true)

	rsp, err := node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	rsp, err = node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

	node.stateCode = atomic.Value{}
	node.stateCode.Store("invalid")
	rsp, err = node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
}

func TestImpl_GetTimeTickChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	rsp, err := node.GetTimeTickChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
}

func TestImpl_GetStatisticsChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	rsp, err := node.GetStatisticsChannel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
}

func TestImpl_WatchDmChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	schema := genTestCollectionSchema()
	req := &queryPb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       schema,
	}

	status, err := node.WatchDmChannels(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.WatchDmChannels(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_LoadSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	schema := genTestCollectionSchema()

	req := &queryPb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		DstNodeID: 0,
		Schema:    schema,
	}

	status, err := node.LoadSegments(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.LoadSegments(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_ReleaseCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
	}

	status, err := node.ReleaseCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.ReleaseCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_ReleasePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
			MsgID:   rand.Int63(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	status, err := node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_GetSegmentInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test GetSegmentInfo", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			SegmentIDs:   []UniqueID{},
			CollectionID: defaultCollectionID,
		}

		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

		req.SegmentIDs = []UniqueID{-1}
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
		assert.Equal(t, 0, len(rsp.GetInfos()))

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})

	t.Run("test no collection in metaReplica", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		req := &queryPb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			SegmentIDs:   []UniqueID{defaultSegmentID},
			CollectionID: defaultCollectionID,
		}

		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	})

	t.Run("test different segment type", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			SegmentIDs:   []UniqueID{defaultSegmentID},
			CollectionID: defaultCollectionID,
		}

		seg, err := node.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
		assert.NoError(t, err)

		seg.setType(segmentTypeSealed)
		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

		seg.setType(segmentTypeGrowing)
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

		seg.setType(-100)
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	})

	t.Run("test GetSegmentInfo with indexed segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		seg, err := node.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
		assert.NoError(t, err)

		seg.setIndexedFieldInfo(simpleFloatVecField.id, &IndexedFieldInfo{
			indexInfo: &queryPb.FieldIndexInfo{
				IndexName: "query-node-test",
				IndexID:   UniqueID(0),
				BuildID:   UniqueID(0),
			},
		})

		req := &queryPb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			SegmentIDs:   []UniqueID{defaultSegmentID},
			CollectionID: defaultCollectionID,
		}

		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})
}

func TestImpl_isHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	isHealthy := node.isHealthy()
	assert.True(t, isHealthy)
}

func TestImpl_ShowConfigurations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	t.Run("test ShowConfigurations", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath, etcdCli)

		pattern := "Cache"
		req := &internalpb.ShowConfigurationsRequest{
			Base:    genCommonMsgBase(commonpb.MsgType_WatchQueryChannels),
			Pattern: pattern,
		}

		resp, err := node.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("test ShowConfigurations node failed", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath, etcdCli)
		node.UpdateStateCode(internalpb.StateCode_Abnormal)

		pattern := "Cache"
		req := &internalpb.ShowConfigurationsRequest{
			Base:    genCommonMsgBase(commonpb.MsgType_WatchQueryChannels),
			Pattern: pattern,
		}

		reqs, err := node.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, reqs.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
	})
}

func TestImpl_GetMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer etcdCli.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test GetMetrics", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath, etcdCli)

		metricReq := make(map[string]string)
		metricReq[metricsinfo.MetricTypeKey] = "system_info"
		mReq, err := json.Marshal(metricReq)
		assert.NoError(t, err)

		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			Request: string(mReq),
		}

		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("test ParseMetricType failed", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
		}

		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})
	wg.Wait()
}

func TestImpl_ReleaseSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test valid", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
			Scope:        queryPb.DataScope_All,
		}

		_, err = node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)

		req.Scope = queryPb.DataScope_Streaming
		_, err = node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)

		req.Scope = queryPb.DataScope_Historical
		_, err = node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("test invalid query node", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		node.UpdateStateCode(internalpb.StateCode_Abnormal)
		_, err = node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("test segment not exists", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	wg.Add(1)
	t.Run("test no collection", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments),
			CollectionID: defaultCollectionID,
		}

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})
	wg.Wait()
}

func TestImpl_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)

	schema := genTestCollectionSchema()
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)
	// shard cluster not synced
	_, err = node.Search(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)

	// shard cluster sync segments
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SyncSegments(nil, segmentStateLoaded)

	_, err = node.Search(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
}

func TestImpl_searchWithDmlChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)

	schema := genTestCollectionSchema()
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SyncSegments(nil, segmentStateLoaded)

	_, err = node.searchWithDmlChannel(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)

	// search for wrong dml channel
	_, err = node.searchWithDmlChannel(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel + "_suffix"},
	}, defaultDMLChannel)
	assert.NoError(t, err)
}

func TestImpl_GetCollectionStatistics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	defer node.Stop()
	require.NoError(t, err)

	req, err := genGetCollectionStatisticRequest()
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)

	_, err = node.GetStatistics(ctx, &queryPb.GetStatisticsRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
}

func TestImpl_GetPartitionStatistics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	defer node.Stop()
	require.NoError(t, err)

	req, err := genGetPartitionStatisticRequest()
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)

	_, err = node.GetStatistics(ctx, &queryPb.GetStatisticsRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
}

func TestImpl_Query(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	defer node.Stop()
	require.NoError(t, err)

	schema := genTestCollectionSchema()
	req, err := genRetrieveRequest(schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)
	// shard cluster not synced
	_, err = node.Query(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)

	// sync cluster segments
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SyncSegments(nil, segmentStateLoaded)

	_, err = node.Query(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
}

func TestImpl_queryWithDmlChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	defer node.Stop()
	require.NoError(t, err)

	schema := genTestCollectionSchema()
	req, err := genRetrieveRequest(schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SyncSegments(nil, segmentStateLoaded)

	_, err = node.queryWithDmlChannel(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)

	// query for wrong dml channel
	_, err = node.queryWithDmlChannel(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel + "_suffix"},
	}, defaultDMLChannel)
	assert.NoError(t, err)
}

func TestImpl_SyncReplicaSegments(t *testing.T) {
	t.Run("QueryNode not healthy", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		defer node.Stop()
		assert.NoError(t, err)

		node.UpdateStateCode(internalpb.StateCode_Abnormal)

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("Sync non-exist channel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		defer node.Stop()
		assert.NoError(t, err)

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{
			VchannelName: defaultDMLChannel,
			ReplicaSegments: []*queryPb.ReplicaSegmentsInfo{
				{
					NodeId:      1,
					PartitionId: defaultPartitionID,
					SegmentIds:  []int64{1},
				},
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("Normal sync segments", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		defer node.Stop()
		assert.NoError(t, err)

		node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{
			VchannelName: defaultDMLChannel,
			ReplicaSegments: []*queryPb.ReplicaSegmentsInfo{
				{
					NodeId:      1,
					PartitionId: defaultPartitionID,
					SegmentIds:  []int64{1},
				},
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		t.Log(resp.GetReason())

		cs, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		segment, ok := cs.getSegment(1)
		require.True(t, ok)
		assert.Equal(t, common.InvalidNodeID, segment.nodeID)
		assert.Equal(t, defaultPartitionID, segment.partitionID)
		assert.Equal(t, segmentStateLoaded, segment.state)

	})
}
