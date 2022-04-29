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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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

func TestImpl_AddQueryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.AddQueryChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
			MsgID:   rand.Int63(),
		},
		NodeID:             0,
		CollectionID:       defaultCollectionID,
		QueryChannel:       genQueryChannel(),
		QueryResultChannel: genQueryResultChannel(),
	}

	status, err := node.AddQueryChannel(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	status, err = node.AddQueryChannel(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_RemoveQueryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	status, err := node.RemoveQueryChannel(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
}

func TestImpl_WatchDmChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

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

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

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

	t.Run("test no collection in historical", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		err = node.historical.replica.removeCollection(defaultCollectionID)
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

	t.Run("test no collection in streaming", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		err = node.streaming.replica.removeCollection(defaultCollectionID)
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

		seg, err := node.streaming.replica.getSegmentByID(defaultSegmentID)
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

		seg, err := node.historical.replica.getSegmentByID(defaultSegmentID)
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

	t.Run("test GetSegmentInfo without streaming partition", func(t *testing.T) {
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

		node.streaming.replica.(*collectionReplica).partitions = make(map[UniqueID]*Partition)
		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})

	t.Run("test GetSegmentInfo without streaming segment", func(t *testing.T) {
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

		node.streaming.replica.(*collectionReplica).segments = make(map[UniqueID]*Segment)
		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})

	t.Run("test GetSegmentInfo without historical partition", func(t *testing.T) {
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

		node.historical.replica.(*collectionReplica).partitions = make(map[UniqueID]*Partition)
		rsp, err := node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})

	t.Run("test GetSegmentInfo without historical segment", func(t *testing.T) {
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

		node.historical.replica.(*collectionReplica).segments = make(map[UniqueID]*Segment)
		rsp, err := node.GetSegmentInfo(ctx, req)
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
		}

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

		err = node.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		err = node.streaming.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})
	wg.Wait()
}

func TestImpl_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)

	_, err = node.Search(ctx, &queryPb.SearchRequest{
		Req:        req,
		DmlChannel: defaultDMLChannel,
	})
	assert.NoError(t, err)
}

func TestImpl_Query(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	defer node.Stop()
	require.NoError(t, err)

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	req, err := genRetrieveRequest(schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)

	_, err = node.Query(ctx, &queryPb.QueryRequest{
		Req:        req,
		DmlChannel: defaultDMLChannel,
	})
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
		assert.Equal(t, int64(1), segment.nodeID)
		assert.Equal(t, defaultPartitionID, segment.partitionID)
		assert.Equal(t, segmentStateLoaded, segment.state)

	})
}
