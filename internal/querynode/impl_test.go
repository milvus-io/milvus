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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/conc"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func TestImpl_GetComponentStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	node.session.UpdateRegistered(true)
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	rsp, err := node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	assert.Equal(t, commonpb.StateCode_Healthy, rsp.GetState().GetStateCode())

	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	rsp, err = node.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	assert.Equal(t, commonpb.StateCode_Abnormal, rsp.GetState().GetStateCode())
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

	t.Run("normal_run", func(t *testing.T) {
		schema := genTestCollectionSchema()
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: node.GetSession().ServerID,
			},
			NodeID:       0,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       schema,
			Infos: []*datapb.VchannelInfo{
				{
					CollectionID: 1000,
					ChannelName:  "1000-dmc0",
				},
			},
		}

		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		originPool := node.taskPool
		defer func() {
			node.taskPool = originPool
		}()
		node.taskPool = conc.NewDefaultPool()
		node.taskPool.Release()
		status, err = node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("target not match", func(t *testing.T) {
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: -1,
			},
		}
		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, status.ErrorCode)
	})

	t.Run("server unhealthy", func(t *testing.T) {
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchDmChannels,
				MsgID:   rand.Int63(),
			},
		}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		defer node.UpdateStateCode(commonpb.StateCode_Healthy)
		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("server stopping", func(t *testing.T) {
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchDmChannels,
				MsgID:   rand.Int63(),
			},
		}
		node.UpdateStateCode(commonpb.StateCode_Stopping)
		defer node.UpdateStateCode(commonpb.StateCode_Healthy)
		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("mock release after loaded", func(t *testing.T) {

		mockTSReplica := &MockTSafeReplicaInterface{}

		oldTSReplica := node.tSafeReplica
		defer func() {
			node.tSafeReplica = oldTSReplica
		}()
		node.tSafeReplica = mockTSReplica
		mockTSReplica.On("addTSafe", mock.Anything).Run(func(_ mock.Arguments) {
			node.ShardClusterService.releaseShardCluster("1001-dmc0")
		})
		schema := genTestCollectionSchema()
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: node.GetSession().ServerID,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       schema,
			Infos: []*datapb.VchannelInfo{
				{
					CollectionID: 1001,
					ChannelName:  "1001-dmc0",
				},
			},
		}

		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})
}

func TestImpl_UnsubDmChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	t.Run("normal run", func(t *testing.T) {
		schema := genTestCollectionSchema()
		req := &queryPb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: node.GetSession().ServerID,
			},
			NodeID:       0,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			Schema:       schema,
			Infos: []*datapb.VchannelInfo{
				{
					CollectionID: 1000,
					ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-dmc0",
				},
			},
		}

		status, err := node.WatchDmChannels(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

		{
			req := &queryPb.UnsubDmChannelRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_UnsubDmChannel,
					MsgID:    rand.Int63(),
					TargetID: node.GetSession().ServerID,
				},
				NodeID:       0,
				CollectionID: defaultCollectionID,
				ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-dmc0",
			}
			originMetaReplica := node.metaReplica
			node.metaReplica = newMockReplicaInterface()
			status, err := node.UnsubDmChannel(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

			node.metaReplica = originMetaReplica
			status, err = node.UnsubDmChannel(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		}
	})

	t.Run("target not match", func(t *testing.T) {
		req := &queryPb.UnsubDmChannelRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_UnsubDmChannel,
				MsgID:    rand.Int63(),
				TargetID: -1,
			},
		}
		status, err := node.UnsubDmChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, status.ErrorCode)
	})

	t.Run("server unhealthy", func(t *testing.T) {
		req := &queryPb.UnsubDmChannelRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_UnsubDmChannel,
				MsgID:   rand.Int63(),
			},
		}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		status, err := node.UnsubDmChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})
}

func TestImpl_LoadSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	schema := genTestCollectionSchema()

	req := &queryPb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchQueryChannels,
			MsgID:    rand.Int63(),
			TargetID: node.GetSession().ServerID,
		},
		DstNodeID: 0,
		Schema:    schema,
	}

	t.Run("normal run", func(t *testing.T) {
		status, err := node.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("target not match", func(t *testing.T) {
		req := &queryPb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchQueryChannels,
				MsgID:    rand.Int63(),
				TargetID: -1,
			},
		}
		status, err := node.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, status.ErrorCode)
	})

	t.Run("server unhealthy", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		defer node.UpdateStateCode(commonpb.StateCode_Healthy)
		status, err := node.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})

	t.Run("server stopping", func(t *testing.T) {
		node.UpdateStateCode(commonpb.StateCode_Stopping)
		defer node.UpdateStateCode(commonpb.StateCode_Healthy)
		status, err := node.LoadSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
	})
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

	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = node.ReleaseCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func TestImpl_LoadPartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			TargetID: paramtable.GetNodeID(),
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	status, err := node.LoadPartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = node.LoadPartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

	node.UpdateStateCode(commonpb.StateCode_Healthy)
	req.Base.TargetID = -1
	status, err = node.LoadPartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, status.ErrorCode)
}

func TestImpl_ReleasePartitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	req := &queryPb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchQueryChannels,
			MsgID:    rand.Int63(),
			TargetID: paramtable.GetNodeID(),
		},
		NodeID:       0,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}

	status, err := node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

	node.UpdateStateCode(commonpb.StateCode_Healthy)
	req.Base.TargetID = -1
	status, err = node.ReleasePartitions(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, status.ErrorCode)
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

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
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

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		rsp, err = node.GetSegmentInfo(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, rsp.Status.ErrorCode)
	})
}

func TestImpl_ShowConfigurations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()

	t.Run("test ShowConfigurations", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.SetSession(sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli))

		pattern := "Cache"
		req := &internalpb.ShowConfigurationsRequest{
			Base:    genCommonMsgBase(commonpb.MsgType_WatchQueryChannels, node.GetSession().ServerID),
			Pattern: pattern,
		}

		resp, err := node.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("test ShowConfigurations node failed", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.SetSession(sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli))
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		pattern := "Cache"
		req := &internalpb.ShowConfigurationsRequest{
			Base:    genCommonMsgBase(commonpb.MsgType_WatchQueryChannels, node.GetSession().ServerID),
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

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("test GetMetrics", func(t *testing.T) {
		defer wg.Done()
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		node.SetSession(sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli))

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

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		_, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})
	wg.Wait()
}

func TestImpl_ReleaseSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test valid", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments, node.GetSession().ServerID),
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

	t.Run("test invalid query node", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments, node.GetSession().ServerID),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("test target not matched", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments, -1),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		resp, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, resp.GetErrorCode())
	})

	t.Run("test segment not exists", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments, node.GetSession().ServerID),
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}

		node.metaReplica.removeSegment(defaultSegmentID, segmentTypeSealed)

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	t.Run("test no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()
		err = node.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		req := &queryPb.ReleaseSegmentsRequest{
			Base:         genCommonMsgBase(commonpb.MsgType_ReleaseSegments, node.GetSession().ServerID),
			CollectionID: defaultCollectionID,
		}

		status, err := node.ReleaseSegments(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})
}

func TestImpl_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)
	defer node.Stop()

	schema := genTestCollectionSchema()
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
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
	sc.SetupFirstVersion()

	_, err = node.Search(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)

	req.GetBase().TargetID = -1
	ret, err := node.Search(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, ret.GetStatus().GetErrorCode())
}

func TestImpl_searchWithDmlChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)
	defer node.Stop()

	schema := genTestCollectionSchema()
	req, err := genSearchRequest(defaultNQ, IndexFaissIDMap, schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SetupFirstVersion()

	_, err = node.searchWithDmlChannel(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)
	// search with ignore growing segment
	req.IgnoreGrowing = true
	_, err = node.searchWithDmlChannel(ctx, &queryPb.SearchRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)
	req.IgnoreGrowing = false

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
	require.NoError(t, err)
	defer node.Stop()

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
	require.NoError(t, err)
	defer node.Stop()

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
	require.NoError(t, err)
	defer node.Stop()

	schema := genTestCollectionSchema()
	req, err := genRetrieveRequest(schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
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
	sc.SetupFirstVersion()

	_, err = node.Query(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)

	req.GetBase().TargetID = -1
	ret, err := node.Query(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, ret.GetStatus().GetErrorCode())
}

func TestImpl_queryWithDmlChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	require.NoError(t, err)
	defer node.Stop()

	schema := genTestCollectionSchema()
	req, err := genRetrieveRequest(schema)
	require.NoError(t, err)

	node.queryShardService.addQueryShard(defaultCollectionID, defaultDMLChannel, defaultReplicaID)
	node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
	sc, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	sc.SetupFirstVersion()

	_, err = node.queryWithDmlChannel(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)

	// query with ignore growing
	req.IgnoreGrowing = true
	_, err = node.queryWithDmlChannel(ctx, &queryPb.QueryRequest{
		Req:             req,
		FromShardLeader: false,
		DmlChannels:     []string{defaultDMLChannel},
	}, defaultDMLChannel)
	assert.NoError(t, err)
	req.IgnoreGrowing = false

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
		require.NoError(t, err)
		defer node.Stop()

		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("Sync non-exist channel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{
			VchannelName: defaultDMLChannel,
			ReplicaSegments: []*queryPb.ReplicaSegmentsInfo{
				{
					NodeId:      1,
					PartitionId: defaultPartitionID,
					SegmentIds:  []int64{1},
					Versions:    []int64{1},
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
		require.NoError(t, err)
		defer node.Stop()

		node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
		cs, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		cs.SetupFirstVersion()

		resp, err := node.SyncReplicaSegments(ctx, &querypb.SyncReplicaSegmentsRequest{
			VchannelName: defaultDMLChannel,
			ReplicaSegments: []*queryPb.ReplicaSegmentsInfo{
				{
					NodeId:      1,
					PartitionId: defaultPartitionID,
					SegmentIds:  []int64{1},
					Versions:    []int64{1},
				},
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		t.Log(resp.GetReason())

		segment, ok := cs.getSegment(1)
		require.True(t, ok)
		assert.Equal(t, common.InvalidNodeID, segment.nodeID)
		assert.Equal(t, defaultPartitionID, segment.partitionID)
		assert.Equal(t, segmentStateLoaded, segment.state)

	})
}

func TestSyncDistribution(t *testing.T) {
	t.Run("QueryNode not healthy", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("Target not match", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		resp, err := node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
			Base:         &commonpb.MsgBase{TargetID: -1},
			CollectionID: defaultCollectionID,
			Channel:      defaultDMLChannel,
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: defaultPartitionID,
					SegmentID:   defaultSegmentID,
					NodeID:      99,
				},
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, resp.GetErrorCode())
	})

	t.Run("Sync non-exist channel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		resp, err := node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
			Base:         &commonpb.MsgBase{TargetID: node.GetSession().ServerID},
			CollectionID: defaultCollectionID,
			Channel:      defaultDMLChannel,
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: defaultPartitionID,
					SegmentID:   defaultSegmentID,
					NodeID:      99,
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
		require.NoError(t, err)
		defer node.Stop()

		node.ShardClusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
		cs, ok := node.ShardClusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		cs.SetupFirstVersion()

		resp, err := node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
			Base:         &commonpb.MsgBase{TargetID: node.GetSession().ServerID},
			CollectionID: defaultCollectionID,
			Channel:      defaultDMLChannel,
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: defaultPartitionID,
					SegmentID:   defaultSegmentID,
					NodeID:      99,
					Version:     1,
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

		segment, ok := cs.getSegment(defaultSegmentID)
		require.True(t, ok)
		assert.Equal(t, common.InvalidNodeID, segment.nodeID)
		assert.Equal(t, defaultPartitionID, segment.partitionID)
		assert.Equal(t, segmentStateLoaded, segment.state)
		assert.EqualValues(t, 1, segment.version)
		resp, err = node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
			Base:         &commonpb.MsgBase{TargetID: node.GetSession().ServerID},
			CollectionID: defaultCollectionID,
			Channel:      defaultDMLChannel,
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Remove,
					PartitionID: defaultPartitionID,
					SegmentID:   defaultSegmentID,
					NodeID:      99,
					Version:     1,
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

		cs, ok = node.ShardClusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		_, ok = cs.getSegment(defaultSegmentID)
		require.False(t, ok)
	})
}

func TestGetDataDistribution(t *testing.T) {
	t.Run("QueryNode not healthy", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		resp, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("Target not match", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		node, err := genSimpleQueryNode(ctx)
		require.NoError(t, err)
		defer node.Stop()

		resp, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{
			Base: &commonpb.MsgBase{TargetID: -1},
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NodeIDNotMatch, resp.GetStatus().GetErrorCode())
	})
}
