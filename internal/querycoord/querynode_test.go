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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

//func waitQueryNodeOnline(cluster *queryNodeCluster, nodeID int64)

func removeNodeSession(id int64) error {
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	if err != nil {
		return err
	}
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	err = kv.Remove(fmt.Sprintf("session/"+typeutil.QueryNodeRole+"-%d", id))
	if err != nil {
		return err
	}
	return nil
}

func removeAllSession() error {
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	if err != nil {
		return err
	}
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	err = kv.RemoveWithPrefix("session")
	if err != nil {
		return err
	}
	return nil
}

func waitAllQueryNodeOffline(cluster Cluster, nodeIDs []int64) bool {
	for {
		allOffline := true
		for _, nodeID := range nodeIDs {
			nodeExist := cluster.hasNode(nodeID)
			if nodeExist {
				allOffline = false
				break
			}
		}
		if allOffline {
			return true
		}
		log.Debug("wait all queryNode offline")
		time.Sleep(100 * time.Millisecond)
	}
}

func waitQueryNodeOnline(cluster Cluster, nodeID int64) {
	for {
		online, err := cluster.isOnline(nodeID)
		if err != nil {
			continue
		}
		if online {
			return
		}
	}
}

func TestQueryNode_MultiNode_stop(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode2.queryNodeID)

	queryNode1.stop()
	err = removeNodeSession(queryNode1.queryNodeID)
	assert.Nil(t, err)

	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	})
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	time.Sleep(100 * time.Millisecond)
	onlineNodeIDs := queryCoord.cluster.onlineNodeIDs()
	assert.NotEqual(t, 0, len(onlineNodeIDs))
	queryNode2.stop()
	err = removeNodeSession(queryNode2.queryNodeID)
	assert.Nil(t, err)

	waitAllQueryNodeOffline(queryCoord.cluster, onlineNodeIDs)
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestQueryNode_MultiNode_reStart(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)

	time.Sleep(100 * time.Millisecond)
	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	})
	queryNode1.stop()
	err = removeNodeSession(queryNode1.queryNodeID)
	assert.Nil(t, err)
	queryNode3, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	onlineNodeIDs := queryCoord.cluster.onlineNodeIDs()
	assert.NotEqual(t, 0, len(onlineNodeIDs))
	queryNode3.stop()
	err = removeNodeSession(queryNode3.queryNodeID)
	assert.Nil(t, err)

	waitAllQueryNodeOffline(queryCoord.cluster, onlineNodeIDs)
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestQueryNode_getMetrics(t *testing.T) {
	log.Info("TestQueryNode_getMetrics, todo")
}

func TestNewQueryNode(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	addr := queryNode1.session.Address
	nodeID := queryNode1.queryNodeID
	node, err := newQueryNode(baseCtx, addr, nodeID, kv)
	assert.Nil(t, err)

	err = node.start()
	assert.Nil(t, err)

	cancel()
	node.stop()
	queryNode1.stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestReleaseCollectionOnOfflineNode(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	node, err := newQueryNode(baseCtx, "test", 100, kv)
	assert.Nil(t, err)

	node.setState(offline)
	req := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}

	err = node.releaseCollection(baseCtx, req)
	assert.Nil(t, err)

	cancel()
}

func TestSealedSegmentChangeAfterQueryNodeStop(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode1.queryNodeID)

	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genDefaultCollectionSchema(false),
	})

	queryNode2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, queryNode2.queryNodeID)

	queryNode1.stop()
	err = removeNodeSession(queryNode1.queryNodeID)
	assert.Nil(t, err)

	for {
		segmentInfos := queryCoord.meta.showSegmentInfos(defaultCollectionID, nil)
		recoverDone := true
		for _, info := range segmentInfos {
			if info.NodeID != queryNode2.queryNodeID {
				recoverDone = false
				break
			}
		}
		if recoverDone {
			break
		}
	}

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestGrpcRequestWithNodeOffline(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	nodeServer, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	address := nodeServer.queryNodeIP
	nodeID := nodeServer.queryNodeID
	node, err := newQueryNodeTest(baseCtx, address, nodeID, kv)
	assert.Equal(t, false, node.isOnline())

	t.Run("Test WatchDmChannels", func(t *testing.T) {
		req := &querypb.WatchDmChannelsRequest{}
		err = node.watchDmChannels(baseCtx, req)
		assert.NotNil(t, err)
	})

	t.Run("Test AddQueryChannel", func(t *testing.T) {
		req := &querypb.AddQueryChannelRequest{}
		err = node.addQueryChannel(baseCtx, req)
		assert.NotNil(t, err)
	})

	t.Run("Test RemoveQueryChannel", func(t *testing.T) {
		req := &querypb.RemoveQueryChannelRequest{}
		err = node.removeQueryChannel(baseCtx, req)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		req := &querypb.ReleaseCollectionRequest{}
		err = node.releaseCollection(baseCtx, req)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		req := &querypb.ReleasePartitionsRequest{}
		err = node.releasePartitions(baseCtx, req)
		assert.Nil(t, err)
	})

	t.Run("Test getSegmentInfo", func(t *testing.T) {
		req := &querypb.GetSegmentInfoRequest{}
		res, err := node.getSegmentInfo(baseCtx, req)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test getComponentInfo", func(t *testing.T) {
		res := node.getComponentInfo(baseCtx)
		assert.Equal(t, internalpb.StateCode_Abnormal, res.StateCode)
	})

	t.Run("Test getMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{}
		res, err := node.getMetrics(baseCtx, req)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test LoadSegment", func(t *testing.T) {
		req := &querypb.LoadSegmentsRequest{}
		err = node.loadSegments(baseCtx, req)
		assert.NotNil(t, err)
	})

	t.Run("Test ReleaseSegments", func(t *testing.T) {
		req := &querypb.ReleaseSegmentsRequest{}
		err = node.releaseSegments(baseCtx, req)
		assert.NotNil(t, err)
	})

	t.Run("Test getNodeInfo", func(t *testing.T) {
		node, err = node.getNodeInfo()
		assert.NotNil(t, err)
		assert.Nil(t, node)
	})

	cancel()
	err = removeAllSession()
	assert.Nil(t, err)

}
