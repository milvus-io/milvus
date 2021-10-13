// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querycoord

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestQueryNodeCluster_getMetrics(t *testing.T) {
	log.Info("TestQueryNodeCluster_getMetrics, todo")
}

func TestReloadClusterFromKV(t *testing.T) {
	t.Run("Test LoadOnlineNodes", func(t *testing.T) {
		refreshParams()
		baseCtx := context.Background()
		kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		assert.Nil(t, err)
		clusterSession := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
		clusterSession.Init(typeutil.QueryCoordRole, Params.Address, true)
		cluster := &queryNodeCluster{
			ctx:       baseCtx,
			client:    kv,
			nodes:     make(map[int64]Node),
			newNodeFn: newQueryNodeTest,
			session:   clusterSession,
		}

		queryNode, err := startQueryNodeServer(baseCtx)
		assert.Nil(t, err)

		cluster.reloadFromKV()

		nodeID := queryNode.queryNodeID
		for {
			_, err = cluster.getNodeByID(nodeID)
			if err == nil {
				break
			}
		}
		queryNode.stop()
		err = removeNodeSession(queryNode.queryNodeID)
		assert.Nil(t, err)
	})

	t.Run("Test LoadOfflineNodes", func(t *testing.T) {
		refreshParams()
		kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		assert.Nil(t, err)
		clusterSession := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
		clusterSession.Init(typeutil.QueryCoordRole, Params.Address, true)
		cluster := &queryNodeCluster{
			client:    kv,
			nodes:     make(map[int64]Node),
			newNodeFn: newQueryNodeTest,
			session:   clusterSession,
		}

		kvs := make(map[string]string)
		session := &sessionutil.Session{
			ServerID: 100,
			Address:  "localhost",
		}
		sessionBlob, err := json.Marshal(session)
		assert.Nil(t, err)
		sessionKey := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, 100)
		kvs[sessionKey] = string(sessionBlob)

		collectionInfo := &querypb.CollectionInfo{
			CollectionID: defaultCollectionID,
		}
		collectionBlobs, err := proto.Marshal(collectionInfo)
		assert.Nil(t, err)
		nodeKey := fmt.Sprintf("%s/%d", queryNodeMetaPrefix, 100)
		kvs[nodeKey] = string(collectionBlobs)

		err = kv.MultiSave(kvs)
		assert.Nil(t, err)

		cluster.reloadFromKV()

		assert.Equal(t, 1, len(cluster.nodes))
		collection := cluster.getCollectionInfosByID(context.Background(), 100)
		assert.Equal(t, defaultCollectionID, collection[0].CollectionID)

		err = removeAllSession()
		assert.Nil(t, err)
	})
}

func TestGrpcRequest(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	clusterSession := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
	clusterSession.Init(typeutil.QueryCoordRole, Params.Address, true)
	meta, err := newMeta(kv)
	assert.Nil(t, err)
	cluster := &queryNodeCluster{
		ctx:         baseCtx,
		cancel:      cancel,
		client:      kv,
		clusterMeta: meta,
		nodes:       make(map[int64]Node),
		newNodeFn:   newQueryNodeTest,
		session:     clusterSession,
	}

	node, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	nodeSession := node.session
	nodeID := node.queryNodeID
	cluster.registerNode(baseCtx, nodeSession, nodeID, disConnect)
	waitQueryNodeOnline(cluster, nodeID)

	t.Run("Test GetComponentInfos", func(t *testing.T) {
		_, err := cluster.getComponentInfos(baseCtx)
		assert.Nil(t, err)
	})

	t.Run("Test LoadSegments", func(t *testing.T) {
		segmentLoadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    defaultSegmentID,
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
		}
		loadSegmentReq := &querypb.LoadSegmentsRequest{
			NodeID: nodeID,
			Infos:  []*querypb.SegmentLoadInfo{segmentLoadInfo},
		}
		err := cluster.loadSegments(baseCtx, nodeID, loadSegmentReq)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseSegments", func(t *testing.T) {
		releaseSegmentReq := &querypb.ReleaseSegmentsRequest{
			NodeID:       nodeID,
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
			SegmentIDs:   []UniqueID{defaultSegmentID},
		}
		err := cluster.releaseSegments(baseCtx, nodeID, releaseSegmentReq)
		assert.Nil(t, err)
	})

	t.Run("Test AddQueryChannel", func(t *testing.T) {
		reqChannel, resChannel, err := cluster.clusterMeta.getQueryChannel(defaultCollectionID)
		assert.Nil(t, err)
		addQueryChannelReq := &querypb.AddQueryChannelRequest{
			NodeID:           nodeID,
			CollectionID:     defaultCollectionID,
			RequestChannelID: reqChannel,
			ResultChannelID:  resChannel,
		}
		err = cluster.addQueryChannel(baseCtx, nodeID, addQueryChannelReq)
		assert.Nil(t, err)
	})

	t.Run("Test RemoveQueryChannel", func(t *testing.T) {
		reqChannel, resChannel, err := cluster.clusterMeta.getQueryChannel(defaultCollectionID)
		assert.Nil(t, err)
		removeQueryChannelReq := &querypb.RemoveQueryChannelRequest{
			NodeID:           nodeID,
			CollectionID:     defaultCollectionID,
			RequestChannelID: reqChannel,
			ResultChannelID:  resChannel,
		}
		err = cluster.removeQueryChannel(baseCtx, nodeID, removeQueryChannelReq)
		assert.Nil(t, err)
	})

	node.stop()
	err = removeAllSession()
	assert.Nil(t, err)
}
