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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

//func waitQueryNodeOnline(cluster *queryNodeCluster, nodeID int64)

func removeNodeSession(id int64) error {
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	if err != nil {
		return err
	}
	err = kv.Remove(fmt.Sprintf("session/"+typeutil.QueryNodeRole+"-%d", id))
	if err != nil {
		return err
	}
	return nil
}

func removeAllSession() error {
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	if err != nil {
		return err
	}
	err = kv.RemoveWithPrefix("session")
	if err != nil {
		return err
	}
	return nil
}

func waitAllQueryNodeOffline(cluster Cluster, nodes map[int64]Node) bool {
	reDoCount := 40
	for {
		if reDoCount <= 0 {
			return false
		}
		allOffline := true
		for nodeID := range nodes {
			_, err := cluster.getNodeByID(nodeID)
			if err == nil {
				allOffline = false
				break
			}
		}
		if allOffline {
			return true
		}
		log.Debug("wait all queryNode offline")
		time.Sleep(time.Second)
		reDoCount--
	}
}

func TestQueryNode_MultiNode_stop(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	queryNode5, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)
	queryNode1.stop()
	err = removeNodeSession(queryNode1.queryNodeID)
	assert.Nil(t, err)

	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	})
	time.Sleep(2 * time.Second)
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)
	nodes, err := queryCoord.cluster.onlineNodes()
	assert.Nil(t, err)
	queryNode5.stop()
	err = removeNodeSession(queryNode5.queryNodeID)
	assert.Nil(t, err)

	allNodeOffline := waitAllQueryNodeOffline(queryCoord.cluster, nodes)
	assert.Equal(t, allNodeOffline, true)
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

	time.Sleep(2 * time.Second)
	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	})
	queryNode1.stop()
	err = removeNodeSession(queryNode1.queryNodeID)
	assert.Nil(t, err)
	queryNode3, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	nodes, err := queryCoord.cluster.onlineNodes()
	assert.Nil(t, err)
	queryNode3.stop()
	err = removeNodeSession(queryNode3.queryNodeID)
	assert.Nil(t, err)

	allNodeOffline := waitAllQueryNodeOffline(queryCoord.cluster, nodes)
	assert.Equal(t, allNodeOffline, true)
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
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

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
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

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
