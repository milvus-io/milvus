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
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestShuffleChannelsToQueryNode(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	clusterSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, etcdCli)
	clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
	clusterSession.Register()
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta, err := newMeta(baseCtx, kv, nil, idAllocator)
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

	firstReq := &querypb.WatchDmChannelsRequest{
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Infos: []*datapb.VchannelInfo{
			{
				ChannelName: "test1",
			},
		},
	}
	secondReq := &querypb.WatchDmChannelsRequest{
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Infos: []*datapb.VchannelInfo{
			{
				ChannelName: "test2",
			},
		},
	}
	reqs := []*querypb.WatchDmChannelsRequest{firstReq, secondReq}

	err = shuffleChannelsToQueryNode(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
	assert.NotNil(t, err)

	node, err := startQueryNodeServer(baseCtx, t)
	assert.Nil(t, err)
	nodeSession := node.session
	nodeID := node.queryNodeID
	cluster.registerNode(baseCtx, nodeSession, nodeID, disConnect)
	waitQueryNodeOnline(cluster, nodeID)

	err = shuffleChannelsToQueryNode(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
	assert.Nil(t, err)

	assert.Equal(t, nodeID, firstReq.NodeID)
	assert.Equal(t, nodeID, secondReq.NodeID)

	err = removeAllSession(t)
	assert.Nil(t, err)
}
