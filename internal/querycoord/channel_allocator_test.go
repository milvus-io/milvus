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
	"errors"
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

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
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
	var cluster Cluster = &queryNodeCluster{
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

	node, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	nodeSession := node.session
	nodeID := node.queryNodeID
	cluster.RegisterNode(baseCtx, nodeSession, nodeID, disConnect)
	waitQueryNodeOnline(cluster, nodeID)

	err = shuffleChannelsToQueryNode(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
	assert.Nil(t, err)

	assert.Equal(t, nodeID, firstReq.NodeID)
	assert.Equal(t, nodeID, secondReq.NodeID)
	t.Run("shuffeChannelsToQueryNode no online node ctx done", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = shuffleChannelsToQueryNode(ctx, reqs, cluster, meta, true, []int64{nodeID}, nil, -1)
		assert.Error(t, err)

		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("shuffeChannelsToQueryNode no online node ctx done", func(t *testing.T) {
		cluster.StopNode(nodeID)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = shuffleChannelsToQueryNode(ctx, reqs, cluster, meta, true, nil, nil, -1)
		assert.Error(t, err)

		assert.True(t, errors.Is(err, context.Canceled))
	})

	err = removeAllSession()
	assert.Nil(t, err)
}
