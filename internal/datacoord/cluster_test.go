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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"stathat.com/c/consistent"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
)

func getMetaKv(t *testing.T) kv.MetaKv {
	rootPath := "/etcd/test/root/" + t.Name()
	kv, err := etcdkv.NewMetaKvFactory(rootPath, &Params.EtcdCfg)
	require.NoError(t, err)

	return kv
}

func getWatchKV(t *testing.T) kv.WatchKV {
	rootPath := "/etcd/test/root/" + t.Name()
	kv, err := etcdkv.NewWatchKVFactory(rootPath, &Params.EtcdCfg)
	require.NoError(t, err)

	return kv
}

func TestClusterCreate(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("startup normally", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)
		dataNodes := sessionManager.GetSessions()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "localhost:8080", dataNodes[0].info.Address)
	})

	t.Run("startup with existed channel data", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		var err error
		info1 := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel1",
			},
		}
		info1Data, err := proto.Marshal(info1)
		assert.NoError(t, err)
		err = kv.Save(Params.CommonCfg.DataCoordWatchSubPath.GetValue()+"/1/channel1", string(info1Data))
		assert.NoError(t, err)

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Startup(ctx, []*NodeInfo{{NodeID: 1, Address: "localhost:9999"}})
		assert.NoError(t, err)

		channels := channelManager.GetChannels()
		assert.EqualValues(t, []*NodeChannelInfo{{1, []*channel{{Name: "channel1", CollectionID: 1}}}}, channels)
	})

	t.Run("remove all nodes and restart with other nodes", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)

		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)

		err = cluster.UnRegister(info)
		assert.NoError(t, err)
		sessions := sessionManager.GetSessions()
		assert.Empty(t, sessions)

		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		clusterReload := NewCluster(sessionManager2, channelManager2)
		defer clusterReload.Close()

		addr = "localhost:8081"
		info = &NodeInfo{
			NodeID:  2,
			Address: addr,
		}
		nodes = []*NodeInfo{info}
		err = clusterReload.Startup(ctx, nodes)
		assert.NoError(t, err)
		sessions = sessionManager2.GetSessions()
		assert.EqualValues(t, 1, len(sessions))
		assert.EqualValues(t, 2, sessions[0].info.NodeID)
		assert.EqualValues(t, addr, sessions[0].info.Address)
		channels := channelManager2.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, 2, channels[0].NodeID)
	})

	t.Run("loadKv Fails", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		metakv := mocks.NewWatchKV(t)
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("failed"))
		_, err := NewChannelManager(metakv, newMockHandler())
		assert.Error(t, err)
	})
}

func TestRegister(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("register to empty cluster", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		assert.NoError(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.NoError(t, err)
		sessions := sessionManager.GetSessions()
		assert.EqualValues(t, 1, len(sessions))
		assert.EqualValues(t, "localhost:8080", sessions[0].info.Address)
	})

	t.Run("register to empty cluster with buffer channels", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		err = channelManager.Watch(&channel{
			Name:         "ch1",
			CollectionID: 0,
		})
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		assert.NoError(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.NoError(t, err)
		bufferChannels := channelManager.GetBufferChannels()
		assert.Empty(t, bufferChannels.Channels)
		nodeChannels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(nodeChannels))
		assert.EqualValues(t, 1, nodeChannels[0].NodeID)
		assert.EqualValues(t, "ch1", nodeChannels[0].Channels[0].Name)
	})

	t.Run("register and restart with no channel", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		assert.NoError(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.NoError(t, err)
		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		restartCluster := NewCluster(sessionManager2, channelManager2)
		defer restartCluster.Close()
		channels := channelManager2.GetChannels()
		assert.Empty(t, channels)
	})
}

func TestUnregister(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("remove node after unregister", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)
		err = cluster.UnRegister(nodes[0])
		assert.NoError(t, err)
		sessions := sessionManager.GetSessions()
		assert.Empty(t, sessions)
	})

	t.Run("move channels to online nodes after unregister", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		nodeInfo1 := &NodeInfo{
			Address: "localhost:8080",
			NodeID:  1,
		}
		nodeInfo2 := &NodeInfo{
			Address: "localhost:8081",
			NodeID:  2,
		}
		nodes := []*NodeInfo{nodeInfo1, nodeInfo2}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)
		err = cluster.Watch("ch1", 1)
		assert.NoError(t, err)
		err = cluster.UnRegister(nodeInfo1)
		assert.NoError(t, err)

		channels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, 2, channels[0].NodeID)
		assert.EqualValues(t, 1, len(channels[0].Channels))
		assert.EqualValues(t, "ch1", channels[0].Channels[0].Name)
	})

	t.Run("remove all channels after unregsiter", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		var mockSessionCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
			return newMockDataNodeClient(1, nil)
		}
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		nodeInfo := &NodeInfo{
			Address: "localhost:8080",
			NodeID:  1,
		}
		err = cluster.Startup(ctx, []*NodeInfo{nodeInfo})
		assert.NoError(t, err)
		err = cluster.Watch("ch_1", 1)
		assert.NoError(t, err)
		err = cluster.UnRegister(nodeInfo)
		assert.NoError(t, err)
		channels := channelManager.GetChannels()
		assert.Empty(t, channels)
		channel := channelManager.GetBufferChannels()
		assert.NotNil(t, channel)
		assert.EqualValues(t, 1, len(channel.Channels))
		assert.EqualValues(t, "ch_1", channel.Channels[0].Name)
	})
}

func TestWatchIfNeeded(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("add deplicated channel to cluster", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		var mockSessionCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
			return newMockDataNodeClient(1, nil)
		}
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}

		err = cluster.Startup(ctx, []*NodeInfo{info})
		assert.NoError(t, err)
		err = cluster.Watch("ch1", 1)
		assert.NoError(t, err)
		channels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, "ch1", channels[0].Channels[0].Name)
	})

	t.Run("watch channel to empty cluster", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Watch("ch1", 1)
		assert.NoError(t, err)

		channels := channelManager.GetChannels()
		assert.Empty(t, channels)
		channel := channelManager.GetBufferChannels()
		assert.NotNil(t, channel)
		assert.EqualValues(t, "ch1", channel.Channels[0].Name)
	})
}

func TestConsistentHashPolicy(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	sessionManager := NewSessionManager()
	chash := consistent.New()
	factory := NewConsistentHashChannelPolicyFactory(chash)
	channelManager, err := NewChannelManager(kv, newMockHandler(), withFactory(factory))
	assert.NoError(t, err)
	cluster := NewCluster(sessionManager, channelManager)
	defer cluster.Close()

	hash := consistent.New()
	hash.Add("1")
	hash.Add("2")
	hash.Add("3")

	nodeInfo1 := &NodeInfo{
		NodeID:  1,
		Address: "localhost:1111",
	}
	nodeInfo2 := &NodeInfo{
		NodeID:  2,
		Address: "localhost:2222",
	}
	nodeInfo3 := &NodeInfo{
		NodeID:  3,
		Address: "localhost:3333",
	}
	err = cluster.Register(nodeInfo1)
	assert.NoError(t, err)
	err = cluster.Register(nodeInfo2)
	assert.NoError(t, err)
	err = cluster.Register(nodeInfo3)
	assert.NoError(t, err)

	channels := []string{"ch1", "ch2", "ch3"}
	for _, c := range channels {
		err = cluster.Watch(c, 1)
		assert.NoError(t, err)
		idstr, err := hash.Get(c)
		assert.NoError(t, err)
		id, err := deformatNodeID(idstr)
		assert.NoError(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("1")
	err = cluster.UnRegister(nodeInfo1)
	assert.NoError(t, err)
	for _, c := range channels {
		idstr, err := hash.Get(c)
		assert.NoError(t, err)
		id, err := deformatNodeID(idstr)
		assert.NoError(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("2")
	err = cluster.UnRegister(nodeInfo2)
	assert.NoError(t, err)
	for _, c := range channels {
		idstr, err := hash.Get(c)
		assert.NoError(t, err)
		id, err := deformatNodeID(idstr)
		assert.NoError(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("3")
	err = cluster.UnRegister(nodeInfo3)
	assert.NoError(t, err)
	bufferChannels := channelManager.GetBufferChannels()
	assert.EqualValues(t, 3, len(bufferChannels.Channels))
}

func TestCluster_Flush(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	sessionManager := NewSessionManager()
	channelManager, err := NewChannelManager(kv, newMockHandler())
	assert.NoError(t, err)
	cluster := NewCluster(sessionManager, channelManager)
	defer cluster.Close()
	addr := "localhost:8080"
	info := &NodeInfo{
		Address: addr,
		NodeID:  1,
	}
	nodes := []*NodeInfo{info}
	err = cluster.Startup(ctx, nodes)
	assert.NoError(t, err)

	err = cluster.Watch("chan-1", 1)
	assert.NoError(t, err)

	// flush empty should impact nothing
	assert.NotPanics(t, func() {
		err := cluster.Flush(context.Background(), 1, "chan-1", []*datapb.SegmentInfo{})
		assert.NoError(t, err)
	})

	// flush not watched channel
	assert.NotPanics(t, func() {
		err := cluster.Flush(context.Background(), 1, "chan-2", []*datapb.SegmentInfo{{ID: 1}})
		assert.Error(t, err)
	})

	// flush from wrong datanode
	assert.NotPanics(t, func() {
		err := cluster.Flush(context.Background(), 2, "chan-1", []*datapb.SegmentInfo{{ID: 1}})
		assert.Error(t, err)
	})

	//TODO add a method to verify datanode has flush request after client injection is available
}

func TestCluster_Import(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	sessionManager := NewSessionManager()
	channelManager, err := NewChannelManager(kv, newMockHandler())
	assert.NoError(t, err)
	cluster := NewCluster(sessionManager, channelManager)
	defer cluster.Close()
	addr := "localhost:8080"
	info := &NodeInfo{
		Address: addr,
		NodeID:  1,
	}
	nodes := []*NodeInfo{info}
	err = cluster.Startup(ctx, nodes)
	assert.NoError(t, err)

	err = cluster.Watch("chan-1", 1)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		cluster.Import(ctx, 1, &datapb.ImportTaskRequest{})
	})
	time.Sleep(500 * time.Millisecond)
}

func TestCluster_ReCollectSegmentStats(t *testing.T) {
	kv := getWatchKV(t)
	defer func() {
		kv.RemoveWithPrefix("")
		kv.Close()
	}()

	t.Run("recollect succeed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		var mockSessionCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
			return newMockDataNodeClient(1, nil)
		}
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)

		err = cluster.Watch("chan-1", 1)
		assert.NoError(t, err)

		assert.NotPanics(t, func() {
			cluster.ReCollectSegmentStats(ctx)
		})
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("recollect failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		assert.NoError(t, err)

		err = cluster.Watch("chan-1", 1)
		assert.NoError(t, err)

		assert.NotPanics(t, func() {
			cluster.ReCollectSegmentStats(ctx)
		})
		time.Sleep(500 * time.Millisecond)
	})
}
