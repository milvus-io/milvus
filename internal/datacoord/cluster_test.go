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

package datacoord

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"
)

func TestClusterCreate(t *testing.T) {
	t.Run("startup normally", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(nodes)
		assert.Nil(t, err)
		dataNodes := sessionManager.GetSessions()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "localhost:8080", dataNodes[0].info.Address)
	})

	t.Run("startup with existed channel data", func(t *testing.T) {
		Params.Init()
		var err error
		kv := memkv.NewMemoryKV()
		info1 := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel1",
			},
		}
		info1Data, err := proto.Marshal(info1)
		assert.Nil(t, err)
		err = kv.Save(Params.ChannelWatchSubPath+"/1/channel1", string(info1Data))
		assert.Nil(t, err)

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Startup([]*NodeInfo{{NodeID: 1, Address: "localhost:9999"}})
		assert.Nil(t, err)

		channels := channelManager.GetChannels()
		assert.EqualValues(t, []*NodeChannelInfo{{1, []*channel{{"channel1", 1}}}}, channels)
	})

	t.Run("remove all nodes and restart with other nodes", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)

		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(nodes)
		assert.Nil(t, err)

		err = cluster.UnRegister(info)
		assert.Nil(t, err)
		sessions := sessionManager.GetSessions()
		assert.Empty(t, sessions)

		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		clusterReload := NewCluster(sessionManager2, channelManager2)
		defer clusterReload.Close()

		addr = "localhost:8081"
		info = &NodeInfo{
			NodeID:  2,
			Address: addr,
		}
		nodes = []*NodeInfo{info}
		err = clusterReload.Startup(nodes)
		assert.Nil(t, err)
		sessions = sessionManager2.GetSessions()
		assert.EqualValues(t, 1, len(sessions))
		assert.EqualValues(t, 2, sessions[0].info.NodeID)
		assert.EqualValues(t, addr, sessions[0].info.Address)
		channels := channelManager2.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, 2, channels[0].NodeID)
	})

	t.Run("loadKv Fails", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		fkv := &loadPrefixFailKV{TxnKV: kv}
		_, err := NewChannelManager(fkv, dummyPosProvider{})
		assert.NotNil(t, err)
	})
}

// a mock kv that always fail when LoadWithPrefix
type loadPrefixFailKV struct {
	kv.TxnKV
}

// LoadWithPrefix override behavior
func (kv *loadPrefixFailKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return []string{}, []string{}, errors.New("mocked fail")
}

func TestRegister(t *testing.T) {
	t.Run("register to empty cluster", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(nil)
		assert.Nil(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.Nil(t, err)
		sessions := sessionManager.GetSessions()
		assert.EqualValues(t, 1, len(sessions))
		assert.EqualValues(t, "localhost:8080", sessions[0].info.Address)
	})

	t.Run("register to empty cluster with buffer channels", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		err = channelManager.Watch(&channel{
			name:         "ch1",
			collectionID: 0,
		})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(nil)
		assert.Nil(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.Nil(t, err)
		bufferChannels := channelManager.GetBuffer()
		assert.Empty(t, bufferChannels.Channels)
		nodeChannels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(nodeChannels))
		assert.EqualValues(t, 1, nodeChannels[0].NodeID)
		assert.EqualValues(t, "ch1", nodeChannels[0].Channels[0].name)
	})

	t.Run("register and restart with no channel", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		addr := "localhost:8080"
		err = cluster.Startup(nil)
		assert.Nil(t, err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		assert.Nil(t, err)
		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		restartCluster := NewCluster(sessionManager2, channelManager2)
		defer restartCluster.Close()
		channels := channelManager2.GetChannels()
		assert.Empty(t, channels)
	})
}

func TestUnregister(t *testing.T) {
	t.Run("remove node after unregister", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(nodes)
		assert.Nil(t, err)
		err = cluster.UnRegister(nodes[0])
		assert.Nil(t, err)
		sessions := sessionManager.GetSessions()
		assert.Empty(t, sessions)
	})

	t.Run("move channels to online nodes after unregister", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
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
		err = cluster.Startup(nodes)
		assert.Nil(t, err)
		err = cluster.Watch("ch1", 1)
		assert.Nil(t, err)
		err = cluster.UnRegister(nodeInfo1)
		assert.Nil(t, err)

		channels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, 2, channels[0].NodeID)
		assert.EqualValues(t, 1, len(channels[0].Channels))
		assert.EqualValues(t, "ch1", channels[0].Channels[0].name)
	})

	t.Run("remove all channels after unregsiter", func(t *testing.T) {
		var mockSessionCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
			return newMockDataNodeClient(1, nil)
		}
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		nodeInfo := &NodeInfo{
			Address: "localhost:8080",
			NodeID:  1,
		}
		err = cluster.Startup([]*NodeInfo{nodeInfo})
		assert.Nil(t, err)
		err = cluster.Watch("ch_1", 1)
		assert.Nil(t, err)
		err = cluster.UnRegister(nodeInfo)
		assert.Nil(t, err)
		channels := channelManager.GetChannels()
		assert.Empty(t, channels)
		channel := channelManager.GetBuffer()
		assert.NotNil(t, channel)
		assert.EqualValues(t, 1, len(channel.Channels))
		assert.EqualValues(t, "ch_1", channel.Channels[0].name)
	})
}

func TestWatchIfNeeded(t *testing.T) {
	t.Run("add deplicated channel to cluster", func(t *testing.T) {
		var mockSessionCreator = func(ctx context.Context, addr string) (types.DataNode, error) {
			return newMockDataNodeClient(1, nil)
		}
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}

		err = cluster.Startup([]*NodeInfo{info})
		assert.Nil(t, err)
		err = cluster.Watch("ch1", 1)
		assert.Nil(t, err)
		channels := channelManager.GetChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, "ch1", channels[0].Channels[0].name)
	})

	t.Run("watch channel to empty cluster", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, dummyPosProvider{})
		assert.Nil(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Watch("ch1", 1)
		assert.Nil(t, err)

		channels := channelManager.GetChannels()
		assert.Empty(t, channels)
		channel := channelManager.GetBuffer()
		assert.NotNil(t, channel)
		assert.EqualValues(t, "ch1", channel.Channels[0].name)
	})
}

func TestConsistentHashPolicy(t *testing.T) {
	kv := memkv.NewMemoryKV()
	sessionManager := NewSessionManager()
	chash := consistent.New()
	factory := NewConsistentHashChannelPolicyFactory(chash)
	channelManager, err := NewChannelManager(kv, dummyPosProvider{}, withFactory(factory))
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	err = cluster.Register(nodeInfo2)
	assert.Nil(t, err)
	err = cluster.Register(nodeInfo3)
	assert.Nil(t, err)

	channels := []string{"ch1", "ch2", "ch3"}
	for _, c := range channels {
		err = cluster.Watch(c, 1)
		assert.Nil(t, err)
		idstr, err := hash.Get(c)
		assert.Nil(t, err)
		id, err := deformatNodeID(idstr)
		assert.Nil(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("1")
	cluster.UnRegister(nodeInfo1)
	for _, c := range channels {
		idstr, err := hash.Get(c)
		assert.Nil(t, err)
		id, err := deformatNodeID(idstr)
		assert.Nil(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("2")
	cluster.UnRegister(nodeInfo2)
	for _, c := range channels {
		idstr, err := hash.Get(c)
		assert.Nil(t, err)
		id, err := deformatNodeID(idstr)
		assert.Nil(t, err)
		match := channelManager.Match(id, c)
		assert.True(t, match)
	}

	hash.Remove("3")
	cluster.UnRegister(nodeInfo3)
	bufferChannels := channelManager.GetBuffer()
	assert.EqualValues(t, 3, len(bufferChannels.Channels))
}
