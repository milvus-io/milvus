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
	"github.com/stretchr/testify/suite"
	"stathat.com/c/consistent"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/testutils"
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

type ClusterSuite struct {
	testutils.PromMetricsSuite

	kv kv.WatchKV
}

func (suite *ClusterSuite) getWatchKV() kv.WatchKV {
	rootPath := "/etcd/test/root/" + suite.T().Name()
	kv, err := etcdkv.NewWatchKVFactory(rootPath, &Params.EtcdCfg)
	suite.Require().NoError(err)

	return kv
}

func (suite *ClusterSuite) SetupTest() {
	kv := getWatchKV(suite.T())
	suite.kv = kv
}

func (suite *ClusterSuite) TearDownTest() {
	if suite.kv != nil {
		suite.kv.RemoveWithPrefix("")
		suite.kv.Close()
	}
}

func (suite *ClusterSuite) TestCreate() {
	kv := suite.kv

	suite.Run("startup_normally", func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		suite.NoError(err)
		dataNodes := sessionManager.GetSessions()
		suite.EqualValues(1, len(dataNodes))
		suite.EqualValues("localhost:8080", dataNodes[0].info.Address)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
	})

	suite.Run("startup_with_existed_channel_data", func() {
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
		suite.NoError(err)
		err = kv.Save(Params.CommonCfg.DataCoordWatchSubPath.GetValue()+"/1/channel1", string(info1Data))
		suite.NoError(err)

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Startup(ctx, []*NodeInfo{{NodeID: 1, Address: "localhost:9999"}})
		suite.NoError(err)

		channels := channelManager.GetAssignedChannels()
		suite.EqualValues([]*NodeChannelInfo{{1, []*channel{{Name: "channel1", CollectionID: 1}}}}, channels)
	})

	suite.Run("remove_all_nodes_and_restart_with_other_nodes", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)

		addr := "localhost:8080"
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		suite.NoError(err)

		err = cluster.UnRegister(info)
		suite.NoError(err)
		sessions := sessionManager.GetSessions()
		suite.Empty(sessions)

		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		clusterReload := NewCluster(sessionManager2, channelManager2)
		defer clusterReload.Close()

		addr = "localhost:8081"
		info = &NodeInfo{
			NodeID:  2,
			Address: addr,
		}
		nodes = []*NodeInfo{info}
		err = clusterReload.Startup(ctx, nodes)
		suite.NoError(err)
		sessions = sessionManager2.GetSessions()
		suite.EqualValues(1, len(sessions))
		suite.EqualValues(2, sessions[0].info.NodeID)
		suite.EqualValues(addr, sessions[0].info.Address)
		channels := channelManager2.GetAssignedChannels()
		suite.EqualValues(1, len(channels))
		suite.EqualValues(2, channels[0].NodeID)
	})

	suite.Run("loadkv_fails", func() {
		defer kv.RemoveWithPrefix("")

		metakv := mocks.NewWatchKV(suite.T())
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("failed"))
		_, err := NewChannelManager(metakv, newMockHandler())
		suite.Error(err)
	})
}

func (suite *ClusterSuite) TestRegister() {
	kv := suite.kv

	suite.Run("register_to_empty_cluster", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		suite.NoError(err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		suite.NoError(err)
		sessions := sessionManager.GetSessions()
		suite.EqualValues(1, len(sessions))
		suite.EqualValues("localhost:8080", sessions[0].info.Address)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
	})

	suite.Run("register_to_empty_cluster_with_buffer_channels", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		err = channelManager.Watch(context.TODO(), &channel{
			Name:         "ch1",
			CollectionID: 0,
		})
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		suite.NoError(err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		suite.NoError(err)
		bufferChannels := channelManager.GetBufferChannels()
		suite.Empty(bufferChannels.Channels)
		nodeChannels := channelManager.GetAssignedChannels()
		suite.EqualValues(1, len(nodeChannels))
		suite.EqualValues(1, nodeChannels[0].NodeID)
		suite.EqualValues("ch1", nodeChannels[0].Channels[0].Name)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
	})

	suite.Run("register_and_restart_with_no_channel", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		addr := "localhost:8080"
		err = cluster.Startup(ctx, nil)
		suite.NoError(err)
		info := &NodeInfo{
			NodeID:  1,
			Address: addr,
		}
		err = cluster.Register(info)
		suite.NoError(err)
		cluster.Close()

		sessionManager2 := NewSessionManager()
		channelManager2, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		restartCluster := NewCluster(sessionManager2, channelManager2)
		defer restartCluster.Close()
		channels := channelManager2.GetAssignedChannels()
		suite.Empty(channels)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
	})
}

func (suite *ClusterSuite) TestUnregister() {
	kv := suite.kv

	suite.Run("remove_node_after_unregister", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &NodeInfo{
			Address: addr,
			NodeID:  1,
		}
		nodes := []*NodeInfo{info}
		err = cluster.Startup(ctx, nodes)
		suite.NoError(err)
		err = cluster.UnRegister(nodes[0])
		suite.NoError(err)
		sessions := sessionManager.GetSessions()
		suite.Empty(sessions)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 0)
	})

	suite.Run("move_channel_to_online_nodes_after_unregister", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
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
		suite.NoError(err)
		err = cluster.Watch(ctx, "ch1", 1)
		suite.NoError(err)
		err = cluster.UnRegister(nodeInfo1)
		suite.NoError(err)

		channels := channelManager.GetAssignedChannels()
		suite.EqualValues(1, len(channels))
		suite.EqualValues(2, channels[0].NodeID)
		suite.EqualValues(1, len(channels[0].Channels))
		suite.EqualValues("ch1", channels[0].Channels[0].Name)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 1)
	})

	suite.Run("remove_all_channels_after_unregsiter", func() {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		mockSessionCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return newMockDataNodeClient(1, nil)
		}
		sessionManager := NewSessionManager(withSessionCreator(mockSessionCreator))
		channelManager, err := NewChannelManager(kv, newMockHandler())
		suite.NoError(err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		nodeInfo := &NodeInfo{
			Address: "localhost:8080",
			NodeID:  1,
		}
		err = cluster.Startup(ctx, []*NodeInfo{nodeInfo})
		suite.NoError(err)
		err = cluster.Watch(ctx, "ch_1", 1)
		suite.NoError(err)
		err = cluster.UnRegister(nodeInfo)
		suite.NoError(err)
		channels := channelManager.GetAssignedChannels()
		suite.Empty(channels)
		channel := channelManager.GetBufferChannels()
		suite.NotNil(channel)
		suite.EqualValues(1, len(channel.Channels))
		suite.EqualValues("ch_1", channel.Channels[0].Name)

		suite.MetricsEqual(metrics.DataCoordNumDataNodes, 0)
	})
}

func TestCluster(t *testing.T) {
	suite.Run(t, new(ClusterSuite))
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

		mockSessionCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
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
		err = cluster.Watch(ctx, "ch1", 1)
		assert.NoError(t, err)
		channels := channelManager.GetAssignedChannels()
		assert.EqualValues(t, 1, len(channels))
		assert.EqualValues(t, "ch1", channels[0].Channels[0].Name)
	})

	t.Run("watch channel to empty cluster", func(t *testing.T) {
		defer kv.RemoveWithPrefix("")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sessionManager := NewSessionManager()
		channelManager, err := NewChannelManager(kv, newMockHandler())
		assert.NoError(t, err)
		cluster := NewCluster(sessionManager, channelManager)
		defer cluster.Close()

		err = cluster.Watch(ctx, "ch1", 1)
		assert.NoError(t, err)

		channels := channelManager.GetAssignedChannels()
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
		err = cluster.Watch(context.TODO(), c, 1)
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

	err = cluster.Watch(context.Background(), "chan-1", 1)
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

	// TODO add a method to verify datanode has flush request after client injection is available
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

	err = cluster.Watch(ctx, "chan-1", 1)
	assert.NoError(t, err)

	assert.NotPanics(t, func() {
		cluster.Import(ctx, 1, &datapb.ImportTaskRequest{})
	})
	time.Sleep(500 * time.Millisecond)
}
