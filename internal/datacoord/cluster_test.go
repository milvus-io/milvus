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
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

type SpyClusterStore struct {
	*NodesInfo
	ch chan interface{}
}

func (s *SpyClusterStore) SetNode(nodeID UniqueID, node *NodeInfo) {
	s.NodesInfo.SetNode(nodeID, node)
	s.ch <- struct{}{}
}

func (s *SpyClusterStore) DeleteNode(nodeID UniqueID) {
	s.NodesInfo.DeleteNode(nodeID)
	s.ch <- struct{}{}
}

func spyWatchPolicy(ch chan interface{}) channelAssignPolicy {
	return func(cluster []*NodeInfo, channel string, collectionID UniqueID) []*NodeInfo {
		for _, node := range cluster {
			for _, c := range node.Info.GetChannels() {
				if c.GetName() == channel && c.GetCollectionID() == collectionID {
					ch <- struct{}{}
					return nil
				}
			}
		}
		ret := make([]*NodeInfo, 0)
		c := &datapb.ChannelStatus{
			Name:         channel,
			State:        datapb.ChannelWatchState_Uncomplete,
			CollectionID: collectionID,
		}
		n := cluster[0].Clone(AddChannels([]*datapb.ChannelStatus{c}))
		ret = append(ret, n)
		return ret
	}
}

// a mock kv that always fail when LoadWithPrefix
type loadPrefixFailKV struct {
	kv.TxnKV
}

// LoadWithPrefix override behavior
func (kv *loadPrefixFailKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return []string{}, []string{}, errors.New("mocked fail")
}

func TestClusterCreate(t *testing.T) {
	ch := make(chan interface{})
	memKv := memkv.NewMemoryKV()
	spyClusterStore := &SpyClusterStore{
		NodesInfo: NewNodesInfo(),
		ch:        ch,
	}
	cluster, err := NewCluster(context.TODO(), memKv, spyClusterStore, dummyPosProvider{})
	assert.Nil(t, err)
	defer cluster.Close()
	addr := "localhost:8080"
	info := &datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	}
	nodes := []*NodeInfo{NewNodeInfo(context.TODO(), info)}
	cluster.Startup(nodes)
	<-ch
	dataNodes := cluster.GetNodes()
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[0].Info.GetAddress())

	t.Run("loadKv Fails", func(t *testing.T) {
		fkv := &loadPrefixFailKV{TxnKV: memKv}
		cluster, err := NewCluster(context.TODO(), fkv, spyClusterStore, dummyPosProvider{})
		assert.NotNil(t, err)
		assert.Nil(t, cluster)
	})
}

func TestRegister(t *testing.T) {
	registerPolicy := newEmptyRegisterPolicy()
	ch := make(chan interface{})
	kv := memkv.NewMemoryKV()
	spyClusterStore := &SpyClusterStore{
		NodesInfo: NewNodesInfo(),
		ch:        ch,
	}
	cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{}, withRegisterPolicy(registerPolicy))
	assert.Nil(t, err)
	defer cluster.Close()
	addr := "localhost:8080"

	cluster.Startup(nil)
	info := &datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	}
	node := NewNodeInfo(context.TODO(), info)
	cluster.Register(node)
	<-ch
	dataNodes := cluster.GetNodes()
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[0].Info.GetAddress())
}

func TestUnregister(t *testing.T) {
	t.Run("remove node after unregister", func(t *testing.T) {
		unregisterPolicy := newEmptyUnregisterPolicy()
		ch := make(chan interface{})
		kv := memkv.NewMemoryKV()
		spyClusterStore := &SpyClusterStore{
			NodesInfo: NewNodesInfo(),
			ch:        ch,
		}
		cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{}, withUnregistorPolicy(unregisterPolicy))
		assert.Nil(t, err)
		defer cluster.Close()
		addr := "localhost:8080"
		info := &datapb.DataNodeInfo{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		}
		nodes := []*NodeInfo{NewNodeInfo(context.TODO(), info)}
		cluster.Startup(nodes)
		<-ch
		dataNodes := cluster.GetNodes()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, "localhost:8080", dataNodes[0].Info.GetAddress())
		cluster.UnRegister(nodes[0])
		<-ch
		dataNodes = cluster.GetNodes()
		assert.EqualValues(t, 0, len(dataNodes))
	})

	t.Run("move channels to online nodes after unregister", func(t *testing.T) {
		ch := make(chan interface{})
		kv := memkv.NewMemoryKV()
		spyClusterStore := &SpyClusterStore{
			NodesInfo: NewNodesInfo(),
			ch:        ch,
		}
		cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{})
		assert.Nil(t, err)
		defer cluster.Close()
		ch1 := &datapb.ChannelStatus{
			Name:         "ch_1",
			State:        datapb.ChannelWatchState_Uncomplete,
			CollectionID: 100,
		}
		nodeInfo1 := &datapb.DataNodeInfo{
			Address:  "localhost:8080",
			Version:  1,
			Channels: []*datapb.ChannelStatus{ch1},
		}
		nodeInfo2 := &datapb.DataNodeInfo{
			Address:  "localhost:8081",
			Version:  2,
			Channels: []*datapb.ChannelStatus{},
		}
		node1 := NewNodeInfo(context.TODO(), nodeInfo1)
		node2 := NewNodeInfo(context.TODO(), nodeInfo2)
		cli1, err := newMockDataNodeClient(1, make(chan interface{}))
		assert.Nil(t, err)
		cli2, err := newMockDataNodeClient(2, make(chan interface{}))
		assert.Nil(t, err)
		node1.client = cli1
		node2.client = cli2
		nodes := []*NodeInfo{node1, node2}
		cluster.Startup(nodes)
		<-ch
		<-ch
		dataNodes := cluster.GetNodes()
		assert.EqualValues(t, 2, len(dataNodes))
		for _, node := range dataNodes {
			if node.Info.GetVersion() == 1 {
				cluster.UnRegister(node)
				<-ch
				<-ch
				break
			}
		}
		dataNodes = cluster.GetNodes()
		assert.EqualValues(t, 1, len(dataNodes))
		assert.EqualValues(t, 2, dataNodes[0].Info.GetVersion())
		assert.EqualValues(t, ch1.Name, dataNodes[0].Info.GetChannels()[0].Name)
	})

	t.Run("remove all channels after unregsiter", func(t *testing.T) {
		ch := make(chan interface{}, 10)
		kv := memkv.NewMemoryKV()
		spyClusterStore := &SpyClusterStore{
			NodesInfo: NewNodesInfo(),
			ch:        ch,
		}
		cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{})
		assert.Nil(t, err)
		defer cluster.Close()
		chstatus := &datapb.ChannelStatus{
			Name:         "ch_1",
			State:        datapb.ChannelWatchState_Uncomplete,
			CollectionID: 100,
		}
		nodeInfo := &datapb.DataNodeInfo{
			Address:  "localhost:8080",
			Version:  1,
			Channels: []*datapb.ChannelStatus{chstatus},
		}
		node := NewNodeInfo(context.TODO(), nodeInfo)
		cli, err := newMockDataNodeClient(1, make(chan interface{}))
		assert.Nil(t, err)
		node.client = cli
		cluster.Startup([]*NodeInfo{node})
		<-ch
		cluster.UnRegister(node)
		<-ch
		spyClusterStore2 := &SpyClusterStore{
			NodesInfo: NewNodesInfo(),
			ch:        ch,
		}
		cluster2, err := NewCluster(context.TODO(), kv, spyClusterStore2, dummyPosProvider{})
		<-ch
		assert.Nil(t, err)
		nodes := cluster2.GetNodes()
		assert.EqualValues(t, 1, len(nodes))
		assert.EqualValues(t, 1, nodes[0].Info.GetVersion())
		assert.EqualValues(t, 0, len(nodes[0].Info.GetChannels()))
	})
}

func TestWatchIfNeeded(t *testing.T) {
	ch := make(chan interface{})
	kv := memkv.NewMemoryKV()
	spyClusterStore := &SpyClusterStore{
		NodesInfo: NewNodesInfo(),
		ch:        ch,
	}

	pch := make(chan interface{})
	spyPolicy := spyWatchPolicy(pch)
	cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{}, withAssignPolicy(spyPolicy))
	assert.Nil(t, err)
	defer cluster.Close()
	addr := "localhost:8080"
	info := &datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	}
	node := NewNodeInfo(context.TODO(), info)
	node.client, err = newMockDataNodeClient(1, make(chan interface{}))
	assert.Nil(t, err)
	nodes := []*NodeInfo{node}
	cluster.Startup(nodes)
	fmt.Println("11111")
	<-ch
	chName := "ch1"
	cluster.Watch(chName, 0)
	fmt.Println("222")
	<-ch
	dataNodes := cluster.GetNodes()
	assert.EqualValues(t, 1, len(dataNodes[0].Info.GetChannels()))
	assert.EqualValues(t, chName, dataNodes[0].Info.Channels[0].Name)
	cluster.Watch(chName, 0)
	<-pch
}
