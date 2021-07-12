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
	"fmt"
	"testing"

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
			for _, c := range node.info.GetChannels() {
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

func TestClusterCreate(t *testing.T) {
	ch := make(chan interface{})
	kv := memkv.NewMemoryKV()
	spyClusterStore := &SpyClusterStore{
		NodesInfo: NewNodesInfo(),
		ch:        ch,
	}
	cluster, err := NewCluster(context.TODO(), kv, spyClusterStore, dummyPosProvider{})
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
	assert.EqualValues(t, "localhost:8080", dataNodes[0].info.GetAddress())
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
	assert.EqualValues(t, "localhost:8080", dataNodes[0].info.GetAddress())
}

func TestUnregister(t *testing.T) {
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
	assert.EqualValues(t, "localhost:8080", dataNodes[0].info.GetAddress())
	cluster.UnRegister(nodes[0])
	<-ch
	dataNodes = cluster.GetNodes()
	assert.EqualValues(t, 0, len(dataNodes))
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
	assert.EqualValues(t, 1, len(dataNodes[0].info.GetChannels()))
	assert.EqualValues(t, chName, dataNodes[0].info.Channels[0].Name)
	cluster.Watch(chName, 0)
	<-pch
}
