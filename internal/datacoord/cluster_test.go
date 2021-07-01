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
	"strings"
	"testing"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestClusterCreate(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	ch := make(chan struct{}, 1)
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), mockValidatorOption(ch))
	addr := "localhost:8080"
	nodes := []*datapb.DataNodeInfo{
		{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err := cluster.startup(nodes)
	assert.Nil(t, err)
	<-ch
	dataNodes, _ := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
}

func TestRegister(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	registerPolicy := newEmptyRegisterPolicy()
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), withRegisterPolicy(registerPolicy))
	addr := "localhost:8080"

	err := cluster.startup(nil)
	assert.Nil(t, err)
	cluster.register(&datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	})
	dataNodes, _ := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
}

func TestUnregister(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	unregisterPolicy := newEmptyUnregisterPolicy()
	ch := make(chan struct{}, 1)
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), withUnregistorPolicy(unregisterPolicy), mockValidatorOption(ch))
	addr := "localhost:8080"
	nodes := []*datapb.DataNodeInfo{
		{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err := cluster.startup(nodes)
	assert.Nil(t, err)
	<-ch
	dataNodes, _ := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
	cluster.unregister(&datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	})
	dataNodes, _ = cluster.dataManager.getDataNodes(false)
	assert.EqualValues(t, 0, len(dataNodes))
}

func TestRefresh(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	ch := make(chan struct{}, 1)
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), clusterOption{
		apply: func(c *cluster) {
			c.candidateManager.validate = func(dn *datapb.DataNodeInfo) error {
				if strings.Contains(dn.Address, "inv") {
					return errors.New("invalid dn")
				}
				return nil
			}
			c.candidateManager.enable = func(dn *datapb.DataNodeInfo) error {
				err := c.enableDataNode(dn)
				ch <- struct{}{}
				return err
			}
		},
	})
	addr := "localhost:8080"
	nodes := []*datapb.DataNodeInfo{
		{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
		{
			Address:  addr + "invalid",
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err := cluster.startup(nodes)
	assert.Nil(t, err)
	<-ch
	dataNodes, _ := cluster.dataManager.getDataNodes(true)
	if !assert.Equal(t, 1, len(dataNodes)) {
		t.FailNow()
	}
	assert.Equal(t, addr, dataNodes[addr].Address)
	addr2 := "localhost:8081"
	nodes = []*datapb.DataNodeInfo{
		{
			Address:  addr2,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
		{
			Address:  addr2 + "invalid",
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err = cluster.refresh(nodes)
	assert.Nil(t, err)
	<-ch
	dataNodes, _ = cluster.dataManager.getDataNodes(true)
	assert.Equal(t, 1, len(dataNodes))
	_, has := dataNodes[addr]
	assert.False(t, has)
	assert.Equal(t, addr2, dataNodes[addr2].Address)
}

func TestWatchIfNeeded(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	ch := make(chan struct{}, 1)
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), mockValidatorOption(ch))
	addr := "localhost:8080"
	nodes := []*datapb.DataNodeInfo{
		{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err := cluster.startup(nodes)
	assert.Nil(t, err)
	<-ch
	dataNodes, _ := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)

	chName := "ch1"
	cluster.watchIfNeeded(chName, 0)
	dataNodes, _ = cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes[addr].Channels))
	assert.EqualValues(t, chName, dataNodes[addr].Channels[0].Name)
	cluster.watchIfNeeded(chName, 0)
	assert.EqualValues(t, 1, len(dataNodes[addr].Channels))
	assert.EqualValues(t, chName, dataNodes[addr].Channels[0].Name)
}

func TestFlushSegments(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	ch := make(chan struct{}, 1)
	cluster := createCluster(t, nil, withStartupPolicy(cPolicy), mockValidatorOption(ch))
	addr := "localhost:8080"
	nodes := []*datapb.DataNodeInfo{
		{
			Address:  addr,
			Version:  1,
			Channels: []*datapb.ChannelStatus{},
		},
	}
	err := cluster.startup(nodes)
	assert.Nil(t, err)
	<-ch
	segments := []*datapb.SegmentInfo{
		{
			ID:            0,
			CollectionID:  0,
			InsertChannel: "ch1",
		},
	}

	cluster.flush(segments)
}

func mockValidatorOption(ch chan<- struct{}) clusterOption {
	return clusterOption{
		apply: func(c *cluster) {
			c.candidateManager.validate = func(dn *datapb.DataNodeInfo) error {
				return nil
			}
			c.candidateManager.enable = func(dn *datapb.DataNodeInfo) error {
				err := c.enableDataNode(dn)
				ch <- struct{}{}
				return err
			}
		},
	}
}

func createCluster(t *testing.T, ch chan interface{}, options ...clusterOption) *cluster {
	kv := memkv.NewMemoryKV()
	sessionManager := newMockSessionManager(ch)
	dataManager, err := newClusterNodeManager(kv)
	assert.Nil(t, err)
	return newCluster(context.TODO(), dataManager, sessionManager, dummyPosProvider{}, options...)
}
