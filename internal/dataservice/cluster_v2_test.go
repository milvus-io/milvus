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
package dataservice

import (
	"testing"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestClusterCreate(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	cluster := createCluster(t, withStartupPolicy(cPolicy))
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
	dataNodes := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
}

func TestRegister(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	registerPolicy := newDoNothingRegisterPolicy()
	cluster := createCluster(t, withStartupPolicy(cPolicy), withRegisterPolicy(registerPolicy))
	addr := "localhost:8080"

	err := cluster.startup(nil)
	assert.Nil(t, err)
	cluster.register(&datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	})
	dataNodes := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
}

func TestUnregister(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	unregisterPolicy := newDoNothingUnregisterPolicy()
	cluster := createCluster(t, withStartupPolicy(cPolicy), withUnregistorPolicy(unregisterPolicy))
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
	dataNodes := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
	cluster.unregister(&datapb.DataNodeInfo{
		Address:  addr,
		Version:  1,
		Channels: []*datapb.ChannelStatus{},
	})
	dataNodes = cluster.dataManager.getDataNodes(false)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, offline, cluster.dataManager.dataNodes[addr].status)
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)
}

func TestWatchIfNeeded(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	cluster := createCluster(t, withStartupPolicy(cPolicy))
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
	dataNodes := cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes))
	assert.EqualValues(t, "localhost:8080", dataNodes[addr].Address)

	chName := "ch1"
	cluster.watchIfNeeded(chName)
	dataNodes = cluster.dataManager.getDataNodes(true)
	assert.EqualValues(t, 1, len(dataNodes[addr].Channels))
	assert.EqualValues(t, chName, dataNodes[addr].Channels[0].Name)
	cluster.watchIfNeeded(chName)
	assert.EqualValues(t, 1, len(dataNodes[addr].Channels))
	assert.EqualValues(t, chName, dataNodes[addr].Channels[0].Name)
}

func TestFlushSegments(t *testing.T) {
	cPolicy := newMockStartupPolicy()
	cluster := createCluster(t, withStartupPolicy(cPolicy))
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
	segments := []*datapb.SegmentInfo{
		{
			ID:            0,
			CollectionID:  0,
			InsertChannel: "ch1",
		},
	}

	cluster.flush(segments)
}

func createCluster(t *testing.T, options ...clusterOption) *cluster {
	kv := memkv.NewMemoryKV()
	sessionManager := newMockSessionManager()
	dataManager, err := newClusterNodeManager(kv)
	assert.Nil(t, err)
	return newCluster(dataManager, sessionManager, options...)
}
