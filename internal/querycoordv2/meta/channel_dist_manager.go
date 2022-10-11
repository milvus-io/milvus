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

package meta

import (
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type DmChannel struct {
	*datapb.VchannelInfo
	Node    int64
	Version int64
}

func DmChannelFromVChannel(channel *datapb.VchannelInfo) *DmChannel {
	return &DmChannel{
		VchannelInfo: channel,
	}
}

func (channel *DmChannel) Clone() *DmChannel {
	return &DmChannel{
		VchannelInfo: proto.Clone(channel.VchannelInfo).(*datapb.VchannelInfo),
		Node:         channel.Node,
		Version:      channel.Version,
	}
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	channels map[UniqueID][]*DmChannel
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		channels: make(map[UniqueID][]*DmChannel),
	}
}

func (m *ChannelDistManager) GetByNode(nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getByNode(nodeID)
}

func (m *ChannelDistManager) getByNode(nodeID UniqueID) []*DmChannel {
	channels, ok := m.channels[nodeID]
	if !ok {
		return nil
	}

	return channels
}

func (m *ChannelDistManager) GetAll() []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	result := make([]*DmChannel, 0)
	for _, channels := range m.channels {
		result = append(result, channels...)
	}
	return result
}

// GetShardLeader returns the node whthin the given replicaNodes and subscribing the given shard,
// returns (0, false) if not found.
func (m *ChannelDistManager) GetShardLeader(replica *Replica, shard string) (int64, bool) {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for node := range replica.Nodes {
		channels := m.channels[node]
		for _, dmc := range channels {
			if dmc.ChannelName == shard {
				return node, true
			}
		}
	}

	return 0, false
}

func (m *ChannelDistManager) GetShardLeadersByReplica(replica *Replica) map[string]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make(map[string]int64)
	for node := range replica.Nodes {
		channels := m.channels[node]
		for _, dmc := range channels {
			if dmc.GetCollectionID() == replica.GetCollectionID() {
				ret[dmc.GetChannelName()] = node
			}
		}
	}
	return ret
}

func (m *ChannelDistManager) GetByCollection(collectionID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*DmChannel, 0)
	for _, channels := range m.channels {
		for _, channel := range channels {
			if channel.CollectionID == collectionID {
				ret = append(ret, channel)
			}
		}
	}
	return ret
}

func (m *ChannelDistManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	channels := make([]*DmChannel, 0)
	for _, channel := range m.getByNode(nodeID) {
		if channel.CollectionID == collectionID {
			channels = append(channels, channel)
		}
	}

	return channels
}

func (m *ChannelDistManager) Update(nodeID UniqueID, channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		channel.Node = nodeID
	}

	m.channels[nodeID] = channels
}
