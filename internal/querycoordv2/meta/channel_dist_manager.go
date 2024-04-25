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
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ChannelDistFilter = func(ch *DmChannel) bool

func WithCollectionID2Channel(collectionID int64) ChannelDistFilter {
	return func(ch *DmChannel) bool {
		return ch.GetCollectionID() == collectionID
	}
}

func WithNodeID2Channel(nodeID int64) ChannelDistFilter {
	return func(ch *DmChannel) bool {
		return ch.Node == nodeID
	}
}

func WithReplica2Channel(replica *Replica) ChannelDistFilter {
	return func(ch *DmChannel) bool {
		return ch.GetCollectionID() == replica.GetCollectionID() && replica.Contains(ch.Node)
	}
}

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

	// CollectionID -> Channels
	collectionIndex map[int64][]*DmChannel
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		channels:        make(map[UniqueID][]*DmChannel),
		collectionIndex: make(map[int64][]*DmChannel),
	}
}

// todo by liuwei: should consider the case of duplicate leader exists
// GetShardLeader returns the node whthin the given replicaNodes and subscribing the given shard,
// returns (0, false) if not found.
func (m *ChannelDistManager) GetShardLeader(replica *Replica, shard string) (int64, bool) {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for _, node := range replica.GetNodes() {
		channels := m.channels[node]
		for _, dmc := range channels {
			if dmc.ChannelName == shard {
				return node, true
			}
		}
	}

	return 0, false
}

// todo by liuwei: should consider the case of duplicate leader exists
func (m *ChannelDistManager) GetShardLeadersByReplica(replica *Replica) map[string]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make(map[string]int64)
	for _, node := range replica.GetNodes() {
		channels := m.channels[node]
		for _, dmc := range channels {
			if dmc.GetCollectionID() == replica.GetCollectionID() {
				ret[dmc.GetChannelName()] = node
			}
		}
	}
	return ret
}

// return all channels in list which match all given filters
func (m *ChannelDistManager) GetByFilter(filters ...ChannelDistFilter) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	mergedFilters := func(ch *DmChannel) bool {
		for _, fn := range filters {
			if fn != nil && !fn(ch) {
				return false
			}
		}

		return true
	}

	ret := make([]*DmChannel, 0)
	for _, channels := range m.channels {
		for _, channel := range channels {
			if mergedFilters(channel) {
				ret = append(ret, channel)
			}
		}
	}
	return ret
}

func (m *ChannelDistManager) GetByCollectionAndFilter(collectionID int64, filters ...ChannelDistFilter) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	mergedFilters := func(ch *DmChannel) bool {
		for _, fn := range filters {
			if fn != nil && !fn(ch) {
				return false
			}
		}

		return true
	}

	ret := make([]*DmChannel, 0)

	// If a collection ID is provided, use the collection index
	for _, channel := range m.collectionIndex[collectionID] {
		if mergedFilters(channel) {
			ret = append(ret, channel)
		}
	}
	return ret
}

func (m *ChannelDistManager) Update(nodeID UniqueID, channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		channel.Node = nodeID
	}

	m.channels[nodeID] = channels

	m.updateCollectionIndex()
}

// update secondary index for channel distribution
func (m *ChannelDistManager) updateCollectionIndex() {
	m.collectionIndex = make(map[int64][]*DmChannel)
	for _, nodeChannels := range m.channels {
		for _, channel := range nodeChannels {
			collectionID := channel.GetCollectionID()
			if channels, ok := m.collectionIndex[collectionID]; !ok {
				m.collectionIndex[collectionID] = []*DmChannel{channel}
			} else {
				m.collectionIndex[collectionID] = append(channels, channel)
			}
		}
	}
}
