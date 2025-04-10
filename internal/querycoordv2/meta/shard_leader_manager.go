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
)

type ShardLeaderManagerInterface interface {
	SetNotifyFunc(notifyFunc NotifyDelegatorChanges)
	Update(shard ...*ShardLeader)
	Remove(nodeID int64)
	RemoveByCollection(collectionID int64)
	GetShardLeaderList(channelName string) map[int64]*ShardLeader
	GetShardLeader(replicaID int64, channelName string) *ShardLeader
}

type ShardLeader struct {
	NodeID       int64  // shard leader's node id
	ChannelName  string // shard leader's channel name
	CollectionID int64  // shard leader's collection id
	ReplicaID    int64  // shard leader's replica id
	Version      int64  // use to check which one is the latest
	Serviceable  bool   // whether this shard leader is serviceable for search/query
}

type NotifyDelegatorChanges = func(collectionID ...int64)

type ShardLeaderManager struct {
	rwmutex        sync.RWMutex
	channelLeaders map[string]map[int64]*ShardLeader // channel -> replicaID -> leader
	notifyFunc     NotifyDelegatorChanges
}

func NewShardLeaderManager() *ShardLeaderManager {
	return &ShardLeaderManager{
		channelLeaders: make(map[string]map[int64]*ShardLeader),
		notifyFunc: func(collectionID ...int64) {
			// do nothing by default
		},
	}
}

func (mgr *ShardLeaderManager) SetNotifyFunc(notifyFunc NotifyDelegatorChanges) {
	mgr.notifyFunc = notifyFunc
}

// Update updates shard leaders based on service status and version, triggers update when:
// 1. No existing leader in the current replica
// 2. Current leader is unserviceable while new leader is serviceable
// 3. Both leaders are serviceable but new version is greater
// Parameters:
//   - shard: New shard leader (nil means remove current leader without replacement)
func (mgr *ShardLeaderManager) Update(shards ...*ShardLeader) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()
	for _, shard := range shards {
		current := mgr.channelLeaders[shard.ChannelName]
		if current == nil {
			current = make(map[int64]*ShardLeader)
		}

		existing := current[shard.ReplicaID]

		updateNeeded := false
		switch {
		case existing == nil:
			// Condition 1: No existing leader
			updateNeeded = true
		case !existing.Serviceable && shard.Serviceable:
			// Condition 2: Replace unserviceable with serviceable
			updateNeeded = true
		case existing.Serviceable && shard.Serviceable && shard.Version > existing.Version:
			// Condition 3: Higher version replacement
			updateNeeded = true
		}

		if updateNeeded {
			current[shard.ReplicaID] = shard
			mgr.notifyFunc(shard.CollectionID)
		}
		mgr.channelLeaders[shard.ChannelName] = current
	}
}

func (mgr *ShardLeaderManager) Remove(nodeID int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for channel, leaders := range mgr.channelLeaders {
		filtered := make(map[int64]*ShardLeader)
		for replicaID, leader := range leaders {
			if leader.NodeID != nodeID {
				filtered[replicaID] = leader
			}
		}
		if len(filtered) < len(leaders) {
			mgr.channelLeaders[channel] = filtered
		}
	}
}

func (mgr *ShardLeaderManager) RemoveByCollection(collectionID int64) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	for channel, leaders := range mgr.channelLeaders {
		filtered := make(map[int64]*ShardLeader)
		for replicaID, leader := range leaders {
			if leader.CollectionID != collectionID {
				filtered[replicaID] = leader
			}
		}
		if len(filtered) < len(leaders) {
			mgr.channelLeaders[channel] = filtered
		}
	}
}

// with given channel name, return replica -> shard leader
func (mgr *ShardLeaderManager) GetShardLeaderList(channelName string) map[int64]*ShardLeader {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.channelLeaders[channelName]
}

// with given replica and channel name, return the only one delegator leader
// all action send to delegator should route by the delegator leader
func (mgr *ShardLeaderManager) GetShardLeader(replicaID int64, channelName string) *ShardLeader {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	if leaders, ok := mgr.channelLeaders[channelName]; ok {
		return leaders[replicaID]
	}
	return nil
}
