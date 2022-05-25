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

package querycoord

import (
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const (
	invalidReplicaID int64 = -1
)

type replicaSlice = []*milvuspb.ReplicaInfo

// ReplicaInfos maintains replica related meta information.
type ReplicaInfos struct {
	globalGuard sync.RWMutex // We have to make sure atomically update replicas and index

	// Persistent Info
	replicas map[UniqueID]*milvuspb.ReplicaInfo // replicaID -> *ReplicaInfo

	// Non-persistent info
	nodeIndex map[UniqueID]map[UniqueID]*milvuspb.ReplicaInfo // nodeID, replicaID -> []*ReplicaInfo
}

// NewReplicaInfos creates a ReplicaInfos instance with internal map created.
func NewReplicaInfos() *ReplicaInfos {
	return &ReplicaInfos{
		globalGuard: sync.RWMutex{},
		replicas:    make(map[int64]*milvuspb.ReplicaInfo),
		nodeIndex:   make(map[int64]map[int64]*milvuspb.ReplicaInfo),
	}
}

// Get returns the ReplicaInfo with provided replicaID.
// If the ReplicaInfo does not exist, nil will be returned
func (rep *ReplicaInfos) Get(replicaID UniqueID) (*milvuspb.ReplicaInfo, bool) {
	rep.globalGuard.RLock()
	defer rep.globalGuard.RUnlock()

	return rep.get(replicaID)
}

// get is the internal common util function to get replica with provided replicaID.
// the lock shall be accquired first
// NO outer invocation is allowed
func (rep *ReplicaInfos) get(replicaID UniqueID) (*milvuspb.ReplicaInfo, bool) {
	info, ok := rep.replicas[replicaID]
	clone := proto.Clone(info).(*milvuspb.ReplicaInfo)
	return clone, ok
}

// Insert atomically updates replica and node index.
func (rep *ReplicaInfos) Insert(info *milvuspb.ReplicaInfo) {
	rep.globalGuard.Lock()
	defer rep.globalGuard.Unlock()

	rep.upsert(info)
}

// upsert is the internal common util function to upsert replica info.
// it also update the related nodeIndex information.
// the lock shall be accquired first
// NO outer invocation is allowed
func (rep *ReplicaInfos) upsert(info *milvuspb.ReplicaInfo) {
	old, ok := rep.replicas[info.ReplicaID]

	info = proto.Clone(info).(*milvuspb.ReplicaInfo)
	rep.replicas[info.ReplicaID] = info

	// This updates ReplicaInfo, not inserts a new one
	if ok {
		for _, nodeID := range old.NodeIds {
			nodeReplicas := rep.nodeIndex[nodeID]
			delete(nodeReplicas, old.ReplicaID)
		}
	}

	for _, nodeID := range info.NodeIds {
		replicas, ok := rep.nodeIndex[nodeID]
		if !ok {
			replicas = make(map[UniqueID]*milvuspb.ReplicaInfo)
		}

		replicas[info.ReplicaID] = info
		rep.nodeIndex[nodeID] = replicas
	}
}

// GetReplicasByNodeID returns the replicas associated with provided node id.
func (rep *ReplicaInfos) GetReplicasByNodeID(nodeID UniqueID) []*milvuspb.ReplicaInfo {
	rep.globalGuard.RLock()
	defer rep.globalGuard.RUnlock()

	// Avoid to create entry if nodeID not found
	replicas, ok := rep.nodeIndex[nodeID]
	if !ok {
		return nil
	}

	clones := make([]*milvuspb.ReplicaInfo, 0, len(replicas))
	for _, replica := range replicas {
		clones = append(clones, proto.Clone(replica).(*milvuspb.ReplicaInfo))
	}

	return clones
}

// Remove deletes provided replica ids from meta.
func (rep *ReplicaInfos) Remove(replicaIds ...UniqueID) {
	rep.globalGuard.Lock()
	defer rep.globalGuard.Unlock()

	for _, replicaID := range replicaIds {
		delete(rep.replicas, replicaID)
	}
	for _, replicaIndex := range rep.nodeIndex {
		for _, replicaID := range replicaIds {
			delete(replicaIndex, replicaID)
		}
	}
}

// ApplyBalancePlan applies balancePlan to replica nodes.
func (rep *ReplicaInfos) ApplyBalancePlan(p *balancePlan, kv kv.MetaKv) error {
	rep.globalGuard.Lock()
	defer rep.globalGuard.Unlock()

	var sourceReplica, targetReplica *milvuspb.ReplicaInfo
	var ok bool

	// check source and target replica ids are valid
	if p.sourceReplica != invalidReplicaID {
		sourceReplica, ok = rep.get(p.sourceReplica)
		if !ok {
			return errors.New("replica not found")
		}
	}

	if p.targetReplica != invalidReplicaID {
		targetReplica, ok = rep.replicas[p.targetReplica]
		if !ok {
			return errors.New("replica not found")
		}
	}

	var replicasChanged []*milvuspb.ReplicaInfo

	// generate ReplicaInfo to save to MetaKv
	if sourceReplica != nil {
		// remove node from replica node list
		removeNodeFromReplica(sourceReplica, p.nodeID)
		replicasChanged = append(replicasChanged, sourceReplica)
	}
	if targetReplica != nil {
		// add node to replica
		targetReplica.NodeIds = append(targetReplica.NodeIds, p.nodeID)
		replicasChanged = append(replicasChanged, targetReplica)
	}

	// save to etcd first
	err := saveReplica(kv, replicasChanged...)
	if err != nil {
		return err
	}

	// apply change to in-memory meta
	if sourceReplica != nil {
		rep.upsert(sourceReplica)
	}

	if targetReplica != nil {
		rep.upsert(targetReplica)
	}

	return nil
}

func (rep *ReplicaInfos) UpdateShardLeader(replicaID UniqueID, dmChannel string, leaderID UniqueID, leaderAddr string, meta kv.MetaKv) error {
	rep.globalGuard.Lock()
	defer rep.globalGuard.Unlock()

	replica, ok := rep.get(replicaID)
	if !ok {
		return fmt.Errorf("replica %v not found", replicaID)
	}

	for _, shard := range replica.ShardReplicas {
		if shard.DmChannelName == dmChannel {
			shard.LeaderID = leaderID
			shard.LeaderAddr = leaderAddr
			break
		}
	}

	err := saveReplica(meta, replica)
	if err != nil {
		return err
	}

	rep.upsert(replica)

	return nil
}

// removeNodeFromReplica helper function to remove nodeID from replica NodeIds list.
func removeNodeFromReplica(replica *milvuspb.ReplicaInfo, nodeID int64) *milvuspb.ReplicaInfo {
	for i := 0; i < len(replica.NodeIds); i++ {
		if replica.NodeIds[i] != nodeID {
			continue
		}
		replica.NodeIds = append(replica.NodeIds[:i], replica.NodeIds[i+1:]...)
		return replica
	}
	return replica
}

// save the replicas into etcd.
func saveReplica(meta kv.MetaKv, replicas ...*milvuspb.ReplicaInfo) error {
	data := make(map[string]string)

	for _, info := range replicas {
		infoBytes, err := proto.Marshal(info)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("%s/%d", ReplicaMetaPrefix, info.ReplicaID)
		data[key] = string(infoBytes)
	}

	return meta.MultiSave(data)
}
