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
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type replicaSlice = []*querypb.ReplicaInfo

type ReplicaInfos struct {
	globalGuard sync.RWMutex // We have to make sure atomically update replicas and index

	// Persistent Info
	replicas map[UniqueID]*querypb.ReplicaInfo // replica_id -> *ReplicaInfo

	// Non-persistent info
	nodeIndex map[UniqueID][]*querypb.ReplicaInfo // node_id -> []*ReplicaInfo
}

func NewReplicaInfos() *ReplicaInfos {
	return &ReplicaInfos{
		globalGuard: sync.RWMutex{},
		replicas:    make(map[int64]*querypb.ReplicaInfo),
		nodeIndex:   make(map[int64][]*querypb.ReplicaInfo),
	}
}

func (rep *ReplicaInfos) Get(replicaID UniqueID) (*querypb.ReplicaInfo, bool) {
	rep.globalGuard.RLock()
	defer rep.globalGuard.RUnlock()

	info, ok := rep.replicas[replicaID]
	return info, ok
}

// Make sure atomically update replica and index
func (rep *ReplicaInfos) Insert(info *querypb.ReplicaInfo) {
	rep.globalGuard.Lock()
	defer rep.globalGuard.Unlock()

	old, ok := rep.replicas[info.ReplicaID]
	// This updates ReplicaInfo, not inserts a new one
	// No need to update nodeIndex
	if ok {
		*old = *info
		return
	}

	rep.replicas[info.ReplicaID] = info

	for _, nodeID := range info.NodeIds {
		replicas, ok := rep.nodeIndex[nodeID]
		if !ok {
			replicas = make([]*querypb.ReplicaInfo, 0)
			rep.nodeIndex[nodeID] = replicas
		}

		replicas = append(replicas, info)
		rep.nodeIndex[nodeID] = replicas
	}
}

func (rep *ReplicaInfos) GetReplicasByNodeID(nodeID UniqueID) []*querypb.ReplicaInfo {
	rep.globalGuard.RLock()
	defer rep.globalGuard.RUnlock()

	// Avoid to create entry if nodeID not found
	replicas, ok := rep.nodeIndex[nodeID]
	if !ok {
		return nil
	}

	return replicas
}
