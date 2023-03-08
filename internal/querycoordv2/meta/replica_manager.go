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
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/merr"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Replica struct {
	*querypb.Replica
	nodes   UniqueSet // a helper field for manipulating replica's Nodes slice field
	rwmutex sync.RWMutex
}

func NewReplica(replica *querypb.Replica, nodes UniqueSet) *Replica {
	return &Replica{
		Replica: replica,
		nodes:   nodes,
	}
}

func (replica *Replica) AddNode(nodes ...int64) {
	replica.rwmutex.Lock()
	defer replica.rwmutex.Unlock()
	replica.nodes.Insert(nodes...)
	replica.Replica.Nodes = replica.nodes.Collect()
}

func (replica *Replica) GetNodes() []int64 {
	replica.rwmutex.RLock()
	defer replica.rwmutex.RUnlock()
	if replica != nil {
		return replica.nodes.Collect()
	}
	return nil
}

func (replica *Replica) Len() int {
	replica.rwmutex.RLock()
	defer replica.rwmutex.RUnlock()
	if replica != nil {
		return replica.nodes.Len()
	}

	return 0
}

func (replica *Replica) Contains(node int64) bool {
	replica.rwmutex.RLock()
	defer replica.rwmutex.RUnlock()
	if replica != nil {
		return replica.nodes.Contain(node)
	}

	return false
}

func (replica *Replica) RemoveNode(nodes ...int64) {
	replica.rwmutex.Lock()
	defer replica.rwmutex.Unlock()
	replica.nodes.Remove(nodes...)
	replica.Replica.Nodes = replica.nodes.Collect()
}

func (replica *Replica) Clone() *Replica {
	replica.rwmutex.RLock()
	defer replica.rwmutex.RUnlock()
	return &Replica{
		Replica: proto.Clone(replica.Replica).(*querypb.Replica),
		nodes:   NewUniqueSet(replica.Replica.Nodes...),
	}
}

type ReplicaManager struct {
	rwmutex sync.RWMutex

	idAllocator func() (int64, error)
	replicas    map[UniqueID]*Replica
	store       Store
}

func NewReplicaManager(idAllocator func() (int64, error), store Store) *ReplicaManager {
	return &ReplicaManager{
		idAllocator: idAllocator,
		replicas:    make(map[int64]*Replica),
		store:       store,
	}
}

// Recover recovers the replicas for given collections from meta store
func (m *ReplicaManager) Recover(collections []int64) error {
	replicas, err := m.store.GetReplicas()
	if err != nil {
		return fmt.Errorf("failed to recover replicas, err=%w", err)
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	for _, replica := range replicas {
		if len(replica.GetResourceGroup()) == 0 {
			replica.ResourceGroup = DefaultResourceGroupName
		}

		if collectionSet.Contain(replica.GetCollectionID()) {
			m.replicas[replica.GetID()] = &Replica{
				Replica: replica,
				nodes:   NewUniqueSet(replica.GetNodes()...),
			}
			log.Info("recover replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()),
			)
		} else {
			err := m.store.ReleaseReplica(replica.GetCollectionID(), replica.GetID())
			if err != nil {
				return err
			}
			log.Info("clear stale replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()),
			)
		}
	}
	return nil
}

func (m *ReplicaManager) Get(id UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.replicas[id]
}

// Spawn spawns replicas of the given number, for given collection,
// this doesn't store these replicas and assign nodes to them.
func (m *ReplicaManager) Spawn(collection int64, replicaNumber int32, rgName string) ([]*Replica, error) {
	var (
		replicas = make([]*Replica, replicaNumber)
		err      error
	)
	for i := range replicas {
		replicas[i], err = m.spawn(collection, rgName)
		if err != nil {
			return nil, err
		}
	}
	return replicas, err
}

func (m *ReplicaManager) Put(replicas ...*Replica) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(replicas...)
}

func (m *ReplicaManager) spawn(collectionID UniqueID, rgName string) (*Replica, error) {
	id, err := m.idAllocator()
	if err != nil {
		return nil, err
	}
	return &Replica{
		Replica: &querypb.Replica{
			ID:            id,
			CollectionID:  collectionID,
			ResourceGroup: rgName,
		},
		nodes: make(UniqueSet),
	}, nil
}

func (m *ReplicaManager) put(replicas ...*Replica) error {
	for _, replica := range replicas {
		err := m.store.SaveReplica(replica.Replica)
		if err != nil {
			return err
		}
		m.replicas[replica.ID] = replica
	}
	return nil
}

// RemoveCollection removes replicas of given collection,
// returns error if failed to remove replica from KV
func (m *ReplicaManager) RemoveCollection(collectionID UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.store.ReleaseReplicas(collectionID)
	if err != nil {
		return err
	}
	for id, replica := range m.replicas {
		if replica.CollectionID == collectionID {
			delete(m.replicas, id)
		}
	}
	return nil
}

func (m *ReplicaManager) GetByCollection(collectionID UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := make([]*Replica, 0, 3)
	for _, replica := range m.replicas {
		if replica.CollectionID == collectionID {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (m *ReplicaManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for _, replica := range m.replicas {
		if replica.CollectionID == collectionID && replica.nodes.Contain(nodeID) {
			return replica
		}
	}

	return nil
}

func (m *ReplicaManager) GetByCollectionAndRG(collectionID int64, rgName string) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.GetCollectionID() == collectionID && replica.GetResourceGroup() == rgName {
			ret = append(ret, replica)
		}
	}

	return ret
}

func (m *ReplicaManager) GetByResourceGroup(rgName string) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.GetResourceGroup() == rgName {
			ret = append(ret, replica)
		}
	}

	return ret
}

func (m *ReplicaManager) AddNode(replicaID UniqueID, nodes ...UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	replica = replica.Clone()
	replica.AddNode(nodes...)
	return m.put(replica)
}

func (m *ReplicaManager) RemoveNode(replicaID UniqueID, nodes ...UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	replica = replica.Clone()
	replica.RemoveNode(nodes...)
	return m.put(replica)
}

func (m *ReplicaManager) GetResourceGroupByCollection(collection UniqueID) typeutil.Set[string] {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	ret := typeutil.NewSet[string]()
	for _, r := range m.replicas {
		if r.GetCollectionID() == collection {
			ret.Insert(r.GetResourceGroup())
		}
	}

	return ret
}
