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
	"context"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Configuration leases span recovery, including ordinary allocation. A changed
// allowlist waits for active recoveries and settles uncertain catalog writes.
// Each RG owns its scheduling lock, complete statistics snapshot and node plan.
type replicaPlacementPolicy struct {
	mu         sync.Mutex
	groups     map[string]*replicaPlacement
	allowed    typeutil.Set[string]
	rawAllowed string
}

type replicaPlacement struct {
	mu          sync.Mutex
	name        string
	scope       map[int64][]int64
	rows        map[int64]int64
	singleShard map[int64]bool
	refreshed   time.Time
	plan        *replicaPlacementPlan
	pending     *Replica
}

type replicaPlacementPlan struct {
	key   string
	rows  map[int64]int64
	nodes map[int64][]int64 // replica ID -> desired RW nodes, stable while RO drains
}

func (m *Meta) RecoverNodesInCollection(ctx context.Context, collectionID int64, rgs map[string]*ResourceGroup) error {
	a, release, err := m.acquirePlacementPolicy(ctx)
	if err != nil {
		return err
	}
	defer release()
	names := m.GetResourceGroupByCollection(ctx, collectionID).Collect()
	slices.Sort(names)
	managed := typeutil.NewSet[string]()
	var refreshErr error
	for _, name := range names {
		if !a.matches(name) {
			continue
		}
		a.mu.Lock()
		g := a.groups[name]
		if g == nil {
			g = &replicaPlacement{name: name}
			a.groups[name] = g
		}
		a.mu.Unlock()
		members, err := m.recoverReplicaPlacement(ctx, g, a)
		if members == nil {
			return err
		} // no legacy writes after an uncertain save
		if members.Contain(collectionID) {
			managed.Insert(name)
		}
		if err != nil {
			refreshErr = err
		}
	}
	if err := m.recoverNodesInCollection(ctx, collectionID, rgs, func(name string) bool { return managed.Contain(name) }); err != nil {
		return err
	}
	return refreshErr
}

func (a *replicaPlacementPolicy) current() bool {
	return a.rawAllowed == paramtable.Get().QueryCoordCfg.ReplicaPlacementResourceGroupAllowlist.GetValue()
}

func (a *replicaPlacementPolicy) matches(rg string) bool {
	return a.allowed.Contain("*") || a.allowed.Contain(rg)
}

func newReplicaPlacementPolicy(raw string) *replicaPlacementPolicy {
	a := &replicaPlacementPolicy{rawAllowed: raw, groups: make(map[string]*replicaPlacement), allowed: typeutil.NewSet[string]()}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			a.allowed.Insert(name)
		}
	}
	return a
}

func (m *Meta) acquirePlacementPolicy(ctx context.Context) (*replicaPlacementPolicy, func(), error) {
	for {
		m.placementMu.RLock()
		if a := m.placementPolicy; a != nil && a.current() {
			return a, m.placementMu.RUnlock, nil
		}
		m.placementMu.RUnlock()
		if err := m.refreshPlacementPolicy(ctx); err != nil {
			return nil, nil, err
		}
	}
}

func (m *Meta) refreshPlacementPolicy(ctx context.Context) error {
	m.placementMu.Lock()
	defer m.placementMu.Unlock()
	old := m.placementPolicy
	if old != nil && old.current() {
		return nil
	}
	a := newReplicaPlacementPolicy(paramtable.Get().QueryCoordCfg.ReplicaPlacementResourceGroupAllowlist.GetValue())
	if old != nil {
		for name, g := range old.groups {
			if g.pending != nil {
				id := g.pending.GetCollectionID()
				m.collLock.Lock(id)
				err := m.settlePlacementWrite(ctx, g)
				m.collLock.Unlock(id)
				if err != nil {
					return err
				}
			}
			if a.matches(name) {
				a.groups[name] = g
			}
		}
	}
	m.placementPolicy = a
	return nil
}

func (m *ReplicaManager) placementMembers(ctx context.Context, name string) []int64 {
	ids := typeutil.NewSet[int64]()
	for _, r := range m.GetByResourceGroup(ctx, name) {
		ids.Insert(r.GetCollectionID())
	}
	members := ids.Collect()
	slices.Sort(members)
	return members
}

func (m *ReplicaManager) lockPlacementMembers(members []int64) func() {
	for _, id := range members {
		m.collLock.Lock(id)
	}
	return func() {
		for i := len(members) - 1; i >= 0; i-- {
			m.collLock.Unlock(members[i])
		}
	}
}

func (m *Meta) placementScope(members []int64) map[int64][]int64 {
	m.CollectionManager.rwmutex.RLock()
	defer m.CollectionManager.rwmutex.RUnlock()
	scope := make(map[int64][]int64, len(members))
	for _, id := range members {
		parts := m.collectionPartitions[id].Collect()
		slices.Sort(parts)
		scope[id] = parts
	}
	return scope
}

func (m *Meta) recoverReplicaPlacement(ctx context.Context, g *replicaPlacement, a *replicaPlacementPolicy) (typeutil.Set[int64], error) {
	// Lock order: policy lease -> RG -> ascending collection replica locks.
	// Global collection/resource metadata locks never span RPC or catalog IO.
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending != nil {
		id := g.pending.GetCollectionID()
		m.collLock.Lock(id)
		err := m.settlePlacementWrite(ctx, g)
		m.collLock.Unlock(id)
		if err != nil {
			return nil, err
		}
	}
	members := m.placementMembers(ctx, g.name)
	scope := m.placementScope(members)
	refreshErr := g.refreshRows(ctx, m.Broker, scope)
	// Membership can change during IO. Lock the latest set, then validate it
	// before every write, including changes caused by a new collection loading.
	members = m.placementMembers(ctx, g.name)
	unlock := m.lockPlacementMembers(members)
	defer unlock()
	currentScope := m.placementScope(members)
	if !reflect.DeepEqual(scope, currentScope) {
		refreshErr = merr.WrapErrServiceUnavailable("replica placement load scope changed during row refresh")
		scope = currentScope
	}
	if !a.current() {
		return nil, merr.WrapErrServiceUnavailable("replica placement configuration changed during row refresh")
	}
	rgs, err := m.GetResourceGroups(ctx, []string{g.name})
	if err != nil {
		return nil, err
	}
	rg := rgs[g.name]
	nodes := rg.GetNodes()
	slices.Sort(nodes)
	replicas := make([]*Replica, 0)
	managed := typeutil.NewSet[int64]()
	for _, id := range members {
		// Unknown members use conservative recovery until a complete snapshot
		// establishes their shard count. Known multi-shard collections stay ordinary.
		if single, known := g.singleShard[id]; known && !single {
			continue
		}
		managed.Insert(id)
		for _, r := range m.GetByCollection(ctx, id) {
			if r.GetResourceGroup() == g.name {
				replicas = append(replicas, r)
			}
		}
	}
	slices.SortFunc(replicas, func(a, b *Replica) int {
		if a.GetID() < b.GetID() {
			return -1
		}
		if a.GetID() > b.GetID() {
			return 1
		}
		return 0
	})
	var plan map[int64][]int64
	if g.rows != nil && reflect.DeepEqual(scope, g.scope) {
		key := placementPlanKey(replicas, nodes)
		if g.plan == nil || g.plan.key != key || placementRowSharesChanged(replicas, len(nodes), g.plan.rows, g.rows) {
			g.plan = &replicaPlacementPlan{key: key, rows: g.rows, nodes: assignReplicaPlacement(replicas, nodes, g.rows)}
		}
		plan = g.plan.nodes
	} else {
		plan = conservativePlacement(replicas, nodes)
	}
	for _, r := range replicas {
		if r.NeedWaitRGReady() && rg.MissingNumOfNodes() > 0 {
			continue
		}
		updated := applyReplicaPlacementPlan(r, m.GetByCollection(ctx, r.GetCollectionID()), nodes, plan)
		if updated == nil {
			continue
		}
		if err := m.validatePlacementSnapshot(ctx, g, a, members, scope, rg, nodes); err != nil {
			return nil, err
		}
		if err := m.put(ctx, r.GetCollectionID(), updated); err != nil {
			// The write may already be durable. Reserve every newly acquired
			// node as RO in memory until a full write settles the uncertainty;
			// recovery in another RG must not lend it to a sibling meanwhile.
			reserved := r.CopyForWrite()
			for _, node := range updated.GetRWNodes() {
				if !r.Contains(node) {
					reserved.AddRONode(node)
				}
			}
			g.pending = reserved.IntoReplica()
			m.putReplicasInMemory(r.GetCollectionID(), g.pending)
			return nil, err
		}
		mlog.Info(ctx, "single-shard replica placement updated", mlog.String("resourceGroup", g.name), mlog.Int64("replicaID", r.GetID()), mlog.Int64s("rwNodes", updated.GetRWNodes()), mlog.Int64s("roNodes", updated.GetRONodes()))
	}
	if err := m.validatePlacementSnapshot(ctx, g, a, members, scope, rg, nodes); err != nil {
		return nil, err
	}
	return managed, refreshErr
}

func (m *Meta) validatePlacementSnapshot(ctx context.Context, g *replicaPlacement, a *replicaPlacementPolicy, members []int64, scope map[int64][]int64, rg *ResourceGroup, nodes []int64) error {
	if !a.current() || !slices.Equal(members, m.placementMembers(ctx, g.name)) || !reflect.DeepEqual(scope, m.placementScope(members)) {
		return merr.WrapErrServiceUnavailable("replica placement inputs changed during recovery")
	}
	m.ResourceManager.rwmutex.RLock()
	defer m.ResourceManager.rwmutex.RUnlock()
	current := m.groups[g.name]
	if current == nil || !proto.Equal(current.cfg, rg.cfg) {
		return merr.WrapErrServiceUnavailable("replica placement resource group changed during recovery")
	}
	currentNodes := current.GetNodes()
	slices.Sort(currentNodes)
	if !slices.Equal(currentNodes, nodes) {
		return merr.WrapErrServiceUnavailable("replica placement resource group nodes changed during recovery")
	}
	return nil
}

// A lost catalog response must never cause a stale target to reclaim nodes
// subsequently assigned to a sibling. Restore the currently published full value
// before replanning. An intervening durable full write already supersedes it.
// Caller holds the collection replica lock and either the RG lock or exclusive policy lock.
func (m *ReplicaManager) settlePlacementWrite(ctx context.Context, g *replicaPlacement) error {
	if before := g.pending; before != nil {
		current := m.Get(ctx, before.GetID())
		if current != nil && proto.Equal(current.replicaPB, before.replicaPB) {
			if err := m.put(ctx, current.GetCollectionID(), current); err != nil {
				return err
			}
		}
		g.pending = nil
	}
	return nil
}
