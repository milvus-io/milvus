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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Policies are immutable. RG scheduling state survives policy changes so that
// enabling/disabling a group never creates two concurrent planners for it.
type replicaPlacementPolicy struct {
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
}

type replicaPlacementPlan struct {
	key   string
	rows  map[int64]int64
	nodes map[int64][]int64
}

type replicaPlacementSnapshot struct {
	group   *replicaPlacement
	members []int64
	scope   map[int64][]int64
	rg      *ResourceGroup
	key     string
	managed typeutil.Set[int64]
	nodes   map[int64][]int64
	plan    *replicaPlacementPlan
}

func (m *Meta) RecoverNodesInCollection(ctx context.Context, collectionID int64, rgs map[string]*ResourceGroup) error {
	a := m.acquirePlacementPolicy()
	before := m.GetByCollection(ctx, collectionID)
	names := typeutil.NewSet[string]()
	enabled := false
	for _, r := range before {
		names.Insert(r.GetResourceGroup())
		enabled = enabled || a.matches(r.GetResourceGroup())
	}
	_, pending := m.placementPending.Load(collectionID)
	if !enabled && !pending {
		return m.ReplicaManager.RecoverNodesInCollection(ctx, collectionID, rgs)
	}
	ordered := names.Collect()
	slices.Sort(ordered)
	currentRGs, err := m.GetResourceGroups(ctx, ordered)
	if err != nil {
		return err
	}
	snapshots := make([]*replicaPlacementSnapshot, 0)
	var refreshErr error
	for _, name := range ordered {
		if !a.matches(name) {
			continue
		}
		snapshot, err := m.prepareReplicaPlacement(ctx, m.getPlacementGroup(name), a)
		if snapshot == nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
		currentRGs[name] = snapshot.rg
		if err != nil {
			refreshErr = err
		}
	}

	// Only this collection is locked while writing. Planning never holds a
	// collection lock, and its RG lock is released before this point.
	m.collLock.Lock(collectionID)
	defer m.collLock.Unlock(collectionID)
	replicas := m.GetByCollection(ctx, collectionID)
	if !samePlacementReplicas(before, replicas) {
		return merr.WrapErrServiceUnavailable("replica placement collection membership changed")
	}
	if len(replicas) == 0 {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	managed := typeutil.NewSet[string]()
	for _, snapshot := range snapshots {
		if snapshot.managed.Contain(collectionID) {
			managed.Insert(snapshot.group.name)
		}
	}
	ordinary, err := m.computeReplicaRecovery(ctx, collectionID, currentRGs, func(name string) bool { return managed.Contain(name) })
	if err != nil {
		return err
	}
	working := slices.Clone(replicas)
	for i, r := range working {
		for _, updated := range ordinary {
			if updated.GetID() == r.GetID() {
				working[i] = updated
				break
			}
		}
	}
	// Use the whole collection's working state, including other RGs' RO nodes.
	// A desired handoff still waits for the old owner's data to drain.
	for _, snapshot := range snapshots {
		if !snapshot.managed.Contain(collectionID) {
			continue
		}
		available := snapshot.rg.GetNodes()
		slices.Sort(available)
		for i, r := range working {
			if r.GetResourceGroup() != snapshot.group.name {
				continue
			}
			if r.NeedWaitRGReady() && snapshot.rg.MissingNumOfNodes() > 0 {
				continue
			}
			if updated := applyReplicaPlacementPlan(r, working, available, snapshot.nodes); updated != nil {
				working[i] = updated
			}
		}
	}
	if err := m.validateCollectionPlacement(ctx, a, snapshots, currentRGs); err != nil {
		return err
	}
	changed := false
	for i, r := range working {
		changed = changed || r != replicas[i]
	}
	_, pending = m.placementPending.Load(collectionID)
	if !changed && !pending {
		return refreshErr
	}
	// Catalog.SaveReplica uses one MultiSave transaction for every replica of
	// this collection, including ordinary resource groups.
	if err := m.put(ctx, collectionID, working...); err != nil {
		reserved := make([]*Replica, 0, len(replicas))
		for i, r := range replicas {
			mutable := r.CopyForWrite()
			for _, node := range working[i].GetRWNodes() {
				if !r.Contains(node) {
					mutable.AddRONode(node)
				}
			}
			reserved = append(reserved, mutable.IntoReplica())
		}
		// A lost transaction response may mean all changes are already durable.
		// Preserve the union of old and possible new ownership until a subsequent
		// whole-collection write confirms current state; never replay an old plan.
		m.placementPending.Store(collectionID, struct{}{})
		m.putReplicasInMemory(collectionID, reserved...)
		return err
	}
	m.placementPending.Delete(collectionID)
	if err := m.validateCollectionPlacement(ctx, a, snapshots, currentRGs); err != nil {
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
	a := &replicaPlacementPolicy{rawAllowed: raw, allowed: typeutil.NewSet[string]()}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			a.allowed.Insert(name)
		}
	}
	return a
}

func (m *Meta) acquirePlacementPolicy() *replicaPlacementPolicy {
	m.placementMu.Lock()
	defer m.placementMu.Unlock()
	if m.placementPolicy == nil || !m.placementPolicy.current() {
		m.placementPolicy = newReplicaPlacementPolicy(paramtable.Get().QueryCoordCfg.ReplicaPlacementResourceGroupAllowlist.GetValue())
	}
	return m.placementPolicy
}

func (m *Meta) getPlacementGroup(name string) *replicaPlacement {
	m.placementMu.Lock()
	defer m.placementMu.Unlock()
	if m.placementGroups == nil {
		m.placementGroups = make(map[string]*replicaPlacement)
	}
	g := m.placementGroups[name]
	if g == nil {
		g = &replicaPlacement{name: name}
		m.placementGroups[name] = g
	}
	return g
}

func samePlacementReplicas(a, b []*Replica) bool {
	if len(a) != len(b) {
		return false
	}
	for i, r := range a {
		if r.GetID() != b[i].GetID() || r.GetCollectionID() != b[i].GetCollectionID() || r.GetResourceGroup() != b[i].GetResourceGroup() {
			return false
		}
	}
	return true
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

func (m *Meta) prepareReplicaPlacement(ctx context.Context, g *replicaPlacement, a *replicaPlacementPolicy) (*replicaPlacementSnapshot, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	members := m.placementMembers(ctx, g.name)
	scope := m.placementScope(members)
	refreshErr := g.refreshRows(ctx, m.Broker, scope)
	members = m.placementMembers(ctx, g.name)
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
	key := placementPlanKey(replicas, nodes)
	var plan map[int64][]int64
	if g.rows != nil && reflect.DeepEqual(scope, g.scope) {
		if g.plan == nil || g.plan.key != key || placementRowSharesChanged(replicas, len(nodes), g.plan.rows, g.rows) {
			g.plan = &replicaPlacementPlan{key: key, rows: g.rows, nodes: assignReplicaPlacement(replicas, nodes, g.rows)}
		}
		plan = g.plan.nodes
	} else {
		plan = conservativePlacement(replicas, nodes)
	}
	return &replicaPlacementSnapshot{group: g, members: members, scope: scope, rg: rg, key: key, managed: managed, nodes: plan, plan: g.plan}, refreshErr
}

func (m *Meta) validateCollectionPlacement(ctx context.Context, a *replicaPlacementPolicy, snapshots []*replicaPlacementSnapshot, rgs map[string]*ResourceGroup) error {
	if !a.current() {
		return merr.WrapErrServiceUnavailable("replica placement configuration changed")
	}
	for _, snapshot := range snapshots {
		g := snapshot.group
		g.mu.Lock()
		samePlan := g.plan == snapshot.plan
		g.mu.Unlock()
		if !samePlan || !slices.Equal(snapshot.members, m.placementMembers(ctx, g.name)) || !reflect.DeepEqual(snapshot.scope, m.placementScope(snapshot.members)) {
			return merr.WrapErrServiceUnavailable("replica placement inputs changed during recovery")
		}
		replicas := make([]*Replica, 0)
		for _, id := range snapshot.members {
			if !snapshot.managed.Contain(id) {
				continue
			}
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
		nodes := snapshot.rg.GetNodes()
		slices.Sort(nodes)
		if placementPlanKey(replicas, nodes) != snapshot.key {
			return merr.WrapErrServiceUnavailable("replica placement replica membership changed")
		}
	}
	m.ResourceManager.rwmutex.RLock()
	defer m.ResourceManager.rwmutex.RUnlock()
	for name, before := range rgs {
		current := m.groups[name]
		if current == nil || !proto.Equal(current.cfg, before.cfg) || !reflect.DeepEqual(current.nodes, before.nodes) {
			return merr.WrapErrServiceUnavailable("replica placement resource group changed")
		}
	}
	return nil
}
