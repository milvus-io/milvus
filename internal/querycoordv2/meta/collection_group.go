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

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// replicaPlacementPolicy is a parsed configuration snapshot, owned by Meta's
// recovery path. Pure node assignment has no configuration or IO dependencies.
type replicaPlacementPolicy struct {
	groups                map[int64]*collectionGroup
	allowed               typeutil.Set[string]
	rawGroups, rawAllowed string
}

type collectionGroup struct {
	mu        sync.Mutex
	name      string
	members   []int64 // sorted, including currently unloaded members
	scope     map[int64][]int64
	rows      map[int64]int64
	refreshed time.Time
	plans     map[string]*collectionGroupPlan
	pending   *collectionGroupWrite
}

// A timed-out write may have reached the catalog. Replay it before replanning,
// unless a later full replica write has already replaced the uncertain value.
type collectionGroupWrite struct {
	before, after *Replica
}

type collectionGroupPlan struct {
	key   string
	nodes map[int64][]int64 // replica ID -> desired RW nodes, stable during RO draining
}

// RecoverNodesInCollection decides placement from current config on each trigger.
// Policy leases let different groups recover concurrently while a config change
// waits for active recoveries, including legacy recovery, before replacing state.
func (m *Meta) RecoverNodesInCollection(ctx context.Context, collectionID int64, rgs map[string]*ResourceGroup) error {
	a, release, err := m.acquirePlacementPolicy(ctx)
	if err != nil {
		return err
	}
	defer release()
	if g := a.groups[collectionID]; g != nil && a.enabled(m.GetByCollection(ctx, collectionID)) {
		err := m.recoverCollectionGroup(ctx, g, a)
		legacyErr := m.recoverNodesInCollection(ctx, collectionID, rgs, a.matches)
		if err != nil {
			return err
		}
		return legacyErr
	}
	return m.ReplicaManager.RecoverNodesInCollection(ctx, collectionID, rgs)
}

func (a *replicaPlacementPolicy) current() bool {
	cfg := &paramtable.Get().QueryCoordCfg
	return a.rawGroups == cfg.ReplicaPlacementCollectionGroups.GetValue() &&
		a.rawAllowed == cfg.ReplicaPlacementResourceGroupAllowlist.GetValue()
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
	if a := m.placementPolicy; a != nil {
		if a.current() {
			return nil
		}
		// Settle uncertain writes before dropping or splitting an old group. No
		// recovery can hold a policy lease while this exclusive lock is held.
		for _, g := range a.groups {
			if g.pending != nil {
				unlock := m.lockGroup(g)
				err := m.resumeCollectionGroupWrite(ctx, g)
				unlock()
				if err != nil {
					return err
				}
			}
		}
	}
	cfg := &paramtable.Get().QueryCoordCfg
	raw, allowed := cfg.ReplicaPlacementCollectionGroups.GetValue(), cfg.ReplicaPlacementResourceGroupAllowlist.GetValue()
	// An empty allowlist disables the policy even while group config is being edited.
	groupConfig := raw
	if strings.Trim(allowed, " ,") == "" {
		groupConfig = "{}"
	}
	a, err := newReplicaPlacementPolicy(groupConfig, strings.Split(allowed, ","))
	if err != nil {
		return err
	}
	a.rawGroups, a.rawAllowed = raw, allowed
	if old := m.placementPolicy; old != nil {
		// Preserve cache and in-flight plans for semantically unchanged groups.
		for id, g := range a.groups {
			if previous := old.groups[id]; previous != nil && previous.name == g.name && slices.Equal(previous.members, g.members) {
				a.groups[id] = previous
			}
		}
	}
	m.placementPolicy = a
	return nil
}

func newReplicaPlacementPolicy(raw string, allowed []string) (*replicaPlacementPolicy, error) {
	a := &replicaPlacementPolicy{groups: make(map[int64]*collectionGroup), allowed: typeutil.NewSet[string]()}
	for _, rg := range allowed {
		if rg = strings.TrimSpace(rg); rg != "" {
			a.allowed.Insert(rg)
		}
	}
	var groups map[string][]int64
	if err := json.Unmarshal([]byte(raw), &groups); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("queryCoord.replicaPlacement.collectionGroups must be a JSON object mapping names to collection ID arrays")
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		ids := groups[name]
		if strings.TrimSpace(name) == "" || len(ids) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("collection group name and members must be nonempty")
		}
		slices.Sort(ids)
		g := &collectionGroup{name: name, members: ids, plans: make(map[string]*collectionGroupPlan)}
		for _, id := range ids {
			if id <= 0 || a.groups[id] != nil {
				return nil, merr.WrapErrParameterInvalidMsg("collection group member %d must be positive and unique", id)
			}
			a.groups[id] = g
		}
	}
	return a, nil
}

func (a *replicaPlacementPolicy) matches(rg string) bool {
	return a.allowed.Contain("*") || a.allowed.Contain(rg)
}

func (a *replicaPlacementPolicy) enabled(replicas []*Replica) bool {
	for _, r := range replicas {
		if a.matches(r.GetResourceGroup()) {
			return true
		}
	}
	return false
}

// lockGroup also locks unloaded members, preventing a concurrent Spawn from
// entering the group after its membership snapshot was taken.
func (m *ReplicaManager) lockGroup(g *collectionGroup) func() {
	for _, id := range g.members {
		m.collLock.Lock(id)
	}
	return func() {
		for i := len(g.members) - 1; i >= 0; i-- {
			m.collLock.Unlock(g.members[i])
		}
	}
}

// groupScopeLocked is read under all member replica locks and collection metadata's read lock.
func (m *Meta) groupScopeLocked(g *collectionGroup, a *replicaPlacementPolicy) map[int64][]int64 {
	scope := make(map[int64][]int64)
	for _, id := range g.members {
		replicas, _ := m.coll2Replicas.Get(id)
		if !a.enabled(replicas) {
			continue
		}
		parts := m.collectionPartitions[id].Collect()
		slices.Sort(parts)
		scope[id] = parts
	}
	return scope
}

func (m *Meta) recoverCollectionGroup(ctx context.Context, g *collectionGroup, a *replicaPlacementPolicy) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	unlock := m.lockGroup(g)
	if err := m.resumeCollectionGroupWrite(ctx, g); err != nil {
		unlock()
		return err
	}
	m.CollectionManager.rwmutex.RLock()
	scope := m.groupScopeLocked(g, a)
	m.CollectionManager.rwmutex.RUnlock()
	unlock()

	refreshErr := g.refreshRows(ctx, m.Broker, scope)

	unlock = m.lockGroup(g)
	defer unlock()
	m.CollectionManager.rwmutex.RLock()
	defer m.CollectionManager.rwmutex.RUnlock()
	if !reflect.DeepEqual(scope, m.groupScopeLocked(g, a)) {
		return merr.WrapErrServiceUnavailable("collection group load scope changed during row refresh")
	}
	if !a.current() {
		return merr.WrapErrServiceUnavailable("replica placement configuration changed during row refresh")
	}
	// Keep the RG snapshot valid through persistence, avoiding a stale node-plan commit.
	m.ResourceManager.rwmutex.RLock()
	defer m.ResourceManager.rwmutex.RUnlock()
	byRG := make(map[string][]*Replica)
	all := make(map[int64][]*Replica)
	for _, id := range g.members {
		replicas, _ := m.coll2Replicas.Get(id)
		all[id] = replicas
		for _, r := range replicas {
			if a.matches(r.GetResourceGroup()) {
				byRG[r.GetResourceGroup()] = append(byRG[r.GetResourceGroup()], r)
			}
		}
	}
	names := make([]string, 0, len(byRG))
	for name := range byRG {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		rg := m.groups[name]
		if rg == nil {
			return merr.WrapErrServiceUnavailable("collection group resource group is unavailable")
		}
		nodes := rg.GetNodes()
		slices.Sort(nodes)
		replicas := byRG[name]
		slices.SortFunc(replicas, func(a, b *Replica) int {
			if a.GetID() < b.GetID() {
				return -1
			}
			if a.GetID() > b.GetID() {
				return 1
			}
			return 0
		})
		plan := &collectionGroupPlan{nodes: make(map[int64][]int64)}
		if g.rows != nil && reflect.DeepEqual(scope, g.scope) {
			key := groupPlanKey(replicas, nodes, g.rows)
			plan = g.plans[name]
			if plan == nil || plan.key != key {
				plan = &collectionGroupPlan{key: key, nodes: assignCollectionGroup(replicas, nodes, g.rows)}
				g.plans[name] = plan
			}
		} else {
			// Without a usable row snapshot recover failures, preserving all
			// surviving RW nodes. Optimize after a successful refresh.
			for _, r := range replicas {
				plan.nodes[r.GetID()] = r.GetRWNodes()
			}
		}
		for _, r := range replicas {
			if r.NeedWaitRGReady() && rg.MissingNumOfNodes() > 0 {
				continue
			}
			updated := applyCollectionGroupPlan(r, all[r.GetCollectionID()], nodes, plan.nodes)
			if updated == nil {
				continue
			}
			// Each write is independently safe: no node can enter a different replica
			// of the collection until its old owner has persisted RO removal.
			if err := m.put(ctx, r.GetCollectionID(), updated); err != nil {
				g.pending = &collectionGroupWrite{before: r, after: updated}
				return err
			}
			all[r.GetCollectionID()], _ = m.coll2Replicas.Get(r.GetCollectionID())
			mlog.Info(ctx, "collection group replica nodes updated", mlog.String("group", g.name), mlog.String("resourceGroup", name), mlog.Int64("replicaID", r.GetID()), mlog.Int64s("rwNodes", updated.GetRWNodes()), mlog.Int64s("roNodes", updated.GetRONodes()))
		}
	}
	return refreshErr
}

// Caller holds the group lock and every member's replica lock. All durable
// replica mutations write a full protobuf. An intervening changed protobuf
// therefore supersedes this uncertain write; memory-only flags do not do so.
func (m *ReplicaManager) resumeCollectionGroupWrite(ctx context.Context, g *collectionGroup) error {
	if pending := g.pending; pending != nil {
		current := m.Get(ctx, pending.before.GetID())
		if current != nil && proto.Equal(current.replicaPB, pending.before.replicaPB) {
			updated := current.CopyForWrite()
			updated.AddRONode(pending.after.GetRONodes()...)
			updated.AddRWNode(pending.after.GetRWNodes()...)
			if updated.RWNodesCount() > 0 {
				updated.SetWaitRGReadyAt(time.Time{})
			}
			if err := m.put(ctx, current.GetCollectionID(), updated.IntoReplica()); err != nil {
				return err
			}
		}
		g.pending = nil
	}
	return nil
}
