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

package rls

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type policySnapshot struct {
	RefreshedAt time.Time
	Policies    []*rlsutil.RowPolicy
}

type collectionState struct {
	mu sync.RWMutex

	// Policy snapshot and refresh state.
	policyGeneration      uint64
	activePolicyRefreshes int
	policyRefreshedAt     time.Time
	policyBackoff         *typeutil.BackoffWithInstant
	policies              map[string]*rlsutil.RowPolicy

	// Principal tags are cached and invalidated independently per principal.
	principalTags          map[string]*principalTagsEntry
	principalRefreshTokens map[string]*principalRefreshToken
}

type principalKey struct {
	collectionID  UniqueID
	principalName string
}

type principalTagsEntry struct {
	refreshedAt time.Time
	// tags is immutable after the entry is published, so cache hits can share it.
	tags map[string]rlsutil.TagValue
}

// Keep this non-zero-sized so separately allocated refresh tokens always have
// distinct addresses.
type principalRefreshToken byte

type manager struct {
	mu sync.RWMutex
	// ponytail: collection states and drop tombstones are unbounded; add an LRU
	// or byte budget if production scale makes this measurable.
	collections map[UniqueID]*collectionState
	// A tombstone prevents a request that resolved schema before DropCollection
	// from recreating RLS cache state after the drop acknowledgement.
	droppedCollections map[UniqueID]struct{}

	lifecycleMu     sync.RWMutex
	coord           CoordClient
	refreshCtx      context.Context
	lifecycleCancel context.CancelFunc

	// Use the native group so canceled callers do not leave one waiter goroutine each.
	policyRefreshes    singleflight.Group
	principalRefreshes singleflight.Group
}

const (
	principalCacheScanInterval  = 10 * time.Minute
	policyRefreshBackoffInitial = time.Second
	policyRefreshBackoffMax     = 30 * time.Second
)

var policyRefreshBackoffConfig = typeutil.BackoffTimerConfig{
	Backoff: typeutil.BackoffConfig{
		InitialInterval: policyRefreshBackoffInitial,
		Multiplier:      2,
		MaxInterval:     policyRefreshBackoffMax,
	},
}

var defaultManager = newManager()

// Init configures the Proxy-local RLS cache for its process lifetime.
func Init(ctx context.Context, coord CoordClient) error {
	return defaultManager.init(ctx, coord)
}

// MarkCollectionDropped removes all cached RLS state and prevents late
// requests from recreating it.
func MarkCollectionDropped(collectionID UniqueID) {
	defaultManager.markCollectionDropped(collectionID)
}

// InvalidatePolicies makes the next RLS check reload the policy snapshot.
func InvalidatePolicies(collectionID UniqueID) {
	defaultManager.invalidatePolicies(collectionID)
}

// InvalidatePrincipalTags evicts one principal's cached tags.
func InvalidatePrincipalTags(collectionID UniqueID, principalName string) {
	defaultManager.invalidatePrincipalTags(collectionID, principalName)
}

func newManager() *manager {
	return &manager{
		collections:        map[UniqueID]*collectionState{},
		droppedCollections: map[UniqueID]struct{}{},
	}
}

func (m *manager) init(ctx context.Context, coord CoordClient) error {
	if m == nil || coord == nil {
		return merr.WrapErrServiceInternalMsg("failed to initialize RLS metadata manager without required dependencies")
	}

	m.lifecycleMu.Lock()
	if m.lifecycleCancel != nil {
		m.lifecycleCancel()
	}
	managerCtx, cancel := context.WithCancel(ctx)
	m.coord = coord
	m.refreshCtx = managerCtx
	m.lifecycleCancel = cancel
	m.lifecycleMu.Unlock()
	if ctx.Done() != nil {
		go m.runPrincipalCacheScanner(managerCtx, principalCacheScanInterval)
	}
	return nil
}

func (m *manager) dependencies() (CoordClient, context.Context) {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()
	return m.coord, m.refreshCtx
}

func (m *manager) getPrincipalTagsEntry(key principalKey) *principalTagsEntry {
	state := m.getCollectionState(key.collectionID)
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.principalTags[key.principalName]
}

func (m *manager) invalidatePolicies(collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.RLock()
	state := m.collections[collectionID]
	if state != nil {
		state.mu.Lock()
		state.policyGeneration++
		state.policyRefreshedAt = time.Time{}
		state.policyBackoff = nil
		state.policies = nil
		state.mu.Unlock()
	}
	m.mu.RUnlock()
	m.policyRefreshes.Forget(policyRefreshKey(collectionID))
}

func (m *manager) invalidatePrincipalTags(collectionID UniqueID, principalName string) {
	if m == nil || collectionID == 0 || principalName == "" {
		return
	}
	key := principalKey{collectionID: collectionID, principalName: principalName}
	m.mu.RLock()
	state := m.collections[collectionID]
	if state != nil {
		state.mu.Lock()
		delete(state.principalTags, principalName)
		delete(state.principalRefreshTokens, principalName)
		state.mu.Unlock()
	}
	m.mu.RUnlock()
	m.principalRefreshes.Forget(principalRefreshKey(key))
}

func (m *manager) markCollectionDropped(collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.Lock()
	state := m.collections[collectionID]
	principals := map[string]struct{}{}
	if state != nil {
		state.mu.RLock()
		for principalName := range state.principalTags {
			principals[principalName] = struct{}{}
		}
		for principalName := range state.principalRefreshTokens {
			principals[principalName] = struct{}{}
		}
		state.mu.RUnlock()
	}
	delete(m.collections, collectionID)
	m.droppedCollections[collectionID] = struct{}{}
	m.mu.Unlock()
	m.policyRefreshes.Forget(policyRefreshKey(collectionID))
	for principalName := range principals {
		m.principalRefreshes.Forget(principalRefreshKey(principalKey{collectionID: collectionID, principalName: principalName}))
	}
}

func (m *manager) runPrincipalCacheScanner(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.expirePrincipalTags(now)
		}
	}
}

func (m *manager) expirePrincipalTags(now time.Time) {
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return
	}
	m.mu.RLock()
	states := make([]*collectionState, 0, len(m.collections))
	for _, state := range m.collections {
		states = append(states, state)
	}
	m.mu.RUnlock()
	for _, state := range states {
		state.mu.Lock()
		for principalName, entry := range state.principalTags {
			if entry == nil || !entry.refreshedAt.Add(refreshTTL).After(now) {
				delete(state.principalTags, principalName)
			}
		}
		state.mu.Unlock()
	}
}

func newCollectionState() *collectionState {
	return &collectionState{
		policies:               map[string]*rlsutil.RowPolicy{},
		principalTags:          map[string]*principalTagsEntry{},
		principalRefreshTokens: map[string]*principalRefreshToken{},
	}
}

func cloneRowPolicy(policy *rlsutil.RowPolicy) *rlsutil.RowPolicy {
	if policy == nil {
		return nil
	}
	clone := *policy
	clone.Actions = slices.Clone(policy.Actions)
	return &clone
}

func (m *manager) getCollectionState(collectionID UniqueID) *collectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.collections[collectionID]
}

func (m *manager) isCollectionDropped(collectionID UniqueID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, dropped := m.droppedCollections[collectionID]
	return dropped
}

func (m *manager) beginPolicyRefresh(collectionID UniqueID) (*collectionState, uint64) {
	m.mu.Lock()
	if _, dropped := m.droppedCollections[collectionID]; dropped {
		m.mu.Unlock()
		return nil, 0
	}
	state := m.collections[collectionID]
	if state == nil {
		state = newCollectionState()
		m.collections[collectionID] = state
	}
	state.mu.Lock()
	state.activePolicyRefreshes++
	generation := state.policyGeneration
	state.mu.Unlock()
	m.mu.Unlock()
	return state, generation
}

func (m *manager) finishPolicyRefresh(collectionID UniqueID, state *collectionState, generation uint64, snapshot *policySnapshot) bool {
	if state == nil {
		return false
	}
	m.mu.Lock()
	state.mu.Lock()
	state.activePolicyRefreshes--
	current := m.collections[collectionID] == state && state.policyGeneration == generation
	if current {
		if snapshot != nil {
			state.setPolicySnapshotLocked(*snapshot)
		} else {
			if state.policyBackoff == nil {
				state.policyBackoff = typeutil.NewBackoffWithInstant(policyRefreshBackoffConfig)
			}
			state.policyBackoff.UpdateInstantWithNextBackOff()
		}
	}
	if m.collections[collectionID] == state && state.activePolicyRefreshes == 0 && state.policyRefreshedAt.IsZero() &&
		state.policyBackoff == nil && len(state.policies) == 0 && len(state.principalTags) == 0 && len(state.principalRefreshTokens) == 0 {
		delete(m.collections, collectionID)
	}
	state.mu.Unlock()
	m.mu.Unlock()
	return current
}

func (m *manager) beginPrincipalRefresh(key principalKey) (*collectionState, *principalRefreshToken) {
	m.mu.RLock()
	state := m.collections[key.collectionID]
	if state == nil {
		m.mu.RUnlock()
		return nil, nil
	}
	state.mu.Lock()
	token := new(principalRefreshToken)
	state.principalRefreshTokens[key.principalName] = token
	state.mu.Unlock()
	m.mu.RUnlock()
	return state, token
}

func (m *manager) finishPrincipalRefresh(key principalKey, state *collectionState, token *principalRefreshToken, entry *principalTagsEntry) bool {
	m.mu.RLock()
	state.mu.Lock()
	current := m.collections[key.collectionID] == state && state.principalRefreshTokens[key.principalName] == token
	if state.principalRefreshTokens[key.principalName] == token {
		delete(state.principalRefreshTokens, key.principalName)
	}
	if current && entry != nil {
		state.principalTags[key.principalName] = entry
	}
	state.mu.Unlock()
	m.mu.RUnlock()
	return current
}

func policyRefreshKey(collectionID UniqueID) string {
	return strconv.FormatInt(collectionID, 10)
}

func principalRefreshKey(key principalKey) string {
	return policyRefreshKey(key.collectionID) + "/" + key.principalName
}

func (state *collectionState) setPolicySnapshotLocked(snapshot policySnapshot) {
	if snapshot.RefreshedAt.IsZero() {
		snapshot.RefreshedAt = time.Now()
	}
	state.policyRefreshedAt = snapshot.RefreshedAt
	state.policyBackoff = nil
	state.policies = make(map[string]*rlsutil.RowPolicy, len(snapshot.Policies))
	for _, policy := range snapshot.Policies {
		state.policies[policy.GetPolicyName()] = cloneRowPolicy(policy)
	}
}
