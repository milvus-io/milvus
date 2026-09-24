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
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type collectionState struct {
	mu sync.RWMutex
	// Exact revisions deduplicate retries without assuming callback order.
	invalidationRevisions     map[typeutil.Timestamp]struct{}
	invalidationRevisionOrder []typeutil.Timestamp

	// Policy snapshot and refresh state.
	// policyCompileMu coalesces cache misses without blocking metadata updates.
	policyCompileMu   sync.Mutex
	policyGeneration  uint64
	policyRefreshedAt time.Time
	policyBackoff     *typeutil.BackoffWithInstant
	// Policy snapshots are immutable after publication, so compilation may use
	// copied pointers without holding the collection lock.
	policies map[string]*rlsutil.RowPolicy
	compiled map[compiledKey]*compiledCacheEntry

	// Principal tags are cached and invalidated independently per principal.
	// Separate queues let miss churn evict misses first without making hits take a write lock.
	principalTags               map[string]*principalTagsEntry
	positivePrincipalCacheOrder *list.List
	negativePrincipalCacheOrder *list.List
	principalCacheBytes         int64
	principalRefreshTokens      map[string]principalRefreshToken
	principalRefreshSequence    principalRefreshToken
	principalBackoffs           map[string]*principalBackoffEntry
	principalBackoffOrder       *list.List
	principalBackoffBytes       int64
}

type principalKey struct {
	collectionID  UniqueID
	principalName string
}

type principalTagsEntry struct {
	refreshedAt time.Time
	// tags is immutable after the entry is published, so cache hits can share it.
	tags         map[string]rlsutil.TagValue
	missing      bool
	cacheBytes   int64
	orderElement *list.Element
}

type principalBackoffEntry struct {
	backoff       *typeutil.BackoffWithInstant
	lastFailureAt time.Time
	cacheBytes    int64
	orderElement  *list.Element
}

type principalRefreshToken uint64

type manager struct {
	mu sync.RWMutex

	// Per-collection cache state.
	// ponytail: collection states and drop tombstones are unbounded; add an LRU
	// or byte budget if production scale makes this measurable.
	collections map[UniqueID]*collectionState
	// A tombstone prevents a request that resolved schema before DropCollection
	// from recreating RLS cache state after the drop acknowledgement.
	droppedCollections map[UniqueID]struct{}

	// Immutable process-lifetime refresh dependencies.
	coord      CoordClient
	refreshCtx context.Context

	// Independent policy and principal refresh coalescing.
	policyRefreshes       singleflight.Group
	principalRefreshes    singleflight.Group
	principalRefreshSlots chan struct{}
}

const (
	principalCacheScanInterval = 10 * time.Minute
	// ponytail: fixed process-wide safety bounds; make them configurable only
	// if cold-miss load tests show that deployments need different values.
	metadataRefreshTimeout          = 30 * time.Second
	maxConcurrentPrincipalRefreshes = 64
	metadataRefreshBackoffInitial   = time.Second
	metadataRefreshBackoffMax       = 30 * time.Second
	// Evicting an old revision can only cause a redundant invalidation.
	rememberedInvalidationLimit = 64
)

var metadataRefreshBackoffConfig = typeutil.BackoffTimerConfig{
	Backoff: typeutil.BackoffConfig{
		InitialInterval: metadataRefreshBackoffInitial,
		Multiplier:      2,
		MaxInterval:     metadataRefreshBackoffMax,
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
func InvalidatePolicies(collectionID UniqueID, revision typeutil.Timestamp) {
	defaultManager.invalidatePolicies(collectionID, revision)
}

// InvalidatePrincipalTags evicts one principal's cached tags.
func InvalidatePrincipalTags(collectionID UniqueID, principalName string, revision typeutil.Timestamp) {
	defaultManager.invalidatePrincipalTags(collectionID, principalName, revision)
}

func newManager() *manager {
	return &manager{
		collections:           map[UniqueID]*collectionState{},
		droppedCollections:    map[UniqueID]struct{}{},
		principalRefreshSlots: make(chan struct{}, maxConcurrentPrincipalRefreshes),
	}
}

func (m *manager) init(ctx context.Context, coord CoordClient) error {
	if m == nil || coord == nil {
		return merr.WrapErrServiceInternalMsg("failed to initialize RLS metadata manager without required dependencies")
	}

	m.coord = coord
	m.refreshCtx = ctx
	if ctx.Done() != nil {
		go m.runPrincipalCacheScanner(ctx, principalCacheScanInterval)
	}
	return nil
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

func principalTagsEntryFresh(entry *principalTagsEntry, refreshTTL time.Duration, now time.Time) bool {
	return entry != nil && entry.refreshedAt.Add(refreshTTL).After(now)
}

func (m *manager) invalidatePolicies(collectionID UniqueID, revision typeutil.Timestamp) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.Lock()
	if _, dropped := m.droppedCollections[collectionID]; dropped {
		m.mu.Unlock()
		return
	}
	state := m.collections[collectionID]
	if state == nil {
		if revision == 0 {
			m.mu.Unlock()
			return
		}
		state = newCollectionState()
		m.collections[collectionID] = state
	}
	state.mu.Lock()
	if !state.rememberInvalidationRevision(revision) {
		state.mu.Unlock()
		m.mu.Unlock()
		return
	}
	state.policyGeneration++
	state.policyRefreshedAt = time.Time{}
	state.policyBackoff = nil
	state.policies = nil
	state.compiled = nil
	state.mu.Unlock()
	m.mu.Unlock()
}

func (m *manager) invalidatePrincipalTags(collectionID UniqueID, principalName string, revision typeutil.Timestamp) {
	if m == nil || collectionID == 0 || principalName == "" {
		return
	}
	m.mu.Lock()
	if _, dropped := m.droppedCollections[collectionID]; dropped {
		m.mu.Unlock()
		return
	}
	state := m.collections[collectionID]
	if state == nil {
		if revision == 0 {
			m.mu.Unlock()
			return
		}
		state = newCollectionState()
		m.collections[collectionID] = state
	}
	state.mu.Lock()
	if !state.rememberInvalidationRevision(revision) {
		state.mu.Unlock()
		m.mu.Unlock()
		return
	}
	state.removePrincipalTagsLocked(principalName)
	delete(state.principalRefreshTokens, principalName)
	state.removePrincipalBackoffLocked(principalName)
	state.mu.Unlock()
	m.mu.Unlock()
}

func (m *manager) markCollectionDropped(collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.mu.Lock()
	delete(m.collections, collectionID)
	m.droppedCollections[collectionID] = struct{}{}
	m.mu.Unlock()
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
		state.expirePrincipalTagsLocked(state.positivePrincipalCacheOrder, refreshTTL, now)
		state.expirePrincipalTagsLocked(state.negativePrincipalCacheOrder, refreshTTL, now)
		state.expirePrincipalBackoffsLocked(refreshTTL, now)
		state.trimPrincipalStateLocked("")
		state.mu.Unlock()
	}
}

func newCollectionState() *collectionState {
	return &collectionState{
		invalidationRevisions:       map[typeutil.Timestamp]struct{}{},
		policies:                    map[string]*rlsutil.RowPolicy{},
		principalTags:               map[string]*principalTagsEntry{},
		positivePrincipalCacheOrder: list.New(),
		negativePrincipalCacheOrder: list.New(),
		principalRefreshTokens:      map[string]principalRefreshToken{},
		principalBackoffs:           map[string]*principalBackoffEntry{},
		principalBackoffOrder:       list.New(),
	}
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
	generation := state.policyGeneration
	state.mu.Unlock()
	m.mu.Unlock()
	return state, generation
}

func (m *manager) finishPolicyRefresh(collectionID UniqueID, state *collectionState, generation uint64, policies map[string]*rlsutil.RowPolicy) bool {
	if state == nil {
		return false
	}
	m.mu.RLock()
	state.mu.Lock()
	current := m.collections[collectionID] == state && state.policyGeneration == generation
	if current {
		if policies != nil {
			state.setPreparedPolicySnapshotLocked(time.Now(), policies)
		} else {
			if state.policyBackoff == nil {
				state.policyBackoff = typeutil.NewBackoffWithInstant(metadataRefreshBackoffConfig)
			}
			state.policyBackoff.UpdateInstantWithNextBackOff()
		}
	}
	state.mu.Unlock()
	m.mu.RUnlock()
	return current
}

func (m *manager) policyRefreshCurrent(collectionID UniqueID, state *collectionState, generation uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state.mu.RLock()
	defer state.mu.RUnlock()
	return m.collections[collectionID] == state && state.policyGeneration == generation
}

func (m *manager) startPrincipalRefresh(
	key principalKey,
	refreshTTL time.Duration,
	refresh func(*collectionState, principalRefreshToken) (any, error),
) (*principalTagsEntry, <-chan singleflight.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state := m.collections[key.collectionID]
	if state == nil {
		return nil, nil, merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed during principal refresh", key.collectionID)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	now := time.Now()
	if entry := state.principalTags[key.principalName]; entry != nil {
		if principalTagsEntryFresh(entry, refreshTTL, now) {
			return entry, nil, nil
		}
		state.removePrincipalTagsLocked(key.principalName)
	}
	if entry := state.principalBackoffs[key.principalName]; entry != nil {
		if entry.backoff != nil && now.Before(entry.backoff.NextInstant()) {
			return nil, nil, merr.WrapErrServiceUnavailableMsg(
				"RLS principal metadata refresh is backing off for collection %d principal %q",
				key.collectionID, key.principalName,
			)
		}
	}
	token, refreshing := state.principalRefreshTokens[key.principalName]
	ownsSlot := false
	if !refreshing {
		select {
		case m.principalRefreshSlots <- struct{}{}:
			ownsSlot = true
		default:
			return nil, nil, merr.WrapErrServiceUnavailableMsg(
				"RLS principal metadata refresh capacity is exhausted for collection %d principal %q",
				key.collectionID, key.principalName,
			)
		}
		state.principalRefreshSequence++
		token = state.principalRefreshSequence
		state.principalRefreshTokens[key.principalName] = token
	}
	resultCh := m.principalRefreshes.DoChan(principalRefreshKey(key, token), func() (any, error) {
		if ownsSlot {
			defer func() { <-m.principalRefreshSlots }()
		}
		return refresh(state, token)
	})
	return nil, resultCh, nil
}

func (m *manager) finishPrincipalRefresh(key principalKey, state *collectionState, token principalRefreshToken, entry *principalTagsEntry, success bool) (bool, error) {
	m.mu.RLock()
	state.mu.Lock()
	var err error
	currentToken, refreshing := state.principalRefreshTokens[key.principalName]
	current := m.collections[key.collectionID] == state && refreshing && currentToken == token
	if refreshing && currentToken == token {
		delete(state.principalRefreshTokens, key.principalName)
	}
	if current {
		if success {
			state.removePrincipalBackoffLocked(key.principalName)
			if entry != nil {
				entry.refreshedAt = time.Now()
				err = state.putPrincipalTagsLocked(key.principalName, entry)
				if err != nil {
					state.recordPrincipalRefreshFailureLocked(key.principalName)
				}
			}
		} else {
			state.recordPrincipalRefreshFailureLocked(key.principalName)
		}
	}
	state.mu.Unlock()
	m.mu.RUnlock()
	return current, err
}

func (m *manager) principalRefreshCurrent(key principalKey, state *collectionState, token principalRefreshToken) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state.mu.RLock()
	defer state.mu.RUnlock()
	currentToken, refreshing := state.principalRefreshTokens[key.principalName]
	return m.collections[key.collectionID] == state && refreshing && currentToken == token
}

func policyRefreshKey(collectionID UniqueID, generation uint64) string {
	return fmt.Sprintf("%d/%d", collectionID, generation)
}

func principalRefreshKey(key principalKey, token principalRefreshToken) string {
	return fmt.Sprintf("%d/%s/%d", key.collectionID, key.principalName, token)
}

// putPrincipalTagsLocked publishes an entry and keeps the per-collection
// principal cache within its count and logical-payload byte limits.
func (state *collectionState) putPrincipalTagsLocked(principalName string, entry *principalTagsEntry) error {
	state.removePrincipalTagsLocked(principalName)
	if entry == nil {
		return nil
	}
	cacheBytes, err := rlsutil.PrincipalTagsSize(principalName, entry.tags)
	if err != nil {
		return err
	}
	entry.cacheBytes = cacheBytes
	maxBytes := paramtable.Get().ProxyCfg.RLSMaxPrincipalCacheBytes.GetAsInt64()
	if entry.cacheBytes <= maxBytes {
		order := state.positivePrincipalCacheOrder
		if entry.missing {
			order = state.negativePrincipalCacheOrder
		}
		entry.orderElement = order.PushBack(principalName)
		state.principalTags[principalName] = entry
		state.principalCacheBytes += entry.cacheBytes
	}
	state.trimPrincipalStateLocked("")
	return nil
}

func (state *collectionState) removePrincipalTagsLocked(principalName string) {
	entry := state.principalTags[principalName]
	if entry == nil {
		delete(state.principalTags, principalName)
		return
	}
	delete(state.principalTags, principalName)
	state.principalCacheBytes -= entry.cacheBytes
	if entry.orderElement != nil {
		order := state.positivePrincipalCacheOrder
		if entry.missing {
			order = state.negativePrincipalCacheOrder
		}
		order.Remove(entry.orderElement)
		entry.orderElement = nil
	}
}

func (state *collectionState) trimPrincipalStateLocked(protectedBackoff string) {
	maxEntries := paramtable.Get().ProxyCfg.RLSMaxPrincipalCacheEntries.GetAsInt()
	maxBytes := paramtable.Get().ProxyCfg.RLSMaxPrincipalCacheBytes.GetAsInt64()
	for len(state.principalTags)+len(state.principalBackoffs) > maxEntries || state.principalCacheBytes+state.principalBackoffBytes > maxBytes {
		if oldest := state.principalBackoffOrder.Front(); oldest != nil && oldest.Value.(string) != protectedBackoff {
			state.removePrincipalBackoffLocked(oldest.Value.(string))
			continue
		}
		oldest := state.negativePrincipalCacheOrder.Front()
		if oldest == nil {
			oldest = state.positivePrincipalCacheOrder.Front()
		}
		if oldest != nil {
			state.removePrincipalTagsLocked(oldest.Value.(string))
			continue
		}
		// The protected backoff itself cannot fit even after all other state was
		// evicted, so drop it to preserve the configured hard bound.
		if oldest := state.principalBackoffOrder.Front(); oldest != nil {
			state.removePrincipalBackoffLocked(oldest.Value.(string))
			continue
		}
		return
	}
}

func (state *collectionState) recordPrincipalRefreshFailureLocked(principalName string) {
	entry := state.principalBackoffs[principalName]
	if entry == nil {
		entry = &principalBackoffEntry{
			backoff:    typeutil.NewBackoffWithInstant(metadataRefreshBackoffConfig),
			cacheBytes: int64(len(principalName)),
		}
		entry.orderElement = state.principalBackoffOrder.PushBack(principalName)
		state.principalBackoffs[principalName] = entry
		state.principalBackoffBytes += entry.cacheBytes
	} else if entry.orderElement != nil {
		state.principalBackoffOrder.MoveToBack(entry.orderElement)
	}
	entry.lastFailureAt = time.Now()
	entry.backoff.UpdateInstantWithNextBackOff()
	state.trimPrincipalStateLocked(principalName)
}

func (state *collectionState) removePrincipalBackoffLocked(principalName string) {
	entry := state.principalBackoffs[principalName]
	delete(state.principalBackoffs, principalName)
	if entry == nil {
		return
	}
	state.principalBackoffBytes -= entry.cacheBytes
	if entry.orderElement != nil {
		state.principalBackoffOrder.Remove(entry.orderElement)
		entry.orderElement = nil
	}
}

func (state *collectionState) expirePrincipalTagsLocked(order *list.List, refreshTTL time.Duration, now time.Time) {
	for oldest := order.Front(); oldest != nil; oldest = order.Front() {
		principalName := oldest.Value.(string)
		entry := state.principalTags[principalName]
		if principalTagsEntryFresh(entry, refreshTTL, now) {
			return
		}
		state.removePrincipalTagsLocked(principalName)
	}
}

func (state *collectionState) expirePrincipalBackoffsLocked(refreshTTL time.Duration, now time.Time) {
	for oldest := state.principalBackoffOrder.Front(); oldest != nil; oldest = state.principalBackoffOrder.Front() {
		principalName := oldest.Value.(string)
		entry := state.principalBackoffs[principalName]
		if entry != nil && entry.lastFailureAt.Add(refreshTTL).After(now) {
			return
		}
		state.removePrincipalBackoffLocked(principalName)
	}
}

func (state *collectionState) rememberInvalidationRevision(revision typeutil.Timestamp) bool {
	if revision == 0 {
		return true
	}
	if _, duplicate := state.invalidationRevisions[revision]; duplicate {
		return false
	}
	if len(state.invalidationRevisionOrder) == rememberedInvalidationLimit {
		delete(state.invalidationRevisions, state.invalidationRevisionOrder[0])
		state.invalidationRevisionOrder = state.invalidationRevisionOrder[1:]
	}
	state.invalidationRevisions[revision] = struct{}{}
	state.invalidationRevisionOrder = append(state.invalidationRevisionOrder, revision)
	return true
}

func (state *collectionState) setPreparedPolicySnapshotLocked(refreshedAt time.Time, policies map[string]*rlsutil.RowPolicy) {
	state.policyGeneration++
	state.policyRefreshedAt = refreshedAt
	state.policyBackoff = nil
	state.compiled = nil
	state.policies = policies
}
