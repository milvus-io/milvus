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
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type policySnapshot struct {
	Version     int64
	RefreshedAt time.Time
	Policies    []*rootcoordpb.RLSPolicyInfo
}

type principalTagsSnapshot struct {
	Version       int64
	RefreshedAt   time.Time
	PrincipalTags map[string]map[string]string
}

type SnapshotVersionAllocator func(ctx context.Context) (uint64, error)

type SnapshotManager interface {
	Init(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) error
	RefreshPolicySnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error
	RefreshPrincipalTagsSnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error
}

type collectionState struct {
	mu                                sync.RWMutex
	policyVersion                     int64
	principalTagVersion               int64
	policyLastSuccessfulRefresh       time.Time
	principalTagLastSuccessfulRefresh time.Time
	policies                          map[string]*rootcoordpb.RLSPolicyInfo
	principalTags                     map[string]map[string]string
}

type collectionKey struct {
	collectionID UniqueID
}

type manager struct {
	mu                sync.RWMutex
	collections       map[collectionKey]*collectionState
	dependencyMu      sync.RWMutex
	coord             CoordClient
	allocVersion      SnapshotVersionAllocator
	metadataRefreshes conc.Singleflight[struct{}]
	refreshLocks      *lock.KeyLock[UniqueID]
}

var defaultManager = newManager()

func DefaultManager() SnapshotManager {
	return defaultManager
}

func RemoveCollection(ctx context.Context, collectionID UniqueID) {
	defaultManager.removeCollection(ctx, collectionID)
}

func newManager() *manager {
	return &manager{
		collections:  map[collectionKey]*collectionState{},
		refreshLocks: lock.NewKeyLock[UniqueID](),
	}
}

func (m *manager) configure(coord CoordClient, allocVersion SnapshotVersionAllocator) {
	m.dependencyMu.Lock()
	defer m.dependencyMu.Unlock()
	m.coord = coord
	m.allocVersion = allocVersion
}

func (m *manager) refreshDependencies() (CoordClient, SnapshotVersionAllocator) {
	m.dependencyMu.RLock()
	defer m.dependencyMu.RUnlock()
	return m.coord, m.allocVersion
}

func (m *manager) ensureFreshMetadata(ctx context.Context, collectionID UniqueID) error {
	if m == nil || collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS metadata freshness with invalid manager or collection id")
	}
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS metadata freshness with invalid TTL %s", refreshTTL)
	}
	m.refreshLocks.RLock(collectionID)
	defer m.refreshLocks.RUnlock(collectionID)
	if policyDue, principalTagsDue := m.snapshotRefreshDue(collectionID, refreshTTL, time.Now()); !policyDue && !principalTagsDue {
		return nil
	}

	_, err, _ := m.metadataRefreshes.Do(strconv.FormatInt(collectionID, 10), func() (struct{}, error) {
		if policyDue, principalTagsDue := m.snapshotRefreshDue(collectionID, refreshTTL, time.Now()); !policyDue && !principalTagsDue {
			return struct{}{}, nil
		}
		coord, allocVersion := m.refreshDependencies()
		if coord == nil || allocVersion == nil {
			return struct{}{}, merr.WrapErrServiceInternalMsg("failed to refresh RLS metadata without required dependencies")
		}
		version, err := allocVersion(ctx)
		if err != nil {
			return struct{}{}, merr.Wrap(err, "failed to allocate RLS metadata refresh version")
		}
		if err := m.refreshSnapshotsUnlocked(ctx, coord, "", "", collectionID, version, true, true); err != nil {
			return struct{}{}, merr.Wrap(err, "failed to refresh expired RLS metadata")
		}
		return struct{}{}, nil
	})
	return err
}

func (m *manager) snapshotRefreshDue(collectionID UniqueID, refreshTTL time.Duration, now time.Time) (bool, bool) {
	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return true, true
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	policyDue := state.policyLastSuccessfulRefresh.IsZero() || !state.policyLastSuccessfulRefresh.Add(refreshTTL).After(now)
	principalDue := state.principalTagLastSuccessfulRefresh.IsZero() || !state.principalTagLastSuccessfulRefresh.Add(refreshTTL).After(now)
	return policyDue, principalDue
}

func (m *manager) setRLSPolicySnapshot(_ string, collectionID UniqueID, snapshot policySnapshot) bool {
	if m == nil || collectionID == 0 {
		return false
	}
	return m.getOrCreateCollectionState(newCollectionKey(collectionID)).setRLSPolicySnapshot(snapshot)
}

func (state *collectionState) setRLSPolicySnapshot(snapshot policySnapshot) bool {
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if isStaleSnapshotVersion(snapshot.Version, state.policyVersion) {
		return false
	}
	if snapshot.RefreshedAt.IsZero() {
		snapshot.RefreshedAt = time.Now()
	}
	state.policyVersion = snapshot.Version
	state.policyLastSuccessfulRefresh = snapshot.RefreshedAt
	state.policies = map[string]*rootcoordpb.RLSPolicyInfo{}
	for _, policy := range snapshot.Policies {
		if policy == nil || policy.GetPolicyName() == "" {
			continue
		}
		state.policies[policy.GetPolicyName()] = proto.Clone(policy).(*rootcoordpb.RLSPolicyInfo)
	}
	return true
}

func (m *manager) setRLSPrincipalTagsSnapshot(_ string, collectionID UniqueID, snapshot principalTagsSnapshot) bool {
	if m == nil || collectionID == 0 {
		return false
	}
	return m.getOrCreateCollectionState(newCollectionKey(collectionID)).setRLSPrincipalTagsSnapshot(snapshot)
}

func (state *collectionState) setRLSPrincipalTagsSnapshot(snapshot principalTagsSnapshot) bool {
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if isStaleSnapshotVersion(snapshot.Version, state.principalTagVersion) {
		return false
	}
	if snapshot.RefreshedAt.IsZero() {
		snapshot.RefreshedAt = time.Now()
	}
	state.principalTagVersion = snapshot.Version
	state.principalTagLastSuccessfulRefresh = snapshot.RefreshedAt
	state.principalTags = map[string]map[string]string{}
	for principalName, tags := range snapshot.PrincipalTags {
		if principalName == "" {
			continue
		}
		state.principalTags[principalName] = clonePrincipalTags(tags)
	}
	return true
}

func (m *manager) removeCollection(_ context.Context, collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.refreshLocks.Lock(collectionID)
	defer m.refreshLocks.Unlock(collectionID)
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.collections, newCollectionKey(collectionID))
}

func newCollectionState() *collectionState {
	return &collectionState{
		policies:      map[string]*rootcoordpb.RLSPolicyInfo{},
		principalTags: map[string]map[string]string{},
	}
}

func newCollectionKey(collectionID UniqueID) collectionKey {
	return collectionKey{
		collectionID: collectionID,
	}
}

func (m *manager) getCollectionState(key collectionKey) *collectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.collections[key]
}

func (m *manager) getOrCreateCollectionState(key collectionKey) *collectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.collections[key]
	if state == nil {
		state = newCollectionState()
		m.collections[key] = state
	}
	return state
}

func clonePrincipalTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	cloned := make(map[string]string, len(tags))
	for key, value := range tags {
		cloned[key] = value
	}
	return cloned
}

func isStaleSnapshotVersion(incomingVersion int64, currentVersion int64) bool {
	if currentVersion == 0 {
		return false
	}
	// Version 0 is used by startup bootstrap and must not overwrite a snapshot
	// delivered by a timestamped invalidation.
	if incomingVersion == 0 {
		return true
	}
	return incomingVersion <= currentVersion
}
