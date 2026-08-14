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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type snapshotTestCoord struct {
	metadataKind   atomic.Int32
	principalCalls atomic.Int32
}

type principalTagsSnapshot struct {
	Version            int64
	RefreshedAt        time.Time
	PrincipalTags      map[string]map[string]string
	TypedPrincipalTags map[string]map[string]rlsutil.TagValue
}

func (m *manager) setRLSPrincipalTagsSnapshot(_ string, collectionID UniqueID, snapshot principalTagsSnapshot) bool {
	if m == nil || collectionID == 0 {
		return false
	}
	if snapshot.RefreshedAt.IsZero() {
		snapshot.RefreshedAt = time.Now()
	}
	state := m.getOrCreateCollectionState(newCollectionKey(collectionID))
	state.mu.Lock()
	state.principalTags = make(map[string]*principalTagsEntry)
	state.mu.Unlock()
	principalTags := snapshot.TypedPrincipalTags
	if principalTags == nil {
		principalTags = make(map[string]map[string]rlsutil.TagValue, len(snapshot.PrincipalTags))
		for principalName, tags := range snapshot.PrincipalTags {
			typed := make(map[string]rlsutil.TagValue, len(tags))
			for key, value := range tags {
				typed[key] = rlsutil.NewStringTagValue(value)
			}
			principalTags[principalName] = typed
		}
	}
	for principalName, tags := range principalTags {
		if principalName == "" {
			continue
		}
		m.setPrincipalTags(principalKey{collectionID: collectionID, principalName: principalName}, &principalTagsEntry{
			refreshedAt: snapshot.RefreshedAt,
			tags:        clonePrincipalTags(tags),
		})
	}
	return true
}

type metadataTestCoord struct {
	metadataCalls  atomic.Int32
	metadataKind   atomic.Int32
	principalCalls atomic.Int32
	policies       []*rootcoordpb.RLSPolicyInfo
	principalTags  map[string]map[string]string
	metadataErr    error
	principalErr   error
}

type blockingPolicyCoord struct {
	*metadataTestCoord
	started chan struct{}
	release chan struct{}
}

type blockingPrincipalCoord struct {
	*metadataTestCoord
	started chan struct{}
	release chan struct{}
}

func (c *blockingPrincipalCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	if req.GetPrincipalName() == "" {
		return c.metadataTestCoord.GetRLSMetadata(context.Background(), req)
	}
	c.principalCalls.Add(1)
	close(c.started)
	<-c.release
	return principalMetadataResponse(req.GetCollectionId(), req.GetPrincipalName(), map[string]string{"tenant": "stale"}), nil
}

func (c *snapshotTestCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataKind.Store(int32(req.GetKind()))
	if req.GetPrincipalName() != "" {
		c.principalCalls.Add(1)
		return principalMetadataResponse(req.GetCollectionId(), req.GetPrincipalName(), map[string]string{"tenant": "acme"}), nil
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			{PolicyName: "tenant"},
		},
	}, nil
}

func (c *metadataTestCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	c.metadataKind.Store(int32(req.GetKind()))
	if c.metadataErr != nil {
		return nil, c.metadataErr
	}
	if req.GetPrincipalName() != "" {
		c.principalCalls.Add(1)
		if c.principalErr != nil {
			return nil, c.principalErr
		}
		tags, ok := c.principalTags[req.GetPrincipalName()]
		if !ok {
			return &rootcoordpb.GetRLSMetadataResponse{
				Status:       merr.Success(),
				CollectionId: req.GetCollectionId(),
			}, nil
		}
		return principalMetadataResponse(req.GetCollectionId(), req.GetPrincipalName(), tags), nil
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       c.policies,
	}, nil
}

func principalMetadataResponse(collectionID int64, principalName string, tags map[string]string) *rootcoordpb.GetRLSMetadataResponse {
	values := make(map[string]rlsutil.TagValue, len(tags))
	for key, value := range tags {
		values[key] = rlsutil.NewStringTagValue(value)
	}
	payload, err := rlsutil.TagsToJSON(values)
	if err != nil {
		panic(err)
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:       merr.Success(),
		CollectionId: collectionID,
		Principals: []*rootcoordpb.RLSPrincipalInfo{{
			CollectionId:  collectionID,
			PrincipalName: principalName,
			Tags:          payload,
		}},
	}
}

func (c *blockingPolicyCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	c.metadataKind.Store(int32(req.GetKind()))
	close(c.started)
	<-c.release
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       c.policies,
	}, nil
}

func TestManagerInitDoesNotLoadMetadata(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{}
	allocCalls := 0
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		allocCalls++
		return 10, nil
	}))
	require.Zero(t, allocCalls)
	require.Zero(t, coord.metadataCalls.Load())
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerEnsureFreshMetadataLoadsMissingSnapshots(t *testing.T) {
	m := newManager()
	coord := &snapshotTestCoord{}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.ensureFreshMetadata(context.Background(), 100))
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[newCollectionKey(100)]
	require.NotNil(t, state)
	require.Contains(t, state.policies, "tenant")
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerEnsureFreshMetadataFailsClosed(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("RLS metadata unavailable"),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))

	err := m.ensureFreshMetadata(context.Background(), 100)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	state := m.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	require.True(t, m.snapshotRefreshDue(100, time.Hour, time.Now()))
}

func TestManagerEnsureFreshMetadataSkipsFreshSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 10, RefreshedAt: now}))
	coord := &metadataTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("refresh should not be called"),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 20, nil
	}))

	require.NoError(t, m.ensureFreshMetadata(context.Background(), 100))
	require.Zero(t, coord.metadataCalls.Load())
}

func TestManagerEnsureFreshMetadataRefreshesExpiredSnapshots(t *testing.T) {
	m := newManager()
	oldRefresh := time.Now().Add(-2 * time.Hour)
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     10,
		RefreshedAt: oldRefresh,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	coord := &metadataTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "new-policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 20, nil
	}))

	require.NoError(t, m.ensureFreshMetadata(context.Background(), 100))
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(20), state.policyVersion)
	require.Contains(t, state.policies, "new-policy")
	require.NotContains(t, state.policies, "old-policy")
	require.True(t, state.policyLastSuccessfulRefresh.After(oldRefresh))
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerEnsureFreshMetadataCoalescesConcurrentRefreshes(t *testing.T) {
	m := newManager()
	coord := &blockingPolicyCoord{
		metadataTestCoord: &metadataTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{{PolicyName: "policy"}},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))

	const concurrency = 8
	errs := make(chan error, concurrency)
	for range concurrency {
		go func() {
			errs <- m.ensureFreshMetadata(context.Background(), 100)
		}()
	}
	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("request-path RLS metadata refresh did not start")
	}
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	close(coord.release)
	for range concurrency {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), coord.metadataCalls.Load())
}

func TestManagerPolicyRefreshDoesNotLoadPrincipalTags(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  10,
		Policies: []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	coord := &metadataTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "new-policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 20))
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(20), state.policyVersion)
	require.Contains(t, state.policies, "new-policy")
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerPrincipalTagsAreLoadedAndCachedPerPrincipal(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		principalTags: map[string]map[string]string{
			"alice": {"tenant": "acme"},
			"bob":   {"tenant": "globex"},
		},
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 1))

	aliceTags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), aliceTags["tenant"])
	require.Equal(t, int32(1), coord.principalCalls.Load())

	aliceTags["tenant"] = rlsutil.NewStringTagValue("mutated")
	cachedAliceTags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), cachedAliceTags["tenant"])
	require.Equal(t, int32(1), coord.principalCalls.Load())

	bobTags, err := m.ensurePrincipalTags(context.Background(), 100, "bob")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("globex"), bobTags["tenant"])
	require.Equal(t, int32(2), coord.principalCalls.Load())
	require.Len(t, m.collections[newCollectionKey(100)].principalTags, 2)
}

func TestManagerPrincipalTagsAreNestedByCollection(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 1, DBName: "db", CollectionName: "coll1"}))
	require.True(t, m.setRLSPolicySnapshot("db", 200, policySnapshot{Version: 1, DBName: "db", CollectionName: "coll2"}))
	m.setPrincipalTags(principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("one")},
	})
	m.setPrincipalTags(principalKey{collectionID: 200, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("two")},
	})

	require.Equal(t, rlsutil.NewStringTagValue("one"), m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}).tags["tenant"])
	require.Equal(t, rlsutil.NewStringTagValue("two"), m.getPrincipalTagsEntry(principalKey{collectionID: 200, principalName: "alice"}).tags["tenant"])
	m.removeCollection(context.Background(), 100)
	require.Nil(t, m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}))
	require.NotNil(t, m.getPrincipalTagsEntry(principalKey{collectionID: 200, principalName: "alice"}))
}

func TestManagerPrincipalLookupUsesCollectionID(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{principalTags: map[string]map[string]string{"alice": {"tenant": "acme"}}}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) { return 1, nil }))
	require.True(t, m.setRLSPolicySnapshot("stale-db", 100, policySnapshot{
		Version:        1,
		DBName:         "stale-db",
		CollectionName: "stale-collection-name",
	}))

	tags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), tags["tenant"])
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS), coord.metadataKind.Load())
}

func TestManagerPrincipalLookupRejectsCollectionMismatch(t *testing.T) {
	m := newManager()
	coord := &managerTestCoordClient{getRLSMetadata: func(context.Context, *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
		return &rootcoordpb.GetRLSMetadataResponse{Status: merr.Success(), CollectionId: 200}, nil
	}}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) { return 1, nil }))
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 1}))

	_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestManagerPrincipalRefreshCoalescesAndInvalidationWins(t *testing.T) {
	m := newManager()
	coord := &blockingPrincipalCoord{
		metadataTestCoord: &metadataTestCoord{},
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 1))

	const concurrency = 8
	results := make(chan error, concurrency)
	for range concurrency {
		go func() {
			_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
			results <- err
		}()
	}
	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("principal refresh did not start")
	}
	require.Equal(t, int32(1), coord.principalCalls.Load())

	invalidated := make(chan struct{})
	go func() {
		m.removePrincipal(context.Background(), 100, "alice")
		close(invalidated)
	}()
	select {
	case <-invalidated:
		t.Fatal("principal invalidation did not wait for in-flight refresh")
	default:
	}

	close(coord.release)
	for range concurrency {
		require.NoError(t, <-results)
	}
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		t.Fatal("principal invalidation did not finish after refresh")
	}
	require.Equal(t, int32(1), coord.principalCalls.Load())
	require.NotContains(t, m.collections[newCollectionKey(100)].principalTags, "alice")
}

func TestManagerPrincipalTTLScannerEvictsExpiredEntries(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		principalTags: map[string]map[string]string{
			"alice": {"tenant": "new"},
			"bob":   {"tenant": "unchanged"},
		},
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 1))
	m.setPrincipalTags(principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now().Add(-2 * time.Hour),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("old")},
	})
	m.setPrincipalTags(principalKey{collectionID: 100, principalName: "bob"}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("unchanged")},
	})

	aliceTags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("old"), aliceTags["tenant"])
	bobTags, err := m.ensurePrincipalTags(context.Background(), 100, "bob")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("unchanged"), bobTags["tenant"])
	require.Zero(t, coord.principalCalls.Load())

	m.expirePrincipalTags(time.Now())
	require.NotContains(t, m.collections[newCollectionKey(100)].principalTags, "alice")
	require.Contains(t, m.collections[newCollectionKey(100)].principalTags, "bob")

	aliceTags, err = m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("new"), aliceTags["tenant"])
	require.Equal(t, int32(1), coord.principalCalls.Load())
}

func TestManagerPrincipalCacheScannerDeletesExpiredEntries(t *testing.T) {
	require.NoError(t, paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.Key, "1"))
	t.Cleanup(func() {
		require.NoError(t, paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.Key))
	})
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 1}))
	m.setPrincipalTags(principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now().Add(-2 * time.Second),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("old")},
	})
	m.dependencyMu.Lock()
	m.validateFreshness = true
	m.dependencyMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.runPrincipalCacheScanner(ctx, time.Millisecond)
	}()
	require.Eventually(t, func() bool {
		return m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}) == nil
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("principal cache scanner did not stop after cancellation")
	}
}

func TestManagerMissingPrincipalIsNotCached(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{principalTags: map[string]map[string]string{}}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 1))

	_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.NotContains(t, m.collections[newCollectionKey(100)].principalTags, "alice")

	coord.principalTags["alice"] = map[string]string{"tenant": "acme"}
	tags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), tags["tenant"])
	require.Equal(t, int32(2), coord.principalCalls.Load())
}

func TestManagerPrincipalInvalidationIsPrincipalScoped(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     1,
		RefreshedAt: now,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "tenant"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		RefreshedAt: now,
		PrincipalTags: map[string]map[string]string{
			"alice": {"tenant": "acme"},
			"bob":   {"tenant": "globex"},
		},
	}))

	m.removePrincipal(context.Background(), 100, "alice")
	require.NotContains(t, m.collections[newCollectionKey(100)].principalTags, "alice")
	require.Contains(t, m.collections[newCollectionKey(100)].principalTags, "bob")
	require.Contains(t, m.collections[newCollectionKey(100)].policies, "tenant")

	m.removePolicyCollection(context.Background(), 100)
	require.Contains(t, m.collections, newCollectionKey(100))
	require.Empty(t, m.collections[newCollectionKey(100)].policies)
	require.Contains(t, m.collections[newCollectionKey(100)].principalTags, "bob")

	m.removeCollection(context.Background(), 100)
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerPolicyAndPrincipalCachesAreIndependent(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 10,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "new"},
		},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       5,
		PrincipalTags: map[string]map[string]string{"alice": {"team": "old"}},
	}))

	require.False(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 9,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "stale"},
		},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       6,
		PrincipalTags: map[string]map[string]string{"alice": {"team": "new"}},
	}))

	state := m.collections[newCollectionKey(100)]
	require.Contains(t, state.policies, "new")
	require.NotContains(t, state.policies, "stale")
	entry := m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"})
	require.NotNil(t, entry)
	require.Equal(t, map[string]rlsutil.TagValue{"team": rlsutil.NewStringTagValue("new")}, entry.tags)
}

func TestManagerRemoveCollection(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "tenant"},
		},
	}))
	m.removeCollection(context.Background(), 100)
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerRemoveDatabaseUsesRLSCollectionState(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db1", 100, policySnapshot{Version: 1, DBName: "db1"}))
	require.True(t, m.setRLSPolicySnapshot("db2", 200, policySnapshot{Version: 1, DBName: "db2"}))
	m.setPrincipalTags(principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{refreshedAt: time.Now()})
	m.setPrincipalTags(principalKey{collectionID: 200, principalName: "bob"}, &principalTagsEntry{refreshedAt: time.Now()})

	m.removeDatabase(context.Background(), "db1")
	require.NotContains(t, m.collections, newCollectionKey(100))
	require.Contains(t, m.collections, newCollectionKey(200))
	require.Contains(t, m.collections[newCollectionKey(200)].principalTags, "bob")
}

func TestManagerRefreshDoesNotRecreateRemovedCollection(t *testing.T) {
	m := newManager()
	coord := &blockingPolicyCoord{
		metadataTestCoord: &metadataTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{{PolicyName: "stale-policy"}},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 10)
	}()

	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("RLS snapshot refresh did not start")
	}
	require.Contains(t, m.collections, newCollectionKey(100))
	removed := make(chan struct{})
	go func() {
		m.removeCollection(context.Background(), 100)
		close(removed)
	}()
	select {
	case <-removed:
		t.Fatal("RLS cache invalidation did not wait for the in-flight refresh")
	default:
	}
	close(coord.release)
	require.NoError(t, <-done)
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("RLS cache invalidation did not finish after the refresh completed")
	}
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerSnapshotsOwnImmutableData(t *testing.T) {
	m := newManager()
	policy := &rlsutil.RowPolicy{PolicyName: "tenant"}
	tags := map[string]string{"tenant": "acme"}
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  1,
		Policies: []*rlsutil.RowPolicy{policy},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       1,
		PrincipalTags: map[string]map[string]string{"alice": tags},
	}))

	policy.PolicyName = "mutated"
	tags["tenant"] = "mutated"
	state := m.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	state.mu.RLock()
	require.Contains(t, state.policies, "tenant")
	require.NotContains(t, state.policies, "mutated")
	state.mu.RUnlock()
	entry := m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"})
	require.NotNil(t, entry)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), entry.tags["tenant"])
}

func TestManagerCollectionStateLocksAreIndependent(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 1}))
	state := m.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	state.mu.Lock()
	defer state.mu.Unlock()

	done := make(chan bool, 1)
	go func() {
		done <- m.setRLSPolicySnapshot("db", 200, policySnapshot{Version: 1})
	}()
	select {
	case updated := <-done:
		require.True(t, updated)
	case <-time.After(time.Second):
		t.Fatal("updating one collection waited for another collection's state lock")
	}
}
