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

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type snapshotTestCoord struct {
	metadataKind atomic.Int32
}

type metadataTestCoord struct {
	metadataCalls atomic.Int32
	metadataKind  atomic.Int32
	policies      []*rootcoordpb.RLSPolicyInfo
	principalTags map[string]map[string]string
	metadataErr   error
}

type blockingPolicyCoord struct {
	*metadataTestCoord
	started chan struct{}
	release chan struct{}
}

func (c *snapshotTestCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataKind.Store(int32(req.GetKind()))
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			{PolicyName: "tenant"},
		},
		Principals: []*rootcoordpb.RLSPrincipalInfo{
			{PrincipalName: "alice", Tags: map[string]string{"tenant": "acme"}},
		},
	}, nil
}

func (c *metadataTestCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	c.metadataKind.Store(int32(req.GetKind()))
	if c.metadataErr != nil {
		return nil, c.metadataErr
	}
	principals := make([]*rootcoordpb.RLSPrincipalInfo, 0, len(c.principalTags))
	for principalName, tags := range c.principalTags {
		principals = append(principals, &rootcoordpb.RLSPrincipalInfo{
			PrincipalName: principalName,
			Tags:          tags,
		})
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       c.policies,
		Principals:     principals,
	}, nil
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
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_ALL), coord.metadataKind.Load())

	state := m.collections[newCollectionKey(100)]
	require.NotNil(t, state)
	require.Contains(t, state.policies, "tenant")
	require.Equal(t, map[string]string{"tenant": "acme"}, state.principalTags["alice"])
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
	policyDue, principalDue := m.snapshotRefreshDue(100, time.Hour, time.Now())
	require.True(t, policyDue)
	require.True(t, principalDue)
}

func TestManagerEnsureFreshMetadataSkipsFreshSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 10, RefreshedAt: now}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{Version: 10, RefreshedAt: now}))
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
		Policies:    []*rootcoordpb.RLSPolicyInfo{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       10,
		RefreshedAt:   oldRefresh,
		PrincipalTags: map[string]map[string]string{"alice": {"tenant": "old"}},
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
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_ALL), coord.metadataKind.Load())

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(20), state.policyVersion)
	require.Equal(t, int64(20), state.principalTagVersion)
	require.Contains(t, state.policies, "new-policy")
	require.NotContains(t, state.policies, "old-policy")
	require.Equal(t, map[string]string{"tenant": "new"}, state.principalTags["alice"])
	require.True(t, state.policyLastSuccessfulRefresh.After(oldRefresh))
	require.True(t, state.principalTagLastSuccessfulRefresh.After(oldRefresh))
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

func TestManagerTargetedRefreshUpdatesOnlyRequestedSnapshot(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  10,
		Policies: []*rootcoordpb.RLSPolicyInfo{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       10,
		PrincipalTags: map[string]map[string]string{"alice": {"tenant": "old"}},
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
	require.Equal(t, int64(10), state.principalTagVersion)
	require.Equal(t, map[string]string{"tenant": "old"}, state.principalTags["alice"])

	require.NoError(t, m.RefreshPrincipalTagsSnapshot(context.Background(), coord, "db", "coll", 100, 21))
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS), coord.metadataKind.Load())
	require.Equal(t, int64(20), state.policyVersion)
	require.Contains(t, state.policies, "new-policy")
	require.Equal(t, int64(21), state.principalTagVersion)
	require.Equal(t, map[string]string{"tenant": "new"}, state.principalTags["alice"])
}

func TestManagerSnapshotsUseSeparateVersionWatermarks(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 10,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			{PolicyName: "new"},
		},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       5,
		PrincipalTags: map[string]map[string]string{"alice": {"team": "old"}},
	}))

	require.False(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 9,
		Policies: []*rootcoordpb.RLSPolicyInfo{
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
	require.Equal(t, map[string]string{"team": "new"}, state.principalTags["alice"])
}

func TestManagerRemoveCollection(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			{PolicyName: "tenant"},
		},
	}))
	m.removeCollection(context.Background(), 100)
	require.NotContains(t, m.collections, newCollectionKey(100))
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
	policy := &rootcoordpb.RLSPolicyInfo{PolicyName: "tenant"}
	tags := map[string]string{"tenant": "acme"}
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  1,
		Policies: []*rootcoordpb.RLSPolicyInfo{policy},
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
	defer state.mu.RUnlock()
	require.Contains(t, state.policies, "tenant")
	require.NotContains(t, state.policies, "mutated")
	require.Equal(t, "acme", state.principalTags["alice"]["tenant"])
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
