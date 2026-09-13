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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type snapshotTestCoord struct {
	metadataKind   atomic.Int32
	principalCalls atomic.Int32
	tags           string
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

type managerTestCoordClient struct {
	getRLSMetadata func(context.Context, *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error)
}

var _ CoordClient = (*managerTestCoordClient)(nil)

func (c *managerTestCoordClient) GetRLSMetadata(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	return c.getRLSMetadata(ctx, req)
}

func getOrCreateCollectionStateForTest(m *manager, collectionID UniqueID) *collectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dropped := m.droppedCollections[collectionID]; dropped {
		return nil
	}
	state := m.collections[collectionID]
	if state == nil {
		state = newCollectionState()
		m.collections[collectionID] = state
	}
	return state
}

func setPolicySnapshotForTest(m *manager, collectionID UniqueID, snapshot policySnapshot) bool {
	if m == nil || collectionID == 0 {
		return false
	}
	for _, policy := range snapshot.Policies {
		if policy == nil || policy.GetPolicyName() == "" {
			return false
		}
	}
	state := getOrCreateCollectionStateForTest(m, collectionID)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.setPolicySnapshotLocked(snapshot)
	return true
}

func setPrincipalTagsForTest(m *manager, key principalKey, entry *principalTagsEntry) bool {
	state := m.getCollectionState(key.collectionID)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.principalRefreshTokens, key.principalName)
	state.principalTags[key.principalName] = entry
	return true
}

func setManagerTestPrincipalTags(m *manager, collectionID UniqueID, principalName string, tags map[string]rlsutil.TagValue) bool {
	getOrCreateCollectionStateForTest(m, collectionID)
	return setPrincipalTagsForTest(m, principalKey{collectionID: collectionID, principalName: principalName}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        rlsutil.CloneTags(tags),
	})
}

type blockingPolicyCoord struct {
	*metadataTestCoord
	started     chan struct{}
	release     chan struct{}
	seenContext chan context.Context
}

type blockingPrincipalCoord struct {
	*metadataTestCoord
	started     chan struct{}
	release     chan struct{}
	seenContext chan context.Context
}

func validPolicyInfo(name string) *rootcoordpb.RLSPolicyInfo {
	return &rootcoordpb.RLSPolicyInfo{
		CollectionId: 100,
		PolicyId:     1,
		PolicyName:   name,
		PolicyType:   milvuspb.RowPolicyType_RowPolicyTypePermissive,
		Actions:      []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query},
		UsingExpr:    "true",
	}
}

func (c *blockingPrincipalCoord) GetRLSMetadata(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	if req.GetPrincipalName() == "" {
		return c.metadataTestCoord.GetRLSMetadata(ctx, req)
	}
	c.principalCalls.Add(1)
	if c.seenContext != nil {
		c.seenContext <- ctx
	}
	close(c.started)
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return principalMetadataResponse(req.GetCollectionId(), req.GetPrincipalName(), map[string]string{"tenant": "stale"}), nil
}

func (c *snapshotTestCoord) GetRLSMetadata(_ context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataKind.Store(int32(req.GetKind()))
	if req.GetPrincipalName() != "" {
		c.principalCalls.Add(1)
		tags := c.tags
		if tags == "" {
			tags = `{"tenant":"acme","level":3,"score":0.75}`
		}
		return principalMetadataJSONResponse(req.GetCollectionId(), req.GetPrincipalName(), tags), nil
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			validPolicyInfo("tenant"),
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
	return principalMetadataJSONResponse(collectionID, principalName, payload)
}

func principalMetadataJSONResponse(collectionID int64, principalName string, tags string) *rootcoordpb.GetRLSMetadataResponse {
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:       merr.Success(),
		CollectionId: collectionID,
		Principals: []*rootcoordpb.RLSPrincipalInfo{{
			CollectionId:  collectionID,
			PrincipalName: principalName,
			Tags:          tags,
		}},
	}
}

func (c *blockingPolicyCoord) GetRLSMetadata(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest, _ ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	c.metadataKind.Store(int32(req.GetKind()))
	if c.seenContext != nil {
		c.seenContext <- ctx
	}
	close(c.started)
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	require.NoError(t, m.init(context.Background(), coord))
	require.Zero(t, coord.metadataCalls.Load())
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerFailsClosedBeforeInitialization(t *testing.T) {
	m := newManager()

	require.ErrorIs(t, m.ensurePoliciesFresh(context.Background(), 100), merr.ErrServiceInternal)
	_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestManagerEnsureFreshMetadataLoadsMissingSnapshots(t *testing.T) {
	m := newManager()
	coord := &snapshotTestCoord{}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[100]
	require.NotNil(t, state)
	require.Contains(t, state.policies, "tenant")
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerPrincipalMetadataPreservesTypes(t *testing.T) {
	m := newManager()
	coord := &snapshotTestCoord{}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))

	tags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, map[string]rlsutil.TagValue{
		"tenant": rlsutil.NewStringTagValue("acme"),
		"level":  rlsutil.NewInt64TagValue(3),
		"score":  rlsutil.NewDoubleTagValue(0.75),
	}, tags)
}

func TestManagerRejectsMalformedPrincipalMetadata(t *testing.T) {
	m := newManager()
	coord := &snapshotTestCoord{tags: `{"tenant":`}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))

	_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestManagerEnsureFreshMetadataFailsClosed(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("RLS metadata unavailable"),
	}
	require.NoError(t, m.init(context.Background(), coord))

	err := m.ensurePoliciesFresh(context.Background(), 100)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	require.ErrorIs(t, m.ensurePoliciesFresh(context.Background(), 100), merr.ErrServiceUnavailable)
	require.Equal(t, int32(1), coord.metadataCalls.Load(), "refreshes during backoff must not issue another RPC")
	require.True(t, m.policyRefreshDue(100, time.Hour, time.Now()))

	coord.metadataErr = nil
	coord.policies = []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("recovered")}
	m.invalidatePolicies(100)
	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))
	require.Equal(t, int32(2), coord.metadataCalls.Load())
	require.Contains(t, m.getCollectionState(100).policies, "recovered")
}

func TestManagerEnsureFreshMetadataSkipsFreshSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{RefreshedAt: now}))
	coord := &metadataTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("refresh should not be called"),
	}
	require.NoError(t, m.init(context.Background(), coord))

	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))
	require.Zero(t, coord.metadataCalls.Load())
}

func TestManagerEnsureFreshMetadataRefreshesExpiredSnapshots(t *testing.T) {
	m := newManager()
	oldRefresh := time.Now().Add(-2 * time.Hour)
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		RefreshedAt: oldRefresh,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	coord := &metadataTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("new-policy")},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	require.NoError(t, m.init(context.Background(), coord))

	require.NoError(t, m.ensurePoliciesFresh(context.Background(), 100))
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[100]
	require.Contains(t, state.policies, "new-policy")
	require.NotContains(t, state.policies, "old-policy")
	require.True(t, state.policyRefreshedAt.After(oldRefresh))
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerEnsureFreshMetadataCoalescesConcurrentRefreshes(t *testing.T) {
	m := newManager()
	coord := &blockingPolicyCoord{
		metadataTestCoord: &metadataTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("policy")},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, m.init(context.Background(), coord))

	const concurrency = 8
	errs := make(chan error, concurrency)
	for range concurrency {
		go func() {
			errs <- m.ensurePoliciesFresh(context.Background(), 100)
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

func TestManagerPolicyInvalidationWinsOverInflightRefresh(t *testing.T) {
	m := newManager()
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
	secondStarted, secondRelease := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	coord := &managerTestCoordClient{getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
		call := calls.Add(1)
		started, release, policyName := firstStarted, firstRelease, "stale-policy"
		if call == 2 {
			started, release, policyName = secondStarted, secondRelease, "fresh-policy"
		}
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &rootcoordpb.GetRLSMetadataResponse{
			Status:       merr.Success(),
			CollectionId: req.GetCollectionId(),
			Policies:     []*rootcoordpb.RLSPolicyInfo{validPolicyInfo(policyName)},
		}, nil
	}}
	require.NoError(t, m.init(context.Background(), coord))

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- m.ensurePoliciesFresh(context.Background(), 100)
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first RLS snapshot refresh did not start")
	}

	invalidated := make(chan struct{})
	go func() {
		m.invalidatePolicies(100)
		close(invalidated)
	}()
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		t.Fatal("RLS policy invalidation waited for the in-flight refresh")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- m.ensurePoliciesFresh(context.Background(), 100)
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second RLS snapshot refresh did not start")
	}
	close(secondRelease)
	require.NoError(t, <-secondDone)

	close(firstRelease)
	require.ErrorIs(t, <-firstDone, merr.ErrServiceUnavailable)
	state := m.getCollectionState(100)
	require.NotNil(t, state)
	require.Contains(t, state.policies, "fresh-policy")
	require.NotContains(t, state.policies, "stale-policy")
	require.Equal(t, int32(2), calls.Load())
}

func TestManagerPolicyRefreshUsesManagerContext(t *testing.T) {
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	m := newManager()
	coord := &blockingPolicyCoord{
		metadataTestCoord: &metadataTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("policy")},
		},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		seenContext: make(chan context.Context, 1),
	}
	require.NoError(t, m.init(lifecycleCtx, coord))

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.ensurePoliciesFresh(requestCtx, 100)
	}()
	var refreshCtx context.Context
	select {
	case refreshCtx = <-coord.seenContext:
	case <-time.After(time.Second):
		t.Fatal("policy refresh did not start")
	}
	cancelRequest()
	require.NoError(t, refreshCtx.Err())
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not stop waiting for policy refresh")
	}
	require.NoError(t, refreshCtx.Err())
	close(coord.release)
	require.Eventually(t, func() bool {
		state := m.getCollectionState(100)
		return state != nil && !m.policyRefreshDue(100, time.Hour, time.Now())
	}, time.Second, time.Millisecond)
}

func TestManagerPolicyRefreshDoesNotLoadPrincipalTags(t *testing.T) {
	m := newManager()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	coord := &metadataTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("new-policy")},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))
	require.Equal(t, int32(rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES), coord.metadataKind.Load())

	state := m.collections[100]
	require.Contains(t, state.policies, "new-policy")
	require.Empty(t, state.principalTags)
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerPolicyRefreshRejectsMalformedMetadata(t *testing.T) {
	wrongCollection := validPolicyInfo("wrong-collection")
	wrongCollection.CollectionId = 200
	missingID := validPolicyInfo("missing-id")
	missingID.PolicyId = 0
	invalidType := validPolicyInfo("invalid-type")
	invalidType.PolicyType = milvuspb.RowPolicyType_RowPolicyTypeUnknown
	duplicateOne := validPolicyInfo("duplicate")
	duplicateTwo := validPolicyInfo("duplicate")
	duplicateTwo.PolicyId = 2

	for name, policies := range map[string][]*rootcoordpb.RLSPolicyInfo{
		"nil policy":          {nil},
		"wrong collection":    {wrongCollection},
		"missing policy id":   {missingID},
		"invalid policy type": {invalidType},
		"duplicate name":      {duplicateOne, duplicateTwo},
	} {
		t.Run(name, func(t *testing.T) {
			m := newManager()
			oldRefresh := time.Now().Add(-time.Hour)
			require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
				RefreshedAt: oldRefresh,
				Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
			}))

			coord := &metadataTestCoord{policies: policies}
			require.NoError(t, m.init(context.Background(), coord))
			err := m.refreshPolicies(100)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)

			state := m.getCollectionState(100)
			require.Equal(t, oldRefresh, state.policyRefreshedAt)
			require.Contains(t, state.policies, "old-policy")
		})
	}
}

func TestManagerPrincipalTagsAreLoadedAndCachedPerPrincipal(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		principalTags: map[string]map[string]string{
			"alice": {"tenant": "acme"},
			"bob":   {"tenant": "globex"},
		},
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))

	aliceTags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), aliceTags["tenant"])
	require.Equal(t, int32(1), coord.principalCalls.Load())

	cachedAliceTags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), cachedAliceTags["tenant"])
	require.Equal(t, reflect.ValueOf(aliceTags).Pointer(), reflect.ValueOf(cachedAliceTags).Pointer())
	require.Equal(t, int32(1), coord.principalCalls.Load())

	bobTags, err := m.ensurePrincipalTags(context.Background(), 100, "bob")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("globex"), bobTags["tenant"])
	require.Equal(t, int32(2), coord.principalCalls.Load())
	require.Len(t, m.collections[100].principalTags, 2)
}

func TestManagerPrincipalTagsAreNestedByCollection(t *testing.T) {
	m := newManager()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))
	require.True(t, setPolicySnapshotForTest(m, 200, policySnapshot{}))
	setPrincipalTagsForTest(m, principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("one")},
	})
	setPrincipalTagsForTest(m, principalKey{collectionID: 200, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now(),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("two")},
	})

	require.Equal(t, rlsutil.NewStringTagValue("one"), m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}).tags["tenant"])
	require.Equal(t, rlsutil.NewStringTagValue("two"), m.getPrincipalTagsEntry(principalKey{collectionID: 200, principalName: "alice"}).tags["tenant"])
	m.markCollectionDropped(100)
	require.Nil(t, m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}))
	require.NotNil(t, m.getPrincipalTagsEntry(principalKey{collectionID: 200, principalName: "alice"}))
}

func TestManagerPrincipalLookupUsesCollectionID(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{principalTags: map[string]map[string]string{"alice": {"tenant": "acme"}}}
	require.NoError(t, m.init(context.Background(), coord))
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))

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
	require.NoError(t, m.init(context.Background(), coord))
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))

	_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestManagerPrincipalRefreshCoalescesConcurrentLookups(t *testing.T) {
	m := newManager()
	coord := &blockingPrincipalCoord{
		metadataTestCoord: &metadataTestCoord{},
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))

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
	close(coord.release)
	for range concurrency {
		require.NoError(t, <-results)
	}
	require.Equal(t, int32(1), coord.principalCalls.Load())
}

func TestManagerPrincipalRefreshUsesManagerContext(t *testing.T) {
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	m := newManager()
	coord := &blockingPrincipalCoord{
		metadataTestCoord: &metadataTestCoord{},
		started:           make(chan struct{}),
		release:           make(chan struct{}),
		seenContext:       make(chan context.Context, 1),
	}
	require.NoError(t, m.init(lifecycleCtx, coord))
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.ensurePrincipalTags(requestCtx, 100, "alice")
		done <- err
	}()
	var refreshCtx context.Context
	select {
	case refreshCtx = <-coord.seenContext:
	case <-time.After(time.Second):
		t.Fatal("principal refresh did not start")
	}
	cancelRequest()
	require.NoError(t, refreshCtx.Err())
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not stop waiting for principal refresh")
	}
	require.NoError(t, refreshCtx.Err())
	close(coord.release)
	require.Eventually(t, func() bool {
		return m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}) != nil
	}, time.Second, time.Millisecond)
}

func TestManagerPrincipalInvalidationWinsOverInflightRefresh(t *testing.T) {
	m := newManager()
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
	secondStarted, secondRelease := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	coord := &managerTestCoordClient{getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
		call := calls.Add(1)
		started, release := firstStarted, firstRelease
		if call == 2 {
			started, release = secondStarted, secondRelease
		}
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if call == 1 {
			return &rootcoordpb.GetRLSMetadataResponse{Status: merr.Success(), CollectionId: req.GetCollectionId()}, nil
		}
		return principalMetadataResponse(req.GetCollectionId(), req.GetPrincipalName(), map[string]string{"tenant": "fresh"}), nil
	}}
	require.NoError(t, m.init(context.Background(), coord))
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))

	firstDone := make(chan error, 1)
	go func() {
		_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first principal refresh did not start")
	}

	invalidated := make(chan struct{})
	go func() {
		m.invalidatePrincipalTags(100, "alice")
		close(invalidated)
	}()
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		t.Fatal("principal invalidation waited for the in-flight refresh")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
		secondDone <- err
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second principal refresh did not start")
	}
	close(secondRelease)
	require.NoError(t, <-secondDone)

	close(firstRelease)
	require.ErrorIs(t, <-firstDone, merr.ErrServiceUnavailable)
	entry := m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"})
	require.NotNil(t, entry)
	require.Equal(t, rlsutil.NewStringTagValue("fresh"), entry.tags["tenant"])
	require.Equal(t, int32(2), calls.Load())
}

func TestManagerPrincipalRefreshInvalidationIsPrincipalScoped(t *testing.T) {
	m := newManager()
	coord := &blockingPrincipalCoord{
		metadataTestCoord: &metadataTestCoord{},
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))

	done := make(chan error, 1)
	go func() {
		_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
		done <- err
	}()
	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("principal refresh did not start")
	}

	m.invalidatePrincipalTags(100, "bob")
	close(coord.release)
	require.NoError(t, <-done)
	require.NotNil(t, m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"}))
}

func TestManagerPrincipalTTLScannerEvictsExpiredEntries(t *testing.T) {
	m := newManager()
	coord := &metadataTestCoord{
		principalTags: map[string]map[string]string{
			"alice": {"tenant": "new"},
			"bob":   {"tenant": "unchanged"},
		},
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))
	setPrincipalTagsForTest(m, principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now().Add(-2 * time.Hour),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("old")},
	})
	setPrincipalTagsForTest(m, principalKey{collectionID: 100, principalName: "bob"}, &principalTagsEntry{
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
	require.NotContains(t, m.collections[100].principalTags, "alice")
	require.Contains(t, m.collections[100].principalTags, "bob")

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
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))
	setPrincipalTagsForTest(m, principalKey{collectionID: 100, principalName: "alice"}, &principalTagsEntry{
		refreshedAt: time.Now().Add(-2 * time.Second),
		tags:        map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("old")},
	})
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
	require.NoError(t, m.init(context.Background(), coord))
	require.NoError(t, m.refreshPolicies(100))

	tags, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Empty(t, tags)
	require.NotContains(t, m.collections[100].principalTags, "alice")

	coord.principalTags["alice"] = map[string]string{"tenant": "acme"}
	tags, err = m.ensurePrincipalTags(context.Background(), 100, "alice")
	require.NoError(t, err)
	require.Equal(t, rlsutil.NewStringTagValue("acme"), tags["tenant"])
	require.Equal(t, int32(2), coord.principalCalls.Load())
}

func TestManagerPrincipalInvalidationIsPrincipalScoped(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		RefreshedAt: now,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "tenant"}},
	}))
	require.True(t, setManagerTestPrincipalTags(m, 100, "alice", map[string]rlsutil.TagValue{
		"tenant": rlsutil.NewStringTagValue("acme"),
	}))
	require.True(t, setManagerTestPrincipalTags(m, 100, "bob", map[string]rlsutil.TagValue{
		"tenant": rlsutil.NewStringTagValue("globex"),
	}))

	m.invalidatePrincipalTags(100, "alice")
	require.NotContains(t, m.collections[100].principalTags, "alice")
	require.Contains(t, m.collections[100].principalTags, "bob")
	require.Contains(t, m.collections[100].policies, "tenant")

	m.invalidatePolicies(100)
	require.Contains(t, m.collections, UniqueID(100))
	require.Empty(t, m.collections[100].policies)
	require.Contains(t, m.collections[100].principalTags, "bob")

	m.markCollectionDropped(100)
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerPolicyAndPrincipalCachesAreIndependent(t *testing.T) {
	m := newManager()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "old-policy"},
		},
	}))
	require.True(t, setManagerTestPrincipalTags(m, 100, "alice", map[string]rlsutil.TagValue{
		"team": rlsutil.NewStringTagValue("old"),
	}))

	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "new-policy"},
		},
	}))
	require.True(t, setManagerTestPrincipalTags(m, 100, "alice", map[string]rlsutil.TagValue{
		"team": rlsutil.NewStringTagValue("new"),
	}))

	state := m.collections[100]
	require.Contains(t, state.policies, "new-policy")
	require.NotContains(t, state.policies, "old-policy")
	entry := m.getPrincipalTagsEntry(principalKey{collectionID: 100, principalName: "alice"})
	require.NotNil(t, entry)
	require.Equal(t, map[string]rlsutil.TagValue{"team": rlsutil.NewStringTagValue("new")}, entry.tags)
}

func TestManagerRemoveCollection(t *testing.T) {
	m := newManager()
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "tenant"},
		},
	}))
	m.markCollectionDropped(100)
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerRefreshDoesNotRecreateAlreadyRemovedCollection(t *testing.T) {
	m := newManager()
	require.NoError(t, m.init(context.Background(), &metadataTestCoord{}))
	m.markCollectionDropped(100)

	err := m.refreshPolicies(100)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerRefreshDoesNotRecreateRemovedCollection(t *testing.T) {
	m := newManager()
	coord := &blockingPolicyCoord{
		metadataTestCoord: &metadataTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{validPolicyInfo("stale-policy")},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, m.init(context.Background(), coord))
	done := make(chan error, 1)
	go func() {
		done <- m.refreshPolicies(100)
	}()

	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("RLS snapshot refresh did not start")
	}
	removed := make(chan struct{})
	go func() {
		m.markCollectionDropped(100)
		close(removed)
	}()
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("RLS cache invalidation waited for the in-flight refresh")
	}
	close(coord.release)
	require.ErrorIs(t, <-done, merr.ErrServiceUnavailable)
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerPrincipalRefreshDoesNotRecreateRemovedCollection(t *testing.T) {
	m := newManager()
	coord := &blockingPrincipalCoord{
		metadataTestCoord: &metadataTestCoord{},
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	require.NoError(t, m.init(context.Background(), coord))
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))

	done := make(chan error, 1)
	go func() {
		_, err := m.ensurePrincipalTags(context.Background(), 100, "alice")
		done <- err
	}()
	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("RLS principal refresh did not start")
	}
	removed := make(chan struct{})
	go func() {
		m.markCollectionDropped(100)
		close(removed)
	}()
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("RLS cache invalidation waited for the in-flight principal refresh")
	}
	close(coord.release)
	require.ErrorIs(t, <-done, merr.ErrServiceUnavailable)
	require.NotContains(t, m.collections, UniqueID(100))
}

func TestManagerSnapshotsOwnImmutableData(t *testing.T) {
	m := newManager()
	policy := &rlsutil.RowPolicy{PolicyName: "tenant"}
	tags := map[string]rlsutil.TagValue{"tenant": rlsutil.NewStringTagValue("acme")}
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{policy},
	}))
	require.True(t, setManagerTestPrincipalTags(m, 100, "alice", tags))

	policy.PolicyName = "mutated"
	tags["tenant"] = rlsutil.NewStringTagValue("mutated")
	state := m.getCollectionState(100)
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
	require.True(t, setPolicySnapshotForTest(m, 100, policySnapshot{}))
	state := m.getCollectionState(100)
	require.NotNil(t, state)
	state.mu.Lock()
	defer state.mu.Unlock()

	done := make(chan bool, 1)
	go func() {
		done <- setPolicySnapshotForTest(m, 200, policySnapshot{})
	}()
	select {
	case updated := <-done:
		require.True(t, updated)
	case <-time.After(time.Second):
		t.Fatal("updating one collection waited for another collection's state lock")
	}
}
