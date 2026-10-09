// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Done is evaluated only after loadRLSPolicies has joined its flight.
type rlsPolicyWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *rlsPolicyWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type rlsPolicyLoadResult struct {
	snapshot rlsPolicySnapshot
	err      error
}

func startPendingRLSPolicyLoad(t *testing.T, ctx context.Context, meta *MetaTable) <-chan rlsPolicyLoadResult {
	t.Helper()
	waitCtx := &rlsPolicyWaitContext{Context: ctx, waiting: make(chan struct{})}
	done := make(chan rlsPolicyLoadResult, 1)
	go func() {
		snapshot, err := meta.loadRLSPolicies(waitCtx, 20)
		done <- rlsPolicyLoadResult{snapshot, err}
	}()
	select {
	case <-waitCtx.waiting:
	case result := <-done:
		t.Fatalf("load returned before joining a pending flight: %+v", result)
	case <-time.After(5 * time.Second):
		t.Fatal("load did not join a flight")
	}
	return done
}

func TestRLSPolicyLoadsShareResults(t *testing.T) {
	for _, outcome := range []string{"policies", "empty", "error"} {
		t.Run(outcome, func(t *testing.T) {
			meta, catalog := newRLSMetaTableForTest(t)
			coll := meta.collID2Meta[20]
			coll.RLSPoliciesUnloaded = true
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			var policies []*model.RLSPolicy
			var loadErr error
			if outcome == "policies" {
				policies = []*model.RLSPolicy{{PolicyName: "tenant", PolicyID: 100}}
			} else if outcome == "error" {
				loadErr = merr.WrapErrServiceUnavailableMsg("catalog unavailable")
			}
			catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
				func(context.Context, int64) ([]*model.RLSPolicy, error) {
					<-release
					return policies, loadErr
				}).Once()
			first := startPendingRLSPolicyLoad(t, context.Background(), meta)
			second := startPendingRLSPolicyLoad(t, context.Background(), meta)
			unblock()
			a, b := <-first, <-second
			require.Zero(t, coll.RLSPolicyExpectedGeneration, "reads must not advance the expectation")
			if loadErr != nil {
				require.ErrorIs(t, a.err, loadErr)
				require.Same(t, a.err, b.err, "all waiters receive the same failure")
				require.True(t, coll.RLSPoliciesUnloaded)
				catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).Return(nil, nil).Once()
				_, err := meta.loadRLSPolicies(context.Background(), 20)
				require.NoError(t, err, "a later request may retry the failed generation")
				return
			}
			require.NoError(t, a.err)
			require.NoError(t, b.err)
			require.True(t, coll.RLSPoliciesCurrent())
			if outcome == "policies" {
				a.snapshot.policies["tenant"].PolicyName = "caller mutation"
				require.Equal(t, "tenant", b.snapshot.policies["tenant"].PolicyName)
				require.Equal(t, "tenant", coll.RLSPolicies["tenant"].PolicyName)
			}
			_, err := meta.loadRLSPolicies(context.Background(), 20)
			require.NoError(t, err, "even an empty snapshot must be cached")
		})
	}
}

func TestRLSPolicyLoadsKeepEntryGeneration(t *testing.T) {
	for _, order := range []string{"old read first", "new read first"} {
		t.Run(order, func(t *testing.T) {
			meta, catalog := newRLSMetaTableForTest(t)
			coll := meta.collID2Meta[20]
			coll.RLSPoliciesUnloaded = true
			oldPolicy := &model.RLSPolicy{CollectionID: 20, PolicyName: "tenant", PolicyID: 100, UsingExpr: "true"}
			newPolicy := model.CloneRLSPolicy(oldPolicy)
			newPolicy.UsingExpr = "false"
			started, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
				func(context.Context, int64) ([]*model.RLSPolicy, error) {
					close(started)
					<-release
					return []*model.RLSPolicy{oldPolicy}, nil
				}).Once()
			oldLoad := startPendingRLSPolicyLoad(t, context.Background(), meta)
			<-started
			catalog.EXPECT().SaveRLSPolicy(mock.Anything, newPolicy).Return(nil).Once()
			require.NoError(t, meta.ApplyAlterRLSPolicy(context.Background(), newPolicy))
			require.Equal(t, uint64(1), coll.RLSPolicyExpectedGeneration)
			catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).Return([]*model.RLSPolicy{newPolicy}, nil).Once()
			checkOld := func() {
				unblock()
				result := <-oldLoad
				require.NoError(t, result.err, "a later mutation must not fail the old request")
				require.Zero(t, result.snapshot.generation)
				require.Equal(t, "true", result.snapshot.policies["tenant"].UsingExpr)
			}
			if order == "old read first" {
				checkOld()
				require.Zero(t, coll.RLSPolicyGeneration)
				require.False(t, coll.RLSPoliciesCurrent(), "old completion must not clear the new expectation")
			}
			latest, err := meta.loadRLSPolicies(context.Background(), 20)
			require.NoError(t, err)
			require.Equal(t, uint64(1), latest.generation)
			require.Equal(t, "false", latest.policies["tenant"].UsingExpr)
			if order == "new read first" {
				checkOld()
			}
			require.Equal(t, uint64(1), coll.RLSPolicyGeneration)
			require.Equal(t, uint64(1), coll.RLSPolicyExpectedGeneration)
			require.Equal(t, "false", coll.RLSPolicies["tenant"].UsingExpr)
		})
	}
}

func TestRLSPolicyGenerationAdvancesOnlyAfterPersist(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	coll := meta.collID2Meta[20]
	policy := &model.RLSPolicy{CollectionID: 20, PolicyID: 100, PolicyName: "tenant"}
	catalog.EXPECT().SaveRLSPolicy(mock.Anything, policy).Return(merr.ErrServiceUnavailable).Once()
	require.ErrorIs(t, meta.ApplyAlterRLSPolicy(context.Background(), policy), merr.ErrServiceUnavailable)
	require.Zero(t, coll.RLSPolicyExpectedGeneration)
	catalog.EXPECT().SaveRLSPolicy(mock.Anything, policy).Return(nil).Once()
	require.NoError(t, meta.ApplyAlterRLSPolicy(context.Background(), policy))
	require.Equal(t, uint64(1), coll.RLSPolicyGeneration)
	require.Equal(t, uint64(1), coll.RLSPolicyExpectedGeneration)
	catalog.EXPECT().DropRLSPolicy(mock.Anything, int64(20), int64(100)).Return(merr.ErrServiceUnavailable).Once()
	require.ErrorIs(t, meta.ApplyDropRLSPolicy(context.Background(), 20, "tenant"), merr.ErrServiceUnavailable)
	require.Equal(t, uint64(1), coll.RLSPolicyExpectedGeneration)
	catalog.EXPECT().DropRLSPolicy(mock.Anything, int64(20), int64(100)).Return(nil).Once()
	require.NoError(t, meta.ApplyDropRLSPolicy(context.Background(), 20, "tenant"))
	require.Equal(t, uint64(2), coll.RLSPolicyExpectedGeneration)
	require.Equal(t, uint64(2), coll.RLSPolicyGeneration)
	require.Empty(t, coll.RLSPolicies)
}

func TestRLSPolicyMutationDoesNotPromoteStaleSnapshot(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	coll := meta.collID2Meta[20]
	coll.RLSPolicies = map[string]*model.RLSPolicy{
		"tenant": {CollectionID: 20, PolicyName: "tenant", PolicyID: 100, UsingExpr: "true"},
	}
	// A late generation-zero read completed after the first policy mutation.
	coll.RLSPolicyExpectedGeneration = 1
	added := &model.RLSPolicy{CollectionID: 20, PolicyName: "additional", PolicyID: 101}
	catalog.EXPECT().SaveRLSPolicy(mock.Anything, added).Return(nil).Once()
	require.NoError(t, meta.ApplyAlterRLSPolicy(context.Background(), added))
	require.Equal(t, uint64(2), coll.RLSPolicyExpectedGeneration)
	require.Zero(t, coll.RLSPolicyGeneration)
	require.False(t, coll.RLSPoliciesCurrent())
	updated := model.CloneRLSPolicy(coll.RLSPolicies["tenant"])
	updated.UsingExpr = "false"
	catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).Return([]*model.RLSPolicy{updated, added}, nil).Once()
	snapshot, err := meta.loadRLSPolicies(context.Background(), 20)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshot.generation)
	require.Len(t, snapshot.policies, 2)
	require.Equal(t, "false", snapshot.policies["tenant"].UsingExpr)
}

func TestRLSPolicyLoadSurvivesCollectionRename(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	meta.collID2Meta[20].RLSPoliciesUnloaded = true
	catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
		func(context.Context, int64) ([]*model.RLSPolicy, error) {
			meta.ddLock.Lock()
			renamed := meta.collID2Meta[20].Clone()
			renamed.DBID = 11
			renamed.Name = "renamed"
			meta.collID2Meta[20] = renamed
			meta.ddLock.Unlock()
			return []*model.RLSPolicy{{DBID: 10, CollectionID: 20, PolicyID: 100, PolicyName: "tenant"}}, nil
		}).Once()
	snapshot, err := meta.loadRLSPolicies(context.Background(), 20)
	require.NoError(t, err)
	require.Equal(t, int64(11), snapshot.policies["tenant"].DBID)
	require.Zero(t, snapshot.generation)
	require.Zero(t, meta.collID2Meta[20].RLSPolicyExpectedGeneration)
	require.True(t, meta.collID2Meta[20].RLSPoliciesCurrent())
}

func TestRLSPolicyLoadCannotResurrectDroppedCollection(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(map[bool]string{false: "dropping", true: "removed"}[removed], func(t *testing.T) {
			meta, catalog := newRLSMetaTableForTest(t)
			coll := meta.collID2Meta[20]
			coll.RLSPoliciesUnloaded = true
			started, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
				func(context.Context, int64) ([]*model.RLSPolicy, error) {
					close(started)
					<-release
					return []*model.RLSPolicy{{PolicyName: "tenant", PolicyID: 100}}, nil
				}).Once()
			done := startPendingRLSPolicyLoad(t, context.Background(), meta)
			<-started
			meta.ddLock.Lock()
			if removed {
				delete(meta.collID2Meta, 20)
			} else {
				coll.State = pb.CollectionState_CollectionDropping
			}
			meta.ddLock.Unlock()
			unblock()
			require.ErrorIs(t, (<-done).err, merr.ErrCollectionNotFound)
			require.Empty(t, coll.RLSPolicies)
			if removed {
				require.NotContains(t, meta.collID2Meta, int64(20))
			}
		})
	}
}

func TestRLSPolicyLoadSurvivesCallerCancellation(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	meta.collID2Meta[20].RLSPoliciesUnloaded = true
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
		func(ctx context.Context, _ int64) ([]*model.RLSPolicy, error) {
			started <- ctx
			<-release
			return nil, nil
		}).Once()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startPendingRLSPolicyLoad(t, ctx, meta)
	loadCtx := <-started
	second := startPendingRLSPolicyLoad(t, context.Background(), meta)
	cancel()
	require.ErrorIs(t, (<-first).err, context.Canceled)
	require.NoError(t, loadCtx.Err())
	deadline, ok := loadCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(rlsPolicyLoadTimeout), deadline, time.Second)
	unblock()
	require.NoError(t, (<-second).err)
}

func TestRLSPolicyLoadStopsWithCoord(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	meta.ctx = ctx
	meta.collID2Meta[20].RLSPoliciesUnloaded = true
	catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
		func(ctx context.Context, _ int64) ([]*model.RLSPolicy, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
	request := startPendingRLSPolicyLoad(t, context.Background(), meta)
	cancel()
	require.ErrorIs(t, (<-request).err, context.Canceled)
	require.True(t, meta.collID2Meta[20].RLSPoliciesUnloaded)
}

func TestWarmupRLSPoliciesSharesDemandLoad(t *testing.T) {
	meta, catalog := newRLSMetaTableForTest(t)
	meta.collID2Meta[20].RLSPoliciesUnloaded = true
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).RunAndReturn(
		func(context.Context, int64) ([]*model.RLSPolicy, error) {
			close(started)
			<-release
			return nil, nil
		}).Once()
	warmupDone := make(chan struct{})
	go func() {
		meta.warmupRLSPolicies(context.Background())
		close(warmupDone)
	}()
	<-started
	request := startPendingRLSPolicyLoad(t, context.Background(), meta)
	unblock()
	require.NoError(t, (<-request).err)
	<-warmupDone
	require.True(t, meta.collID2Meta[20].RLSPoliciesCurrent())
}

func TestWarmupRLSPoliciesConfigurationAndScope(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			value := "false"
			if enabled {
				value = "true"
			}
			require.NoError(t, paramtable.Get().Save(Params.RootCoordCfg.RLSPolicyWarmupEnabled.Key, value))
			t.Cleanup(func() { paramtable.Get().Reset(Params.RootCoordCfg.RLSPolicyWarmupEnabled.Key) })
			meta, catalog := newRLSMetaTableForTest(t)
			meta.collID2Meta[20].RLSPoliciesUnloaded = true
			for _, id := range []int64{21, 22} {
				coll := meta.collID2Meta[20].Clone()
				coll.CollectionID = id
				if id == 21 {
					coll.Properties = common.NewKeyValuePairs(map[string]string{common.RLSEnabledKey: "false"})
				} else {
					coll.State = pb.CollectionState_CollectionDropping
				}
				meta.collID2Meta[id] = coll
			}
			if enabled {
				catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).Return(nil, merr.ErrServiceUnavailable).Once()
			}
			meta.warmupRLSPolicies(context.Background())
			require.True(t, meta.collID2Meta[20].RLSPoliciesUnloaded)
			catalog.EXPECT().ListRLSPolicies(mock.Anything, int64(20)).Return(nil, nil).Once()
			_, err := meta.loadRLSPolicies(context.Background(), 20)
			require.NoError(t, err, "warmup failure or disabling warmup must not disable demand loading")
		})
	}
}
