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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// applySnapshot is a test helper: apply a non-draining gate snapshot and require it committed.
func applySnapshot(t *testing.T, g *dataViewGate, collID UniqueID, fields []UniqueID, paused bool, gen uint64) {
	applied, err := g.ApplyGateSnapshot(context.Background(), collID, fields, paused, gen, false)
	require.NoError(t, err)
	require.True(t, applied)
}

func TestDataViewGate_ReadBlockAndRelease(t *testing.T) {
	g := newDataViewGate()

	// no gate installed -> nothing blocked.
	_, blocked := g.AnyFieldBlocked(1, 100, 101)
	assert.False(t, blocked)

	applySnapshot(t, g, 1, []UniqueID{100, 101}, false, 1)
	f, blocked := g.AnyFieldBlocked(1, 200, 101)
	assert.True(t, blocked)
	assert.Equal(t, UniqueID(101), f)
	_, blocked = g.AnyFieldBlocked(1, 200, 300)
	assert.False(t, blocked)
	// a different collection is unaffected.
	_, blocked = g.AnyFieldBlocked(2, 100)
	assert.False(t, blocked)

	// a newer snapshot drops 100 (releasing it), the residual set keeps 101 blocked.
	applySnapshot(t, g, 1, []UniqueID{101}, false, 2)
	_, blocked = g.AnyFieldBlocked(1, 100)
	assert.False(t, blocked)
	f, blocked = g.AnyFieldBlocked(1, 101)
	assert.True(t, blocked)
	assert.Equal(t, UniqueID(101), f)

	// an empty snapshot clears the collection entry entirely.
	applySnapshot(t, g, 1, nil, false, 3)
	_, blocked = g.AnyFieldBlocked(1, 101)
	assert.False(t, blocked)
}

func TestMetaCache_PullDataViewGate(t *testing.T) {
	mixCoord := mocks.NewMockMixCoordClient(t)
	m := &MetaCache{mixCoord: mixCoord}
	ctx := context.Background()

	// success: the pulled snapshot is applied to the process-wide proxy gate.
	mixCoord.EXPECT().GetDataViewGate(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetDataViewGateResponse{
			Status:              merr.Success(),
			GatedFieldIds:       []int64{9100},
			ComplexDeletePaused: true,
			Generation:          1,
		}, nil).Once()
	require.NoError(t, m.pullDataViewGate(ctx, 9001))
	_, blocked := globalDataViewGate.AnyFieldBlocked(9001, 9100)
	assert.True(t, blocked)
	assert.False(t, globalDataViewGate.TryRegisterComplexDelete(9001, globalDataViewGate.CurrentGen(9001))) // paused

	// RPC error: fail-closed, error propagated and no gate published for a fresh collection.
	mixCoord.EXPECT().GetDataViewGate(mock.Anything, mock.Anything).
		Return(nil, errors.New("boom")).Once()
	assert.Error(t, m.pullDataViewGate(ctx, 9002))
	_, blocked = globalDataViewGate.AnyFieldBlocked(9002, 9100)
	assert.False(t, blocked)

	// a non-OK status in a non-nil response is also fail-closed.
	mixCoord.EXPECT().GetDataViewGate(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetDataViewGateResponse{Status: merr.Status(merr.WrapErrServiceInternal("not ready"))}, nil).Once()
	assert.Error(t, m.pullDataViewGate(ctx, 9003))
}

// TestDataViewGate_IgnoresLocalKillSwitch: the proxy gate enforces installed state ONLY and never reads
// the DataViewGateEnabled flag — RootCoord (the single owner) disables by releasing state. Flipping the
// local flag off must NOT bypass an installed gate (regression guard for the RootCoord-single-owner model).
func TestDataViewGate_IgnoresLocalKillSwitch(t *testing.T) {
	paramtable.Init()
	g := newDataViewGate()
	applySnapshot(t, g, 8001, []UniqueID{8100}, false, 1)
	applySnapshot(t, g, 8002, nil, true, 1)

	// flag on: read blocked + complex-delete paused.
	_, blocked := g.AnyFieldBlocked(8001, 8100)
	assert.True(t, blocked)
	assert.False(t, g.TryRegisterComplexDelete(8002, g.CurrentGen(8002)))

	// flag off: still enforced (the proxy never consults the flag).
	key := paramtable.Get().RootCoordCfg.DataViewGateEnabled.Key
	paramtable.Get().Save(key, "false")
	defer paramtable.Get().Save(key, "true")
	_, blocked = g.AnyFieldBlocked(8001, 8100)
	assert.True(t, blocked)
	assert.False(t, g.TryRegisterComplexDelete(8002, g.CurrentGen(8002)))
}

func TestDataViewGate_CheckReadFieldGate(t *testing.T) {
	old := globalDataViewGate
	defer func() { globalDataViewGate = old }()
	globalDataViewGate = newDataViewGate()

	assert.NoError(t, checkReadFieldGate(1, 100))
	applySnapshot(t, globalDataViewGate, 1, []UniqueID{100}, false, 1)
	assert.Error(t, checkReadFieldGate(1, 99, 100))
	assert.NoError(t, checkReadFieldGate(1, 99, 101))
	// unrelated collection stays readable.
	assert.NoError(t, checkReadFieldGate(2, 100))
}

func TestDataViewGate_SnapshotReplacesReadGate(t *testing.T) {
	g := newDataViewGate()

	applySnapshot(t, g, 1, []UniqueID{100, 101}, false, 1)
	_, blocked := g.AnyFieldBlocked(1, 101)
	assert.True(t, blocked)

	// a newer snapshot with a different set: the old members are no longer blocked.
	applySnapshot(t, g, 1, []UniqueID{200}, false, 2)
	_, blocked = g.AnyFieldBlocked(1, 101)
	assert.False(t, blocked)
	_, blocked = g.AnyFieldBlocked(1, 200)
	assert.True(t, blocked)

	// an empty snapshot clears the collection.
	applySnapshot(t, g, 1, nil, false, 3)
	_, blocked = g.AnyFieldBlocked(1, 200)
	assert.False(t, blocked)
}

// TestDataViewGate_GenerationGuard: a snapshot commits only when its generation exceeds the last applied
// one; a stale/equal generation is a no-op.
func TestDataViewGate_GenerationGuard(t *testing.T) {
	g := newDataViewGate()

	// fresh collection: gen 0; a snapshot at gen 1 commits.
	assert.Equal(t, uint64(0), g.CurrentGen(1))
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, false, 1, false)
	require.NoError(t, err)
	assert.True(t, applied)
	_, blocked := g.AnyFieldBlocked(1, 100)
	assert.True(t, blocked)

	// the same generation is rejected (not strictly newer).
	applied, err = g.ApplyGateSnapshot(context.Background(), 1, nil, false, 1, false)
	require.NoError(t, err)
	assert.False(t, applied)
	_, blocked = g.AnyFieldBlocked(1, 100)
	assert.True(t, blocked) // unchanged

	// a newer generation commits.
	applied, err = g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{200}, false, 2, false)
	require.NoError(t, err)
	assert.True(t, applied)
	_, blocked = g.AnyFieldBlocked(1, 200)
	assert.True(t, blocked)
}

// TestDataViewGate_StaleDrainReturnsError: a drop-install (drain) snapshot superseded by a strictly newer
// snapshot may not have actually drained in-flight complex-deletes, so it must return an error (not a
// silent no-op success) — else RootCoord would mistake it for a completed drain and let the drop cross an
// in-flight complex-delete. A stale NON-drain snapshot stays a benign no-op.
func TestDataViewGate_StaleDrainReturnsError(t *testing.T) {
	g := newDataViewGate()

	// a newer snapshot lands first (gen 10).
	applySnapshot(t, g, 1, nil, false, 10)

	// a stale DRAIN snapshot (gen 5) must fail, not silently succeed as a no-op drain.
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, true, 5, true)
	assert.Error(t, err)
	assert.False(t, applied)
	assert.True(t, errors.Is(err, merr.ErrCollectionSchemaChangeInProgress))

	// a stale NON-drain snapshot is still a benign no-op (no error).
	applied, err = g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, false, 5, false)
	assert.NoError(t, err)
	assert.False(t, applied)
}

// TestDataViewGate_StaleSnapshotDroppedAfterReorder is the #2 regression: a stale SET/RELEASE arriving
// after a newer snapshot must be dropped so it cannot flip the pause and let a complex-delete cross a
// drop. Here a newer drop snapshot (gen 101, paused) must survive a reordered older snapshot (gen 100,
// not paused) that would otherwise clear the pause.
func TestDataViewGate_StaleSnapshotDroppedAfterReorder(t *testing.T) {
	g := newDataViewGate()

	// newer snapshot: field 200 gated, complex-deletes paused (an active drop).
	applySnapshot(t, g, 1, []UniqueID{200}, true, 101)
	_, blocked := g.AnyFieldBlocked(1, 200)
	require.True(t, blocked)
	require.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // paused

	// a reordered OLDER snapshot (gen 100) that would clear the block + resume complex-deletes arrives
	// late. It must be dropped (100 <= 101), so the pause and block still stand.
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, nil, false, 100, false)
	require.NoError(t, err)
	assert.False(t, applied)
	_, blocked = g.AnyFieldBlocked(1, 200)
	assert.True(t, blocked)
	assert.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // still paused
}

func TestDataViewGate_ComplexDeletePauseAndRefcount(t *testing.T) {
	g := newDataViewGate()

	// register / deregister while not paused.
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // refcount 2
	g.DeregisterComplexDelete(1)
	g.DeregisterComplexDelete(1) // back to 0, entry removed

	// a snapshot with paused=true rejects new registrations.
	applySnapshot(t, g, 1, nil, true, 1)
	assert.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	// a newer snapshot that clears the pause re-enables them.
	applySnapshot(t, g, 1, nil, false, 2)
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	g.DeregisterComplexDelete(1)
}

// TestDataViewGate_ComplexDeleteRejectsStaleGeneration: a complex-delete whose plan was built (generation
// captured in Init) before a drop installed/released must be rejected at registration — otherwise it
// could be missed by the drain barrier and execute a plan referencing the dropped field.
func TestDataViewGate_ComplexDeleteRejectsStaleGeneration(t *testing.T) {
	paramtable.Init()
	g := newDataViewGate()

	// plan built now → capture the generation; the same generation registers fine.
	gen0 := g.CurrentGen(1)
	require.True(t, g.TryRegisterComplexDelete(1, gen0))
	g.DeregisterComplexDelete(1)

	// a drop installs then releases between plan-build and registration → the generation advances.
	applySnapshot(t, g, 1, []UniqueID{100}, true, 1)
	applySnapshot(t, g, 1, nil, false, 2)
	require.NotEqual(t, gen0, g.CurrentGen(1))

	// the pre-drop plan's stale generation is rejected even though no pause is active (the drop released)
	// → the delete retries and rebuilds on the current schema.
	assert.False(t, g.TryRegisterComplexDelete(1, gen0))
	// a freshly-captured generation registers again.
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	g.DeregisterComplexDelete(1)
}

// TestDataViewGate_RefcountSymmetric: register/deregister must stay balanced so an unrelated delete's
// deregister cannot zero out another in-flight delete's refcount and prematurely release a drain.
func TestDataViewGate_RefcountSymmetric(t *testing.T) {
	g := newDataViewGate()

	// delete X registers -> one in-flight complex-delete.
	require.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))

	// delete Y registers and finishes; balanced register/deregister must not touch X's refcount.
	require.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	g.DeregisterComplexDelete(1)

	// X is still in-flight, so installing a drop gate must NOT drain within the timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := g.ApplyGateSnapshot(ctx, 1, []UniqueID{100}, true, 1, true)
	assert.Error(t, err)

	// once X finishes, the drain completes.
	g.DeregisterComplexDelete(1)
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, true, 2, true)
	require.NoError(t, err)
	assert.True(t, applied)
}

func TestDataViewGate_DrainNoInflight(t *testing.T) {
	g := newDataViewGate()
	// no in-flight complex-delete: the drain returns immediately.
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, true, 1, true)
	require.NoError(t, err)
	assert.True(t, applied)
	// pause installed -> a new complex-delete is rejected.
	assert.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	// field is read-blocked.
	_, blocked := g.AnyFieldBlocked(1, 100)
	assert.True(t, blocked)
}

func TestDataViewGate_DrainWaitsForInflight(t *testing.T) {
	g := newDataViewGate()
	require.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // one in-flight

	done := make(chan error, 1)
	go func() {
		_, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, true, 1, true)
		done <- err
	}()

	// the drain must block while a complex-delete is still in-flight.
	select {
	case <-done:
		t.Fatal("ApplyGateSnapshot returned before the in-flight complex-delete drained")
	case <-time.After(100 * time.Millisecond):
	}

	g.DeregisterComplexDelete(1) // drain to zero
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("ApplyGateSnapshot did not return after the drain completed")
	}
	_, blocked := g.AnyFieldBlocked(1, 100)
	assert.True(t, blocked)
}

func TestDataViewGate_DrainTimeoutUndoes(t *testing.T) {
	g := newDataViewGate()
	require.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // stuck in-flight, never deregistered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	applied, err := g.ApplyGateSnapshot(ctx, 1, []UniqueID{100}, true, 1, true)
	assert.Error(t, err)
	assert.False(t, applied)

	// on timeout the snapshot is undone: the read block it added is gone...
	_, blocked := g.AnyFieldBlocked(1, 100)
	assert.False(t, blocked)
	// ...the pause is lifted, so new complex-deletes are accepted again...
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
	// ...and the generation is rolled back so a later snapshot at that generation still applies.
	assert.Equal(t, uint64(0), g.CurrentGen(1))
}

// TestDataViewGate_PauseFollowsSnapshot: the complex-delete pause is exactly what the snapshot carries —
// it stays paused while any drop remains (paused=true) and resumes only when the snapshot clears it.
func TestDataViewGate_PauseFollowsSnapshot(t *testing.T) {
	g := newDataViewGate()
	applied, err := g.ApplyGateSnapshot(context.Background(), 1, []UniqueID{100}, true, 1, true)
	require.NoError(t, err)
	require.True(t, applied)
	assert.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1))) // paused by the drop

	// another dropping field remains: the snapshot still says paused -> pause stays.
	applySnapshot(t, g, 1, []UniqueID{101}, true, 2)
	assert.False(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))

	// last dropping field released: the snapshot clears the pause -> resumes.
	applySnapshot(t, g, 1, nil, false, 3)
	assert.True(t, g.TryRegisterComplexDelete(1, g.CurrentGen(1)))
}

func TestProxy_SyncDataViewGateHandler(t *testing.T) {
	old := globalDataViewGate
	defer func() { globalDataViewGate = old }()
	globalDataViewGate = newDataViewGate()

	node := &Proxy{}

	// unhealthy node -> error status, no gate change.
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	st, err := node.SyncDataViewGate(context.Background(), &proxypb.SyncDataViewGateRequest{
		CollectionID: 1, GatedFieldIds: []int64{100}, Generation: 1,
	})
	assert.NoError(t, err)
	assert.Error(t, merr.Error(st))
	_, blocked := globalDataViewGate.AnyFieldBlocked(1, 100)
	assert.False(t, blocked)

	node.UpdateStateCode(commonpb.StateCode_Healthy)

	// a snapshot installs the read block.
	st, err = node.SyncDataViewGate(context.Background(), &proxypb.SyncDataViewGateRequest{
		CollectionID: 1, GatedFieldIds: []int64{100}, Generation: 1,
	})
	assert.NoError(t, err)
	assert.NoError(t, merr.Error(st))
	_, blocked = globalDataViewGate.AnyFieldBlocked(1, 100)
	assert.True(t, blocked)

	// a newer empty snapshot removes it.
	st, err = node.SyncDataViewGate(context.Background(), &proxypb.SyncDataViewGateRequest{
		CollectionID: 1, GatedFieldIds: nil, Generation: 2,
	})
	assert.NoError(t, err)
	assert.NoError(t, merr.Error(st))
	_, blocked = globalDataViewGate.AnyFieldBlocked(1, 100)
	assert.False(t, blocked)
}
