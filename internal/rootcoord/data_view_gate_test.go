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

package rootcoord

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	internalmocks "github.com/milvus-io/milvus/internal/mocks"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type gateTestDeps struct {
	m       *dataViewGateManager
	catalog *mocks.RootCoordCatalog
	pcm     *proxyutil.MockProxyClientManager
	meta    *mockMetaTable
	broker  *mockBroker
}

func newGateTestDeps(t *testing.T) *gateTestDeps {
	paramtable.Init()
	catalog := mocks.NewRootCoordCatalog(t)
	pcm := proxyutil.NewMockProxyClientManager(t)
	meta := newMockMetaTable()
	broker := &mockBroker{}
	// Snapshot pushes stamp each state change with a monotonic TSO generation; a simple increasing
	// counter is enough for tests (the matcher ignores the exact value).
	tsoAlloc := mocktso.NewAllocator(t)
	var genCtr atomic.Uint64
	tsoAlloc.EXPECT().GenerateTSO(mock.Anything).RunAndReturn(func(uint32) (uint64, error) {
		return genCtr.Add(1), nil
	}).Maybe()
	core := &Core{
		ctx:                context.Background(),
		catalog:            catalog,
		proxyClientManager: pcm,
		meta:               meta,
		broker:             broker,
		tsoAllocator:       tsoAlloc,
	}
	m := newDataViewGateManager(core)
	// Default drop gates to "still pending" so recovery keeps them, isolating tests from the global
	// broadcaster singleton (a test needing the orphan-discard path overrides this).
	m.hasPendingAlterBroadcast = func(int64, int32) bool { return true }
	return &gateTestDeps{
		m:       m,
		catalog: catalog,
		pcm:     pcm,
		meta:    meta,
		broker:  broker,
	}
}

// snapReqMatch builds a mockery argument matcher for the SyncDataViewGate SNAPSHOT the manager pushes:
// the collection's full residual gated-field set (not a delta), its resulting complex-delete pause, and
// whether this push drains. Generation is not matched (a monotonic TSO, non-deterministic across runs).
func snapReqMatch(collID int64, drain bool, paused bool, gatedFields ...int64) interface{} {
	return mock.MatchedBy(func(req *proxypb.SyncDataViewGateRequest) bool {
		if req.GetCollectionID() != collID {
			return false
		}
		if req.GetDrainComplexDelete() != drain || req.GetComplexDeletePaused() != paused {
			return false
		}
		got := req.GetGatedFieldIds()
		if len(got) != len(gatedFields) {
			return false
		}
		gotSet := typeutil.NewSet[int64](got...)
		for _, f := range gatedFields {
			if !gotSet.Contain(f) {
				return false
			}
		}
		return true
	})
}

func (d *gateTestDeps) addOp(collID int64, opVersion int32, kind model.DataViewGateKind, fieldIDs ...int64) {
	d.m.mu.Lock()
	d.m.addOpLocked(&dataViewGateOp{collectionID: collID, opVersion: opVersion, fieldIDs: fieldIDs, kind: kind})
	d.m.mu.Unlock()
}

// TestDataViewGateManager_RecoverDropGatesDiscardsOrphans: recovery keeps a persisted drop gate whose
// AlterCollection broadcast is still pending (the re-driven ack will release it), and discards an orphan
// whose broadcast never durablized (its ack would never fire → it would stay stuck forever).
func TestDataViewGateManager_RecoverDropGatesDiscardsOrphans(t *testing.T) {
	d := newGateTestDeps(t)
	// coll 1's drop (v5) still has a pending broadcast -> keep; coll 2's (v6) has none -> orphan, discard.
	d.m.hasPendingAlterBroadcast = func(collectionID int64, schemaVersion int32) bool {
		return collectionID == 1 && schemaVersion == 5
	}

	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateDrop},
		{CollectionID: 2, OpVersion: 6, FieldIDs: []int64{200}, Kind: model.DataViewGateDrop},
	}, nil).Once()

	// the orphan (coll 2) is discarded: its residual snapshot is empty (block cleared, pause resumed) + unpersist.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything, snapReqMatch(2, false, false)).Return(nil).Once()
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(2), int32(6)).Return(nil).Once()

	require.NoError(t, d.m.recoverDropGatesForPull(context.Background()))

	// coll 1 kept (pending broadcast), coll 2 discarded (orphan).
	assert.True(t, d.m.hasDropOp(1))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
	assert.False(t, d.m.hasDropOp(2))
	assert.Nil(t, d.m.gatedFieldIDs(2))
}

func TestDataViewGateManager_AdmitAndOps(t *testing.T) {
	d := newGateTestDeps(t)
	m := d.m

	// no op in flight -> admitted.
	require.NoError(t, m.admitSchemaChange(1))
	d.addOp(1, 5, model.DataViewGateDrop, 100)
	// a second schema change on the same collection is rejected (retriable) while one is in flight.
	err := m.admitSchemaChange(1)
	assert.ErrorIs(t, err, merr.ErrCollectionSchemaChangeInProgress)
	// a different collection is admitted.
	require.NoError(t, m.admitSchemaChange(2))

	assert.ElementsMatch(t, []int64{100}, m.gatedFieldIDs(1))
	assert.True(t, m.hasDropOp(1))

	// removing the op frees the slot and clears the gated set.
	m.mu.Lock()
	m.removeOpLocked(1, 5)
	m.mu.Unlock()
	assert.Empty(t, m.gatedFieldIDs(1))
	assert.False(t, m.hasDropOp(1))
	require.NoError(t, m.admitSchemaChange(1))
}

func TestDataViewGateManager_ForceReleaseCollection(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(5, 1, model.DataViewGateDrop, 100)
	d.addOp(5, 2, model.DataViewGateAdd, 101)
	// releaseGate pushes RELEASE + unpersists per op (best-effort); allow both, order-independent.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(nil).Twice()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()

	assert.Equal(t, 2, d.m.forceReleaseCollection(context.Background(), 5))
	assert.Empty(t, d.m.gatedFieldIDs(5))
	assert.NoError(t, d.m.admitSchemaChange(5)) // un-frozen
	// no ops on the collection -> nothing to release.
	assert.Equal(t, 0, d.m.forceReleaseCollection(context.Background(), 999))
}

func TestDataViewGateManager_KillSwitch(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(7, 1, model.DataViewGateDrop, 100)
	assert.Error(t, d.m.admitSchemaChange(7)) // enabled -> rejects while an op is in flight

	key := paramtable.Get().RootCoordCfg.DataViewGateEnabled.Key
	paramtable.Get().Save(key, "false")
	defer paramtable.Get().Save(key, "true")

	assert.NoError(t, d.m.admitSchemaChange(7))                                      // disabled -> always admit
	assert.NoError(t, d.m.installDropGate(context.Background(), 8, 1, []int64{200})) // disabled -> install is a no-op
	assert.Empty(t, d.m.gatedFieldIDs(8))
}

func TestDataViewGateManager_GatedFieldIDsUnion(t *testing.T) {
	d := newGateTestDeps(t)
	// two ops on one collection (recovery loads bypass admission): gatedFieldIDs is their union.
	d.addOp(3, 1, model.DataViewGateAdd, 300, 301)
	d.addOp(3, 2, model.DataViewGateDrop, 301, 302)
	assert.ElementsMatch(t, []int64{300, 301, 302}, d.m.gatedFieldIDs(3))
	assert.True(t, d.m.hasDropOp(3))
	assert.Nil(t, d.m.gatedFieldIDs(404)) // unknown collection
}

func TestCore_GetDataViewGate(t *testing.T) {
	d := newGateTestDeps(t)
	core := d.m.core
	core.dataViewGate = d.m
	core.UpdateStateCode(commonpb.StateCode_Healthy)
	// fan-out membership source: proxy 100 is registered, others are not.
	clients := typeutil.NewConcurrentMap[int64, types.ProxyClient]()
	clients.Insert(100, internalmocks.NewMockProxyClient(t))
	d.pcm.EXPECT().GetProxyClients().Return(clients)

	// a drop op: gated fields returned + complex-delete paused.
	d.addOp(7, 1, model.DataViewGateDrop, 700, 701)
	resp, err := core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{CollectionID: 7})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.ElementsMatch(t, []int64{700, 701}, resp.GetGatedFieldIds())
	assert.True(t, resp.GetComplexDeletePaused())

	// requester_in_fanout: true when the caller's ServerID is registered, false otherwise.
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithSourceID(100)), CollectionID: 7,
	})
	require.NoError(t, err)
	assert.True(t, resp.GetRequesterInFanout())
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithSourceID(999)), CollectionID: 7,
	})
	require.NoError(t, err)
	assert.False(t, resp.GetRequesterInFanout())

	// escape hatch: with the gate disabled, in_fanout is reported true unconditionally so a proxy never
	// blocks its startup membership wait (even an unregistered ServerID).
	key := paramtable.Get().RootCoordCfg.DataViewGateEnabled.Key
	paramtable.Get().Save(key, "false")
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithSourceID(999)), CollectionID: 7,
	})
	require.NoError(t, err)
	assert.True(t, resp.GetRequesterInFanout())
	paramtable.Get().Save(key, "true")

	// an add-only op: gated but not complex-delete-paused.
	d.addOp(8, 1, model.DataViewGateAdd, 800)
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{CollectionID: 8})
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{800}, resp.GetGatedFieldIds())
	assert.False(t, resp.GetComplexDeletePaused())

	// unknown collection: empty gate, success.
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{CollectionID: 404})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.Empty(t, resp.GetGatedFieldIds())
	assert.False(t, resp.GetComplexDeletePaused())

	// unhealthy coord returns an error status (retriable), never gate data.
	core.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = core.GetDataViewGate(context.Background(), &rootcoordpb.GetDataViewGateRequest{CollectionID: 7})
	require.NoError(t, err)
	assert.Error(t, merr.Error(resp.GetStatus()))
}

func TestDataViewGateManager_InstallDropGate(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().SaveDataViewGate(mock.Anything, mock.MatchedBy(func(g *model.DataViewGate) bool {
		return g.CollectionID == 1 && g.OpVersion == 5 && g.Kind == model.DataViewGateDrop
	})).Return(nil).Once()
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, true, true, 100)).Return(nil).Once()

	require.NoError(t, d.m.installDropGate(context.Background(), 1, 5, []int64{100}))
	assert.True(t, d.m.hasDropOp(1))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))

	// empty field set is a no-op.
	require.NoError(t, d.m.installDropGate(context.Background(), 2, 1, nil))
}

func TestDataViewGateManager_InstallDropGate_SaveFailsRollsBack(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().SaveDataViewGate(mock.Anything, mock.Anything).Return(errors.New("etcd down")).Once()
	// no push happens when persist fails.
	assert.Error(t, d.m.installDropGate(context.Background(), 1, 5, []int64{100}))
	// rollback: reservation freed, nothing gated.
	assert.Empty(t, d.m.gatedFieldIDs(1))
	assert.False(t, d.m.hasDropOp(1))
}

func TestDataViewGateManager_InstallAddGate(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().SaveDataViewGate(mock.Anything, mock.MatchedBy(func(g *model.DataViewGate) bool {
		return g.CollectionID == 1 && g.OpVersion == 5 && g.Kind == model.DataViewGateAdd
	})).Return(nil).Once()
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false, 100)).Return(nil).Once()

	require.NoError(t, d.m.installAddGate(context.Background(), 1, 5, []int64{100}))
	assert.False(t, d.m.hasDropOp(1)) // add, not drop
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_InstallAddGate_EmptyAndSaveFail(t *testing.T) {
	d := newGateTestDeps(t)
	// empty field set is a no-op (no persist, no push).
	require.NoError(t, d.m.installAddGate(context.Background(), 1, 5, nil))
	assert.Empty(t, d.m.gatedFieldIDs(1))

	// persist failure rolls back the reservation, nothing gated.
	d.catalog.EXPECT().SaveDataViewGate(mock.Anything, mock.Anything).Return(errors.New("etcd down")).Once()
	assert.Error(t, d.m.installAddGate(context.Background(), 2, 3, []int64{200}))
	assert.Empty(t, d.m.gatedFieldIDs(2))
}

func TestDataViewGateManager_InstallAddGate_PushFailsLeaksThenReleaseRollsBack(t *testing.T) {
	d := newGateTestDeps(t)
	// persist SUCCEEDS but the SET push (SyncDataViewGate) FAILS: installAddGate returns the push
	// error and — unlike the persist-fail path — leaves the op in-memory + etcd. Without the caller's
	// rollback this freezes the collection's admission forever (backfill never reaches opVersion since
	// the field was never added). Guards the add-vs-drop asymmetry fix.
	ctx := context.Background()
	d.catalog.EXPECT().SaveDataViewGate(mock.Anything, mock.Anything).Return(nil).Once()
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false, 100)).Return(errors.New("proxy down")).Once()
	assert.Error(t, d.m.installAddGate(ctx, 1, 5, []int64{100}))
	// leaked: admission is now frozen on the collection.
	assert.Error(t, d.m.admitSchemaChange(1))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))

	// the caller's rollback (releaseGate) clears it: RELEASE push + unpersist -> admission un-frozen.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()
	d.m.releaseGate(ctx, 1, 5)
	assert.NoError(t, d.m.admitSchemaChange(1))
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_ReleaseGate_DropResumes(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateDrop, 100)
	// last dropping field released -> residual snapshot empty, complex-deletes resumed.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	d.m.releaseGate(context.Background(), 1, 5)
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_ReleaseGate_DropWithAnotherDropDoesNotResume(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateDrop, 100)
	d.addOp(1, 6, model.DataViewGateDrop, 101)
	// releasing op 5 while another drop (op 6) remains -> residual keeps 101, pause stays.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, true, 101)).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	d.m.releaseGate(context.Background(), 1, 5)
	assert.True(t, d.m.hasDropOp(1)) // op 6 remains
}

func TestDataViewGateManager_ReleaseGate_AddDoesNotResume(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateAdd, 100)
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	d.m.releaseGate(context.Background(), 1, 5)
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_ReleaseGate_NotFound(t *testing.T) {
	d := newGateTestDeps(t)
	// no op present -> early return, no push, no unpersist (mocks assert zero calls at cleanup).
	d.m.releaseGate(context.Background(), 999, 1)
}

func TestDataViewGateManager_RecoverDropGatesForPull(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateDrop},
		{CollectionID: 2, OpVersion: 3, FieldIDs: []int64{200}, Kind: model.DataViewGateAdd},
	}, nil).Once()

	require.NoError(t, d.m.recoverDropGatesForPull(context.Background()))
	// only the drop gate is loaded (load-only: no push, no release).
	assert.True(t, d.m.hasDropOp(1))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
	assert.Empty(t, d.m.gatedFieldIDs(2)) // the add gate is not loaded here
}

func TestDataViewGateManager_RecoverAddGates_Live(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
		{CollectionID: 9, OpVersion: 1, FieldIDs: []int64{900}, Kind: model.DataViewGateDrop}, // ignored by add recovery
	}, nil).Once()
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return &model.Collection{CollectionID: 1, Fields: []*model.Field{{FieldID: 100}}}, nil
	}
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		return 4, nil // < opVersion 5 -> backfill not complete
	}
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false, 100)).Return(nil).Once()

	require.NoError(t, d.m.recoverAddGates(context.Background()))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_RecoverAddGates_StaleCollectionDropped(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
	}, nil).Once()
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	// discarded: best-effort release push + cache invalidation + unpersist; NOT re-installed.
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	require.NoError(t, d.m.recoverAddGates(context.Background()))
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_RecoverAddGates_StaleFieldRemoved(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
	}, nil).Once()
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return &model.Collection{CollectionID: 1, Fields: []*model.Field{{FieldID: 999}}}, nil // field 100 gone
	}
	// no pending broadcast -> the absent field is a genuine removal (not a not-yet-applied add), so discard.
	d.m.hasPendingAlterBroadcast = func(int64, int32) bool { return false }
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	require.NoError(t, d.m.recoverAddGates(context.Background()))
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_RecoverAddGates_StaleBackfillComplete(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
	}, nil).Once()
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return &model.Collection{CollectionID: 1, Fields: []*model.Field{{FieldID: 100}}}, nil
	}
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		return 5, nil // >= opVersion 5 -> already backfilled
	}
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	require.NoError(t, d.m.recoverAddGates(context.Background()))
	assert.Empty(t, d.m.gatedFieldIDs(1))
}

// TestDataViewGateManager_RecoverAddGates_TSOFailReturnsError: a TSO failure during the re-push must fail
// recovery (so the caller's bounded retry re-runs it) rather than leave generation=0 — which a new proxy's
// pull would read as "no gate" and ignore, exposing the un-backfilled field.
func TestDataViewGateManager_RecoverAddGates_TSOFailReturnsError(t *testing.T) {
	d := newGateTestDeps(t)
	failTSO := mocktso.NewAllocator(t)
	failTSO.EXPECT().GenerateTSO(mock.Anything).Return(uint64(0), errors.New("tso down")).Maybe()
	d.m.core.tsoAllocator = failTSO

	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
	}, nil).Once()
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return &model.Collection{CollectionID: 1, Fields: []*model.Field{{FieldID: 100}}}, nil
	}
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		return 4, nil // backfill not complete -> gate kept -> re-push -> snapshotForPush TSO fails
	}

	assert.Error(t, d.m.recoverAddGates(context.Background()))
}

func TestDataViewGateManager_RecoverAddGates_TransientMetaErrorReinstalls(t *testing.T) {
	d := newGateTestDeps(t)
	d.catalog.EXPECT().ListDataViewGates(mock.Anything).Return([]*model.DataViewGate{
		{CollectionID: 1, OpVersion: 5, FieldIDs: []int64{100}, Kind: model.DataViewGateAdd},
	}, nil).Once()
	// a non-not-found error is transient -> conservative re-install (not discarded).
	d.meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
		return nil, errors.New("etcd timeout")
	}
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false, 100)).Return(nil).Once()

	require.NoError(t, d.m.recoverAddGates(context.Background()))
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_ReleaseCompletedAddGates(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateAdd, 100)
	d.addOp(2, 8, model.DataViewGateAdd, 200)
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		if collectionID == 1 {
			return 5, nil // reached opVersion -> release
		}
		return 3, nil // coll 2 not complete
	}
	// add-release invalidates the collection meta cache (force re-pull) BEFORE releasing; only collection 1.
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.MatchedBy(func(req *proxypb.InvalidateCollMetaCacheRequest) bool {
		return req.GetCollectionID() == 1
	})).Return(nil).Once()
	d.pcm.EXPECT().SyncDataViewGate(mock.Anything,
		snapReqMatch(1, false, false)).Return(nil).Once()
	d.catalog.EXPECT().DropDataViewGate(mock.Anything, int64(1), int32(5)).Return(nil).Once()

	d.m.releaseCompletedAddGates(context.Background())
	assert.Empty(t, d.m.gatedFieldIDs(1))
	assert.ElementsMatch(t, []int64{200}, d.m.gatedFieldIDs(2)) // still gated
}

// TestDataViewGateManager_ReleaseCompletedAddGates_InvalidateFailKeepsGate: if the cache invalidation
// fails, the add gate is NOT released (no push, no unpersist) so the next check-loop tick retries — the
// loop is this path's equivalent of the drop ack callback's retry-till-success.
func TestDataViewGateManager_ReleaseCompletedAddGates_InvalidateFailKeepsGate(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateAdd, 100)
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		return 5, nil // complete
	}
	d.pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(errors.New("proxy down")).Once()

	d.m.releaseCompletedAddGates(context.Background())
	// invalidation failed -> gate kept (no releaseGate push / unpersist), retried next tick.
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_ReleaseCompletedAddGates_QueryErrorKeepsGate(t *testing.T) {
	d := newGateTestDeps(t)
	d.addOp(1, 5, model.DataViewGateAdd, 100)
	d.broker.AliveSegmentMinSchemaVersionFunc = func(ctx context.Context, collectionID int64) (int32, error) {
		return 0, errors.New("datacoord unavailable")
	}
	// query failed -> gate is kept, retried next tick; no release push.
	d.m.releaseCompletedAddGates(context.Background())
	assert.ElementsMatch(t, []int64{100}, d.m.gatedFieldIDs(1))
}

func TestDataViewGateManager_StartStop(t *testing.T) {
	d := newGateTestDeps(t)
	// no ops -> the loop starts and stops cleanly without touching any dependency.
	d.m.Start()
	d.m.Stop()
}
