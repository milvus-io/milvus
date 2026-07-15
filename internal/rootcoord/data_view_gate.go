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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DataViewGate — RootCoord side.
//
// RootCoord OWNS the whole gate lifecycle through a single dataViewGateManager per Core: it is the
// authoritative per-operation state (source of truth for BOTH the push to already-connected proxies
// and the pull by new/recovered proxies), its per-operation etcd persistence, and the ONE background
// loop that releases completed add-gates. Nothing else spawns gate goroutines or holds gate state.
//
// A gate operation corresponds 1-1 to a schema-change DDL: exactly one install and one release. Each
// DDL increments coll.SchemaVersion, so opVersion (the schema version the DDL produced) uniquely keys
// an operation on a collection; for an add it is also the completion target (release once every alive
// segment reaches it). Deltas are pushed via the dedicated proxy RPC SyncDataViewGate (Set/Release) —
// it updates only the proxy read blocklist / complex-delete pause, never the schema cache; its
// blocking fan-out is also the drop's complex-delete drain barrier.

// dataViewGateOp is one gate operation (one schema-change DDL). Set and Release correspond 1-1.
type dataViewGateOp struct {
	collectionID int64
	opVersion    int32 // schema version this DDL produced; unique per collection; for add, the completion target
	fieldIDs     []int64
	// kind drives the release path: Drop is released synchronously by the DDL; Add is released by the
	// check loop on backfill completion. Carrying the Kind enum (not a bool) keeps this extensible if a
	// future gate kind needs a different release rule.
	kind model.DataViewGateKind
}

type dataViewGateManager struct {
	core   *Core
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu  sync.RWMutex
	ops map[int64]map[int32]*dataViewGateOp // collectionID -> opVersion -> op
	// gen is the latest RootCoord-monotonic generation stamped on each collection's gate snapshot. Every
	// state change (install/release/recover) allocates a fresh TSO and bumps it; the push carries it and
	// the pull returns it, so a proxy applies a snapshot only when its generation is newer — making
	// reordered/stale SET/RELEASE deltas harmless (no wire version was needed before, hence the bug).
	gen map[int64]uint64 // collectionID -> latest generation

	// hasPendingAlterBroadcast reports whether an AlterCollection broadcast producing the given schema
	// version on the collection is still pending; recovery uses it to discard orphan drop gates. A field
	// (defaulting to the broadcaster singleton) so tests can inject without touching the global singleton.
	hasPendingAlterBroadcast func(collectionID int64, schemaVersion int32) bool
}

func newDataViewGateManager(core *Core) *dataViewGateManager {
	ctx, cancel := context.WithCancel(core.ctx)
	return &dataViewGateManager{
		core:                     core,
		ctx:                      ctx,
		cancel:                   cancel,
		ops:                      make(map[int64]map[int32]*dataViewGateOp),
		gen:                      make(map[int64]uint64),
		hasPendingAlterBroadcast: broadcast.HasPendingAlterCollectionBroadcast,
	}
}

// gateKindLabel maps a gate kind to its metric label value.
func gateKindLabel(kind model.DataViewGateKind) string {
	if kind == model.DataViewGateAdd {
		return "add"
	}
	return "drop"
}

// drainResultLabel classifies a drop gate's drain outcome for the drain-duration metric.
func drainResultLabel(drainCtx context.Context, err error) string {
	if err == nil {
		return "ok"
	}
	if drainCtx.Err() == context.DeadlineExceeded {
		return "timeout"
	}
	return "error"
}

func (m *dataViewGateManager) addOpLocked(op *dataViewGateOp) {
	byVer, ok := m.ops[op.collectionID]
	if !ok {
		byVer = make(map[int32]*dataViewGateOp)
		m.ops[op.collectionID] = byVer
	}
	byVer[op.opVersion] = op
}

// admitSchemaChange is the DataViewGate admission gate: it rejects a new schema-change DDL when the
// collection still has a gate op in flight — a drop/add_function whose POST-APPLY gate window (backfill
// or GC) has not closed. Check-only (adds nothing): the broadcast exclusive-collection resource key
// already serializes the broadcast->apply window crash-safely (re-locked on recovery), so add_field,
// which has no post-apply window, needs no op at all — just this check. Called once at the common place
// (broadcastAlterCollectionSchema) for every kind. The error is retriable; the proxy admission-retries.
// enabled reports the DataViewGate master kill-switch. When off the gate is inert: RootCoord stops
// admitting/installing and proxies stop enforcing, so a stuck gate can never freeze DDLs/reads.
func (m *dataViewGateManager) enabled() bool {
	return paramtable.Get().RootCoordCfg.DataViewGateEnabled.GetAsBool()
}

func (m *dataViewGateManager) admitSchemaChange(collID int64) error {
	if !m.enabled() {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.ops[collID]) > 0 {
		metrics.RootCoordDataViewGateAdmissionReject.Inc()
		mlog.Debug(m.ctx, "data view gate: schema-change admission rejected, another gate op is in flight on the collection",
			mlog.Int64("collectionID", collID))
		return merr.WrapErrCollectionSchemaChangeInProgress(collID, "another schema change is draining/backfilling on the collection")
	}
	return nil
}

// forceReleaseCollection force-releases every gate op on the collection through releaseGate, so etcd +
// in-memory ops + proxy blocks are all cleared. Operator escape hatch to un-freeze a collection whose
// DDLs are stuck behind a gate op that never released (e.g. an add_function backfill that never
// completes). Returns the number of ops released. DANGEROUS: it voids the released ops' guarantees (a
// still-backfilling field becomes readable with incomplete data), so it must be a deliberate action.
func (m *dataViewGateManager) forceReleaseCollection(ctx context.Context, collID int64) int {
	m.mu.RLock()
	versions := make([]int32, 0, len(m.ops[collID]))
	for v := range m.ops[collID] {
		versions = append(versions, v)
	}
	m.mu.RUnlock()
	for _, v := range versions {
		m.releaseGate(ctx, collID, v)
	}
	if len(versions) > 0 {
		mlog.Warn(ctx, "data view gate: force-released all gate ops on collection (operator escape hatch)",
			mlog.Int64("collectionID", collID), mlog.Int("releasedCount", len(versions)))
	}
	return len(versions)
}

func (m *dataViewGateManager) removeOpLocked(collID int64, opVersion int32) {
	byVer, ok := m.ops[collID]
	if !ok {
		return
	}
	delete(byVer, opVersion)
	if len(byVer) == 0 {
		delete(m.ops, collID)
	}
}

// hasDropOpLocked reports whether any drop op remains on the collection (the pause must stay).
func (m *dataViewGateManager) hasDropOpLocked(collID int64) bool {
	for _, op := range m.ops[collID] {
		if op.kind == model.DataViewGateDrop {
			return true
		}
	}
	return false
}

// hasDropOp reports whether the collection currently has an active drop gate — served on the
// DescribeCollection pull so a new/recovered proxy pauses complex-deletes for the drop window.
func (m *dataViewGateManager) hasDropOp(collID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hasDropOpLocked(collID)
}

// gatedFieldIDs returns every field currently gated on the collection (union across its ops), served
// in DescribeCollectionResponse so a (re)describing proxy can rebuild its read blocklist (the pull).
func (m *dataViewGateManager) gatedFieldIDs(collID int64) []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := typeutil.NewSet[int64]()
	for _, op := range m.ops[collID] {
		s.Insert(op.fieldIDs...)
	}
	if s.Len() == 0 {
		return nil
	}
	return s.Collect()
}

// gateStateLocked returns the collection's complete currently-gated field set (union across its ops)
// and complex-delete pause flag (any drop op present). Caller holds m.mu. This is the single source of
// the snapshot both the push and the pull carry.
func (m *dataViewGateManager) gateStateLocked(collID int64) ([]int64, bool) {
	s := typeutil.NewSet[int64]()
	paused := false
	for _, op := range m.ops[collID] {
		s.Insert(op.fieldIDs...)
		if op.kind == model.DataViewGateDrop {
			paused = true
		}
	}
	if s.Len() == 0 {
		return nil, paused
	}
	return s.Collect(), paused
}

// gateSnapshot returns the collection's (gated fields, paused, generation) under a SINGLE RLock, so the
// DescribeCollection pull serves a consistent triple. The generation lets the pulling proxy drop the
// snapshot if a newer push already landed.
func (m *dataViewGateManager) gateSnapshot(collID int64) ([]int64, bool, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fields, paused := m.gateStateLocked(collID)
	return fields, paused, m.gen[collID]
}

// snapshotForPush allocates a fresh monotonic generation and, atomically with it, reads the collection's
// current full gate state — so the (state, generation) pair pushed to proxies is consistent and the
// generation order matches the state-mutation order (both taken under one lock acquisition; TSO alloc is
// in-memory-fast). Callers mutate m.ops first, then call this, then fan the returned triple out.
func (m *dataViewGateManager) snapshotForPush(ctx context.Context, collID int64) (fields []int64, paused bool, gen uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gen, err = m.core.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return nil, false, 0, err
	}
	m.gen[collID] = gen
	fields, paused = m.gateStateLocked(collID)
	return fields, paused, gen, nil
}

// pushActiveGatesToProxy re-syncs every active gate to a single newly-joined proxy, closing the window
// where a proxy that pulled before a gate installed AND joined the fan-out after the install's push would
// learn of the gate via neither path. Per-proxy (not fan-out) so already-synced proxies are not re-drained;
// async because a Set carrying a drop's complex-delete drain blocks until the proxy drains and the session
// watcher must not stall on it. Best-effort: on failure the DescribeCollection pull converges.
func (m *dataViewGateManager) pushActiveGatesToProxy(proxyID int64) {
	if m == nil || !m.enabled() {
		return
	}
	type gatePush struct {
		collID   int64
		fieldIDs []int64
		paused   bool
		gen      uint64
	}
	m.mu.RLock()
	pushes := make([]gatePush, 0, len(m.ops))
	for collID := range m.ops {
		fields, paused := m.gateStateLocked(collID)
		if len(fields) > 0 || paused {
			// Reuse the collection's current generation (no fresh alloc): catch-up re-sends the SAME
			// authoritative snapshot, so a proxy that already has it (>= gen) idempotently ignores it.
			pushes = append(pushes, gatePush{collID: collID, fieldIDs: fields, paused: paused, gen: m.gen[collID]})
		}
	}
	m.mu.RUnlock()
	if len(pushes) == 0 {
		return
	}
	client, ok := m.core.proxyClientManager.GetProxyClients().Get(proxyID)
	if !ok {
		mlog.Warn(m.ctx, "data view gate: joined proxy not in client manager yet, skip catch-up push (pull converges)",
			mlog.Int64("proxyID", proxyID))
		return
	}
	go func() {
		for _, p := range pushes {
			sta, callErr := client.SyncDataViewGate(m.ctx, &proxypb.SyncDataViewGateRequest{
				CollectionID:        p.collID,
				GatedFieldIds:       p.fieldIDs,
				ComplexDeletePaused: p.paused,
				Generation:          p.gen,
				// Catch-up never drains: a freshly-joined proxy has no in-flight complex-deletes; the drain
				// barrier is only the drop install's synchronous fan-out.
				DrainComplexDelete: false,
			})
			if err := merr.CheckRPCCall(sta, callErr); err != nil {
				mlog.Warn(m.ctx, "data view gate: catch-up push to joined proxy failed; pull converges",
					mlog.Int64("proxyID", proxyID), mlog.Int64("collectionID", p.collID), mlog.Err(err))
			}
		}
	}()
}

// installDropGate registers a drop op, blocks reads on the dropped fields, and drains in-flight
// complex-deletes on the collection — all BEFORE the schema-change broadcast (drain-then-DDL orders
// those deletes ahead of the drop). Persist precedes push: the complex-delete pause is push-only
// (not served on the pull), so a crash after pushing must be recoverable to resume paused proxies.
// Returns error if any proxy fails; the caller must releaseGate to roll back.
func (m *dataViewGateManager) installDropGate(ctx context.Context, collID int64, opVersion int32, fieldIDs []int64) error {
	if !m.enabled() || len(fieldIDs) == 0 {
		return nil
	}
	// Admission was granted at broadcastAlterCollectionSchema; register the drop op (recovery re-adds the
	// same way). The op covers the post-apply drop-GC window that the broadcast resource key does not.
	m.mu.Lock()
	m.addOpLocked(&dataViewGateOp{collectionID: collID, opVersion: opVersion, fieldIDs: fieldIDs, kind: model.DataViewGateDrop})
	m.mu.Unlock()

	if err := m.core.catalog.SaveDataViewGate(ctx, &model.DataViewGate{
		CollectionID: collID, OpVersion: opVersion, FieldIDs: fieldIDs, Kind: model.DataViewGateDrop,
	}); err != nil {
		m.mu.Lock()
		m.removeOpLocked(collID, opVersion)
		m.mu.Unlock()
		return err
	}
	metrics.RootCoordDataViewGateActive.WithLabelValues("drop").Inc()
	metrics.RootCoordDataViewGateOpTotal.WithLabelValues("drop", "install").Inc()
	mlog.Info(ctx, "data view gate: installed drop gate, draining in-flight complex-deletes before the drop",
		mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.Int64s("fieldIDs", fieldIDs))
	// Snapshot the collection's full gate state + a fresh generation, then fan it out. drain=true makes
	// this push also pause + drain in-flight complex-deletes before ACK (the drop barrier).
	fields, paused, gen, err := m.snapshotForPush(ctx, collID)
	if err != nil {
		m.mu.Lock()
		m.removeOpLocked(collID, opVersion)
		m.mu.Unlock()
		return err
	}
	// Bounded drain: cap how long the fan-out waits for proxies to drain in-flight complex-deletes, so
	// a stuck complex-delete makes the drop fail-and-retry instead of hanging the RPC / DDL forever.
	drainCtx, cancel := context.WithTimeout(ctx, paramtable.Get().RootCoordCfg.DataViewGateDrainTimeout.GetAsDuration(time.Second))
	defer cancel()
	start := time.Now()
	err = m.core.proxyClientManager.SyncDataViewGate(drainCtx, &proxypb.SyncDataViewGateRequest{
		CollectionID:        collID,
		GatedFieldIds:       fields,
		ComplexDeletePaused: paused,
		Generation:          gen,
		DrainComplexDelete:  true,
	})
	elapsed := time.Since(start)
	result := drainResultLabel(drainCtx, err)
	metrics.RootCoordDataViewGateDrainDuration.WithLabelValues(result).Observe(float64(elapsed.Milliseconds()))
	// Success is covered by the install log above + the drain-duration metric; only a non-clean drain
	// (timeout/error, which rolls the drop back) is worth a log.
	if err != nil {
		mlog.Warn(ctx, "data view gate: drop gate drain did not complete cleanly, drop will roll back and retry",
			mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion),
			mlog.String("result", result), mlog.Duration("elapsed", elapsed), mlog.Err(err))
	}
	return err
}

// installAddGate registers an add op and blocks reads on the added function-output field. No
// complex-delete drain (a brand-new field has nothing in-flight). The field stays gated until the
// check loop releases it once the backfill has reached opVersion on every alive segment.
func (m *dataViewGateManager) installAddGate(ctx context.Context, collID int64, opVersion int32, fieldIDs []int64) error {
	if !m.enabled() || len(fieldIDs) == 0 {
		return nil
	}
	// Admission was granted at broadcastAlterCollectionSchema; register the add op (recovery re-adds the
	// same way). The op covers the post-apply backfill window that the broadcast resource key does not.
	m.mu.Lock()
	m.addOpLocked(&dataViewGateOp{collectionID: collID, opVersion: opVersion, fieldIDs: fieldIDs, kind: model.DataViewGateAdd})
	m.mu.Unlock()

	if err := m.core.catalog.SaveDataViewGate(ctx, &model.DataViewGate{
		CollectionID: collID, OpVersion: opVersion, FieldIDs: fieldIDs, Kind: model.DataViewGateAdd,
	}); err != nil {
		m.mu.Lock()
		m.removeOpLocked(collID, opVersion)
		m.mu.Unlock()
		return err
	}
	metrics.RootCoordDataViewGateActive.WithLabelValues("add").Inc()
	metrics.RootCoordDataViewGateOpTotal.WithLabelValues("add", "install").Inc()
	mlog.Info(ctx, "data view gate: installed add gate, field(s) gated until backfill reaches this version on every alive segment",
		mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.Int64s("fieldIDs", fieldIDs))
	fields, paused, gen, err := m.snapshotForPush(ctx, collID)
	if err != nil {
		m.mu.Lock()
		m.removeOpLocked(collID, opVersion)
		m.mu.Unlock()
		return err
	}
	return m.core.proxyClientManager.SyncDataViewGate(ctx, &proxypb.SyncDataViewGateRequest{
		CollectionID:        collID,
		GatedFieldIds:       fields,
		ComplexDeletePaused: paused,
		Generation:          gen,
		DrainComplexDelete:  false,
	})
}

// releaseGate ends one gate operation: it unblocks the op's fields on all proxies and, when the op is
// a drop AND no drop op remains on the collection, resumes complex-deletes there (an add release, or a
// drop while another drop is still active, leaves the pause untouched). Idempotent. Best-effort push /
// unpersist: a proxy that misses the release self-heals on its next meta re-sync, and a failed
// unpersist is reconciled on the next restart.
func (m *dataViewGateManager) releaseGate(ctx context.Context, collID int64, opVersion int32) {
	m.mu.Lock()
	op, ok := m.ops[collID][opVersion]
	if !ok {
		m.mu.Unlock()
		return
	}
	kind := op.kind
	m.removeOpLocked(collID, opVersion)
	m.mu.Unlock()

	metrics.RootCoordDataViewGateActive.WithLabelValues(gateKindLabel(kind)).Dec()
	metrics.RootCoordDataViewGateOpTotal.WithLabelValues(gateKindLabel(kind), "release").Inc()
	mlog.Info(ctx, "data view gate: releasing gate, field(s) visible again",
		mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.String("kind", gateKindLabel(kind)))

	// Push the collection's post-release full snapshot; the paused flag is recomputed (stays true while
	// any other drop op remains), so no explicit resume flag is needed and a reordered snapshot from a
	// concurrent op is dropped by the generation check on the proxy.
	fields, paused, gen, err := m.snapshotForPush(ctx, collID)
	if err != nil {
		mlog.Warn(ctx, "data view gate: release generation alloc failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.Err(err))
	} else if err := m.core.proxyClientManager.SyncDataViewGate(ctx, &proxypb.SyncDataViewGateRequest{
		CollectionID:        collID,
		GatedFieldIds:       fields,
		ComplexDeletePaused: paused,
		Generation:          gen,
		DrainComplexDelete:  false,
	}); err != nil {
		mlog.Warn(ctx, "data view gate: release push failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.Err(err))
	}
	if err := retry.Do(ctx, func() error {
		return m.core.catalog.DropDataViewGate(ctx, collID, opVersion)
	}, retry.Attempts(3)); err != nil {
		mlog.Warn(ctx, "data view gate: unpersist failed; reconciled on next restart",
			mlog.Int64("collectionID", collID), mlog.Int32("opVersion", opVersion), mlog.Err(err))
	}
}

// recoverDropGatesForPull LOADS persisted drop gates into memory (no push, no release) so the
// DescribeCollection pull serves them (complex-delete pause + read block) right away, but ONLY those a
// pending AlterCollection broadcast still backs — an orphan gate whose broadcast never durablized (crash
// before Broadcast) or already completed is discarded, since its sole releaser (the ack callback) will
// never fire and it would otherwise stay stuck forever. It MUST run BEFORE RegisterDDLCallbacks so the
// re-driven ack callback finds the (kept) op to release — same recover-before-register discipline as the
// file-resource recovery (#48612); and it runs AFTER streamingcoord recovers the broadcaster (awaited in
// streamingcoord.Start before RootCoord.Init), so HasPendingAlterCollectionBroadcast is authoritative. A
// kept drop gate then stays active (pull-served + kept on surviving proxies) until the re-driven ack
// releases it; no re-drain (the pre-commit drain already WAL-ordered the in-flight deletes).
func (m *dataViewGateManager) recoverDropGatesForPull(ctx context.Context) error {
	gates, err := m.core.catalog.ListDataViewGates(ctx)
	if err != nil {
		return err
	}
	for _, g := range gates {
		if g.Kind != model.DataViewGateDrop {
			continue
		}
		// A persisted drop gate is legit only while its AlterCollection broadcast is still pending (its
		// re-driven ack callback — the sole releaser — will run). No pending broadcast means the ack will
		// never fire: the drop either never durablized (crash before Broadcast) or already completed, so
		// the gate is an orphan that would otherwise stay stuck forever (reads blocked, deletes paused,
		// admission frozen) — discard it. Safe here: streamingcoord.Start recovers + awaits the broadcaster
		// (loading all pending tasks) before RootCoord.Init runs this, so the query is authoritative.
		if !m.hasPendingAlterBroadcast(g.CollectionID, g.OpVersion) {
			m.discardStaleDropGate(ctx, g)
			continue
		}
		m.mu.Lock()
		m.addOpLocked(&dataViewGateOp{collectionID: g.CollectionID, opVersion: g.OpVersion, fieldIDs: g.FieldIDs, kind: g.Kind})
		// Assign a generation so the pull (GetDataViewGate) serves a non-zero, authoritative version;
		// without it a fresh proxy (lastGen=0) would reject the pulled snapshot (0 not > 0). Load-only —
		// no push here; the pull is what serves recovered drop gates.
		gen, genErr := m.core.tsoAllocator.GenerateTSO(1)
		if genErr != nil {
			m.mu.Unlock()
			return genErr
		}
		m.gen[g.CollectionID] = gen
		m.mu.Unlock()
		metrics.RootCoordDataViewGateActive.WithLabelValues("drop").Inc()
	}
	return nil
}

// discardStaleDropGate clears an orphan drop gate (no pending broadcast will ever release it): a
// best-effort release push drops the block + resumes complex-deletes on any proxy that loaded it via the
// pull, then the record is unpersisted (bounded retry; a persistent failure is reconciled on the next
// restart since the gate stays listed and is re-validated).
// invalidateCollectionMetaCache forces surviving proxies to re-describe (and thus re-pull the current
// gate) for collID, so a proxy that missed a best-effort gate push still converges. Bounded retry; a
// persistent failure is benign — the next describe of the collection re-pulls anyway.
func (m *dataViewGateManager) invalidateCollectionMetaCache(ctx context.Context, collID int64) error {
	return retry.Do(ctx, func() error {
		return m.core.proxyClientManager.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
			CollectionID: collID,
		})
	}, retry.Attempts(3))
}

func (m *dataViewGateManager) discardStaleDropGate(ctx context.Context, g *model.DataViewGate) {
	mlog.Info(ctx, "data view gate: discarding orphan drop gate on recovery (no pending broadcast will release it)",
		mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Int64s("fieldIDs", g.FieldIDs))
	// The orphan is not in m.ops, so the snapshot reflects the collection's residual state (this gate's
	// fields cleared, complex-delete pause dropped unless another drop remains) at a fresh generation.
	fields, paused, gen, err := m.snapshotForPush(ctx, g.CollectionID)
	if err != nil {
		mlog.Warn(ctx, "data view gate: orphan drop-gate discard generation alloc failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	} else if err := m.core.proxyClientManager.SyncDataViewGate(ctx, &proxypb.SyncDataViewGateRequest{
		CollectionID:        g.CollectionID,
		GatedFieldIds:       fields,
		ComplexDeletePaused: paused,
		Generation:          gen,
		DrainComplexDelete:  false,
	}); err != nil {
		mlog.Warn(ctx, "data view gate: orphan drop-gate release push failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
	// The release push is best-effort; invalidate surviving proxies' meta cache so one that missed it
	// re-pulls and drops the now-orphan pause (a stale drop pause blocks complex-deletes = write stall).
	if err := m.invalidateCollectionMetaCache(ctx, g.CollectionID); err != nil {
		mlog.Warn(ctx, "data view gate: orphan drop-gate cache invalidation failed; surviving proxies self-heal on next describe",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
	if err := retry.Do(ctx, func() error {
		return m.core.catalog.DropDataViewGate(ctx, g.CollectionID, g.OpVersion)
	}, retry.Attempts(3)); err != nil {
		mlog.Warn(ctx, "data view gate: orphan drop-gate unpersist failed; reconciled on next restart",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
}

// recoverAddGates RE-INSTALLS persisted add gates after restart (re-push the read block; the check
// loop then releases them on backfill completion). Runs in startInternal — it needs the
// proxyClientManager, unlike the load-only drop recovery. Add gates are not released by the ack
// callback, so they have no recover-vs-ack race.
//
// Each gate is validated against live meta before re-installing: one whose collection was dropped,
// whose field no longer exists, or whose backfill already completed is stale — re-pushing it would
// transiently gate a ready or absent field, so it is discarded instead. Drop gates are intentionally
// NOT validated (see recoverDropGatesForPull): the ack callback is their sole releaser and must
// always find them.
func (m *dataViewGateManager) recoverAddGates(ctx context.Context) error {
	gates, err := m.core.catalog.ListDataViewGates(ctx)
	if err != nil {
		return err
	}
	for _, g := range gates {
		if g.Kind != model.DataViewGateAdd {
			continue
		}
		if reason, stale := m.addGateStale(ctx, g); stale {
			m.discardStaleAddGate(ctx, g, reason)
			continue
		}
		m.mu.Lock()
		m.addOpLocked(&dataViewGateOp{collectionID: g.CollectionID, opVersion: g.OpVersion, fieldIDs: g.FieldIDs, kind: g.Kind})
		m.mu.Unlock()
		metrics.RootCoordDataViewGateActive.WithLabelValues("add").Inc()
		fields, paused, gen, err := m.snapshotForPush(ctx, g.CollectionID)
		if err != nil {
			// A failed generation alloc leaves m.gen[collID]=0, which a new proxy's pull reads as "no gate"
			// (0 not > its lastGen) and ignores — exposing the un-backfilled field. Fail recovery so the
			// caller's bounded retry re-runs it and assigns a real generation, rather than proceeding at 0.
			return err
		}
		if err := m.core.proxyClientManager.SyncDataViewGate(ctx, &proxypb.SyncDataViewGateRequest{
			CollectionID:        g.CollectionID,
			GatedFieldIds:       fields,
			ComplexDeletePaused: paused,
			Generation:          gen,
			DrainComplexDelete:  false,
		}); err != nil {
			// Push is best-effort: the DescribeCollection pull converges a proxy that missed it (the
			// generation is already assigned above, so the pull is authoritative).
			mlog.Warn(ctx, "data view gate: failed to re-push add gate on recovery, will rely on loop + pull",
				mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
		}
	}
	return nil
}

// addGateStale reports whether a persisted add gate no longer needs re-installing: its collection was
// dropped, its gated field was removed from the schema, or its backfill already reached opVersion on
// every alive segment. A transient meta / completion-query error is treated as NOT stale (conservative:
// re-install and let the check loop + pull converge). Runs after the meta table has reloaded from KV,
// so a not-found here is authoritative.
func (m *dataViewGateManager) addGateStale(ctx context.Context, g *model.DataViewGate) (string, bool) {
	coll, err := m.core.meta.GetCollectionByID(ctx, "", g.CollectionID, typeutil.MaxTimestamp, false)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return "collection dropped", true
	}
	if err != nil {
		return "", false
	}
	for _, fid := range g.FieldIDs {
		if !collectionHasField(coll, fid) {
			// A field is absent either because it was removed, OR because the add's AlterCollection ack has
			// not applied yet (Broadcast durablized, then a crash before apply). While that broadcast is
			// still pending the field is not-yet-added, not removed — keep the gate (mirror
			// recoverDropGatesForPull) so the re-driven ack re-adds it with the gate intact; discarding here
			// would expose the un-backfilled function-output field (empty/wrong values) once the ack applies.
			if m.hasPendingAlterBroadcast(g.CollectionID, g.OpVersion) {
				return "", false
			}
			return "gated field removed from schema", true
		}
	}
	if minVersion, err := m.core.broker.AliveSegmentMinSchemaVersion(ctx, g.CollectionID); err == nil && minVersion >= g.OpVersion {
		return "backfill already complete", true
	}
	return "", false
}

// discardStaleAddGate clears a stale add gate: a best-effort release push drops the block from any proxy
// that survived the restart still holding it, then the record is unpersisted (bounded retry; a
// persistent failure is reconciled on the next restart since the gate stays listed).
func (m *dataViewGateManager) discardStaleAddGate(ctx context.Context, g *model.DataViewGate, reason string) {
	mlog.Info(ctx, "data view gate: discarding stale add gate on recovery",
		mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.String("reason", reason))
	// The stale gate is not in m.ops, so the snapshot reflects the collection's residual state at a fresh
	// generation — clearing this gate's field on any proxy that survived the restart still holding it.
	fields, paused, gen, err := m.snapshotForPush(ctx, g.CollectionID)
	if err != nil {
		mlog.Warn(ctx, "data view gate: stale add-gate discard generation alloc failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	} else if err := m.core.proxyClientManager.SyncDataViewGate(ctx, &proxypb.SyncDataViewGateRequest{
		CollectionID:        g.CollectionID,
		GatedFieldIds:       fields,
		ComplexDeletePaused: paused,
		Generation:          gen,
		DrainComplexDelete:  false,
	}); err != nil {
		mlog.Warn(ctx, "data view gate: stale add-gate release push failed; proxies self-heal on next meta re-sync",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
	// Best-effort push may be missed; invalidate surviving proxies' meta cache so one that missed it
	// re-pulls and drops the now-orphan read block on the added field.
	if err := m.invalidateCollectionMetaCache(ctx, g.CollectionID); err != nil {
		mlog.Warn(ctx, "data view gate: stale add-gate cache invalidation failed; surviving proxies self-heal on next describe",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
	if err := retry.Do(ctx, func() error {
		return m.core.catalog.DropDataViewGate(ctx, g.CollectionID, g.OpVersion)
	}, retry.Attempts(3)); err != nil {
		mlog.Warn(ctx, "data view gate: stale add-gate unpersist failed; reconciled on next restart",
			mlog.Int64("collectionID", g.CollectionID), mlog.Int32("opVersion", g.OpVersion), mlog.Err(err))
	}
}

// collectionHasField reports whether fieldID is still present in the collection's schema.
func collectionHasField(coll *model.Collection, fieldID int64) bool {
	for _, f := range coll.Fields {
		if f.FieldID == fieldID {
			return true
		}
	}
	return false
}

// Start launches the single background loop that releases completed add-gates.
func (m *dataViewGateManager) Start() {
	m.wg.Add(1)
	go m.checkLoop()
}

// Stop cancels the loop and waits for it to exit.
func (m *dataViewGateManager) Stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *dataViewGateManager) checkLoop() {
	defer m.wg.Done()
	for {
		interval := paramtable.Get().RootCoordCfg.DataViewGateCheckInterval.GetAsDuration(time.Second)
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(interval):
			if !m.enabled() {
				// Kill-switch flipped off: RootCoord is the single owner + reader of the flag, and proxies
				// enforce installed state only, so RootCoord must drive the release — clear every active
				// gate here so a disabled DataViewGate really becomes inert everywhere.
				m.releaseAllGates(m.ctx)
				continue
			}
			m.releaseCompletedAddGates(m.ctx)
		}
	}
}

// releaseAllGates releases every active gate op on every collection — driven by the check loop when the
// master kill-switch is off. Since proxies no longer read the flag (they enforce installed state only),
// RootCoord releasing the state is how "disabled => inert everywhere" is realized.
func (m *dataViewGateManager) releaseAllGates(ctx context.Context) {
	m.mu.RLock()
	collIDs := make([]int64, 0, len(m.ops))
	for collID := range m.ops {
		collIDs = append(collIDs, collID)
	}
	m.mu.RUnlock()
	for _, collID := range collIDs {
		m.forceReleaseCollection(ctx, collID)
	}
}

// releaseCompletedAddGates queries the min alive-segment schema_version ONCE per collection that has
// add ops, then releases every add op whose opVersion has been reached — so a collection with many
// add ops costs one DataCoord call, and the goroutine count is fixed at one regardless of gate count.
//
// SCOPE (add side solves HALF the problem, intentionally): AliveSegmentMinSchemaVersion signals
// STORAGE-side backfill completion, which BOUNDS but does not eliminate the add exposure window — a
// just-released field may still read default-filled on a QueryNode segment that has not yet reloaded the
// new binlog/index (schema-only reopen). This gate shrinks the exposure from [DDL-apply .. served] to
// [storage-backfilled .. served] (cutting the long backfill compute/write tail); the remaining
// QueryNode serving-readiness half is a follow-up (see PR #51558's segcore reopen). The drop side, by
// contrast, is fully covered here (read-block + complex-delete drain). Do NOT read this as an add
// correctness guarantee.
func (m *dataViewGateManager) releaseCompletedAddGates(ctx context.Context) {
	m.mu.RLock()
	pending := make(map[int64][]int32) // collectionID -> add opVersions
	for collID, byVer := range m.ops {
		for ver, op := range byVer {
			if op.kind == model.DataViewGateAdd {
				pending[collID] = append(pending[collID], ver)
			}
		}
	}
	m.mu.RUnlock()

	for collID, versions := range pending {
		minVersion, err := m.core.broker.AliveSegmentMinSchemaVersion(ctx, collID)
		if err != nil {
			mlog.Warn(ctx, "data view gate: completion query failed, will retry",
				mlog.Int64("collectionID", collID), mlog.Err(err))
			continue
		}
		completed := make([]int32, 0, len(versions))
		for _, ver := range versions {
			if minVersion >= ver {
				completed = append(completed, ver)
			}
		}
		if len(completed) == 0 {
			continue
		}
		// Force proxies to re-pull the now-unblocked gate. Unlike drop-release (whose ack callback runs
		// ExpireCaches before releasing), add-release only best-effort-pushes the unblock; a proxy that
		// missed that push while holding a warm meta cache would keep the field spuriously read-blocked
		// with nothing to force a re-pull. Invalidate the collection meta cache (by id) so it re-describes
		// and re-pulls the released snapshot. Reliable via retry: on failure we do NOT release, so the next
		// check-loop tick (completion still met) retries — the loop is this path's ack-re-fire equivalent.
		if err := m.core.proxyClientManager.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
			CollectionID: collID,
		}); err != nil {
			mlog.Warn(ctx, "data view gate: add-release cache invalidation failed, keeping gate, will retry next tick",
				mlog.Int64("collectionID", collID), mlog.Err(err))
			continue
		}
		for _, ver := range completed {
			m.releaseGate(ctx, collID, ver)
			mlog.Info(ctx, "data view gate: add backfill complete, field(s) now visible",
				mlog.Int64("collectionID", collID), mlog.Int32("opVersion", ver))
		}
	}
}
