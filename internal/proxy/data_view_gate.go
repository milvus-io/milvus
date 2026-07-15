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
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// dataViewGate is the proxy-side enforcement of the DataViewGate. It is intentionally OPAQUE to the
// reason a field is gated (drop vs add): RootCoord owns the whole lifecycle and pushes SET/RELEASE
// deltas over InvalidateCollectionMetaCache. The proxy only knows "these (collection, field) pairs
// are blocked" and "complex-deletes on these collections are paused".
//
// It holds two things guarded by ONE lock so a drop's SetGate can atomically pause complex-deletes
// and snapshot the in-flight refcount (close-then-drain):
//   - blockedFields: per-collection set of field ids -> reject search/query/outputFields that
//     reference them (cheap membership check at task PreExecute).
//   - complex-delete pause + per-collection in-flight refcount -> during a drop, reject NEW
//     complex-deletes on the collection and drain in-flight ones to zero.
type dataViewGate struct {
	// RWMutex: the read-hot AnyFieldBlocked takes RLock; all mutators and the cond drain barrier take
	// the write Lock (sync.Cond is bound to the write Lock via sync.Locker, so cond.Wait releases and
	// re-acquires it — the drain path must never use RLock).
	mu   sync.RWMutex
	cond *sync.Cond

	blockedFields         map[UniqueID]typeutil.Set[UniqueID] // collectionID -> blocked field ids
	complexDeletePaused   typeutil.Set[UniqueID]              // collections whose complex-deletes are paused
	complexDeleteInflight map[UniqueID]int                    // collectionID -> in-flight complex-delete count
	// lastGen is the highest gate-snapshot generation applied per collection. Every snapshot (push or
	// pull) carries a RootCoord-monotonic generation; ApplyGateSnapshot commits it only when it exceeds
	// lastGen, so a reordered/stale snapshot (an old SET arriving after a newer RELEASE, or a pull racing
	// a push) is dropped instead of overwriting newer state. Retained per collection (never reset), so
	// the monotonic bound is stable across the collection's lifetime.
	lastGen map[UniqueID]uint64
}

func newDataViewGate() *dataViewGate {
	g := &dataViewGate{
		blockedFields:         make(map[UniqueID]typeutil.Set[UniqueID]),
		complexDeletePaused:   typeutil.NewSet[UniqueID](),
		complexDeleteInflight: make(map[UniqueID]int),
		lastGen:               make(map[UniqueID]uint64),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// globalDataViewGate is the process-wide proxy gate, mirroring globalMetaCache. Pure in-memory, so
// it needs no explicit init; RootCoord re-pushes the current gate set on proxy (re)join.
var globalDataViewGate = newDataViewGate()

// AnyFieldBlocked reports the first blocked field id among fieldIDs of collID, if any. Read paths
// (search / query / delete-by-query / outputFields) call this at PreExecute to reject a request
// that references a gated field.
func (g *dataViewGate) AnyFieldBlocked(collID UniqueID, fieldIDs ...UniqueID) (UniqueID, bool) {
	// Enforcement is driven purely by installed gate state (pushed/pulled from RootCoord, the single owner);
	// the local kill-switch flag is never consulted here — RootCoord disables by RELEASING the installed
	// state, which clears the block. Reading the flag here would reintroduce the config-divergence window
	// (a proxy that read the flag stale while RootCoord had already pushed a gate).
	g.mu.RLock()
	defer g.mu.RUnlock()
	s, ok := g.blockedFields[collID]
	if !ok {
		return 0, false
	}
	for _, f := range fieldIDs {
		if s.Contain(f) {
			return f, true
		}
	}
	return 0, false
}

// TryRegisterComplexDelete registers an in-flight complex-delete on collID. It returns false — the
// caller must then reject the delete — when either (a) complex-deletes on collID are currently paused
// (a drop is draining), or (b) the gate generation changed since the caller captured expectedGen (via
// CurrentGen) while building the delete plan: a drop that installed/released between plan-build and here
// would make the plan reference a since-dropped field, so the delete must NOT execute (the drain barrier
// can miss it — the plan predated registration). Both checks and the refcount increment share the gate
// lock, so a snapshot that pauses + snapshots the refcount cannot race a delete that already passed
// (close-then-drain), and a delete registered before a later drop is correctly drained (not gen-rejected).
func (g *dataViewGate) TryRegisterComplexDelete(collID UniqueID, expectedGen uint64) bool {
	// Enforcement is driven purely by installed gate state (the pause / generation pushed from RootCoord,
	// the single owner), never by the local kill-switch flag: a proxy that read the flag stale while
	// RootCoord had already pushed the pause would ACK the drain yet keep admitting deletes here.
	// register/deregister stay balanced (both unconditional). RootCoord disables by RELEASING the state.
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.complexDeletePaused.Contain(collID) || g.lastGen[collID] != expectedGen {
		return false
	}
	g.complexDeleteInflight[collID]++
	return true
}

// DeregisterComplexDelete is deferred by the complex-delete runner when it finishes.
func (g *dataViewGate) DeregisterComplexDelete(collID UniqueID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n := g.complexDeleteInflight[collID]; n > 1 {
		g.complexDeleteInflight[collID] = n - 1
	} else {
		delete(g.complexDeleteInflight, collID)
	}
	g.cond.Broadcast()
}

// ApplyGateSnapshot commits a FULL per-collection gate snapshot from RootCoord — read blocklist set to
// exactly fieldIDs and complex-delete pause set to paused — but ONLY when gen exceeds the last applied
// generation for collID. A reordered or stale snapshot (an old SET arriving after a newer RELEASE, or a
// pull racing a push) has gen <= lastGen and is dropped, so it can never overwrite newer state; returns
// (false, nil) for such a no-op. This is the ordering guarantee that the old wire-versionless delta
// protocol lacked. Both the push RPC handler and the DescribeCollection pull call this — ONE apply path.
//
// When drain is true (a drop install) it also BLOCKS until every in-flight complex-delete on collID has
// drained — the drop barrier. The drain honors ctx: on ctx cancellation / deadline (RootCoord bounds it
// with a drain timeout) it UNDOES this snapshot (restores the prior state + generation) and returns the
// ctx error, so a stuck complex-delete makes the drop fail-and-retry instead of hanging the DDL forever.
//
// A drop-install (drain) snapshot that is STALE (a strictly newer snapshot already applied) may not have
// actually drained in-flight complex-deletes, so it must NOT be reported as a completed drain: it returns
// a retriable error so RootCoord rolls the drop back and retries with a fresh (higher) generation, rather
// than mistaking the no-op for a drained collection and letting the drop cross an in-flight delete.
func (g *dataViewGate) ApplyGateSnapshot(ctx context.Context, collID UniqueID, fieldIDs []UniqueID, paused bool, gen uint64, drain bool) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen <= g.lastGen[collID] {
		if drain && gen < g.lastGen[collID] {
			return false, merr.WrapErrCollectionSchemaChangeInProgress(collID,
				"drop drain snapshot superseded by a newer gate update; retry")
		}
		return false, nil
	}
	// Capture the prior state so a failed drain can restore this snapshot exactly (it never half-lands).
	prevBlocked, hadBlocked := g.blockedFields[collID]
	prevPaused := g.complexDeletePaused.Contain(collID)
	prevGen := g.lastGen[collID]

	g.lastGen[collID] = gen
	g.replaceReadGateLocked(collID, fieldIDs)

	if drain {
		// Pause under the lock BEFORE waiting: no new TryRegisterComplexDelete can slip through once the
		// pause is visible, so the refcount only decreases -> reaches zero.
		g.complexDeletePaused.Insert(collID)
		// sync.Cond has no timed wait, so a watchdog wakes the waiter when ctx is done.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				g.mu.Lock()
				g.cond.Broadcast()
				g.mu.Unlock()
			case <-stop:
			}
		}()
		for g.complexDeleteInflight[collID] > 0 {
			if ctx.Err() != nil {
				// Only undo if no newer snapshot superseded us while cond.Wait released the lock (under
				// admission serialization this cannot happen during a drop's own drain, but stay safe).
				if g.lastGen[collID] == gen {
					if hadBlocked {
						g.blockedFields[collID] = prevBlocked
					} else {
						delete(g.blockedFields, collID)
					}
					g.setComplexDeletePauseLocked(collID, prevPaused)
					g.lastGen[collID] = prevGen
				}
				return false, ctx.Err()
			}
			g.cond.Wait()
		}
	} else {
		g.setComplexDeletePauseLocked(collID, paused)
	}
	return true, nil
}

// CurrentGen returns collID's last applied gate-snapshot generation. A complex-delete plan captures it at
// build time and passes it to TryRegisterComplexDelete; any snapshot applied in between bumps it and
// rejects the now-stale plan.
func (g *dataViewGate) CurrentGen(collID UniqueID) uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lastGen[collID]
}

// checkReadFieldGate rejects a read (search / query) whose referenced fields include one currently
// gated on the collection. Callers pass the field ids the request would EXPOSE — output fields, the
// anns/vector field, group-by fields. Filter-expr references are intentionally not walked here:
// during a drop the field is still in the schema (the read merely has no result guarantee, which is
// accepted) and after the DDL removes it the plan parser's existence check rejects it. For an
// add_function_field gate the still-backfilling output is a vector field, which cannot appear in a
// filter predicate, so gating filter-expr fields is unnecessary for the add case too.
func checkReadFieldGate(collID UniqueID, fieldIDs ...UniqueID) error {
	if f, blocked := globalDataViewGate.AnyFieldBlocked(collID, fieldIDs...); blocked {
		// Transient: the gate clears when the add backfill / drop drain completes, so this is a retriable
		// System error (ErrCollectionSchemaChangeInProgress), not a permanent InputError — retry.Do
		// consumers and SDK clients should retry the momentary window instead of giving up.
		return merr.WrapErrCollectionSchemaChangeInProgress(collID,
			"field %d is not readable while a schema change (add/drop) is in progress", f)
	}
	return nil
}

// replaceReadGateLocked sets the read blocklist for collID to exactly fieldIDs (snapshot semantics).
func (g *dataViewGate) replaceReadGateLocked(collID UniqueID, fieldIDs []UniqueID) {
	if len(fieldIDs) == 0 {
		delete(g.blockedFields, collID)
		return
	}
	g.blockedFields[collID] = typeutil.NewSet[UniqueID](fieldIDs...)
}

func (g *dataViewGate) setComplexDeletePauseLocked(collID UniqueID, paused bool) {
	if paused {
		g.complexDeletePaused.Insert(collID)
	} else {
		g.complexDeletePaused.Remove(collID)
	}
	// No cond.Broadcast: this changes only the pause flag, not complexDeleteInflight (what the drain
	// waiter blocks on), so there is no waiter whose condition this could satisfy.
}
