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

package coordinator

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// bump_defence — proxy-side gate that protects search/query on a backfilled field from
// returning silent partial results until ALL in-scope segments can correctly serve it.
//
// This file holds the registry — the single, etcd-backed, round-major source of truth
// for active defences. It is mechanism-agnostic: internal add_function_field and external
// Spark backfill both funnel through Register/Revoke.
//
// Design invariants (see workflows/bump_defence/plan.md):
//   - Atomic unit = the backfill ROUND, never per-field. A round's whole field set is
//     revealed together; the registry key is (collectionID, roundID).
//   - etcd write-once at Register / delete-once at Revoke. Per-segment readiness
//     progress is NEVER persisted — it is re-derived each sweep by the observer.
//   - Proxy knows whether, not why: only the union Set<fieldID> is pushed to proxies;
//     the segment scope / V_commit stay here on mixCoord.

// backfillGateMetaPrefix is the etcd key prefix (relative to the coordinator meta root)
// under which round entries are persisted.
const backfillGateMetaPrefix = "defence"

// BackfillScopeKind selects how the in-scope segment set + readiness threshold are expressed.
type BackfillScopeKind int32

const (
	// ScopeWatermark — internal add_function_field: every old segment with
	// schema_version < Watermark still needs the backfill. The bump-schema compaction
	// advances schema_version, so no segment list needs to be stored. The watermark is
	// consumed ONLY by the write-side DataView check.
	ScopeWatermark BackfillScopeKind = iota
	// ScopeExternal — external Spark backfill: the round registers after the manifest
	// apply, so the write side holds by construction and the round carries no scope
	// payload at all — readiness is certified by the target promote barrier alone.
	ScopeExternal
)

// BackfillScope selects the round's write-side semantics.
type BackfillScope struct {
	Kind      BackfillScopeKind `json:"kind"`
	Watermark int32             `json:"watermark,omitempty"` // ScopeWatermark: schema_version V
	// SchemaChangeTimeTick is the WAL timetick of the schema-change DDL (max across its
	// vchannel appends). The write side is clear only when every vchannel checkpoint has
	// passed it: the DDL fences+flushes all pre-V growing segments, and the checkpoint
	// advances only after prior DML is durably persisted -- so checkpoint >= tick proves
	// no pre-V data is still hiding in growing segments (invisible to the sealed-segment
	// DataView check). Zero disables the check (defensive for legacy rounds).
	SchemaChangeTimeTick uint64 `json:"schemaChangeTimeTick,omitempty"`
}

// BackfillRound is the atomic unit of bump_defence: one backfill round, holding the
// round's field IDs (revealed together) and its scope.
type BackfillRound struct {
	CollectionID int64         `json:"collectionID"`
	RoundID      int64         `json:"roundID"`
	Fields       []int64       `json:"fields"`
	Scope        BackfillScope `json:"scope"`
	// Source is the round's logical identity for idempotency: registrations can be
	// retried (broadcast replay, ack-callback retry) with a freshly-allocated roundID
	// each time, so Register dedupes by (collectionID, Source) and reuses the existing
	// roundID. Watermark rounds use "watermark:<V>" (the schema version uniquely
	// identifies the DDL round); external rounds use "batchmanifest:<broadcastID>" (the
	// WAL broadcast ID, stable across ack retries and restart recovery).
	Source string `json:"source,omitempty"`
	// writeDoneObservedAtNanos is the round's T_c: the first sweep time at which the
	// write side was observed complete (watermark: DataView clear; external: the first
	// sweep at all, since registration after the manifest apply makes the write side
	// hold by construction). Transient by the "identity persisted, progress re-derived"
	// invariant: after a restart it is re-observed later, which only delays revoke
	// (fail-closed). Touched only by the observer's sweep goroutine.
	writeDoneObservedAtNanos int64
}

// gateKV is the narrow slice of kv.MetaKv the registry needs (interface segregation
// keeps the registry decoupled and unit-testable). The real kv.MetaKv satisfies it.
type gateKV interface {
	Save(ctx context.Context, key, value string) error
	Remove(ctx context.Context, key string) error
	LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error)
}

// ProxyGatePusher pushes the union gated-field set of a collection to all proxies.
// The concrete adapter (proxy client manager fan-out) is wired in mixCoord.
type ProxyGatePusher interface {
	PushGatedFields(ctx context.Context, collectionID int64, fieldIDs []int64) error
}

// BackfillAtomicGate is the single source of truth for active bump_defence rounds.
type BackfillAtomicGate struct {
	metaKV gateKV
	pusher ProxyGatePusher

	mu     sync.RWMutex
	rounds map[int64]map[int64]*BackfillRound // collectionID -> roundID -> round
}

// NewBackfillAtomicGate constructs a registry over the given meta KV and proxy pusher.
func NewBackfillAtomicGate(metaKV gateKV, pusher ProxyGatePusher) *BackfillAtomicGate {
	return &BackfillAtomicGate{
		metaKV: metaKV,
		pusher: pusher,
		rounds: make(map[int64]map[int64]*BackfillRound),
	}
}

func backfillRoundKey(collectionID, roundID int64) string {
	return path.Join(backfillGateMetaPrefix,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(roundID, 10))
}

// Register persists a round (write-once) and pushes the collection's updated union
// gated-field set to proxies. Idempotent: re-registering the same round overwrites it.
func (r *BackfillAtomicGate) Register(ctx context.Context, round *BackfillRound) error {
	if round == nil || round.CollectionID == 0 || len(round.Fields) == 0 {
		return merr.WrapErrParameterInvalidMsg("invalid bump_defence round: nil/empty fields")
	}
	// Defence: with the promote barrier disabled nothing could ever certify this round,
	// so registering would block its fields FOREVER. Stand down instead of arming an
	// un-revokable gate. Mid-flight rounds stand down the same way in the readiness check.
	if !paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.GetAsBool() {
		mlog.Warn(ctx, "bump_defence registration skipped: queryCoord.updateTargetNeedSegmentDataReady is disabled, "+
			"target promotion cannot certify segment data readiness",
			mlog.FieldCollectionID(round.CollectionID),
			mlog.Int64("roundID", round.RoundID))
		return nil
	}
	// Idempotency: a round carrying a Source (e.g. an external backfill result path) is
	// keyed by that source. A retried or CONCURRENT registration must reuse the existing
	// roundID rather than create a duplicate gate (the caller-allocated roundID is unique
	// per call, but the SAME backfill round may be committed more than once, and the
	// batches of one Spark commit ack concurrently). The write lock spans the whole
	// check-and-save: registration is low-frequency, so holding it across the etcd Save
	// only briefly delays a concurrent sweep, in exchange for an atomic source-dedup.
	r.mu.Lock()
	if round.Source != "" {
		for _, existing := range r.rounds[round.CollectionID] {
			if existing.Source == round.Source {
				round.RoundID = existing.RoundID
				break
			}
		}
	}
	data, err := json.Marshal(round)
	if err != nil {
		r.mu.Unlock()
		return merr.Wrap(err, "marshal defence round")
	}
	if err := r.metaKV.Save(ctx, backfillRoundKey(round.CollectionID, round.RoundID), string(data)); err != nil {
		r.mu.Unlock()
		return merr.Wrap(err, "persist defence round")
	}
	if r.rounds[round.CollectionID] == nil {
		r.rounds[round.CollectionID] = make(map[int64]*BackfillRound)
	}
	r.rounds[round.CollectionID][round.RoundID] = round
	fields := r.gatedFieldsLocked(round.CollectionID)
	r.mu.Unlock()

	mlog.Info(ctx, "registered bump_defence round",
		mlog.FieldCollectionID(round.CollectionID),
		mlog.Int64("roundID", round.RoundID),
		mlog.Int("fields", len(round.Fields)))
	return r.push(ctx, round.CollectionID, fields)
}

// Revoke releases a round in the conventional distributed order: notify the external
// party first, then drop the volatile state, then the durable state.
//  1. Push the REDUCED union (excluding this round) to proxies. On failure the round is
//     still in memory and etcd, so the next sweep retries the whole revoke — a transient
//     push failure never strands proxies on the stale, larger gated set.
//  2. Drop the in-memory round (List/RepushAll stop seeing it).
//  3. Delete the etcd entry. A crash between 2 and 3 re-arms the round on reload
//     (fail-closed) and the next sweep re-revokes idempotently.
func (r *BackfillAtomicGate) Revoke(ctx context.Context, collectionID, roundID int64) error {
	r.mu.RLock()
	coll := r.rounds[collectionID]
	if coll == nil || coll[roundID] == nil {
		r.mu.RUnlock()
		return nil // already gone — idempotent
	}
	remaining := r.gatedFieldsExcludingLocked(collectionID, roundID)
	r.mu.RUnlock()

	if err := r.push(ctx, collectionID, remaining); err != nil {
		return err
	}

	r.mu.Lock()
	if coll := r.rounds[collectionID]; coll != nil {
		delete(coll, roundID)
		if len(coll) == 0 {
			delete(r.rounds, collectionID)
		}
	}
	r.mu.Unlock()

	if err := r.metaKV.Remove(ctx, backfillRoundKey(collectionID, roundID)); err != nil {
		return merr.Wrap(err, "delete defence round")
	}
	mlog.Info(ctx, "revoked bump_defence round",
		mlog.FieldCollectionID(collectionID), mlog.Int64("roundID", roundID))
	return nil
}

// List returns a snapshot of all active rounds.
func (r *BackfillAtomicGate) List() []*BackfillRound {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*BackfillRound
	for _, coll := range r.rounds {
		for _, round := range coll {
			out = append(out, round)
		}
	}
	return out
}

// GatedFields returns the union of gated field IDs for a collection (what the proxy holds).
func (r *BackfillAtomicGate) GatedFields(collectionID int64) []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gatedFieldsLocked(collectionID)
}

// Reload restores active rounds from etcd on startup and re-pushes each collection's
// union to proxies.
func (r *BackfillAtomicGate) Reload(ctx context.Context) error {
	_, values, err := r.metaKV.LoadWithPrefix(ctx, backfillGateMetaPrefix)
	if err != nil {
		return merr.Wrap(err, "reload defence rounds")
	}
	r.mu.Lock()
	r.rounds = make(map[int64]map[int64]*BackfillRound)
	for _, v := range values {
		round := &BackfillRound{}
		if err := json.Unmarshal([]byte(v), round); err != nil {
			mlog.Warn(ctx, "skip corrupt defence round entry", mlog.Err(err))
			continue
		}
		if r.rounds[round.CollectionID] == nil {
			r.rounds[round.CollectionID] = make(map[int64]*BackfillRound)
		}
		r.rounds[round.CollectionID][round.RoundID] = round
	}
	colls := make([]int64, 0, len(r.rounds))
	for collID := range r.rounds {
		colls = append(colls, collID)
	}
	r.mu.Unlock()

	for _, collID := range colls {
		if err := r.push(ctx, collID, r.GatedFields(collID)); err != nil {
			mlog.Warn(ctx, "failed to re-push defence after reload",
				mlog.FieldCollectionID(collID), mlog.Err(err))
		}
	}
	return nil
}

// gatedFieldsLocked returns the sorted union of all field IDs gated for a collection
// across its active rounds. Caller holds r.mu.
func (r *BackfillAtomicGate) gatedFieldsLocked(collectionID int64) []int64 {
	return r.gatedFieldsExcludingLocked(collectionID, 0)
}

// gatedFieldsExcludingLocked returns the sorted union of gated field IDs for a collection
// across its active rounds, excluding the given roundID (0 excludes nothing). Used by
// Revoke to compute the union the proxies must hold AFTER a round is released, while the
// round is still registered. Caller holds r.mu.
func (r *BackfillAtomicGate) gatedFieldsExcludingLocked(collectionID, excludeRoundID int64) []int64 {
	set := make(map[int64]struct{})
	for roundID, round := range r.rounds[collectionID] {
		if roundID == excludeRoundID {
			continue
		}
		for _, id := range round.Fields {
			set[id] = struct{}{}
		}
	}
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (r *BackfillAtomicGate) push(ctx context.Context, collectionID int64, fields []int64) error {
	if r.pusher == nil {
		return nil
	}
	return r.pusher.PushGatedFields(ctx, collectionID, fields)
}

// RepushAll re-pushes every active collection's union gated set to proxies. Called by the
// observer each sweep so a proxy that started AFTER a register event (and missed its push)
// still converges within one sweep interval (closes the bootstrap gap).
func (r *BackfillAtomicGate) RepushAll(ctx context.Context) {
	r.mu.RLock()
	colls := make([]int64, 0, len(r.rounds))
	for collID := range r.rounds {
		colls = append(colls, collID)
	}
	r.mu.RUnlock()
	for _, collID := range colls {
		if err := r.push(ctx, collID, r.GatedFields(collID)); err != nil {
			mlog.Warn(ctx, "bump_defence re-push failed", mlog.FieldCollectionID(collID), mlog.Err(err))
		}
	}
}
