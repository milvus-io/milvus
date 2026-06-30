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
)

// bump_defence — proxy-side gate that protects search/query on a backfilled field from
// returning silent partial results until ALL in-scope segments can correctly serve it.
//
// This file holds the registry — the single, etcd-backed, round-major source of truth
// for active defences. It is mechanism-agnostic: internal add_function_field, external
// Spark backfill, and external-collection refresh all funnel through Register/Revoke.
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
	// ScopeWatermark — internal & external-collection: every old segment with
	// schema_version < Watermark still needs the backfill. Both paths bump
	// schema_version (internal via compaction, external via the external refresh task),
	// so no segment list needs to be stored.
	ScopeWatermark BackfillScopeKind = iota
	// ScopeSegmentList — external Spark: the committed segment set, each with the
	// V_commit (manifest/data version) at which the round's columns landed. Spark does
	// NOT bump schema_version, so a watermark cannot be used.
	ScopeSegmentList
)

// BackfillScope is the frozen in-scope segment set of a round.
type BackfillScope struct {
	Kind      BackfillScopeKind `json:"kind"`
	Watermark int32             `json:"watermark,omitempty"` // ScopeWatermark: schema_version V
	Segments  map[int64]int64   `json:"segments,omitempty"`  // ScopeSegmentList: segmentID -> V_commit
}

// BackfillRound is the atomic unit of bump_defence: one backfill round, holding the
// round's field IDs (revealed together) and its frozen segment scope.
type BackfillRound struct {
	CollectionID int64         `json:"collectionID"`
	RoundID      int64         `json:"roundID"`
	Fields       []int64       `json:"fields"`
	Scope        BackfillScope `json:"scope"`
	// Source is the round's external identity for idempotency (e.g. an external backfill
	// result path). When set, Register dedupes by (collectionID, Source) so a retried
	// registration reuses the existing roundID instead of creating a duplicate gate.
	// Empty for watermark rounds, whose RoundID (= schema version) is already unique.
	Source string `json:"source,omitempty"`
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
	// Idempotency: a round carrying a Source (e.g. an external backfill result path) is
	// keyed by that source. A retried registration must reuse the existing roundID rather
	// than create a duplicate gate (the caller-allocated roundID is unique per call, but
	// the SAME backfill round may be committed more than once).
	if round.Source != "" {
		r.mu.RLock()
		for _, existing := range r.rounds[round.CollectionID] {
			if existing.Source == round.Source {
				round.RoundID = existing.RoundID
				break
			}
		}
		r.mu.RUnlock()
	}
	data, err := json.Marshal(round)
	if err != nil {
		return merr.Wrap(err, "marshal defence round")
	}
	if err := r.metaKV.Save(ctx, backfillRoundKey(round.CollectionID, round.RoundID), string(data)); err != nil {
		return merr.Wrap(err, "persist defence round")
	}

	r.mu.Lock()
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

// Revoke releases the round on proxies (push the remaining union) and THEN deletes the
// etcd entry. The order is crash-safe: a crash before the delete re-derives "still
// defended" on reload and never prematurely drops the gate.
func (r *BackfillAtomicGate) Revoke(ctx context.Context, collectionID, roundID int64) error {
	r.mu.Lock()
	coll := r.rounds[collectionID]
	if coll == nil || coll[roundID] == nil {
		r.mu.Unlock()
		return nil // already gone — idempotent
	}
	delete(coll, roundID)
	if len(coll) == 0 {
		delete(r.rounds, collectionID)
	}
	fields := r.gatedFieldsLocked(collectionID)
	r.mu.Unlock()

	// release on proxies first ...
	if err := r.push(ctx, collectionID, fields); err != nil {
		return err
	}
	// ... then delete the etcd entry.
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
	set := make(map[int64]struct{})
	for _, round := range r.rounds[collectionID] {
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
