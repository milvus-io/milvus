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

// Package manifestmanager holds the only durable writer of the primary key
// index manifests.
package manifestmanager

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// PointerStore is the metadata store of the manifest pointers and the
// pchannel claim records.
type PointerStore interface {
	// Load returns the value of key and whether key exists.
	Load(ctx context.Context, key string) (value string, found bool, err error)
	// MultiLoad returns the value of each key and whether it exists, in the
	// order of keys, from one read. An error applies to all keys.
	MultiLoad(ctx context.Context, keys []string) (values []string, found []bool, err error)
	// Save writes all saves in one transaction, only when every condition
	// holds. A condition on a key that is not in saves must still conflict
	// with a concurrent write of that key. An error does not tell whether the
	// write took effect.
	Save(ctx context.Context, saves map[string]string, conds ...manifest.Cond) error
	// List returns all keys with prefix and their values. Keys are relative to
	// the store root, in the same form as the keys passed to Load.
	List(ctx context.Context, prefix string) (keys, values []string, err error)
}

var marshal = proto.MarshalOptions{Deterministic: true}

// pointerBatchSize is the number of keys in one MultiLoad of the pointers. It
// stays within the default limit of 128 operations in one etcd transaction.
const pointerBatchSize = 128

// readBackTimeout is the time limit of the read of a key after a write whose
// result is not known.
const readBackTimeout = 5 * time.Second

// entry is the current version of one vchannel and the stored pointer value
// that names it.
type entry struct {
	manifest *pkindexpb.Manifest
	pointer  string
}

// claimEntry is the claim record of one pchannel and its stored value.
type claimEntry struct {
	claim *pkindexpb.PChannelClaim
	value string
}

// Manager is the only writer of the manifests and the pchannel claim records.
// It applies the edits of one vchannel one at a time. The manifests it
// returns are shared with its cache; the caller must not change them.
//
// The cache of a vchannel or a pchannel is filled on its first use and is
// refreshed only after a failed write; a flush also reloads the claim record
// when its term is above the cached term. The cached claim term only goes up.
// LoadPointers does not use the cache. Until then, Get does not see the
// commits of another coordinator. The conditions of each write keep commits
// safe when the cache is stale: a flush writes on the condition that the
// claim record still holds the flush term.
type Manager struct {
	pointers PointerStore
	store    manifest.ObjectStore
	layout   manifest.Layout
	locks    *lock.KeyLock[string]

	mu     sync.Mutex
	cache  map[string]*entry      // by pointer key
	claims map[string]*claimEntry // by pchannel
}

// New returns a Manager that keeps the pointers and the claim records in
// pointers and the manifest bodies in store, at the keys of layout.
func New(pointers PointerStore, store manifest.ObjectStore, layout manifest.Layout) *Manager {
	return &Manager{
		pointers: pointers,
		store:    store,
		layout:   layout,
		locks:    lock.NewKeyLock[string](),
		cache:    map[string]*entry{},
		claims:   map[string]*claimEntry{},
	}
}

// ClaimPChannel claims a pchannel for term with manifest.ApplyPChannelClaim.
// It reads the claim record from the store, not from the cache.
func (m *Manager) ClaimPChannel(ctx context.Context, pchannel string, term int64) error {
	key := manifest.ClaimKey(pchannel)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	cur, err := m.loadClaim(ctx, pchannel)
	if err != nil {
		return err
	}
	next, changed, err := manifest.ApplyPChannelClaim(cur.get(), pchannel, term)
	if err != nil {
		return err
	}
	if !changed {
		m.rememberClaim(pchannel, cur)
		return nil
	}
	b, err := marshal.Marshal(next)
	if err != nil {
		return errors.Wrapf(err, "marshal claim record of %s", pchannel)
	}
	value := string(b)
	cond := manifest.KeyAbsent(key)
	if cur != nil {
		cond = manifest.ValueIs(key, cur.value)
	}
	if err := m.pointers.Save(ctx, map[string]string{key: value}, cond); err != nil {
		// the write may have taken effect: the stored record decides
		if err := m.decideByReadBack(ctx, key, value, err); err != nil {
			m.forgetClaim(pchannel)
			return markUnavailable(err, "write claim record %s term %d", key, term)
		}
	}
	m.rememberClaim(pchannel, &claimEntry{claim: next, value: value})
	return nil
}

// CommitFlush applies a flush with manifest.ApplyFlush and returns the
// resulting version. The pchannel of the flush is the pchannel of the
// vchannel name. The flush term must equal the term of the claim record of
// that pchannel (manifest.CheckFlushTerm), and the pointer write is
// conditional on the claim record still holding it. The first version of a
// vchannel gets the SST base path of the layout.
func (m *Manager) CommitFlush(ctx context.Context, collectionID int64, vchannel string, f manifest.Flush) (*pkindexpb.Manifest, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	claim, err := m.flushClaim(ctx, pchannel, f.PChannelTerm)
	if err != nil {
		return nil, err
	}
	f.CollectionID, f.VChannel = collectionID, vchannel
	f.SSTBasePath = m.layout.SSTBasePath(collectionID, vchannel)
	return m.commit(ctx, collectionID, vchannel, &guard{pchannel: pchannel, claim: claim}, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
		return manifest.ApplyFlush(cur, f)
	})
}

// CommitCompaction applies a compaction with manifest.ApplyCompaction and returns
// the resulting version.
func (m *Manager) CommitCompaction(ctx context.Context, collectionID int64, vchannel string, c manifest.Compaction) (*pkindexpb.Manifest, error) {
	return m.commit(ctx, collectionID, vchannel, nil, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
		return manifest.ApplyCompaction(cur, c)
	})
}

// Get returns the current manifest of a vchannel, and false when the vchannel
// has no manifest.
func (m *Manager) Get(ctx context.Context, collectionID int64, vchannel string) (*pkindexpb.Manifest, bool, error) {
	key := manifest.PointerKey(collectionID, vchannel)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	e, err := m.load(ctx, key)
	if err != nil || e == nil {
		return nil, false, err
	}
	return e.manifest, true, nil
}

// Target names the manifest of one vchannel.
type Target struct {
	CollectionID int64
	VChannel     string
}

// PointerResult is the manifest pointer of one Target. Found and Pointer are
// set only when Err is nil and the pointer exists.
type PointerResult struct {
	Found   bool
	Pointer *pkindexpb.ManifestPointer
	Err     error
}

// LoadPointers reads the manifest pointer of each target from the store, not
// from the cache, in batches of pointerBatchSize keys. It returns one result
// per target, in the order of the targets. The failure of a batch read
// applies to the targets of that batch only.
func (m *Manager) LoadPointers(ctx context.Context, targets []Target) []PointerResult {
	results := make([]PointerResult, len(targets))
	for start := 0; start < len(targets); start += pointerBatchSize {
		end := min(start+pointerBatchSize, len(targets))
		keys := make([]string, 0, end-start)
		for _, t := range targets[start:end] {
			keys = append(keys, manifest.PointerKey(t.CollectionID, t.VChannel))
		}
		values, found, err := m.pointers.MultiLoad(ctx, keys)
		if err == nil && (len(values) != len(keys) || len(found) != len(keys)) {
			err = errors.Newf("pointer store returned %d values and %d flags for %d keys", len(values), len(found), len(keys))
		}
		for i, key := range keys {
			r := &results[start+i]
			switch {
			case err != nil:
				r.Err = markUnavailable(err, "load manifest pointer %s", key)
			case found[i]:
				r.Pointer, r.Err = decodePointer(key, values[i])
				r.Found = r.Err == nil
			}
		}
	}
	return results
}

// guard is the claim record that a flush writes under.
type guard struct {
	pchannel string
	claim    *claimEntry
}

// flushClaim returns the claim record of pchannel when its term equals term.
// It reads the cached record, and reads the store when the cache has no
// record or term is above the cached term, because a claim through another
// coordinator may have raised the stored record.
func (m *Manager) flushClaim(ctx context.Context, pchannel string, term int64) (*claimEntry, error) {
	c := m.cachedClaim(pchannel)
	if c == nil || term > c.claim.GetPchannelTerm() {
		var err error
		if c, err = m.loadClaim(ctx, pchannel); err != nil {
			return nil, err
		}
		if c != nil {
			m.rememberClaim(pchannel, c)
		}
	}
	if err := manifest.CheckFlushTerm(c.get(), pchannel, term); err != nil {
		return nil, err
	}
	return c, nil
}

// commit applies one edit to the current version of a vchannel. When the edit
// changes the manifest, it writes the new body under a unique key, then
// writes the pointer on the condition that the stored pointer is still the
// one it loaded, or does not exist for the first version. With a guard, the
// same write also requires the claim record to hold the guard value; it does
// not write the claim record.
func (m *Manager) commit(ctx context.Context, collectionID int64, vchannel string, g *guard,
	apply func(*pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error),
) (*pkindexpb.Manifest, error) {
	key := manifest.PointerKey(collectionID, vchannel)
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	cur, err := m.load(ctx, key)
	if err != nil {
		return nil, err
	}
	var curManifest *pkindexpb.Manifest
	if cur != nil {
		curManifest = cur.manifest
	}
	next, changed, err := apply(curManifest)
	if err != nil {
		return nil, err
	}
	if !changed {
		return curManifest, nil
	}

	bodyKey := m.layout.ManifestBodyKey(collectionID, vchannel, next.GetVersion(), uuid.NewString())
	body, err := marshal.Marshal(next)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal manifest of %s", vchannel)
	}
	if err := m.store.Write(ctx, bodyKey, body); err != nil {
		return nil, markUnavailable(err, "write manifest body %s", bodyKey)
	}
	ptrBytes, err := marshal.Marshal(&pkindexpb.ManifestPointer{Version: next.GetVersion(), BodyKey: bodyKey})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal manifest pointer of %s", vchannel)
	}
	ptr := string(ptrBytes)

	saves := map[string]string{key: ptr}
	conds := []manifest.Cond{manifest.KeyAbsent(key)}
	if cur != nil {
		conds[0] = manifest.ValueIs(key, cur.pointer)
	}
	if g != nil {
		conds = append(conds, manifest.ValueIs(manifest.ClaimKey(g.pchannel), g.claim.value))
	}
	if err := m.pointers.Save(ctx, saves, conds...); err != nil {
		// the write may have taken effect: the stored pointer decides
		if err := m.decideByReadBack(ctx, key, ptr, err); err != nil {
			m.forget(key)
			if g != nil {
				m.forgetClaim(g.pchannel)
			}
			return nil, markUnavailable(err, "write manifest pointer %s version %d", key, next.GetVersion())
		}
	}
	m.remember(key, &entry{manifest: next, pointer: ptr})
	return next, nil
}

// decideByReadBack reads key after a write of value that returned writeErr,
// whose result is not known. It returns nil when key holds value, and
// otherwise writeErr with the read error, if any, attached. The read does not
// use the cancellation of ctx, and it stops after readBackTimeout.
func (m *Manager) decideByReadBack(ctx context.Context, key, value string, writeErr error) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readBackTimeout)
	defer cancel()
	got, found, err := m.pointers.Load(rctx, key)
	if err != nil {
		return errors.WithSecondaryError(writeErr, err)
	}
	if !found || got != value {
		return writeErr
	}
	return nil
}

// load returns the cached entry, or reads it from the stores. It returns nil
// when the vchannel has no manifest.
func (m *Manager) load(ctx context.Context, key string) (*entry, error) {
	m.mu.Lock()
	e := m.cache[key]
	m.mu.Unlock()
	if e != nil {
		return e, nil
	}
	ptr, found, err := m.pointers.Load(ctx, key)
	if err != nil {
		return nil, markUnavailable(err, "load manifest pointer %s", key)
	}
	if !found {
		return nil, nil
	}
	p, err := decodePointer(key, ptr)
	if err != nil {
		return nil, err
	}
	body, err := m.store.Read(ctx, p.GetBodyKey())
	if err != nil {
		return nil, markUnavailable(err, "read manifest body %s", p.GetBodyKey())
	}
	var mf pkindexpb.Manifest
	if err := proto.Unmarshal(body, &mf); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest body %s", p.GetBodyKey()), pkerr.ErrCorrupted)
	}
	if mf.GetVersion() != p.GetVersion() {
		return nil, errors.Mark(errors.Newf("manifest body %s has version %d, pointer has %d", p.GetBodyKey(), mf.GetVersion(), p.GetVersion()), pkerr.ErrCorrupted)
	}
	e = &entry{manifest: &mf, pointer: ptr}
	m.remember(key, e)
	return e, nil
}

// decodePointer decodes the stored value of the pointer key. It returns an
// error marked with pkerr.ErrCorrupted when the value does not decode or names
// no body.
func decodePointer(key, value string) (*pkindexpb.ManifestPointer, error) {
	var p pkindexpb.ManifestPointer
	if err := proto.Unmarshal([]byte(value), &p); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest pointer %s", key), pkerr.ErrCorrupted)
	}
	if p.GetVersion() == 0 || p.GetBodyKey() == "" {
		return nil, errors.Mark(errors.Newf("manifest pointer %s has version %d and body key %q", key, p.GetVersion(), p.GetBodyKey()), pkerr.ErrCorrupted)
	}
	return &p, nil
}

// loadClaim reads the claim record of pchannel from the store. It returns nil
// when the pchannel has no claim record.
func (m *Manager) loadClaim(ctx context.Context, pchannel string) (*claimEntry, error) {
	key := manifest.ClaimKey(pchannel)
	value, found, err := m.pointers.Load(ctx, key)
	if err != nil {
		return nil, markUnavailable(err, "load claim record %s", key)
	}
	if !found {
		return nil, nil
	}
	var c pkindexpb.PChannelClaim
	if err := proto.Unmarshal([]byte(value), &c); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode claim record %s", key), pkerr.ErrCorrupted)
	}
	if c.GetPchannelTerm() <= 0 {
		return nil, errors.Mark(errors.Newf("claim record %s has term %d", key, c.GetPchannelTerm()), pkerr.ErrCorrupted)
	}
	return &claimEntry{claim: &c, value: value}, nil
}

// get returns the claim record of c, or nil when c is nil.
func (c *claimEntry) get() *pkindexpb.PChannelClaim {
	if c == nil {
		return nil
	}
	return c.claim
}

// markUnavailable adds context to a failure of a remote store and marks it as
// pkerr.ErrUnavailable. A mark that err already has, such as pkerr.ErrCorrupted
// for a missing object, stays.
func markUnavailable(err error, format string, args ...any) error {
	return errors.Mark(errors.Wrapf(err, format, args...), pkerr.ErrUnavailable)
}

func (m *Manager) remember(key string, e *entry) {
	m.mu.Lock()
	m.cache[key] = e
	m.mu.Unlock()
}

func (m *Manager) forget(key string) {
	m.mu.Lock()
	delete(m.cache, key)
	m.mu.Unlock()
}

func (m *Manager) cachedClaim(pchannel string) *claimEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claims[pchannel]
}

// rememberClaim caches c when the cache has no record of pchannel or a lower
// term, so that a concurrent read of an older record does not lower it.
func (m *Manager) rememberClaim(pchannel string, c *claimEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur := m.claims[pchannel]; cur == nil || c.claim.GetPchannelTerm() > cur.claim.GetPchannelTerm() {
		m.claims[pchannel] = c
	}
}

func (m *Manager) forgetClaim(pchannel string) {
	m.mu.Lock()
	delete(m.claims, pchannel)
	m.mu.Unlock()
}
