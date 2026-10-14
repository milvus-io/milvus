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
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// MetaStore is the metadata store of the manifest pointers and the pchannel
// claim records.
type MetaStore interface {
	// Load returns the value of key and whether key exists.
	Load(ctx context.Context, key string) (value string, found bool, err error)
	// MultiLoad returns the value of each key and whether it exists, in the
	// order of keys, from one read. An error applies to all keys.
	MultiLoad(ctx context.Context, keys []string) (values []string, found []bool, err error)
	// Save writes all saves in one transaction, only when every predicate
	// holds. A predicate on a key that is not in saves must still conflict
	// with a concurrent write of that key. An error does not tell whether the
	// write took effect.
	Save(ctx context.Context, saves map[string]string, preds ...predicates.Predicate) error
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

// vchannelManifest is the current version of the manifest of one vchannel.
// pointer is the pointer to it as it is stored in the meta store.
type vchannelManifest struct {
	pb      *pkindexpb.Manifest
	pointer string
}

// Manager is the only writer of the manifests and the pchannel claim records.
// It applies the edits of one vchannel one at a time. The manifests it
// returns are shared with its cache; the caller must not change them.
//
// The cache of a vchannel is filled on its first use and is refreshed only
// after a failed write. LoadPointers does not use the cache. Until then, Get
// does not see the commits of another coordinator. The predicates of each
// write keep commits safe when the cache is stale: a flush writes on the
// predicate that the claim record still holds the flush term.
type Manager struct {
	claims *pchannelClaims
	meta   MetaStore
	store  manifest.ObjectStore
	layout manifest.Layout
	locks  *lock.KeyLock[string]

	mu        sync.Mutex
	manifests map[string]*vchannelManifest // by pointer key
}

// New returns a Manager that keeps the pointers and the claim records in meta
// and the manifest bodies in store, at the keys of layout.
func New(meta MetaStore, store manifest.ObjectStore, layout manifest.Layout) *Manager {
	return &Manager{
		claims:    newPChannelClaims(meta),
		meta:      meta,
		store:     store,
		layout:    layout,
		locks:     lock.NewKeyLock[string](),
		manifests: map[string]*vchannelManifest{},
	}
}

// ClaimPChannel claims a pchannel for term with manifest.ApplyPChannelClaim.
// It reads the claim record from the meta store, not from the cache.
func (m *Manager) ClaimPChannel(ctx context.Context, pchannel string, term int64) error {
	return m.claims.claim(ctx, pchannel, term)
}

// CommitFlush applies a flush with manifest.ApplyFlush and returns the
// resulting version. The pchannel of the flush is the pchannel of the
// vchannel name. The flush term must equal the term of the claim record of
// that pchannel (manifest.CheckFlushTerm), and the pointer write has the
// predicate that the claim record still holds it. The first version of a
// vchannel gets the SST base path of the layout.
func (m *Manager) CommitFlush(ctx context.Context, collectionID int64, vchannel string, f manifest.Flush) (*pkindexpb.Manifest, error) {
	cp, err := m.claims.checkFlush(ctx, funcutil.ToPhysicalChannel(vchannel), f.PChannelTerm)
	if err != nil {
		return nil, err
	}
	f.CollectionID, f.VChannel = collectionID, vchannel
	f.SSTBasePath = m.layout.SSTBasePath(collectionID, vchannel)
	return m.commit(ctx, collectionID, vchannel, cp, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
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
	cur, err := m.load(ctx, key)
	if err != nil || cur == nil {
		return nil, false, err
	}
	return cur.pb, true, nil
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
		values, found, err := m.meta.MultiLoad(ctx, keys)
		if err == nil && (len(values) != len(keys) || len(found) != len(keys)) {
			err = errors.Newf("meta store returned %d values and %d flags for %d keys", len(values), len(found), len(keys))
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

// commit applies one edit to the current version of a vchannel. When the edit
// changes the manifest, it writes the new body under a unique key, then
// writes the pointer on the predicate that the stored pointer is still the
// one it loaded, or does not exist for the first version. With cp, the same
// write also requires cp.pred; it does not write the claim record.
func (m *Manager) commit(ctx context.Context, collectionID int64, vchannel string, cp *pchannelClaimPred,
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
		curManifest = cur.pb
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

	preds := []predicates.Predicate{predicates.KeyNotExist(key)}
	if cur != nil {
		preds[0] = predicates.ValueEqual(key, cur.pointer)
	}
	if cp != nil {
		preds = append(preds, cp.pred)
	}
	if err := m.meta.Save(ctx, map[string]string{key: ptr}, preds...); err != nil {
		// the write may have taken effect: the stored pointer decides
		if err := decideByReadBack(ctx, m.meta, key, ptr, err); err != nil {
			m.forget(key)
			if cp != nil {
				m.claims.forget(cp.pchannel)
			}
			return nil, markUnavailable(err, "write manifest pointer %s version %d", key, next.GetVersion())
		}
	}
	m.remember(key, &vchannelManifest{pb: next, pointer: ptr})
	return next, nil
}

// decideByReadBack reads key from meta after a write of value that returned
// writeErr, whose result is not known. It returns nil when key holds value,
// and otherwise writeErr with the read error, if any, attached. The read does
// not use the cancellation of ctx, and it stops after readBackTimeout.
func decideByReadBack(ctx context.Context, meta MetaStore, key, value string, writeErr error) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readBackTimeout)
	defer cancel()
	got, found, err := meta.Load(rctx, key)
	if err != nil {
		return errors.WithSecondaryError(writeErr, err)
	}
	if !found || got != value {
		return writeErr
	}
	return nil
}

// load returns the cached manifest of the pointer key, or reads it from the
// stores. It returns nil when the vchannel has no manifest.
func (m *Manager) load(ctx context.Context, key string) (*vchannelManifest, error) {
	m.mu.Lock()
	cur := m.manifests[key]
	m.mu.Unlock()
	if cur != nil {
		return cur, nil
	}
	ptr, found, err := m.meta.Load(ctx, key)
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
	cur = &vchannelManifest{pb: &mf, pointer: ptr}
	m.remember(key, cur)
	return cur, nil
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

// markUnavailable adds context to a failure of a remote store and marks it as
// pkerr.ErrUnavailable. A mark that err already has, such as pkerr.ErrCorrupted
// for a missing object, stays.
func markUnavailable(err error, format string, args ...any) error {
	return errors.Mark(errors.Wrapf(err, format, args...), pkerr.ErrUnavailable)
}

func (m *Manager) remember(key string, v *vchannelManifest) {
	m.mu.Lock()
	m.manifests[key] = v
	m.mu.Unlock()
}

func (m *Manager) forget(key string) {
	m.mu.Lock()
	delete(m.manifests, key)
	m.mu.Unlock()
}
