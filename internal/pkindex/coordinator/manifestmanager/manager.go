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
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// PointerStore is the metadata store of the manifest pointers.
type PointerStore interface {
	// Load returns the value of key and whether key exists.
	Load(ctx context.Context, key string) (value string, found bool, err error)
	// Create writes the value only when the key does not exist. An error does
	// not tell whether the write took effect.
	Create(ctx context.Context, key, value string) error
	// Swap writes the value only when the stored value equals expected. An
	// error does not tell whether the write took effect.
	Swap(ctx context.Context, key, expected, value string) error
	// List returns all keys with prefix and their values. Keys are relative to
	// the store root, in the same form as the keys passed to Load.
	List(ctx context.Context, prefix string) (keys, values []string, err error)
}

var marshal = proto.MarshalOptions{Deterministic: true}

// readBackTimeout is the time limit of the pointer read after a pointer write
// whose result is not known.
const readBackTimeout = 5 * time.Second

// entry is the current version of one vchannel and the stored pointer value
// that names it.
type entry struct {
	manifest *pkindexpb.Manifest
	pointer  string
}

// Manager is the only writer of the manifests. It applies the edits of one
// vchannel one at a time. The manifests it returns are shared with its cache;
// the caller must not change them.
//
// The cache of a vchannel is filled on its first use and is refreshed only
// after a failed pointer write. Until then, Get does not see the commits of
// another coordinator. The conditional pointer write keeps commits safe when
// the cache is stale.
type Manager struct {
	pointers PointerStore
	store    manifest.ObjectStore
	layout   manifest.Layout
	locks    *lock.KeyLock[string]

	mu    sync.Mutex
	cache map[string]*entry
}

// New returns a Manager that keeps the pointers in pointers and the manifest
// bodies in store, at the keys of layout.
func New(pointers PointerStore, store manifest.ObjectStore, layout manifest.Layout) *Manager {
	return &Manager{
		pointers: pointers,
		store:    store,
		layout:   layout,
		locks:    lock.NewKeyLock[string](),
		cache:    map[string]*entry{},
	}
}

// Claim applies a claim with manifest.ApplyClaim and returns the resulting
// version. The first version gets the SST base path of the layout.
func (m *Manager) Claim(ctx context.Context, c manifest.Claim) (*pkindexpb.Manifest, error) {
	c.SSTBasePath = m.layout.SSTBasePath(c.CollectionID, c.VChannel)
	return m.commit(ctx, c.CollectionID, c.VChannel, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
		return manifest.ApplyClaim(cur, c)
	})
}

// CommitFlush applies a flush with manifest.ApplyFlush and returns the
// resulting version.
func (m *Manager) CommitFlush(ctx context.Context, collectionID int64, vchannel string, f manifest.Flush) (*pkindexpb.Manifest, error) {
	return m.commit(ctx, collectionID, vchannel, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
		return manifest.ApplyFlush(cur, f)
	})
}

// CommitCompact applies a compaction with manifest.ApplyCompact and returns
// the resulting version.
func (m *Manager) CommitCompact(ctx context.Context, collectionID int64, vchannel string, c manifest.Compact) (*pkindexpb.Manifest, error) {
	return m.commit(ctx, collectionID, vchannel, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, bool, error) {
		return manifest.ApplyCompact(cur, c)
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

// commit applies one edit to the current version of a vchannel. When the edit
// changes the manifest, it writes the new body under a unique key, then
// writes the pointer on the condition that the stored pointer is still the
// one it loaded, or does not exist for the first version.
func (m *Manager) commit(ctx context.Context, collectionID int64, vchannel string,
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

	if cur == nil {
		err = m.pointers.Create(ctx, key, ptr)
	} else {
		err = m.pointers.Swap(ctx, key, cur.pointer, ptr)
	}
	if err != nil {
		// the write may have taken effect: the stored pointer decides
		got, found, rerr := m.readBack(ctx, key)
		if rerr != nil || !found || got != ptr {
			m.forget(key)
			if rerr != nil {
				err = errors.WithSecondaryError(err, rerr)
			}
			return nil, markUnavailable(err, "write manifest pointer %s version %d", key, next.GetVersion())
		}
	}
	m.remember(key, &entry{manifest: next, pointer: ptr})
	return next, nil
}

// readBack loads the pointer after a pointer write whose result is not known.
// It does not use the cancellation of ctx, and it stops after readBackTimeout.
func (m *Manager) readBack(ctx context.Context, key string) (string, bool, error) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readBackTimeout)
	defer cancel()
	return m.pointers.Load(rctx, key)
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
	var p pkindexpb.ManifestPointer
	if err := proto.Unmarshal([]byte(ptr), &p); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest pointer %s", key), pkerr.ErrCorrupted)
	}
	if p.GetVersion() == 0 || p.GetBodyKey() == "" {
		return nil, errors.Mark(errors.Newf("manifest pointer %s has version %d and body key %q", key, p.GetVersion(), p.GetBodyKey()), pkerr.ErrCorrupted)
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
