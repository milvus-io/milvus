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

// Package manifest holds the only durable writer of the primary key index
// manifests: the flush and compaction rules, the pchannel term check of a
// flush, the commit protocol, and the meta keys.
package manifest

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MetaStore is the metadata store of the manifest pointers. It also holds the
// streamingcoord pchannel meta, which a flush reads but does not write.
type MetaStore interface {
	// Load returns the value of key and whether key exists.
	Load(ctx context.Context, key string) (value string, found bool, err error)
	// MultiLoad returns the value of each key and whether it exists, in the
	// order of keys. An error applies to all keys.
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

// ObjectStore is the object storage of the manifest bodies and the SSTs.
// All keys are complete object keys.
type ObjectStore interface {
	Write(ctx context.Context, key string, data []byte) error
	// Read returns an error marked with pkindex.ErrCorrupted when the key does not exist.
	Read(ctx context.Context, key string) ([]byte, error)
	// Walk calls fn for every object under prefix, recursively, until fn returns false.
	Walk(ctx context.Context, prefix string, fn func(key string, modTime time.Time) bool) error
	MultiRemove(ctx context.Context, keys []string) error
}

var (
	// ErrFenced reports a flush from a pchannel term below the term of the
	// streamingcoord pchannel meta.
	ErrFenced = errors.New("pchannel term is below the assigned term")
	// ErrTermNotAssigned reports a flush whose pchannel has no streamingcoord
	// pchannel meta, or whose term is above the term of that meta.
	ErrTermNotAssigned = errors.New("pchannel term is not the assigned term")
	// ErrNoManifest reports a compaction on a vchannel that has no manifest.
	ErrNoManifest = errors.New("vchannel has no manifest")
	// ErrCheckpointRegressed reports a flush whose checkpoint is not after
	// the manifest checkpoint and whose term is not the last flush term.
	ErrCheckpointRegressed = errors.New("checkpoint time tick does not move forward")
	// ErrDuplicateSST reports an edit that adds an SST ID the manifest already
	// has, or that lists the same SST ID twice.
	ErrDuplicateSST = errors.New("sst ID is already in the manifest or repeated in the edit")
	// ErrStaleEdit reports a compaction whose inputs are not a contiguous run
	// of the manifest SST list, except an applied retry: no input is in the
	// list and all outputs are in the list.
	ErrStaleEdit = errors.New("compaction inputs are not in the manifest")
)

var marshal = proto.MarshalOptions{Deterministic: true}

// vchannelManifest is the current version of the manifest of one vchannel.
// pointer is the pointer to it as it is stored in the meta store.
type vchannelManifest struct {
	pb      *pkindexpb.Manifest
	pointer string
}

// Manager is the only writer of the manifests. It applies the edits of one
// vchannel one at a time. The manifests it returns are shared with its cache;
// the caller must not change them.
//
// The cache of a vchannel is filled on its first use and is refreshed only
// after a failed write. Until then, the manager does not see the commits of
// another coordinator. LoadPointers does not use the cache. The predicates of
// each write keep commits safe when the cache is stale: a flush writes on the
// predicate that the pchannel meta still holds the value that the flush term
// was checked against.
type Manager struct {
	meta     MetaStore
	store    ObjectStore
	rootPath string
	locks    *lock.KeyLock[string]

	mu        sync.Mutex
	manifests map[string]*vchannelManifest // by pointer key
}

// New returns a Manager that keeps the pointers in meta and the manifest bodies
// in store, under rootPath of the object storage. meta also holds the
// streamingcoord pchannel meta.
func New(meta MetaStore, store ObjectStore, rootPath string) *Manager {
	return &Manager{
		meta:      meta,
		store:     store,
		rootPath:  rootPath,
		locks:     lock.NewKeyLock[string](),
		manifests: map[string]*vchannelManifest{},
	}
}

// CommitFlush applies a flush (see applyFlush) and returns the resulting
// version. The flush names the index by its collection ID, index ID and
// vchannel. The pchannel of the flush is the pchannel of the vchannel name.
// The flush term must equal the term of the streamingcoord pchannel meta of
// that pchannel (see checkPChannelTerm), and the pointer write has the
// predicate that the pchannel meta has not changed since the check. The first
// version of an index on a vchannel gets the SST base path of the index on
// the vchannel under the root path.
func (m *Manager) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	pchannelPred, err := checkPChannelTerm(ctx, m.meta, funcutil.ToPhysicalChannel(req.GetVchannel()), req.GetPchannelTerm())
	if err != nil {
		return nil, err
	}
	sstBasePath := pkindex.SSTBasePath(m.rootPath, req.GetCollectionId(), req.GetIndexId(), req.GetVchannel())
	return m.commit(ctx, req.GetCollectionId(), req.GetIndexId(), req.GetVchannel(), []predicates.Predicate{pchannelPred}, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, error) {
		return applyFlush(cur, req, sstBasePath)
	})
}

// CommitCompaction applies a compaction (see applyCompaction) and returns the
// resulting version.
func (m *Manager) CommitCompaction(ctx context.Context, collectionID, indexID int64, vchannel string, c Compaction) (*pkindexpb.Manifest, error) {
	return m.commit(ctx, collectionID, indexID, vchannel, nil, func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, error) {
		return applyCompaction(cur, c)
	})
}

// Target names the manifest of one index on one vchannel.
type Target struct {
	CollectionID int64
	IndexID      int64
	VChannel     string
}

// PointerResult is the manifest pointer of one Target. Pointer is nil when Err
// is set or the vchannel has no manifest.
type PointerResult struct {
	Pointer *pkindexpb.ManifestPointer
	Err     error
}

// LoadPointers reads the manifest pointer of each target from the store, not
// from the cache. It returns one result per target, in the order of the
// targets. A failed read fails all targets.
func (m *Manager) LoadPointers(ctx context.Context, targets []Target) []PointerResult {
	keys := make([]string, 0, len(targets))
	for _, t := range targets {
		keys = append(keys, PointerKey(t.CollectionID, t.IndexID, t.VChannel))
	}
	values, found, err := m.meta.MultiLoad(ctx, keys)
	if err == nil && (len(values) != len(keys) || len(found) != len(keys)) {
		err = errors.Newf("meta store returned %d values and %d flags for %d keys", len(values), len(found), len(keys))
	}
	results := make([]PointerResult, len(targets))
	for i, key := range keys {
		r := &results[i]
		switch {
		case err != nil:
			r.Err = markUnavailable(err, "load manifest pointer %s", key)
		case found[i]:
			r.Pointer, r.Err = decodePointer(key, values[i])
		}
	}
	return results
}

// commit applies one edit to the current version of an index on a vchannel. apply returns
// the new version, or nil when the edit does not change the manifest. When the
// edit changes the manifest, commit writes the new body under a unique key, then
// writes the pointer on the predicate that the stored pointer is still the
// one it loaded, or does not exist for the first version. The same write also
// requires preds; it does not write their keys.
func (m *Manager) commit(ctx context.Context, collectionID, indexID int64, vchannel string, preds []predicates.Predicate,
	apply func(cur *pkindexpb.Manifest) (*pkindexpb.Manifest, error),
) (*pkindexpb.Manifest, error) {
	key := PointerKey(collectionID, indexID, vchannel)
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
	next, err := apply(curManifest)
	if err != nil {
		return nil, err
	}
	if next == nil {
		return curManifest, nil
	}

	bodyKey := pkindex.ManifestBodyKey(pkindex.ManifestDir(m.rootPath, collectionID, indexID, vchannel), next.GetVersion(), uuid.NewString())
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

	pointerPred := predicates.KeyNotExist(key)
	if cur != nil {
		pointerPred = predicates.ValueEqual(key, cur.pointer)
	}
	if err := m.meta.Save(ctx, map[string]string{key: ptr}, append([]predicates.Predicate{pointerPred}, preds...)...); err != nil {
		// The write may have taken effect. The next commit loads the stored
		// pointer, and a retry of an applied edit returns its version.
		m.forget(key)
		return nil, markUnavailable(err, "write manifest pointer %s version %d", key, next.GetVersion())
	}
	m.remember(key, &vchannelManifest{pb: next, pointer: ptr})
	return next, nil
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
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest body %s", p.GetBodyKey()), pkindex.ErrCorrupted)
	}
	if mf.GetVersion() != p.GetVersion() {
		return nil, errors.Mark(errors.Newf("manifest body %s has version %d, pointer has %d", p.GetBodyKey(), mf.GetVersion(), p.GetVersion()), pkindex.ErrCorrupted)
	}
	cur = &vchannelManifest{pb: &mf, pointer: ptr}
	m.remember(key, cur)
	return cur, nil
}

// decodePointer decodes the stored value of the pointer key. It returns an
// error marked with pkindex.ErrCorrupted when the value does not decode or names
// no body.
func decodePointer(key, value string) (*pkindexpb.ManifestPointer, error) {
	var p pkindexpb.ManifestPointer
	if err := proto.Unmarshal([]byte(value), &p); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest pointer %s", key), pkindex.ErrCorrupted)
	}
	if p.GetVersion() == 0 || p.GetBodyKey() == "" {
		return nil, errors.Mark(errors.Newf("manifest pointer %s has version %d and body key %q", key, p.GetVersion(), p.GetBodyKey()), pkindex.ErrCorrupted)
	}
	return &p, nil
}

// permanentStoreErrors are the remote store failures that a retry does not
// fix: the configuration of the store is wrong, or the object is too large.
// ErrIoFailed is not one of them, because it is also the code of an unknown
// or transient failure.
var permanentStoreErrors = []error{
	merr.ErrIoPermissionDenied,
	merr.ErrIoBucketNotFound,
	merr.ErrIoInvalidCredentials,
	merr.ErrIoEntityTooLarge,
}

// markUnavailable adds context to a failure of a remote store and marks it as
// pkindex.ErrUnavailable. A permanent failure (see permanentStoreErrors) is not
// marked and keeps its merr code. A mark that err already has, such as
// pkindex.ErrCorrupted for a missing object, stays.
func markUnavailable(err error, format string, args ...any) error {
	err = errors.Wrapf(err, format, args...)
	for _, target := range permanentStoreErrors {
		if errors.Is(err, target) {
			return err
		}
	}
	return errors.Mark(err, pkindex.ErrUnavailable)
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

// Compaction is the edit that replaces a contiguous run of SSTs with merged SSTs.
type Compaction struct {
	Inputs  []int64
	Outputs []*pkindexpb.SSTRef // newest first
}

// applyFlush applies a flush to cur, which is nil when the vchannel has no
// manifest; then the flush applies to an empty manifest of version 0 that
// takes the collection, the index and the vchannel from the flush and
// sstBasePath. It returns
// a new version when the checkpoint moves forward: the new SSTs go in front of
// the SST list, the checkpoint is raised, and the flush term becomes the last
// flush term. A flush with no SSTs only moves the
// checkpoint. When the checkpoint does not move forward and the flush term
// equals the last flush term, the flush is an applied retry and applyFlush
// returns nil; it does not look at the SSTs of the flush. It returns
// ErrCheckpointRegressed when the checkpoint does not move forward and the
// flush term differs from the last flush term, and ErrDuplicateSST when the
// checkpoint moves forward and an SST is already in cur or is listed twice in
// the flush. It does not check the flush term against the pchannel meta. It
// does not change cur.
func applyFlush(cur *pkindexpb.Manifest, req *pkindexpb.CommitFlushRequest, sstBasePath string) (*pkindexpb.Manifest, error) {
	base := cur
	if base == nil {
		base = &pkindexpb.Manifest{
			CollectionId: req.GetCollectionId(),
			IndexId:      req.GetIndexId(),
			Vchannel:     req.GetVchannel(),
			SstBasePath:  sstBasePath,
		}
	}
	if req.GetCheckpointTimeTick() <= base.GetCheckpointTimeTick() {
		if req.GetPchannelTerm() != base.GetLastFlushTerm() {
			return nil, errors.Wrapf(ErrCheckpointRegressed, "vchannel %s: manifest %d at term %d, flush %d at term %d",
				base.GetVchannel(), base.GetCheckpointTimeTick(), base.GetLastFlushTerm(), req.GetCheckpointTimeTick(), req.GetPchannelTerm())
		}
		return nil, nil
	}
	if err := checkNewSSTs(base.GetVchannel(), idSet(base.GetSsts()), req.GetAddSsts()); err != nil {
		return nil, err
	}
	next := nextVersion(base)
	next.LastFlushTerm = req.GetPchannelTerm()
	next.CheckpointTimeTick = req.GetCheckpointTimeTick()
	next.Ssts = append(cloneSSTs(req.GetAddSsts()), next.GetSsts()...)
	return next, nil
}

// applyCompaction applies a compaction to cur. When the inputs are a non-empty
// contiguous run of the SST list of cur, in the same order, it returns a new
// version, in which the outputs replace the inputs at the position of
// the inputs; the checkpoint and the last flush term do not change. When the inputs are
// not empty, no input is in the SST list of cur and all outputs are in it, the
// compaction is an applied retry and applyCompaction returns nil. It returns
// ErrNoManifest when cur is nil, ErrStaleEdit in all other cases where
// the inputs are not such a run, and ErrDuplicateSST when an output ID is
// already in the SST list of cur or is listed twice in the outputs. It does
// not change cur.
func applyCompaction(cur *pkindexpb.Manifest, c Compaction) (*pkindexpb.Manifest, error) {
	if cur == nil {
		return nil, ErrNoManifest
	}
	list := cur.GetSsts()
	if isAppliedCompaction(idSet(list), c) {
		return nil, nil
	}
	start := -1
	for i, s := range list {
		if len(c.Inputs) > 0 && s.GetId() == c.Inputs[0] {
			start = i
			break
		}
	}
	if len(c.Inputs) == 0 || start < 0 || start+len(c.Inputs) > len(list) {
		return nil, errors.Wrapf(ErrStaleEdit, "vchannel %s: inputs %v", cur.GetVchannel(), c.Inputs)
	}
	for i, id := range c.Inputs {
		if list[start+i].GetId() != id {
			return nil, errors.Wrapf(ErrStaleEdit, "vchannel %s: inputs %v", cur.GetVchannel(), c.Inputs)
		}
	}
	if err := checkNewSSTs(cur.GetVchannel(), idSet(list), c.Outputs); err != nil {
		return nil, err
	}
	next := nextVersion(cur)
	merged := make([]*pkindexpb.SSTRef, 0, len(list)-len(c.Inputs)+len(c.Outputs))
	merged = append(merged, next.GetSsts()[:start]...)
	merged = append(merged, cloneSSTs(c.Outputs)...)
	merged = append(merged, next.GetSsts()[start+len(c.Inputs):]...)
	next.Ssts = merged
	return next, nil
}

// isAppliedCompaction reports whether the inputs of c are not empty, no input is
// in present, and all outputs of c are in present.
func isAppliedCompaction(present map[int64]struct{}, c Compaction) bool {
	if len(c.Inputs) == 0 {
		return false
	}
	for _, id := range c.Inputs {
		if _, ok := present[id]; ok {
			return false
		}
	}
	for _, s := range c.Outputs {
		if _, ok := present[s.GetId()]; !ok {
			return false
		}
	}
	return true
}

// checkNewSSTs returns ErrDuplicateSST when an SST of add is in present or is
// listed twice in add.
func checkNewSSTs(vchannel string, present map[int64]struct{}, add []*pkindexpb.SSTRef) error {
	seen := make(map[int64]struct{}, len(add))
	for _, s := range add {
		id := s.GetId()
		if _, ok := present[id]; ok {
			return errors.Wrapf(ErrDuplicateSST, "vchannel %s: sst %d", vchannel, id)
		}
		if _, ok := seen[id]; ok {
			return errors.Wrapf(ErrDuplicateSST, "vchannel %s: sst %d is listed twice", vchannel, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func nextVersion(cur *pkindexpb.Manifest) *pkindexpb.Manifest {
	next := proto.Clone(cur).(*pkindexpb.Manifest)
	next.Version = cur.GetVersion() + 1
	return next
}

func cloneSSTs(in []*pkindexpb.SSTRef) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(in))
	for _, s := range in {
		out = append(out, proto.Clone(s).(*pkindexpb.SSTRef))
	}
	return out
}

func idSet(list []*pkindexpb.SSTRef) map[int64]struct{} {
	set := make(map[int64]struct{}, len(list))
	for _, s := range list {
		set[s.GetId()] = struct{}{}
	}
	return set
}
