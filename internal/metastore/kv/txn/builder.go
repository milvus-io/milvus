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

// Package txn provides a shared composite-op builder and commit executor for
// metastore KV catalogs. Callers accumulate Save/Remove ops (plus
// a trailing set of "commit" ops that mark the composite write visible) into a
// Builder, then hand it to Commit, which either applies everything atomically
// in a single etcd txn, or - when the op set exceeds the txn size
// limit - flushes the non-commit ops in caller-recorded order via chunked
// batches and applies the commit ops as the final txn.
package txn

import "github.com/milvus-io/milvus/pkg/v3/kv/predicates"

// opKind identifies the kind of a single recorded operation.
type opKind int

const (
	opPut opKind = iota
	opDel
)

// op is a single recorded Save/Remove (or Commit* variant) call.
type op struct {
	key    string
	value  string
	kind   opKind
	commit bool
}

// Builder accumulates an ordered set of KV ops, to be applied by Commit.
// Order across Save/Remove calls is preserved, since it matters for the
// non-atomic fallback path (see Commit).
type Builder struct {
	ops []op
	// readRevision, when positive, guards every op (see GuardUnchangedSince).
	readRevision int64
	// require holds extra predicates of the whole commit (see Require).
	require []predicates.Predicate
}

// New returns an empty Builder.
func New() *Builder {
	return &Builder{}
}

// Save records a put of key/value.
func (b *Builder) Save(key, value string) {
	b.ops = append(b.ops, op{key: key, value: value, kind: opPut})
}

// Remove records an exact-key delete.
func (b *Builder) Remove(key string) {
	b.ops = append(b.ops, op{key: key, kind: opDel})
}

// CommitSave records a put that also serves as a visibility marker: in the
// fallback (non-atomic) path, all CommitSave/CommitRemove ops are applied
// together in the final txn, after every non-commit op has been
// flushed.
func (b *Builder) CommitSave(key, value string) {
	b.ops = append(b.ops, op{key: key, value: value, kind: opPut, commit: true})
}

// GuardUnchangedSince makes the whole write conditional on a read at
// revision: it applies only when no key it writes changed after revision, and
// fails with an error matching predicates.ErrNotMet otherwise. A guarded
// write is never chunked: Commit applies it in one txn or fails without
// writing.
//
// A key deleted after revision has no revision left to compare, so the guard
// alone passes for it; Require predicates.KeyExist for a key that must still
// be there.
func (b *Builder) GuardUnchangedSince(revision int64) {
	b.readRevision = revision
}

// Require adds preds to the predicates the whole write is conditional on.
func (b *Builder) Require(preds ...predicates.Predicate) {
	b.require = append(b.require, preds...)
}

// CommitRemove records an exact-key delete that also serves as a visibility
// marker (see CommitSave).
func (b *Builder) CommitRemove(key string) {
	b.ops = append(b.ops, op{key: key, kind: opDel, commit: true})
}
