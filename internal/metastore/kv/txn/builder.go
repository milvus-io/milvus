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
// metastore KV catalogs. Callers accumulate Save/Remove/RemovePrefix ops (plus
// a trailing set of "commit" ops that mark the composite write visible) into a
// Builder, then hand it to Commit, which either applies everything atomically
// in a single guarded etcd txn, or - when the op set exceeds the txn size
// limit - flushes the non-commit ops in caller-recorded order via chunked
// batches and applies the commit ops as the final guarded txn.
package txn

// opKind identifies the kind of a single recorded operation.
type opKind int

const (
	opPut opKind = iota
	opDel
	opDelPrefix
)

// op is a single recorded Save/Remove/RemovePrefix (or Commit* variant) call.
type op struct {
	key    string
	value  string
	kind   opKind
	commit bool
}

// Builder accumulates an ordered set of KV ops, to be applied by Commit.
// Order across Save/Remove/RemovePrefix calls is preserved, since it matters
// for the non-atomic fallback path (see Commit).
type Builder struct {
	ops []op
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

// RemovePrefix records a prefix delete.
//
// Asymmetry to know before mixing: an op set that combines exact removals
// (Remove) and prefix removals (RemovePrefix) commits fine on the chunked
// fallback path (each kind is flushed with its own method) but is REJECTED on
// the atomic path (commitAtomic) - a single etcd txn cannot express both
// without widening the exact deletes to prefixes. So the same mixed set may
// succeed or error depending only on whether it fits the txn limit. No Update
// caller emits RemovePrefix today; if one does, either keep exact and prefix
// removals in separate Update calls, or teach commitAtomic to split them.
func (b *Builder) RemovePrefix(prefix string) {
	b.ops = append(b.ops, op{key: prefix, kind: opDelPrefix})
}

// CommitSave records a put that also serves as a visibility marker: in the
// fallback (non-atomic) path, all CommitSave/CommitRemove ops are applied
// together in the final guarded txn, after every non-commit op has been
// flushed.
func (b *Builder) CommitSave(key, value string) {
	b.ops = append(b.ops, op{key: key, value: value, kind: opPut, commit: true})
}

// CommitRemove records an exact-key delete that also serves as a visibility
// marker (see CommitSave).
func (b *Builder) CommitRemove(key string) {
	b.ops = append(b.ops, op{key: key, kind: opDel, commit: true})
}
