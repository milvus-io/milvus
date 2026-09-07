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

package txn

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Commit applies every op recorded in b against txn.
//
// The atomic-vs-fallback threshold is the store's own per-transaction op limit
// (txn.MaxTxnOps): etcd reports a small cap, TiKV a large one, so the same
// composite write commits atomically on TiKV where it would have to chunk on
// etcd. Commit is storage-agnostic - it never hard-codes a backend limit.
//
// When the whole op set (every Save/Remove/RemovePrefix/CommitSave/
// CommitRemove call) fits within that limit, it is applied atomically, in a
// single guarded txn.
//
// Otherwise Commit falls back to a caller-ordered, chunked flush: every
// non-commit op is flushed first, in the order it was recorded. Consecutive
// ops of the same kind are coalesced into a run and chunked into contiguous
// slices of up to limit entries in recorded index order, so BOTH cross-kind
// ordering (a Remove that must be visible before a later Save of the same
// key) AND within-kind ordering across batches (an earlier put group must
// persist before a later one, e.g. compactTo before compactFrom) are
// preserved. Finally, the commit ops (CommitSave/CommitRemove) are applied
// together as the last guarded txn; this final txn is the sole visibility
// marker for the whole composite write; if the caller (or process) fails
// partway through the flush, the non-commit ops sit inert until the commit
// txn lands, since nothing but the commit txn is guarded/atomic.
func Commit(ctx context.Context, txn kv.TxnKV, b *Builder) error {
	total := len(b.ops)
	if total == 0 {
		return nil
	}
	limit := txn.MaxTxnOps()
	if limit <= 0 {
		return merr.WrapErrParameterInvalidMsg("composite txn limit must be positive")
	}
	if total <= limit {
		return commitAtomic(ctx, txn, b)
	}
	return commitFallback(ctx, txn, limit, b)
}

// commitAtomic applies every op in a single guarded txn.
// commitAtomic folds every op into one guarded etcd txn. Note: puts and
// exact removals are collected into a map/slice, so a Save and a Remove of the
// SAME key within one Update do NOT preserve their recorded order here (unlike
// the ordered fallback). No caller stages a same-key save+remove in one Update
// today (replica save/release sets are disjoint; segment/child/tombstone key
// spaces don't overlap), so this is unreachable; revisit if that changes.
func commitAtomic(ctx context.Context, txn kv.TxnKV, b *Builder) error {
	saves := make(map[string]string)
	var removals []string
	var prefixRemovals []string
	for _, o := range b.ops {
		switch o.kind {
		case opPut:
			saves[o.key] = o.value
		case opDel:
			removals = append(removals, o.key)
		case opDelPrefix:
			prefixRemovals = append(prefixRemovals, o.key)
		}
	}
	// A single etcd txn cannot express exact and prefix deletes together:
	// MultiSaveAndRemoveWithPrefix deletes EVERY listed key by prefix, so an
	// exact Remove("coll-1") routed through it would also nuke "coll-10" and
	// "coll-1/x". Reject the mix rather than silently widen the delete.
	if len(removals) > 0 && len(prefixRemovals) > 0 {
		return merr.WrapErrParameterInvalidMsg("composite update cannot mix exact and prefix removals in one atomic transaction")
	}
	if len(prefixRemovals) > 0 {
		return txn.MultiSaveAndRemoveWithPrefix(ctx, saves, prefixRemovals)
	}
	return txn.MultiSaveAndRemove(ctx, saves, removals)
}

// commitFallback flushes non-commit ops in recorded order, chunked by limit,
// then applies the commit ops as the final guarded txn.
func commitFallback(ctx context.Context, txn kv.TxnKV, limit int, b *Builder) error {
	nonCommit := make([]op, 0, len(b.ops))
	commitSaves := make(map[string]string)
	var commitRemovals []string
	for _, o := range b.ops {
		if !o.commit {
			nonCommit = append(nonCommit, o)
			continue
		}
		switch o.kind {
		case opPut:
			commitSaves[o.key] = o.value
		case opDel, opDelPrefix:
			// NOTE: commit-marked prefix removals are unsupported - the public
			// Builder API only emits opDel for commit markers (CommitRemove), so
			// opDelPrefix is unreachable here today. If a CommitRemovePrefix is
			// ever added, this folds it into commitRemovals (flushed as an EXACT
			// delete via MultiSaveAndRemove below), which is WRONG for a prefix.
			// Revisit this branch before adding that method.
			commitRemovals = append(commitRemovals, o.key)
		}
	}

	if len(commitSaves)+len(commitRemovals) > limit {
		return merr.WrapErrParameterInvalidMsg("composite commit set exceeds txn limit")
	}

	mlog.Warn(ctx, "composite txn exceeds atomic limit, falling back to chunked commit",
		mlog.Int("total", len(b.ops)), mlog.Int("limit", limit))

	if err := flushNonCommitOps(ctx, txn, limit, nonCommit); err != nil {
		return err
	}

	// Nothing to commit: no commit-marked ops (the DataCoord over-limit case,
	// which never attaches any). Issuing the final guarded txn here would just
	// be an empty MultiSaveAndRemove round trip against etcd, so skip it.
	if len(commitSaves)+len(commitRemovals) == 0 {
		return nil
	}

	return txn.MultiSaveAndRemove(ctx, commitSaves, commitRemovals)
}

// flushNonCommitOps applies non-commit ops in recorded order. It groups
// consecutive ops of the same kind into a run and flushes each run in
// contiguous index-ordered chunks, so a change of kind always starts a new
// flush (relative order across kinds intact) AND, within a run, an earlier
// chunk is always persisted before a later one (relative order within a kind
// intact - a plain map + randomized iteration would break this).
func flushNonCommitOps(ctx context.Context, txn kv.TxnKV, limit int, ops []op) error {
	for i := 0; i < len(ops); {
		kind := ops[i].kind
		j := i + 1
		for j < len(ops) && ops[j].kind == kind {
			j++
		}
		if err := flushRun(ctx, txn, limit, kind, ops[i:j]); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// flushRun flushes a single run of same-kind ops in contiguous chunks of up
// to limit entries, in the run's recorded (index) order. Chunking the slice
// directly - rather than routing through a map-based helper - is what
// preserves within-run cross-batch ordering: chunk k always lands before
// chunk k+1.
func flushRun(ctx context.Context, txn kv.TxnKV, limit int, kind opKind, run []op) error {
	for i := 0; i < len(run); i += limit {
		end := i + limit
		if end > len(run) {
			end = len(run)
		}
		chunk := run[i:end]
		var err error
		switch kind {
		case opPut:
			kvs := make(map[string]string, len(chunk))
			for _, o := range chunk {
				kvs[o.key] = o.value
			}
			err = txn.MultiSave(ctx, kvs)
		case opDel:
			keys := make([]string, 0, len(chunk))
			for _, o := range chunk {
				keys = append(keys, o.key)
			}
			err = txn.MultiSaveAndRemove(ctx, nil, keys)
		case opDelPrefix:
			keys := make([]string, 0, len(chunk))
			for _, o := range chunk {
				keys = append(keys, o.key)
			}
			err = txn.MultiSaveAndRemoveWithPrefix(ctx, nil, keys)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
