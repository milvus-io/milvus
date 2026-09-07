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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
)

func TestCommit_AtomicWhenFits(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(64).Maybe()
	tk.EXPECT().MultiSaveAndRemove(mock.Anything,
		map[string]string{"a": "1", "b": "2"}, []string(nil)).Return(nil).Once()

	b := New()
	b.Save("a", "1")
	b.Save("b", "2")
	assert.NoError(t, Commit(context.Background(), tk, b))
}

func TestCommit_FallbackOrdersCommitLast(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(2).Maybe()
	// pre-commit children flushed first (batch of <=limit), commit marker last.
	tk.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Maybe()
	tk.EXPECT().MultiSaveAndRemove(mock.Anything, map[string]string{"coll": "v"}, []string(nil)).
		Return(nil).Once()

	b := New()
	b.Save("c1", "x")
	b.Save("c2", "y")
	b.CommitSave("coll", "v")
	assert.NoError(t, Commit(context.Background(), tk, b)) // limit=2 forces fallback
}

func TestCommit_Empty(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(64).Maybe()
	assert.NoError(t, Commit(context.Background(), tk, New()))
}

// TestCommit_FallbackPutBatchesPreserveOrder proves FIX 1: within a single
// put run, chunks are contiguous slices of the recorded ops (index order),
// never a randomized Go-map iteration. With three ordered puts and limit=2,
// batch 1 must be exactly {k1,k2} and batch 2 exactly {k3}; no later key may
// appear in an earlier batch. This is the crash-safety guarantee needed for
// e.g. compactTo-before-compactFrom, where both are puts in one run.
func TestCommit_FallbackPutBatchesPreserveOrder(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(2).Maybe()

	var batches []map[string]string
	tk.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		batches = append(batches, kvs)
		return nil
	}).Twice()
	// commit marker (final guarded txn); the fallback needs a commit op so
	// total>limit routes through the chunked flush.
	tk.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	b := New()
	b.Save("k1", "1")
	b.Save("k2", "2")
	b.Save("k3", "3")
	b.CommitSave("marker", "v")
	assert.NoError(t, Commit(context.Background(), tk, b)) // limit=2 forces fallback, 2 puts per chunk

	assert.Equal(t, []map[string]string{
		{"k1": "1", "k2": "2"},
		{"k3": "3"},
	}, batches)
}

// TestCommit_AtomicMixedRemovalsRejected proves FIX 2: a single etcd txn
// cannot express both exact and prefix deletes via MultiSaveAndRemoveWithPrefix
// (which deletes every listed key by prefix). Mixing them in the atomic path
// must return an error and issue no KV call at all.
func TestCommit_AtomicMixedRemovalsRejected(t *testing.T) {
	tk := kvmocks.NewTxnKV(t) // no EXPECT: any KV call fails the test
	tk.EXPECT().MaxTxnOps().Return(64).Maybe()

	b := New()
	b.Save("a", "1")
	b.Remove("coll-1")
	b.RemovePrefix("coll-1/")
	assert.Error(t, Commit(context.Background(), tk, b))
}

// TestCommit_AtomicOnlyPrefixRemoval covers the prefix-only atomic path: it
// must route through MultiSaveAndRemoveWithPrefix with exactly the prefix
// removals (no exact removals folded in).
func TestCommit_AtomicOnlyPrefixRemoval(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(64).Maybe()
	tk.EXPECT().MultiSaveAndRemoveWithPrefix(mock.Anything,
		map[string]string{"a": "1"}, []string{"pfx"}).Return(nil).Once()

	b := New()
	b.Save("a", "1")
	b.RemovePrefix("pfx")
	assert.NoError(t, Commit(context.Background(), tk, b))
}

// TestCommit_FallbackPreservesOrderAcrossKinds is a self-review addition (not
// from the task brief). It asserts the hard requirement called out in the
// design notes: ordering across Save/Remove calls must survive the fallback
// path. A Remove sandwiched between two puts of overlapping keys must be
// flushed as its own batch, strictly before the following put, so the last
// write for "x" observed by etcd is the second Save, not the Remove.
func TestCommit_FallbackPreservesOrderAcrossKinds(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(1).Maybe()

	var calls []string
	tk.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		calls = append(calls, "save:"+kvs["x"])
		return nil
	}).Twice()
	tk.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			if len(saves) == 0 && len(removals) > 0 {
				calls = append(calls, "remove:"+removals[0])
			} else {
				calls = append(calls, "commit")
			}
			return nil
		}).Twice() // one non-commit removal batch, one final commit txn

	b := New()
	b.Save("x", "first")  // run 1: put
	b.Remove("x")         // run 2: del
	b.Save("x", "second") // run 3: put
	b.CommitSave("marker", "v")
	assert.NoError(t, Commit(context.Background(), tk, b)) // limit=1 forces fallback, one op per batch

	assert.Equal(t, []string{"save:first", "remove:x", "save:second", "commit"}, calls)
}

// TestCommit_FallbackExceedsCommitLimit is a self-review addition. When the
// commit-marked ops cannot fit in a single guarded txn even after the
// non-commit ops are chunked away, Commit must reject the request rather
// than silently split the commit set (which would defeat the "commit ops
// apply atomically" guarantee).
func TestCommit_FallbackExceedsCommitLimit(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(1).Maybe()
	tk.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Maybe()

	b := New()
	b.Save("c1", "x")
	b.CommitSave("m1", "v1")
	b.CommitSave("m2", "v2")
	assert.Error(t, Commit(context.Background(), tk, b))
}

// TestCommit_NonPositiveLimit proves FIX 1: a non-positive limit must be
// rejected up front rather than reaching flushRun, whose `for i := 0; i <
// len(run); i += limit` loop never advances (and, for limit<0, regresses)
// when limit<=0 - an infinite loop that would hammer etcd with empty
// MultiSaves. Since paramtable.MaxEtcdTxnNum is refreshable with no min
// validation, limit=0 is reachable in production (e.g. the DataCoord path,
// which has no commit ops to trip the commitFallback over-limit guard). No
// KV call must be issued.
func TestCommit_NonPositiveLimit(t *testing.T) {
	tk := kvmocks.NewTxnKV(t) // no EXPECT: any KV call fails the test
	tk.EXPECT().MaxTxnOps().Return(0).Maybe()

	b := New()
	b.Save("c1", "x")
	assert.Error(t, Commit(context.Background(), tk, b))
}

// TestCommit_FallbackSkipsEmptyTrailingCommit proves FIX 2: when the
// fallback path has zero commit-marked ops (e.g. the DataCoord over-limit
// case, which never attaches commit markers), the final guarded txn is pure
// overhead - an empty MultiSaveAndRemove round trip - and must be skipped
// once the non-commit ops are flushed.
func TestCommit_FallbackSkipsEmptyTrailingCommit(t *testing.T) {
	tk := kvmocks.NewTxnKV(t)
	tk.EXPECT().MaxTxnOps().Return(2).Maybe()
	tk.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Twice()
	// No MultiSaveAndRemove EXPECT: a trailing empty commit txn would be an
	// unexpected call and fail the test.

	b := New()
	b.Save("k1", "1")
	b.Save("k2", "2")
	b.Save("k3", "3")
	assert.NoError(t, Commit(context.Background(), tk, b)) // limit=2 forces fallback, no commit ops
}
