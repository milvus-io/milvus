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

package dedup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
)

func newTestDecider(t *testing.T) (*Decider, authority.Authority) {
	t.Helper()
	a, err := authority.New("v1")
	require.NoError(t, err)
	d := New(a, 16)
	t.Cleanup(func() { _ = d.Close() })
	return d, a
}

func int64PKs(values ...int64) []authority.PK {
	pks := make([]authority.PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, authority.Int64PK(v))
	}
	return pks
}

func seed(t *testing.T, a authority.Authority, segmentID int64, values ...int64) {
	t.Helper()
	b := a.NewBatch()
	for _, pk := range int64PKs(values...) {
		b.Put(pk, authority.Entry{SegmentID: segmentID})
	}
	require.NoError(t, a.Write(context.Background(), b))
}

func requireSegment(t *testing.T, a authority.Authority, pk int64, segmentID int64) {
	t.Helper()
	entry, err := a.Get(context.Background(), authority.Int64PK(pk))
	require.NoError(t, err)
	if segmentID == 0 {
		require.Nil(t, entry)
		return
	}
	require.NotNil(t, entry)
	require.Equal(t, segmentID, entry.SegmentID)
}

func TestAutocommitInsert(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1)

	intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2)})
	require.NoError(t, err)
	require.Equal(t, Decision{CompanionDeletePKs: int64PKs(1)}, intent.Decision())

	intent.Apply(ctx, Applied{SegmentID: 500, TimeTick: 10})
	requireSegment(t, a, 1, 500)
	requireSegment(t, a, 2, 500)
}

func TestAutocommitInsertDiscard(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	intent.Discard(ctx, errors.New("append failed"))
	requireSegment(t, a, 1, 0)

	// the stripes were released: the same key can be decided again.
	again, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	again.Discard(ctx, nil)
}

func TestAutocommitDelete(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1, 2)

	t.Run("all present", func(t *testing.T) {
		intent, err := d.Decide(ctx, Request{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1, 2)})
		require.NoError(t, err)
		require.Equal(t, Decision{PresentPKs: int64PKs(1, 2)}, intent.Decision())
		intent.Discard(ctx, nil)
	})
	t.Run("partly present", func(t *testing.T) {
		intent, err := d.Decide(ctx, Request{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1, 9)})
		require.NoError(t, err)
		require.Equal(t, Decision{PresentPKs: int64PKs(1)}, intent.Decision())
		intent.Apply(ctx, Applied{TimeTick: 10})
		requireSegment(t, a, 1, 0)
		requireSegment(t, a, 2, 100)
	})
	t.Run("none present", func(t *testing.T) {
		intent, err := d.Decide(ctx, Request{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(8, 9)})
		require.NoError(t, err)
		require.Equal(t, Decision{}, intent.Decision())
		// The caller may still append the message. Apply must then be a no-op on the index.
		intent.Apply(ctx, Applied{TimeTick: 11})
		requireSegment(t, a, 2, 100)
	})
}

func TestTxnDecidesAtCommit(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	const txnID = 7

	body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(8), TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, body.Decision())
	body.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})
	requireSegment(t, a, 8, 0) // not applied before commit

	// an autocommit insert of the same key lands in between.
	auto, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(8)})
	require.NoError(t, err)
	require.Empty(t, auto.Decision().CompanionDeletePKs)
	auto.Apply(ctx, Applied{SegmentID: 600, TimeTick: 13})

	commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, int64PKs(8), commit.Decision().CompanionDeletePKs)
	commit.Apply(ctx, Applied{TimeTick: 15})
	requireSegment(t, a, 8, 500)

	// the pending set is gone: committing again decides nothing.
	again, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, again.Decision())
	again.Discard(ctx, nil)
}

func TestTxnOwnDeleteSuppressesCompanionDelete(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1, 2)
	const txnID = 9

	// an upsert-like transaction: insert 1 and 2, delete 1.
	ins, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2), TxnID: txnID})
	require.NoError(t, err)
	ins.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})
	del, err := d.Decide(ctx, Request{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, del.Decision())
	del.Apply(ctx, Applied{TimeTick: 12})

	commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, int64PKs(2), commit.Decision().CompanionDeletePKs)
	commit.Apply(ctx, Applied{TimeTick: 13})
	// the insert takes effect at the commit time tick and survives the delete of the same transaction.
	requireSegment(t, a, 1, 500)
	requireSegment(t, a, 2, 500)
}

func TestTxnDeleteOnly(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1)
	const txnID = 11

	del, err := d.Decide(ctx, Request{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	del.Apply(ctx, Applied{TimeTick: 12})
	commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Empty(t, commit.Decision().CompanionDeletePKs)
	commit.Apply(ctx, Applied{TimeTick: 13})
	requireSegment(t, a, 1, 0)
}

func TestTxnRollbackAndDrop(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	for _, end := range []func(txnID int64){
		func(txnID int64) {
			rollback, err := d.Decide(ctx, Request{Kind: KindRollbackTxn, VChannel: "v1", TxnID: txnID})
			require.NoError(t, err)
			rollback.Apply(ctx, Applied{TimeTick: 20})
		},
		func(txnID int64) { d.DropTxn(txnID) },
	} {
		const txnID = 21
		body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(5), TxnID: txnID})
		require.NoError(t, err)
		body.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})
		end(txnID)

		commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
		require.NoError(t, err)
		commit.Apply(ctx, Applied{TimeTick: 30})
		requireSegment(t, a, 5, 0)
	}
}

func TestTxnCommitDiscardKeepsPending(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	const txnID = 31

	body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(5), TxnID: txnID})
	require.NoError(t, err)
	body.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})

	commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	commit.Discard(ctx, errors.New("commit append failed"))

	// the client retries the commit.
	retry, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	retry.Apply(ctx, Applied{TimeTick: 20})
	requireSegment(t, a, 5, 500)
}

func TestPendingTxnIDs(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	require.Empty(t, d.PendingTxnIDs())

	for _, txnID := range []int64{41, 42} {
		body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(txnID), TxnID: txnID})
		require.NoError(t, err)
		body.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})
	}
	require.ElementsMatch(t, []int64{41, 42}, d.PendingTxnIDs())

	d.DropTxn(41)
	require.Equal(t, []int64{42}, d.PendingTxnIDs())
}

func TestIntentResolvedOnce(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	intent.Apply(ctx, Applied{SegmentID: 1, TimeTick: 1})
	require.Panics(t, func() { intent.Apply(ctx, Applied{SegmentID: 1, TimeTick: 1}) })
	require.Panics(t, func() { intent.Discard(ctx, nil) })
}

func TestDecideHonorsContextWhileWaitingForReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := New(&notReadyAuthority{ready: make(chan struct{})}, 4)
	_, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDecideReturnsProbeError(t *testing.T) {
	ctx := context.Background()
	a, err := authority.New("v1")
	require.NoError(t, err)
	d := New(a, 4)
	require.NoError(t, a.Close()) // the memory engine fails every call after Close

	_, err = d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.Error(t, err)

	// the stripes were released on the error path.
	done := make(chan struct{})
	go func() {
		_, _ = d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stripes were not released after a failed decision")
	}
}

// A CommitTxn decision probes the index through the same present() path as an
// autocommit decision, so it can fail the same way and must release the same way.
func TestDecideCommitReturnsProbeError(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	const txnID = 51

	body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	body.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})

	require.NoError(t, a.Close()) // the memory engine fails every call after Close

	_, err = d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.Error(t, err)

	// the stripes were released on the error path. The memory engine still fails
	// every call, so this Decide also returns an error, the point is that it returns.
	done := make(chan struct{})
	go func() {
		_, _ = d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stripes were not released after a failed commit decision")
	}
}

func TestApplyPanicsWhenTheEngineWriteFails(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	require.NoError(t, a.Close()) // the memory engine fails every call after Close

	require.Panics(t, func() {
		intent.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})
	})
}

func TestApplyWritesOnACanceledContext(t *testing.T) {
	d, _ := newTestDecider(t)
	ctx, cancel := context.WithCancel(context.Background())

	intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	cancel()
	intent.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})

	next, err := d.Decide(context.Background(), Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	require.Len(t, next.Decision().CompanionDeletePKs, 1, "the write applied after the cancel must be visible")
	next.Discard(context.Background(), nil)
}

// A body applied between the commit snapshot and the commit Apply breaks the
// WAL contract that a producer waits for every body before it commits. The
// index of those keys would be wrong from then on, so Apply ends the process.
func TestBodyAppliedAfterCommitDecisionPanics(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	const txnID = 77

	first, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	first.Apply(ctx, Applied{SegmentID: 500, TimeTick: 11})

	commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)

	// the late body: its keys are not the ones the commit holds, so it does not block
	late, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(2), TxnID: txnID})
	require.NoError(t, err)
	late.Apply(ctx, Applied{SegmentID: 500, TimeTick: 12})

	require.PanicsWithValue(t,
		"a body of transaction 77 on vchannel v1 was applied after its commit decision, "+
			"a producer must wait for the result of every body before it appends the CommitTxn",
		func() { commit.Apply(ctx, Applied{TimeTick: 13}) })
}

// The decision order of one key must equal the order in which writers pass the
// critical section: exactly the first writer misses, every later one hits.
func TestSameKeyDecisionsAreSerialized(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	const writers = 64
	var timeTick atomic.Uint64
	var misses atomic.Int64
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
			if err != nil {
				t.Error(err)
				return
			}
			if len(intent.Decision().CompanionDeletePKs) == 0 {
				misses.Add(1)
			}
			time.Sleep(time.Millisecond) // stands for the WAL append
			tt := timeTick.Add(1)        // stands for the time tick allocated downstream, inside the critical section
			intent.Apply(ctx, Applied{SegmentID: int64(tt), TimeTick: tt})
		}()
	}
	close(barrier)
	wg.Wait()
	require.Equal(t, int64(1), misses.Load())
	requireSegment(t, a, 1, int64(writers)) // the last writer wins
}

// A transaction commit and an autocommit write take the same key stripe, so a
// mix of both must still serialize correctly: across the whole run, exactly
// one decision finds the key absent.
func TestTxnAndAutocommitOnSameKeyConcurrently(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)

	const (
		txnWriters  = 8
		autoWriters = 8
		rounds      = 20
	)
	var timeTick atomic.Uint64
	var misses atomic.Int64
	var nextTxnID atomic.Int64
	var wg sync.WaitGroup
	barrier := make(chan struct{})

	for w := 0; w < txnWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-barrier
			for r := 0; r < rounds; r++ {
				txnID := nextTxnID.Add(1)
				body, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
				if err != nil {
					t.Error(err)
					return
				}
				body.Apply(ctx, Applied{SegmentID: int64(w*rounds + r), TimeTick: timeTick.Add(1)})

				commit, err := d.Decide(ctx, Request{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
				if err != nil {
					t.Error(err)
					return
				}
				if len(commit.Decision().CompanionDeletePKs) == 0 {
					misses.Add(1)
				}
				commit.Apply(ctx, Applied{TimeTick: timeTick.Add(1)})
			}
		}(w)
	}
	for w := 0; w < autoWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			for r := 0; r < rounds; r++ {
				intent, err := d.Decide(ctx, Request{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
				if err != nil {
					t.Error(err)
					return
				}
				if len(intent.Decision().CompanionDeletePKs) == 0 {
					misses.Add(1)
				}
				tt := timeTick.Add(1)
				intent.Apply(ctx, Applied{SegmentID: int64(tt), TimeTick: tt})
			}
		}()
	}
	close(barrier)
	wg.Wait()

	require.Equal(t, int64(1), misses.Load())
	entry, err := a.Get(ctx, authority.Int64PK(1))
	require.NoError(t, err)
	require.NotNil(t, entry)
}

type notReadyAuthority struct {
	authority.Authority
	ready chan struct{}
}

func (a *notReadyAuthority) Ready() <-chan struct{} { return a.ready }
func (a *notReadyAuthority) Close() error           { return nil }
