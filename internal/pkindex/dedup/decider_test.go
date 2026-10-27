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

	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

func newTestDecider(t *testing.T) (*decider, *MemoryEngine) {
	t.Helper()
	eng := NewMemoryEngine()
	d := New(eng, Config{}).(*decider)
	t.Cleanup(func() {
		d.Close()
		_ = eng.Close()
	})
	return d, eng
}

func int64PKs(values ...int64) []PK {
	pks := make([]PK, 0, len(values))
	for _, v := range values {
		pks = append(pks, Int64PK(v))
	}
	return pks
}

func seed(t *testing.T, eng engine.RW, segmentID int64, values ...int64) {
	t.Helper()
	require.NoError(t, writeRecords(context.Background(), eng, segmentID, int64PKs(values...), nil))
}

func requireSegment(t *testing.T, eng engine.RW, pk int64, segmentID int64) {
	t.Helper()
	entry := lookupOne(t, eng, Int64PK(pk))
	if segmentID == 0 {
		require.Nil(t, entry)
		return
	}
	require.NotNil(t, entry)
	require.Equal(t, segmentID, entry.SegmentID)
}

// decideInsert decides an autocommit insert and returns its companion delete keys.
func decideInsert(t *testing.T, d Decider, values ...int64) (*WriteIntent, []PK) {
	t.Helper()
	intent, err := d.Decide(context.Background(), DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(values...)})
	require.NoError(t, err)
	return intent, intent.Decision().CompanionDeletePKs
}

func TestAutocommitInsert(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1)

	intent, companion := decideInsert(t, d, 1, 2)
	require.Equal(t, int64PKs(1), companion)
	intent.Appended(ctx, 10)
	// Appended does not write the engine, the keys stay in the in-flight set.
	requireSegment(t, a, 2, 0)
	require.Equal(t, 2, InflightLenForTest(d))

	next, companion := decideInsert(t, d, 2, 3)
	require.Equal(t, int64PKs(2), companion, "an applied write is present before observe")
	next.Appended(ctx, 11)

	require.NoError(t, ObserveForTest(ctx, d, 10, 500, int64PKs(1, 2), int64PKs(1)))
	require.NoError(t, ObserveForTest(ctx, d, 11, 501, int64PKs(2, 3), int64PKs(2)))
	require.Equal(t, 0, InflightLenForTest(d))
	requireSegment(t, a, 1, 500)
	requireSegment(t, a, 2, 501)
	requireSegment(t, a, 3, 501)

	_, companion = decideInsert(t, d, 3)
	require.Equal(t, int64PKs(3), companion, "an observed write is present through the engine")
}

func TestAutocommitInsertDiscard(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	intent, _ := decideInsert(t, d, 1)
	intent.NotAppended(ctx, errors.New("redo"))
	require.Equal(t, 0, InflightLenForTest(d))

	// the key is free again and was never written.
	again, companion := decideInsert(t, d, 1)
	require.Empty(t, companion)
	again.NotAppended(ctx, nil)
}

// An append that failed but may be in the WAL keeps its keys present until
// observe passes the time tick it may have been written at, and its later
// writers no longer wait for it.
func TestAutocommitInsertInDoubt(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	intent, _ := decideInsert(t, d, 1)
	intent.InDoubt(ctx, errors.New("commit may have been persisted"), 15)
	require.Equal(t, 1, InflightLenForTest(d))

	again, companion := decideInsert(t, d, 1)
	require.Equal(t, int64PKs(1), companion)
	again.Appended(ctx, 20)

	// Below 15 the in-doubt write may still be applied later, so it stays.
	ObserveTimeTickForTest(d, 14)
	require.Equal(t, 2, InflightLenForTest(d))
	// The write was not in the WAL: observe reaches 15 without meeting it.
	ObserveTimeTickForTest(d, 15)
	require.Equal(t, 1, InflightLenForTest(d))
	require.NoError(t, ObserveForTest(ctx, d, 20, 500, int64PKs(1), nil))
	require.Equal(t, 0, InflightLenForTest(d))
}

// An in-doubt write whose time tick observe already passed is removed at once.
func TestInDoubtBelowObserved(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	intent, _ := decideInsert(t, d, 1)
	ObserveTimeTickForTest(d, 30)
	intent.InDoubt(ctx, errors.New("failed"), 25)
	require.Equal(t, 0, InflightLenForTest(d))
}

func TestAutocommitDelete(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1, 2)

	t.Run("all present", func(t *testing.T) {
		intent, err := d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1, 2)})
		require.NoError(t, err)
		require.Equal(t, Decision{PresentPKs: int64PKs(1, 2)}, intent.Decision())
		intent.NotAppended(ctx, nil)
	})
	t.Run("partly present", func(t *testing.T) {
		intent, err := d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1, 9)})
		require.NoError(t, err)
		require.Equal(t, Decision{PresentPKs: int64PKs(1)}, intent.Decision())
		intent.NotAppended(ctx, nil)
	})
	t.Run("none present", func(t *testing.T) {
		intent, err := d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(8, 9)})
		require.NoError(t, err)
		require.Equal(t, Decision{}, intent.Decision())
		intent.Appended(ctx, 11)
	})
	t.Run("an applied delete counts as present", func(t *testing.T) {
		// The set only answers whether a key exists. A key of a delete in
		// flight gets a companion delete, which is always correct.
		_, companion := decideInsert(t, d, 8)
		require.Equal(t, int64PKs(8), companion)
	})
}

func TestTxnDecidesAtCommit(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	const txnID = 7

	body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(8), TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, body.Decision())
	body.Appended(ctx, 11)
	require.Equal(t, 0, InflightLenForTest(d), "a body registers nothing, it takes effect at commit")

	// an autocommit insert of the same key lands in between.
	auto, companion := decideInsert(t, d, 8)
	require.Empty(t, companion)
	auto.Appended(ctx, 13)

	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, int64PKs(8), commit.Decision().CompanionDeletePKs)
	commit.Appended(ctx, 15)
	require.Equal(t, 2, InflightLenForTest(d))

	// the pending set is gone: committing again decides nothing.
	again, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, again.Decision())
	again.NotAppended(ctx, nil)
}

func TestTxnOwnDeleteSuppressesCompanionDelete(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1, 2)
	const txnID = 9

	// an upsert-like transaction: insert 1 and 2, delete 1.
	ins, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2), TxnID: txnID})
	require.NoError(t, err)
	ins.Appended(ctx, 11)
	del, err := d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, Decision{}, del.Decision())
	del.Appended(ctx, 12)

	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, int64PKs(2), commit.Decision().CompanionDeletePKs)
	commit.Appended(ctx, 13)
	require.Equal(t, 2, InflightLenForTest(d), "the commit registers every key of its bodies")
}

func TestTxnDeleteOnly(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	seed(t, a, 100, 1)
	const txnID = 11

	del, err := d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	del.Appended(ctx, 12)
	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Empty(t, commit.Decision().CompanionDeletePKs)
	commit.Appended(ctx, 13)
}

func TestTxnRollbackAndDrop(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

	for _, end := range []func(txnID int64){
		func(txnID int64) {
			rollback, err := d.Decide(ctx, DecideRequest{Kind: KindRollbackTxn, VChannel: "v1", TxnID: txnID})
			require.NoError(t, err)
			rollback.Appended(ctx, 20)
		},
		func(txnID int64) { d.DropEndedTxns(func(id int64) bool { return id == txnID }) },
	} {
		const txnID = 21
		body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(5), TxnID: txnID})
		require.NoError(t, err)
		body.Appended(ctx, 11)
		end(txnID)

		commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
		require.NoError(t, err)
		commit.Appended(ctx, 30)
		require.Equal(t, 0, InflightLenForTest(d))
	}
}

func TestTxnCommitDiscardKeepsPending(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	const txnID = 31

	body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(5), TxnID: txnID})
	require.NoError(t, err)
	body.Appended(ctx, 11)

	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	commit.NotAppended(ctx, errors.New("an extra body failed, the commit was not appended"))
	require.Equal(t, 0, InflightLenForTest(d))

	// the client retries the commit.
	retry, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	retry.Appended(ctx, 20)
	require.Equal(t, 1, InflightLenForTest(d))
}

func TestDropEndedTxns(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	require.Empty(t, PendingTxnIDsForTest(d))

	for _, txnID := range []int64{41, 42} {
		body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(txnID), TxnID: txnID})
		require.NoError(t, err)
		body.Appended(ctx, 11)
	}
	require.ElementsMatch(t, []int64{41, 42}, PendingTxnIDsForTest(d))

	d.DropEndedTxns(func(txnID int64) bool { return txnID == 41 })
	require.Equal(t, []int64{42}, PendingTxnIDsForTest(d))
}

func TestIntentResolvedOnce(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	intent, _ := decideInsert(t, d, 1)
	intent.Appended(ctx, 1)
	require.Panics(t, func() { intent.Appended(ctx, 1) })
	require.Panics(t, func() { intent.NotAppended(ctx, nil) })
	require.Panics(t, func() { intent.InDoubt(ctx, nil, 1) })
}

// A write of a not-ready index waits for the install: Decide returns a
// WaitError and registers nothing. A transaction body and a rollback do not
// wait. Once the index is ready, the decision is exact.
func TestNotReadyWaits(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	seed(t, eng, 7, 1)
	ready := make(chan struct{})
	d := New(eng, Config{Ready: ready})
	defer d.Close()

	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2)})
	wait := requireWaitError(t, err, 0)
	require.Contains(t, err.Error(), "installed")
	_, err = d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1)})
	requireWaitError(t, err, 0)
	require.Equal(t, 0, InflightLenForTest(d))
	body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(3), TxnID: 8})
	require.NoError(t, err, "a transaction body does not wait")
	body.Appended(ctx, 5)
	_, err = d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: 8})
	requireWaitError(t, err, 0)

	done := make(chan error, 1)
	go func() { done <- wait.Wait(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("Wait returned before the index was ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(ready)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the index was ready")
	}

	insert, companion := decideInsert(t, d, 1, 2)
	require.Equal(t, int64PKs(1), companion, "ready: only the present key")
	insert.Appended(ctx, 6)
}

// A Decide after Close fails with the closed error. An open intent still
// ends on its own.
func TestDecideAfterClose(t *testing.T) {
	ctx := context.Background()
	d := New(NewMemoryEngine(), Config{})
	intent, _ := decideInsert(t, d, 1)
	d.Close()
	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(2)})
	require.ErrorIs(t, err, engine.ErrClosed)
	intent.Appended(ctx, 1)
	require.Equal(t, 1, InflightLenForTest(d))
}

func TestDecideReturnsProbeError(t *testing.T) {
	ctx := context.Background()
	eng := NewMemoryEngine()
	d := New(eng, Config{})
	require.NoError(t, eng.Close()) // the memory engine fails every call after Close

	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.Error(t, err)
	// nothing was appended, so the registration is gone and does not block.
	require.Equal(t, 0, InflightLenForTest(d))
	requireDecideReturns(t, d, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
}

// A CommitTxn decision probes the index through the same present() path as an
// autocommit decision, so it can fail the same way and must abort the same way.
func TestDecideCommitReturnsProbeError(t *testing.T) {
	ctx := context.Background()
	d, a := newTestDecider(t)
	const txnID = 51

	body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	body.Appended(ctx, 11)

	require.NoError(t, a.Close()) // the memory engine fails every call after Close

	_, err = d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.Error(t, err)
	require.Equal(t, 0, InflightLenForTest(d))
	requireDecideReturns(t, d, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
}

// requireDecideReturns checks that no stale registration blocks req. The
// decision itself may fail.
func requireDecideReturns(t *testing.T, d Decider, req DecideRequest) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_, _ = d.Decide(context.Background(), req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stale registration blocks the decision")
	}
}

// Appended only fills in the time tick, so a canceled context does not matter.
func TestApplyOnACanceledContext(t *testing.T) {
	d, _ := newTestDecider(t)
	ctx, cancel := context.WithCancel(context.Background())

	intent, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	require.NoError(t, err)
	cancel()
	intent.Appended(ctx, 11)

	_, companion := decideInsert(t, d, 1)
	require.Equal(t, int64PKs(1), companion, "the write applied after the cancel must be visible")
}

// A body applied between the commit snapshot and Appended of the commit breaks the
// WAL contract that a producer waits for every body before it commits. The
// companion deletes were decided without it, so Appended ends the process.
func TestBodyAppliedAfterCommitDecisionPanics(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	const txnID = 77

	first, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err)
	first.Appended(ctx, 11)

	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)

	// the late body: a body registers nothing, so it does not wait for the commit.
	late, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(2), TxnID: txnID})
	require.NoError(t, err)
	late.Appended(ctx, 12)

	require.PanicsWithValue(t,
		"a body of transaction 77 on vchannel v1 was applied after its commit decision, "+
			"a producer must wait for the result of every body before it appends the CommitTxn",
		func() { commit.Appended(ctx, 13) })
}

// requireWaitError asserts that err is a *WaitError for tt and returns it.
func requireWaitError(t *testing.T, err error, tt uint64) *WaitError {
	t.Helper()
	var wait *WaitError
	require.ErrorAs(t, err, &wait)
	require.Equal(t, tt, wait.TimeTick)
	return wait
}

// While observe has not reached the time tick of WaitUntil, every write that
// registers keys gets a WaitError and registers nothing. A transaction body and
// a rollback do not wait. Once observe reaches the time tick, the decision is
// exact.
func TestWaitUntil(t *testing.T) {
	ctx := context.Background()
	d, eng := newTestDecider(t)
	seed(t, eng, 100, 1)
	ObserveTimeTickForTest(d, 5)
	d.WaitUntil(10)
	d.WaitUntil(8) // never lowers

	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2)})
	requireWaitError(t, err, 10)
	_, err = d.Decide(ctx, DecideRequest{Kind: KindDelete, VChannel: "v1", PKs: int64PKs(1)})
	requireWaitError(t, err, 10)
	require.Equal(t, 0, InflightLenForTest(d), "a write that waits registers nothing")

	const txnID = 7
	body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
	require.NoError(t, err, "a transaction body does not wait")
	body.Appended(ctx, 6)
	_, err = d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	requireWaitError(t, err, 10)
	require.Equal(t, 0, InflightLenForTest(d))
	rollback, err := d.Decide(ctx, DecideRequest{Kind: KindRollbackTxn, VChannel: "v1", TxnID: 8})
	require.NoError(t, err, "a rollback does not wait")
	rollback.Appended(ctx, 6)
	commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: 9})
	require.NoError(t, err, "a commit without pending writes does not wait")
	commit.Appended(ctx, 6)

	ObserveTimeTickForTest(d, 9)
	_, err = d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1, 2)})
	requireWaitError(t, err, 10)

	ObserveTimeTickForTest(d, 10)
	insert, companion := decideInsert(t, d, 1, 2)
	require.Equal(t, int64PKs(1), companion, "exact once observe reached the time tick")
	insert.Appended(ctx, 11)
	commit, err = d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
	require.NoError(t, err)
	require.Equal(t, int64PKs(1), commit.Decision().CompanionDeletePKs)
}

// Wait returns once observe reaches the time tick, with ctx.Err() when ctx ends
// first, and with the closed error when the decider is closed first.
func TestWaitErrorWait(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	d.WaitUntil(10)
	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	wait := requireWaitError(t, err, 10)

	done := make(chan error, 1)
	go func() { done <- wait.Wait(ctx) }()
	ObserveTimeTickForTest(d, 9)
	select {
	case err := <-done:
		t.Fatalf("Wait returned before observe reached the time tick: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	ObserveTimeTickForTest(d, 10)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after observe reached the time tick")
	}
	require.NoError(t, wait.Wait(ctx), "returns at once when observe already reached the time tick")

	d.WaitUntil(20)
	_, err = d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	wait = requireWaitError(t, err, 20)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, wait.Wait(cctx), context.Canceled)

	go func() { done <- wait.Wait(ctx) }()
	d.Close()
	select {
	case err := <-done:
		require.ErrorIs(t, err, engine.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Close")
	}
}

// A not-ready index below the time tick of WaitUntil waits for both: Wait
// returns only when the index is ready and observe reached the time tick.
// Close ends the wait for the install.
func TestNotReadyAndWaitUntil(t *testing.T) {
	ctx := context.Background()
	ready := make(chan struct{})
	d := New(NewMemoryEngine(), Config{Ready: ready}).(*decider)
	defer d.Close()
	d.WaitUntil(10)
	_, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	wait := requireWaitError(t, err, 10)

	done := make(chan error, 1)
	go func() { done <- wait.Wait(ctx) }()
	ObserveTimeTickForTest(d, 10)
	select {
	case err := <-done:
		t.Fatalf("Wait returned before the index was ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(ready)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the index was ready")
	}

	notReady := New(NewMemoryEngine(), Config{Ready: make(chan struct{})})
	_, err = notReady.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
	wait = requireWaitError(t, err, 0)
	go func() { done <- wait.Wait(ctx) }()
	notReady.Close()
	select {
	case err := <-done:
		require.ErrorIs(t, err, engine.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Close")
	}
}

// A WaitUntil that comes after a write passed the wait check, while the write
// waits for an earlier write of the same key, makes that write wait too: it
// gets a WaitError and its registration is removed.
func TestWaitUntilAfterTheCheck(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)
	first, _ := decideInsert(t, d, 1)

	type result struct {
		intent *WriteIntent
		err    error
	}
	second := make(chan result, 1)
	go func() {
		intent, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
		second <- result{intent, err}
	}()
	require.Eventually(t, func() bool { return InflightLenForTest(d) == 2 }, 5*time.Second, time.Millisecond,
		"the second write waits behind the first")
	d.WaitUntil(10)
	first.Appended(ctx, 5)

	r := <-second
	require.Nil(t, r.intent)
	requireWaitError(t, r.err, 10)
	require.Equal(t, 1, InflightLenForTest(d), "only the committed first write stays")
}

// The decision order of one key must equal the order in which writers pass the
// critical section: exactly the first writer misses, every later one hits.
func TestSameKeyDecisionsAreSerialized(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

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
			intent, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
			if err != nil {
				t.Error(err)
				return
			}
			if len(intent.Decision().CompanionDeletePKs) == 0 {
				misses.Add(1)
			}
			time.Sleep(time.Millisecond) // stands for the WAL append
			intent.Appended(ctx, timeTick.Add(1))
		}()
	}
	close(barrier)
	wg.Wait()
	require.Equal(t, int64(1), misses.Load())
}

// A transaction commit and an autocommit write register the same key, so a mix
// of both must still serialize correctly: across the whole run, exactly one
// decision finds the key absent.
func TestTxnAndAutocommitOnSameKeyConcurrently(t *testing.T) {
	ctx := context.Background()
	d, _ := newTestDecider(t)

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
		go func() {
			defer wg.Done()
			<-barrier
			for r := 0; r < rounds; r++ {
				txnID := nextTxnID.Add(1)
				body, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1), TxnID: txnID})
				if err != nil {
					t.Error(err)
					return
				}
				body.Appended(ctx, timeTick.Add(1))

				commit, err := d.Decide(ctx, DecideRequest{Kind: KindCommitTxn, VChannel: "v1", TxnID: txnID})
				if err != nil {
					t.Error(err)
					return
				}
				if len(commit.Decision().CompanionDeletePKs) == 0 {
					misses.Add(1)
				}
				commit.Appended(ctx, timeTick.Add(1))
			}
		}()
	}
	for w := 0; w < autoWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			for r := 0; r < rounds; r++ {
				intent, err := d.Decide(ctx, DecideRequest{Kind: KindInsert, VChannel: "v1", PKs: int64PKs(1)})
				if err != nil {
					t.Error(err)
					return
				}
				if len(intent.Decision().CompanionDeletePKs) == 0 {
					misses.Add(1)
				}
				intent.Appended(ctx, timeTick.Add(1))
			}
		}()
	}
	close(barrier)
	wg.Wait()

	require.Equal(t, int64(1), misses.Load())
}
