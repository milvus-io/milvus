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

package reqregistry

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRegisterUnregister(t *testing.T) {
	r := New()
	ctx, e := r.Register(context.Background(), Info{Type: TypeSearch, DBName: "db", CollectionName: "c", User: "u"})
	require.NotNil(t, e)
	assert.Same(t, e, FromContext(ctx))
	assert.Equal(t, 1, r.Len())
	assert.Same(t, e, r.Get(e.RequestID()))
	assert.Nil(t, ctx.Err())

	snap := e.Snapshot(time.Now())
	assert.Equal(t, StateQueued, snap.State)
	assert.True(t, snap.Cancellable)
	assert.False(t, snap.StartTime.IsZero())

	r.Unregister(e)
	assert.Equal(t, 0, r.Len())
	assert.Nil(t, r.Get(e.RequestID()))
	// the derived ctx is released, but not as an operator cancel
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Nil(t, CancelCause(ctx))

	// idempotent and nil-safe
	r.Unregister(e)
	r.Unregister(nil)
}

func TestFromContextWithoutEntry(t *testing.T) {
	assert.Nil(t, FromContext(context.Background()))
	assert.Nil(t, CancelCause(context.Background()))
}

func TestCancel(t *testing.T) {
	r := New()
	ctx, e := r.Register(context.Background(), Info{Type: TypeQuery, DBName: "db", CollectionName: "c"})
	e.AddTask(100)
	e.AddTask(101)
	e.MarkRunning(30 * time.Millisecond)

	now := time.Now()
	missing := e.RequestID() + "0"
	canceled, repeated, notFound := r.Cancel([]string{e.RequestID(), missing}, "", "root", "too heavy", now)
	assert.Empty(t, repeated)
	require.Len(t, canceled, 1)
	assert.Equal(t, []string{missing}, notFound)
	assert.Equal(t, e.RequestID(), canceled[0].RequestID)
	assert.Equal(t, []int64{100, 101}, canceled[0].TaskIDs)
	assert.Equal(t, StateRunning, canceled[0].State)
	assert.Equal(t, int64(30), canceled[0].QueuedMS)

	// the ctx is done with the operator cancel as its cause
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	cause := CancelCause(ctx)
	require.Error(t, cause)
	assert.ErrorIs(t, cause, merr.ErrRequestCanceled)
	assert.Contains(t, cause.Error(), "root")
	assert.Contains(t, cause.Error(), "too heavy")

	at, ok := e.CanceledAt()
	assert.True(t, ok)
	assert.True(t, at.Equal(now))

	// a second cancel of the same request, before it has left, reports it as
	// repeated, in state Canceling, so the id does not vanish from the answer
	canceled, repeated, notFound = r.Cancel([]string{e.RequestID()}, "", "root", "again", now)
	assert.Empty(t, canceled)
	assert.Empty(t, notFound)
	require.Len(t, repeated, 1)
	assert.Equal(t, e.RequestID(), repeated[0].RequestID)
	assert.Equal(t, StateCanceling, repeated[0].State)
	// and the cause stays the first operator's
	assert.Contains(t, CancelCause(ctx).Error(), "too heavy")

	// until it leaves, the list shows it as canceling
	listed := r.List(Filter{}, now)
	require.Len(t, listed, 1)
	assert.Equal(t, StateCanceling, listed[0].State)

	// Unregister after cancel keeps the operator cause on the ctx
	r.Unregister(e)
	assert.ErrorIs(t, CancelCause(ctx), merr.ErrRequestCanceled)
}

// An id given more than once in one call is answered once, in the result it
// belongs to, instead of once per repetition.
func TestCancelHandlesARepeatedIDOnce(t *testing.T) {
	r := New()
	_, e := r.Register(context.Background(), Info{Type: TypeSearch})
	defer r.Unregister(e)
	id, missing := e.RequestID(), e.RequestID()+"0"
	now := time.Now()

	canceled, repeated, notFound := r.Cancel([]string{id, missing, id, id, missing}, "", "root", "", now)
	require.Len(t, canceled, 1)
	assert.Equal(t, id, canceled[0].RequestID)
	assert.Equal(t, StateQueued, canceled[0].State)
	assert.Empty(t, repeated)
	assert.Equal(t, []string{missing}, notFound)

	// a later call still reports the request as already canceled, once
	canceled, repeated, notFound = r.Cancel([]string{id, id}, "", "root", "", now)
	assert.Empty(t, canceled)
	assert.Empty(t, notFound)
	require.Len(t, repeated, 1)
	assert.Equal(t, StateCanceling, repeated[0].State)
}

func TestCancelCauseIgnoresClientCancellation(t *testing.T) {
	r := New()
	parent, parentCancel := context.WithCancel(context.Background())
	ctx, e := r.Register(parent, Info{})
	defer r.Unregister(e)

	parentCancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Nil(t, CancelCause(ctx), "a client-side cancellation is not an operator cancel")
}

func TestListFilters(t *testing.T) {
	r := New()
	_, e1 := r.Register(context.Background(), Info{DBName: "db1", CollectionName: "a", User: "alice"})
	_, e2 := r.Register(context.Background(), Info{DBName: "db1", CollectionName: "b", User: "bob"})
	_, e3 := r.Register(context.Background(), Info{DBName: "db2", CollectionName: "a", User: "alice"})
	defer r.Unregister(e1)
	defer r.Unregister(e2)
	defer r.Unregister(e3)
	id1, id2, id3 := e1.RequestID(), e2.RequestID(), e3.RequestID()

	ids := func(infos []Info) []string {
		out := make([]string, 0, len(infos))
		for _, info := range infos {
			out = append(out, info.RequestID)
		}
		return out
	}
	now := time.Now()

	assert.ElementsMatch(t, []string{id1, id2, id3}, ids(r.List(Filter{}, now)))
	assert.ElementsMatch(t, []string{id1, id2}, ids(r.List(Filter{DBName: "db1"}, now)))
	assert.ElementsMatch(t, []string{id1, id3}, ids(r.List(Filter{CollectionName: "a"}, now)))
	assert.ElementsMatch(t, []string{id1}, ids(r.List(Filter{DBName: "db1", User: "alice"}, now)))
	assert.Empty(t, ids(r.List(Filter{DBName: "db2", User: "bob"}, now)))

	// elapsed is measured against the time passed to List
	assert.Empty(t, ids(r.List(Filter{MinElapsedMs: time.Hour.Milliseconds()}, now)))
	assert.ElementsMatch(t, []string{id1, id2, id3}, ids(r.List(Filter{MinElapsedMs: time.Hour.Milliseconds()}, now.Add(2*time.Hour))))
	for _, info := range r.List(Filter{}, now.Add(time.Second)) {
		assert.GreaterOrEqual(t, info.ElapsedMS, int64(1000))
	}

	// A threshold too large to be held in a time.Duration still filters. As
	// nanoseconds the first two wrap to a negative value and the third to
	// less than a millisecond, either of which would let every request through.
	for _, ms := range []int64{math.MaxInt64, math.MaxInt64/int64(time.Millisecond) + 1, 18446744073710} {
		assert.Empty(t, ids(r.List(Filter{MinElapsedMs: ms}, now.Add(2*time.Hour))), "minElapsedMs %d", ms)
	}
	// a negative threshold is no constraint, as before
	assert.Len(t, r.List(Filter{MinElapsedMs: -1}, now), 3)
}

func TestExprPrefixIsBounded(t *testing.T) {
	r := New()
	long := strings.Repeat("x", ExprPrefixLimit*3)
	_, e := r.Register(context.Background(), Info{Expr: long})
	defer r.Unregister(e)
	assert.Len(t, e.Snapshot(time.Now()).Expr, ExprPrefixLimit)
}

func TestExprPrefixNeverSplitsACharacter(t *testing.T) {
	// A three-byte character starting at byte 255 straddles the limit; a
	// four-byte one starting at byte 253 too. The cut must back up to the
	// character boundary so that the prefix stays valid UTF-8 and the proto3
	// string field it is copied into still marshals.
	for _, tc := range []struct {
		name string
		expr string
		want int
	}{
		{"cjk straddles the limit", strings.Repeat("x", ExprPrefixLimit-1) + strings.Repeat("中", 100), ExprPrefixLimit - 1},
		{"emoji straddles the limit", strings.Repeat("x", ExprPrefixLimit-3) + strings.Repeat("\U0001F600", 100), ExprPrefixLimit - 3},
		{"character ends exactly at the limit", strings.Repeat("x", ExprPrefixLimit-3) + strings.Repeat("中", 100), ExprPrefixLimit},
		{"all multi-byte", strings.Repeat("中", 200), ExprPrefixLimit - ExprPrefixLimit%3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			_, e := r.Register(context.Background(), Info{Expr: tc.expr})
			defer r.Unregister(e)
			got := e.Snapshot(time.Now()).Expr
			assert.Len(t, got, tc.want)
			assert.True(t, utf8.ValidString(got))
			assert.True(t, strings.HasPrefix(tc.expr, got))
			_, err := proto.Marshal(&milvuspb.RunningRequestInfo{Expr: got})
			assert.NoError(t, err)
		})
	}
	// and the unfixed cut really would have been rejected by the marshaller
	bad := (strings.Repeat("x", ExprPrefixLimit-1) + "中")[:ExprPrefixLimit]
	require.False(t, utf8.ValidString(bad))
	_, err := proto.Marshal(&milvuspb.RunningRequestInfo{Expr: bad})
	require.Error(t, err)
}

func TestSnapshotIsolatesTaskIDs(t *testing.T) {
	r := New()
	_, e := r.Register(context.Background(), Info{})
	defer r.Unregister(e)
	e.AddTask(1)
	snap := e.Snapshot(time.Now())
	snap.TaskIDs[0] = 99
	assert.Equal(t, []int64{1}, e.Snapshot(time.Now()).TaskIDs)
}

func TestMarkRunningOnlyFirstTaskSetsQueueTime(t *testing.T) {
	r := New()
	_, e := r.Register(context.Background(), Info{})
	defer r.Unregister(e)
	e.MarkRunning(10 * time.Millisecond)
	e.MarkRunning(500 * time.Millisecond)
	snap := e.Snapshot(time.Now())
	assert.Equal(t, StateRunning, snap.State)
	assert.Equal(t, int64(10), snap.QueuedMS)
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			ctx, e := r.Register(context.Background(), Info{DBName: "db"})
			e.AddTask(i * 10)
			e.MarkRunning(time.Millisecond)
			if i%2 == 0 {
				canceled, _, _ := r.Cancel([]string{e.RequestID()}, "", "op", "", time.Now())
				assert.Len(t, canceled, 1)
				assert.ErrorIs(t, CancelCause(ctx), merr.ErrRequestCanceled)
			}
			_ = r.List(Filter{DBName: "db"}, time.Now())
			r.Unregister(e)
		}(int64(i))
	}
	wg.Wait()
	assert.Equal(t, 0, r.Len())
}

func TestCancelHeldToOwner(t *testing.T) {
	r := New()
	aliceCtx, alice := r.Register(context.Background(), Info{Type: TypeSearch, User: "alice"})
	bobCtx, bob := r.Register(context.Background(), Info{Type: TypeSearch, User: "bob"})

	// alice names both ids: hers is canceled, bob's is reported exactly like
	// an id that does not exist, and is left running.
	canceled, _, notFound := r.Cancel([]string{alice.RequestID(), bob.RequestID(), "nope"}, "alice", "alice", "", time.Now())
	require.Len(t, canceled, 1)
	assert.Equal(t, alice.RequestID(), canceled[0].RequestID)
	assert.Equal(t, []string{bob.RequestID(), "nope"}, notFound)
	assert.ErrorIs(t, aliceCtx.Err(), context.Canceled)
	assert.NoError(t, bobCtx.Err())
}

// A cancel that reaches a request before it finished must be the one the
// client sees; a cancel that comes after must change nothing and say so.
func TestFinishDecidesBetweenCancelAndResult(t *testing.T) {
	t.Run("cancel before finish", func(t *testing.T) {
		r := New()
		ctx, e := r.Register(context.Background(), Info{Type: TypeQuery})
		canceled, _, notFound := r.Cancel([]string{e.RequestID()}, "", "root", "heavy", time.Now())
		require.Len(t, canceled, 1)
		require.Empty(t, notFound)

		e.Finish()
		assert.ErrorIs(t, CancelCause(ctx), merr.ErrRequestCanceled, "the RPC method answers with the cancel")
		// until it leaves, the request is still listed and a second cancel still names it
		listed := r.List(Filter{}, time.Now())
		require.Len(t, listed, 1)
		assert.Equal(t, StateCanceling, listed[0].State)
		_, repeated, notFound := r.Cancel([]string{e.RequestID()}, "", "root", "again", time.Now())
		assert.Len(t, repeated, 1)
		assert.Empty(t, notFound)
	})

	t.Run("cancel after finish", func(t *testing.T) {
		r := New()
		ctx, e := r.Register(context.Background(), Info{Type: TypeQuery})
		e.Finish()

		// The entry is still registered: the RPC method has not returned yet.
		require.Equal(t, 1, r.Len())
		canceled, repeated, notFound := r.Cancel([]string{e.RequestID()}, "", "root", "late", time.Now())
		assert.Empty(t, canceled)
		assert.Empty(t, repeated)
		assert.Equal(t, []string{e.RequestID()}, notFound)
		assert.NoError(t, ctx.Err(), "a late cancel leaves the ctx alone")
		assert.Nil(t, CancelCause(ctx))
		_, canceledAt := e.CanceledAt()
		assert.False(t, canceledAt)
		assert.Empty(t, r.List(Filter{}, time.Now()), "a finished request is no longer listed")
	})

	t.Run("cancel on an entry that already left", func(t *testing.T) {
		// Registry.Cancel looks the entry up and then cancels it. A request
		// that finishes and unregisters in between must not be reported as
		// canceled.
		r := New()
		ctx, e := r.Register(context.Background(), Info{Type: TypeQuery})
		e.Finish()
		r.Unregister(e)
		_, first, held := e.Cancel("root", "late", time.Now())
		assert.False(t, first)
		assert.False(t, held)
		assert.Nil(t, CancelCause(ctx))
	})

	t.Run("nil entry", func(t *testing.T) {
		var e *Entry
		e.Finish()
	})
}

// Whichever of Cancel and Finish runs first, the operator's answer and the
// request's ctx agree: canceled if and only if the ctx carries the cancel.
func TestCancelAndFinishNeverDisagree(t *testing.T) {
	r := New()
	for i := 0; i < 2000; i++ {
		ctx, e := r.Register(context.Background(), Info{Type: TypeQuery})
		id := e.RequestID()
		var canceled []Info
		var notFound []string
		var clientSeesCancel bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			canceled, _, notFound = r.Cancel([]string{id}, "", "root", "race", time.Now())
		}()
		go func() {
			defer wg.Done()
			// what the RPC method does on its way out
			e.Finish()
			clientSeesCancel = CancelCause(ctx) != nil
			r.Unregister(e)
		}()
		wg.Wait()
		require.Equal(t, 1, len(canceled)+len(notFound))
		require.Equal(t, len(canceled) == 1, clientSeesCancel,
			"canceled=%d notFound=%d clientSeesCancel=%v", len(canceled), len(notFound), clientSeesCancel)
	}
}
