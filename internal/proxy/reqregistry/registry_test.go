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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRegisterUnregister(t *testing.T) {
	r := New()
	ctx, e := r.Register(context.Background(), Info{RequestID: 1, Type: TypeSearch, DBName: "db", CollectionName: "c", User: "u"})
	require.NotNil(t, e)
	assert.Same(t, e, FromContext(ctx))
	assert.Equal(t, 1, r.Len())
	assert.Same(t, e, r.Get(1))
	assert.Nil(t, ctx.Err())

	snap := e.Snapshot(time.Now())
	assert.Equal(t, StateQueued, snap.State)
	assert.True(t, snap.Cancellable)
	assert.False(t, snap.StartTime.IsZero())

	r.Unregister(e)
	assert.Equal(t, 0, r.Len())
	assert.Nil(t, r.Get(1))
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
	ctx, e := r.Register(context.Background(), Info{RequestID: 7, Type: TypeQuery, DBName: "db", CollectionName: "c"})
	e.AddTask(100)
	e.AddTask(101)
	e.MarkRunning(30 * time.Millisecond)

	now := time.Now()
	cancelled, notFound := r.Cancel([]int64{7, 8}, "root", "too heavy", now)
	require.Len(t, cancelled, 1)
	assert.Equal(t, []int64{8}, notFound)
	assert.Equal(t, int64(7), cancelled[0].RequestID)
	assert.Equal(t, []int64{100, 101}, cancelled[0].TaskIDs)
	assert.Equal(t, StateRunning, cancelled[0].State)
	assert.Equal(t, int64(30), cancelled[0].QueuedMS)

	// the ctx is done with the operator cancel as its cause
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	cause := CancelCause(ctx)
	require.Error(t, cause)
	assert.ErrorIs(t, cause, merr.ErrRequestCancelled)
	assert.Contains(t, cause.Error(), "root")
	assert.Contains(t, cause.Error(), "too heavy")

	at, ok := e.CancelledAt()
	assert.True(t, ok)
	assert.Equal(t, now, at)

	// a second cancel of the same request is not reported again
	cancelled, notFound = r.Cancel([]int64{7}, "root", "again", now)
	assert.Empty(t, cancelled)
	assert.Empty(t, notFound)

	// Unregister after cancel keeps the operator cause on the ctx
	r.Unregister(e)
	assert.ErrorIs(t, CancelCause(ctx), merr.ErrRequestCancelled)
}

func TestCancelCauseIgnoresClientCancellation(t *testing.T) {
	r := New()
	parent, parentCancel := context.WithCancel(context.Background())
	ctx, e := r.Register(parent, Info{RequestID: 3})
	defer r.Unregister(e)

	parentCancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Nil(t, CancelCause(ctx), "a client-side cancellation is not an operator cancel")
}

func TestListFilters(t *testing.T) {
	r := New()
	_, e1 := r.Register(context.Background(), Info{RequestID: 1, DBName: "db1", CollectionName: "a", User: "alice"})
	_, e2 := r.Register(context.Background(), Info{RequestID: 2, DBName: "db1", CollectionName: "b", User: "bob"})
	_, e3 := r.Register(context.Background(), Info{RequestID: 3, DBName: "db2", CollectionName: "a", User: "alice"})
	defer r.Unregister(e1)
	defer r.Unregister(e2)
	defer r.Unregister(e3)

	ids := func(infos []Info) []int64 {
		out := make([]int64, 0, len(infos))
		for _, info := range infos {
			out = append(out, info.RequestID)
		}
		return out
	}
	now := time.Now()

	assert.ElementsMatch(t, []int64{1, 2, 3}, ids(r.List(Filter{}, now)))
	assert.ElementsMatch(t, []int64{1, 2}, ids(r.List(Filter{DBName: "db1"}, now)))
	assert.ElementsMatch(t, []int64{1, 3}, ids(r.List(Filter{CollectionName: "a"}, now)))
	assert.ElementsMatch(t, []int64{1}, ids(r.List(Filter{DBName: "db1", User: "alice"}, now)))
	assert.Empty(t, ids(r.List(Filter{DBName: "db2", User: "bob"}, now)))

	// elapsed is measured against the time passed to List
	assert.Empty(t, ids(r.List(Filter{MinElapsed: time.Hour}, now)))
	assert.ElementsMatch(t, []int64{1, 2, 3}, ids(r.List(Filter{MinElapsed: time.Hour}, now.Add(2*time.Hour))))
	for _, info := range r.List(Filter{}, now.Add(time.Second)) {
		assert.GreaterOrEqual(t, info.ElapsedMS, int64(1000))
	}
}

func TestExprPrefixIsBounded(t *testing.T) {
	r := New()
	long := strings.Repeat("x", ExprPrefixLimit*3)
	_, e := r.Register(context.Background(), Info{RequestID: 1, Expr: long})
	defer r.Unregister(e)
	assert.Len(t, e.Snapshot(time.Now()).Expr, ExprPrefixLimit)
}

func TestSnapshotIsolatesTaskIDs(t *testing.T) {
	r := New()
	_, e := r.Register(context.Background(), Info{RequestID: 1})
	defer r.Unregister(e)
	e.AddTask(1)
	snap := e.Snapshot(time.Now())
	snap.TaskIDs[0] = 99
	assert.Equal(t, []int64{1}, e.Snapshot(time.Now()).TaskIDs)
}

func TestMarkRunningOnlyFirstTaskSetsQueueTime(t *testing.T) {
	r := New()
	_, e := r.Register(context.Background(), Info{RequestID: 1})
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
		go func(id int64) {
			defer wg.Done()
			ctx, e := r.Register(context.Background(), Info{RequestID: id, DBName: "db"})
			e.AddTask(id * 10)
			e.MarkRunning(time.Millisecond)
			if id%2 == 0 {
				cancelled, _ := r.Cancel([]int64{id}, "op", "", time.Now())
				assert.Len(t, cancelled, 1)
				assert.ErrorIs(t, CancelCause(ctx), merr.ErrRequestCancelled)
			}
			_ = r.List(Filter{DBName: "db"}, time.Now())
			r.Unregister(e)
		}(int64(i))
	}
	wg.Wait()
	assert.Equal(t, 0, r.Len())
}
