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

package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// shortMaxFenceRetryWait makes a write give up waiting for a split soon.
func shortMaxFenceRetryWait(t *testing.T) {
	old := paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue("300ms")
	t.Cleanup(func() { paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue(old) })
}

// A write whose budget runs out after the rows of an unsplit shard landed says
// so: the request was partially applied, and how much of it. The error stays
// retriable, as a partly failed multi-vchannel append is on master.
func TestInsertThatGivesUpAfterPartOfItLandedSaysItWasPartiallyApplied(t *testing.T) {
	useSingleMessageRepack(t)
	shortMaxFenceRetryWait(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(16))

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	landed := len(wal.insertedRowIDs[splitSibling])
	require.Positive(t, landed)
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.True(t, st.GetRetriable())
	assert.Contains(t, st.GetReason(), fmt.Sprintf("the request was partially applied: %d of 16 rows are durable", landed))
}

func TestDeleteThatGivesUpAfterPartOfItLandedSaysItWasPartiallyApplied(t *testing.T) {
	shortMaxFenceRetryWait(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.deleteTask(seqPKs(16))

	err := task.Execute(context.Background())
	landed := len(wal.deletedPKs[splitSibling])
	require.Positive(t, landed)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Contains(t, err.Error(), fmt.Sprintf("the request was partially applied: %d of 16 deletions are durable", landed))
}

func TestUpsertThatGivesUpAfterPartOfItLandedSaysItWasPartiallyApplied(t *testing.T) {
	useSingleMessageRepack(t)
	shortMaxFenceRetryWait(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(16)
	task := f.upsertTask(t, pks, pks)

	err := task.appendUpsertAttempt(context.Background(), nil)
	rows, deletions := len(wal.insertedRowIDs[splitSibling]), len(wal.deletedPKs[splitSibling])
	require.Positive(t, rows)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Contains(t, err.Error(), fmt.Sprintf("%d of 16 rows and %d of 16 deletions are durable", rows, deletions))
}

// Nothing landed: no claim of a partial application.
func TestInsertThatGivesUpBeforeAnythingLandedDoesNotClaimAPartialApplication(t *testing.T) {
	useSingleMessageRepack(t)
	shortMaxFenceRetryWait(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	installSplitFenceTestWAL(t, splitSource)
	task := f.insertTask(seqPKs(8))

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.NotContains(t, st.GetReason(), "partially applied")
}

// retry.Handle returns the last refusal when the context ends while a write
// waits. The write says that it was the context that ended it, and keeps the
// refusal's retriable code.
func TestInsertWhoseContextEndsWhileWaitingSaysSo(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.staleRefreshes = 1 << 30
	installSplitFenceTestWAL(t, splitSource)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onEvict = func(eviction int) {
		if eviction == 2 {
			cancel()
		}
	}
	task := f.insertTask(seqPKs(8))

	require.NoError(t, task.Execute(ctx))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.Contains(t, st.GetReason(), "because its context ended (context canceled)")
	assert.Contains(t, st.GetReason(), "refused the write during a shard split", "the last refusal stays in the message")
}

func TestSplitFenceFinalError(t *testing.T) {
	boom := merr.WrapErrServiceUnavailableMsg("boom")
	t.Run("a write that never routed by a split or met a refusal keeps its error", func(t *testing.T) {
		fence := newSplitFence()
		pending := newPendingRows(2)
		require.NoError(t, pending.settleLanded("a", []int{0}))
		assert.Equal(t, boom, fence.finalError(context.Background(), 1, boom, writtenPart{what: "rows", pending: pending}))
	})
	t.Run("nil stays nil", func(t *testing.T) {
		fence := newSplitFence()
		fence.strict = true
		assert.NoError(t, fence.finalError(context.Background(), 1, nil))
	})
	t.Run("a context error is not explained by itself", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fence := newSplitFence()
		fence.strict = true
		err := fence.finalError(ctx, 1, context.Canceled, writtenPart{what: "rows"})
		assert.Equal(t, context.Canceled, err)
	})
	t.Run("an error that already names the context is not wrapped again", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fence := newSplitFence()
		fence.strict = true
		named := merr.Wrapf(boom, "ambient context %s", context.Canceled)
		assert.Equal(t, named.Error(), fence.finalError(ctx, 1, named).Error())
	})
	t.Run("the code survives both additions", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fence := newSplitFence()
		fence.strict = true
		pending := newPendingRows(3)
		require.NoError(t, pending.settleLanded("a", []int{0, 2}))
		err := fence.finalError(ctx, 1, boom, writtenPart{what: "rows", pending: pending})
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable))
		assert.True(t, merr.IsRetryableErr(err))
		assert.Contains(t, err.Error(), "2 of 3 rows are durable")
		assert.Contains(t, err.Error(), "context canceled")
	})
}
