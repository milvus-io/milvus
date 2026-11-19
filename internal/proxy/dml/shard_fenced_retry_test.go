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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	streamingmessage "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func fencedErr(vchannel string) error {
	return status.NewShardFenced(vchannel)
}

// appendResponses builds one response per error; a nil error is a landed
// message whose time tick is 10 plus its position.
func appendResponses(errs ...error) streaming.AppendResponses {
	resp := streaming.AppendResponses{Responses: make([]streaming.AppendResponse, 0, len(errs))}
	for i, err := range errs {
		r := streaming.AppendResponse{Error: err}
		if err == nil {
			r.AppendResult = &streamingtypes.AppendResult{TimeTick: uint64(10 + i)}
		}
		resp.Responses = append(resp.Responses, r)
	}
	return resp
}

func fenceTestMessages(t *testing.T, vchannels ...string) []streamingmessage.MutableMessage {
	t.Helper()
	msgs := make([]streamingmessage.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		msg, err := streamingmessage.NewDeleteMessageBuilderV1().
			WithHeader(&streamingmessage.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
			WithBody(&msgpb.DeleteRequest{}).
			WithVChannel(vchannel).
			BuildMutable()
		require.NoError(t, err)
		msgs = append(msgs, msg)
	}
	return msgs
}

func TestAllRowOffsets(t *testing.T) {
	assert.Equal(t, []int{0, 1, 2}, allRowOffsets(3))
	assert.Empty(t, allRowOffsets(0))
}

func TestRowSetRetainsOnlyPendingRowsAndDropsEmptyChannels(t *testing.T) {
	var none rowSet
	all := map[string][]int{"a": {0, 1}}
	assert.Equal(t, all, none.retain(all), "a nil set retains everything")

	set := newRowSet([]int{1, 3})
	got := set.retain(map[string][]int{"a": {0, 1}, "b": {2}, "c": {3}})
	assert.Equal(t, map[string][]int{"a": {1}, "c": {3}}, got)

	set.remove([]int{1})
	assert.Equal(t, rowSet{3: {}}, set)
}

// A vchannel that refused is not remembered: the rows routed to it stay pending
// and are offered to whatever vchannel the next attempt routes them to, the
// one that refused included. A target of a split may refuse until it opens.
func TestPendingRowsRetainsRowsRoutedToAVChannelThatRefused(t *testing.T) {
	fence := newSplitFence()
	fence.markFenced("a", fencedErr("a"))
	pending := newPendingRows(4)

	routed := map[string][]int{"a": {0, 1}, "b": {2, 3}}
	assert.Equal(t, routed, pending.retain(routed))
	assert.False(t, pending.done())

	require.NoError(t, pending.settleLanded("b", []int{2, 3}))
	assert.Equal(t, rowSet{0: {}, 1: {}}, pending.pendingSet())
	assert.Equal(t, map[string][]int{"a": {0, 1}}, pending.retain(routed))
	require.NoError(t, pending.settleLanded("a", []int{0, 1}))
	assert.True(t, pending.done())
}

// The helpers are also called outside a retry loop, with no pending rows.
func TestPendingRowsIsNilSafe(t *testing.T) {
	var pending *pendingRows
	all := map[string][]int{"a": {0}}
	assert.Equal(t, all, pending.retain(all))
	assert.Nil(t, pending.pendingSet())
}

// settle settles the offsets of every message that landed, records every
// vchannel that refused with SHARD_FENCED, and reports the first other error.
func TestSplitFenceSettleClassifiesEveryMessage(t *testing.T) {
	fence := newSplitFence()
	msgs := fenceTestMessages(t, "a", "b", "c", "d")
	boom := errors.New("boom")
	pending := newPendingRows(5)
	assert.False(t, fence.refused())

	err := fence.settle(appendResponses(nil, fencedErr("b"), boom, nil), msgs, [][]int{{0, 1}, {2}, {3}, {4}}, pending)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, rowSet{2: {}, 3: {}}, pending.pendingSet())
	assert.True(t, fence.refused())
	assert.Equal(t, map[string]struct{}{"b": {}}, fence.fenced, "an error that is not a refusal is not recorded as one")
	assert.EqualValues(t, 13, fence.maxTimeTick)
	assert.ErrorIs(t, fence.refusal, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(fence.refusal))
	assert.True(t, status.AsStreamingError(fence.refusal).IsShardFenced(), "the refusal of the vchannel stays in the chain")
}

// A message whose response carries no error has landed, whatever else the
// response carries: that is how an append was read before splits existed, and a
// collection that never splits must not see another reading.
func TestSplitFenceSettleReadsAResponseWithoutAnErrorAsLanded(t *testing.T) {
	t.Run("a response with neither an error nor a result", func(t *testing.T) {
		fence := newSplitFence()
		pending := newPendingRows(2)
		resp := appendResponses(nil)
		resp.Responses = append(resp.Responses, streaming.AppendResponse{})
		require.NoError(t, fence.settle(resp, fenceTestMessages(t, "a", "b"), [][]int{{0}, {1}}, pending))
		assert.True(t, pending.done())
	})
	t.Run("fewer responses than messages", func(t *testing.T) {
		fence := newSplitFence()
		pending := newPendingRows(2)
		require.NoError(t, fence.settle(streaming.AppendResponses{}, fenceTestMessages(t, "a", "b"), [][]int{{0}, {1}}, pending))
		assert.True(t, pending.done())
		assert.False(t, fence.refused())
	})
}

// Once an attempt routes by a residue table, or follows a refusal, a message
// the append did not answer fails the write: its outcome is unknown, and a row
// read as landed would never be sent again.
func TestSplitFenceSettleFailsOnAMissingResponseOnceStrict(t *testing.T) {
	_, post := oneShardSplit()
	splitRoute := &writeRoute{table: post.SplitRouting.Table, vchannels: post.VChannels}
	for name, arm := range map[string]func(*splitFence){
		"a split route": func(f *splitFence) { f.routedBy(splitRoute) },
		"after a refusal": func(f *splitFence) {
			f.markFenced("a", fencedErr("a"))
			f.routedBy(legacyWriteRoute([]string{"a", "b"}))
		},
	} {
		t.Run(name, func(t *testing.T) {
			fence := newSplitFence()
			arm(fence)
			pending := newPendingRows(2)
			err := fence.settle(appendResponses(nil), fenceTestMessages(t, "a", "b"), [][]int{{0}, {1}}, pending)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.Contains(t, err.Error(), "answered 1 of 2 messages")
			assert.Equal(t, rowSet{1: {}}, pending.pendingSet(), "the answered message still settles")
		})
	}
	t.Run("a never-split route before any refusal stays lenient", func(t *testing.T) {
		fence := newSplitFence()
		fence.routedBy(legacyWriteRoute([]string{"a", "b"}))
		assert.False(t, fence.strict)
	})
}

func TestSplitFenceKeepsTheHighestTimeTickAcrossAttempts(t *testing.T) {
	fence := newSplitFence()
	fence.observe(appendResponses(nil, nil, nil))
	fence.observe(appendResponses(nil))
	assert.EqualValues(t, 12, fence.maxTimeTick)
}

// refresh evicts the collection and asks for another attempt, reporting the
// latest refusal (or the cause it is given) as the retriable error.
func TestSplitFenceRefreshEvictsTheCollectionAndStaysRetriable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cache := NewMockCache(t)
	cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Return().Times(3)

	fence := newSplitFence()
	retryAgain, err := fence.refresh(ctx, cache, 7, nil)
	assert.True(t, retryAgain)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable, "no refusal yet: rows left unplaced")

	fence.markFenced("a", fencedErr("a"))
	retryAgain, err = fence.refresh(ctx, cache, 7, nil)
	assert.True(t, retryAgain)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.Contains(t, err.Error(), "vchannel a refused the write")

	// An uncoded cause -- a transport error carries no Milvus code, as the
	// comment atop the file explains -- is wrapped as a retriable
	// ServiceUnavailable, never returned bare: retry.Handle can return this
	// value verbatim, unwrapped, if the deadline is too close for another
	// sleep, and the file's contract is that the write then "fails with a
	// retriable ServiceUnavailable" either way.
	cause := errors.New("cause")
	retryAgain, err = fence.refresh(ctx, cache, 7, cause)
	assert.True(t, retryAgain)
	assert.NotEqual(t, cause.Error(), err.Error(), "an uncoded cause is wrapped, not returned bare")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.ErrorIs(t, err, cause, "the original cause is preserved, not stringified")
}

// A cause that already carries a Milvus code is reported as itself: refresh
// only classifies an uncoded cause, it never overrides an existing code.
func TestSplitFenceRefreshLeavesACodedCauseAsItIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cache := NewMockCache(t)
	cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Return().Once()

	fence := newSplitFence()
	cause := merr.WrapErrCollectionNotFound("c")
	retryAgain, err := fence.refresh(ctx, cache, 7, cause)
	assert.True(t, retryAgain)
	assert.Equal(t, cause.Error(), err.Error(), "an already-coded cause is reported unchanged")
	assert.Equal(t, merr.Code(cause), merr.Code(err))
}

// retryPreparation ends the request, rather than backing off, once the
// context it is asked to retry under has already ended -- even when the error
// that triggered it does not itself look like a context error.
func TestRetryPreparationEndsOnAnAlreadyEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := NewMockCache(t)

	fence := newSplitFence()
	transient := errors.New("transient, unrelated to the context")
	retryAgain, err := fence.retryPreparation(ctx, cache, 7, transient)
	assert.False(t, retryAgain, "an already-ended context ends the request instead of backing off")
	// Ending the request here must not leak the raw, uncoded err: it is
	// classified exactly as refresh() classifies an uncoded cause (a retriable
	// ServiceUnavailable), with the context's own reason folded into the
	// message rather than silently dropped, and the original cause still
	// reachable through the chain.
	assert.NotEqual(t, transient.Error(), err.Error())
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
	assert.ErrorIs(t, err, transient, "the original cause is preserved, not stringified")
	assert.Contains(t, err.Error(), "context canceled", "the context's own reason is visible in the message")
}

// retryPreparation's already-ended-context exit does not override an already
// coded cause -- it leaves it exactly as refresh() would -- while still
// folding the context's own reason into the message.
func TestRetryPreparationOnAnAlreadyEndedContextLeavesACodedCauseCoded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := NewMockCache(t)

	fence := newSplitFence()
	coded := merr.WrapErrCollectionNotFound("c")
	retryAgain, err := fence.retryPreparation(ctx, cache, 7, coded)
	assert.False(t, retryAgain)
	assert.Equal(t, merr.Code(coded), merr.Code(err), "an already-coded cause keeps its own code")
	assert.ErrorIs(t, err, coded)
	assert.Contains(t, err.Error(), "context canceled")
}

// A write stops retrying after proxy.shardSplit.maxFenceRetryWait, measured
// from its first refresh, whether its request has a deadline or not.
func TestSplitFenceRefreshCapsTheWait(t *testing.T) {
	old := paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue("50ms")
	t.Cleanup(func() { paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue(old) })
	withDeadline, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	for name, ctx := range map[string]context.Context{"no deadline": context.Background(), "a deadline far away": withDeadline} {
		t.Run(name, func(t *testing.T) {
			cache := NewMockCache(t)
			cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Return().Once()

			fence := newSplitFence()
			fence.markFenced("a", fencedErr("a"))
			retryAgain, err := fence.refresh(ctx, cache, 7, nil)
			require.True(t, retryAgain)
			require.Error(t, err)

			time.Sleep(60 * time.Millisecond)
			retryAgain, err = fence.refresh(ctx, cache, 7, nil)
			assert.False(t, retryAgain, "the wait is spent")
			assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
			assert.True(t, merr.IsRetryableErr(err))
		})
	}
}

func TestIsShardRefusal(t *testing.T) {
	assert.True(t, isShardRefusal(fencedErr("a")))
	assert.False(t, isShardRefusal(errors.New("x")))
	assert.False(t, isShardRefusal(status.NewOnShutdownError("x")))
	assert.False(t, isShardRefusal(nil))
}

// answeredResponses is one landed response per answer; a non-nil answer is an
// idempotency window's duplicate answer naming those offsets.
func answeredResponses(t *testing.T, answers ...[]uint32) streaming.AppendResponses {
	t.Helper()
	resp := appendResponses(make([]error, len(answers))...)
	for i, offsets := range answers {
		if offsets == nil {
			continue
		}
		ids := make([]int64, len(offsets))
		for k, offset := range offsets {
			ids[k] = int64(offset) * 10
		}
		answer, err := anypb.New(streamingmessage.NewIdempotentInsertResult(offsets, int64IDs(ids...)))
		require.NoError(t, err)
		resp.Responses[i].AppendResult.Extra = answer
	}
	return resp
}

// checkedPendingRows is a keyed insert's pending rows once rows may have been
// re-routed by a fence.
func checkedPendingRows(numRows int) *pendingRows {
	p := newKeyedPendingRows(numRows)
	p.checkPlacementFrom()
	return p
}

// Once rows may have been re-routed, a message an idempotency window
// answered as a duplicate settles the offsets the window answered for -- all
// of them, since every message of one transaction gets the transaction's
// answer -- and nothing else.
func TestSplitFenceSettleCountsOnlyTheOffsetsAWindowAnsweredFor(t *testing.T) {
	fence := newSplitFence()
	pending := checkedPendingRows(8)
	resp := answeredResponses(t, []uint32{1, 5}, []uint32{1, 5}, nil)

	err := fence.settle(resp, fenceTestMessages(t, "a", "a", "b"), [][]int{{1}, {5}, {2}}, pending)
	require.NoError(t, err)
	assert.Equal(t, rowSet{0: {}, 3: {}, 4: {}, 6: {}, 7: {}}, pending.pendingSet())
	assert.Equal(t, map[int]string{1: "a", 5: "a", 2: "b"}, pending.placedOn)

	// The same window answering again, for a vchannel fenced later on, is no
	// news.
	require.NoError(t, pending.settleAnswered("a", []int{1, 5}, nil))
}

// Before anything may have been re-routed, a window's answer is the
// idempotent-write contract of master: the answered message's own rows and
// the answered ones are settled, whatever payload the key came with.
func TestSplitFenceSettleKeepsTheIdempotentWriteContractWithoutAReroute(t *testing.T) {
	fence := newSplitFence()
	pending := newKeyedPendingRows(8)
	resp := answeredResponses(t, []uint32{0, 2, 100}, nil)

	err := fence.settle(resp, fenceTestMessages(t, "a", "b"), [][]int{{0, 4}, {2, 6}}, pending)
	require.NoError(t, err, "a row landing after another vchannel answered for it is no error either")
	assert.Equal(t, rowSet{1: {}, 3: {}, 5: {}, 7: {}}, pending.pendingSet())
}

// Once rows may have been re-routed, an answer that does not match where this
// insert placed its rows fails the request rather than settling a row never
// written here: a reused key as an input error, an inconsistent placement as
// a System error.
func TestSplitFenceSettleRefusesAnAnswerThatDoesNotMatchThePlacement(t *testing.T) {
	t.Run("an answer for a row another vchannel took in the same append", func(t *testing.T) {
		fence := newSplitFence()
		pending := checkedPendingRows(8)
		resp := answeredResponses(t, nil, []uint32{0, 2})

		err := fence.settle(resp, fenceTestMessages(t, "b", "a"), [][]int{{2, 6}, {0, 2}}, pending)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.False(t, merr.IsRetryableErr(err))
	})
	t.Run("a landing of a row another vchannel answered for in the same append", func(t *testing.T) {
		fence := newSplitFence()
		pending := checkedPendingRows(8)
		resp := answeredResponses(t, []uint32{0, 2}, nil)

		err := fence.settle(resp, fenceTestMessages(t, "a", "b"), [][]int{{0, 2}, {2, 6}}, pending)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.Contains(t, err.Error(), "row 2 was written on vchannel b")
	})
	t.Run("an answer that leaves a carried row out", func(t *testing.T) {
		pending := checkedPendingRows(8)
		err := pending.settleAnswered("a", []int{0}, []int{0, 4})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Equal(t, 8, len(pending.pendingSet()), "nothing is settled")
	})
	t.Run("an answer for a row this write never tracked", func(t *testing.T) {
		pending := newPendingRows(8)
		pending.checkPlacementFrom()
		require.NoError(t, pending.settleLanded("a", []int{0}))
		assert.ErrorIs(t, pending.settleAnswered("a", []int{0}, nil), merr.ErrServiceInternal)
	})
	t.Run("an answer for a row outside the request", func(t *testing.T) {
		pending := checkedPendingRows(4)
		err := pending.settleAnswered("a", []int{1, 100}, nil)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Equal(t, 4, len(pending.pendingSet()), "nothing is settled")
	})
}

func TestDuplicateAnsweredOffsets(t *testing.T) {
	offsets, duplicate, err := duplicateAnsweredOffsets(streaming.AppendResponse{})
	assert.NoError(t, err)
	assert.False(t, duplicate)
	assert.Nil(t, offsets)

	other, err := anypb.New(&messagespb.PartialUpdateCAS{ReadTs: 1})
	require.NoError(t, err)
	_, duplicate, err = duplicateAnsweredOffsets(streaming.AppendResponse{AppendResult: &streamingtypes.AppendResult{Extra: other}})
	assert.NoError(t, err)
	assert.False(t, duplicate, "an extra of another kind is no answer")

	corrupt := &anypb.Any{TypeUrl: "type.googleapis.com/milvus.proto.messages.IdempotentInsertResult", Value: []byte{0xff, 0xff}}
	_, _, err = duplicateAnsweredOffsets(streaming.AppendResponse{AppendResult: &streamingtypes.AppendResult{Extra: corrupt}})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	fence := newSplitFence()
	resp := appendResponses(nil)
	resp.Responses[0].AppendResult.Extra = corrupt
	err = fence.settle(resp, fenceTestMessages(t, "a"), [][]int{{0}}, newPendingRows(1))
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

// Only a split source is probed: a vchannel the route names as one, or a
// vchannel that refused this write and that the route does not list as a shard
// any more. A vchannel that refused and is still a shard gets its rows again
// instead; it may be a target that has not opened yet.
func TestSplitFenceUnprobedListsOnlySplitSources(t *testing.T) {
	fence := newSplitFence()
	fence.markFenced("s", fencedErr("s"))

	stale := &writeRoute{vchannels: []string{"s", "u"}}
	vchannels, _ := fence.unprobed(stale, nil, 0)
	assert.Empty(t, vchannels, "the source is still a shard of a stale route")

	fence.markFenced("t", fencedErr("t"))
	fresh := &writeRoute{vchannels: []string{"t", "u"}}
	vchannels, rows := fence.unprobed(fresh, nil, 2)
	assert.Equal(t, []string{"s"}, vchannels, "the target that refused is still a shard")
	assert.Equal(t, 2, rows["s"], "a source learned from a refusal carries the first pending row")

	named := &writeRoute{vchannels: []string{"t", "u"}}
	vchannels, rows = fence.unprobed(named, map[string]int{"s": 1, "r": 3}, 0)
	assert.Equal(t, []string{"r", "s"}, vchannels)
	assert.Equal(t, map[string]int{"r": 3, "s": 1}, rows, "a named source keeps the row chosen for it")
	fence.markProbed("s")
	vchannels, _ = fence.unprobed(named, map[string]int{"s": 1, "r": 3}, 0)
	assert.Equal(t, []string{"r"}, vchannels)

	pending := newPendingRows(0)
	assert.Equal(t, -1, pending.first())
	pending = newPendingRows(3)
	require.NoError(t, pending.settleLanded("a", []int{0}))
	assert.Equal(t, 1, pending.first())
	assert.Equal(t, []int{1, 2}, pending.sortedOffsets())
}

// A write whose first append lands whole -- every write to a collection no
// split touches -- never builds its per-row set: tracking what is pending costs
// the same whatever the row count. Only the messages are looked at.
func TestPendingRowsCostNothingPerRowWhenTheFirstAppendLandsWhole(t *testing.T) {
	msgs := fenceTestMessages(t, "a", "b")
	run := func(numRows int) (float64, *pendingRows) {
		routed := map[string][]int{"a": allRowOffsets(numRows)[:numRows/2], "b": allRowOffsets(numRows)[numRows/2:]}
		offsets := [][]int{routed["a"], routed["b"]}
		resp := appendResponses(nil, nil)
		var last *pendingRows
		allocs := testing.AllocsPerRun(20, func() {
			fence := newSplitFence()
			pending := newKeyedPendingRows(numRows)
			_ = pending.retain(routed)
			_ = pending.pendingSet()
			if err := fence.settle(resp, msgs, offsets, pending); err != nil {
				t.Fatal(err)
			}
			last = pending
		})
		return allocs, last
	}
	small, smallPending := run(10)
	large, largePending := run(100_000)
	assert.Equal(t, small, large, "the allocations of a whole landing do not grow with the rows")
	for _, pending := range []*pendingRows{smallPending, largePending} {
		assert.True(t, pending.done())
		assert.Nil(t, pending.rows, "no per-row set is built")
		assert.Nil(t, pending.placedOn)
		assert.Equal(t, pending.numRows, pending.landed())
	}
}

// The per-row set is built at the first answer that has to be told apart row
// by row, and from the state the write was in: every row pending.
func TestPendingRowsMaterializeAtTheFirstRefusal(t *testing.T) {
	fence := newSplitFence()
	pending := newPendingRows(4)
	assert.Zero(t, pending.landed())
	assert.Equal(t, 0, pending.first())
	assert.Equal(t, []int{0, 1, 2, 3}, pending.sortedOffsets())
	require.NoError(t, fence.settle(appendResponses(nil, fencedErr("b")), fenceTestMessages(t, "a", "b"), [][]int{{0, 1}, {2, 3}}, pending))
	assert.Equal(t, rowSet{2: {}, 3: {}}, pending.pendingSet())
	assert.Equal(t, 2, pending.landed())
	assert.Equal(t, 2, pending.first())

	t.Run("messages that do not carry every row", func(t *testing.T) {
		fence := newSplitFence()
		pending := newPendingRows(3)
		require.NoError(t, fence.settle(appendResponses(nil), fenceTestMessages(t, "a"), [][]int{{0, 1}}, pending))
		assert.Equal(t, rowSet{2: {}}, pending.pendingSet(), "a row no message carried stays pending")
	})
	t.Run("an empty write is done from the start", func(t *testing.T) {
		pending := newPendingRows(0)
		assert.True(t, pending.done())
		assert.Equal(t, -1, pending.first())
		assert.Empty(t, pending.sortedOffsets())
	})
}

// A write settled whole places nothing more: the insert half of an upsert that
// landed whole while its tombstones were refused is not sent again.
func TestPendingRowsSettledWholeRetainNothing(t *testing.T) {
	fence := newSplitFence()
	pending := newPendingRows(2)
	require.NoError(t, fence.settle(appendResponses(nil), fenceTestMessages(t, "a"), [][]int{{0, 1}}, pending))
	require.True(t, pending.done())
	assert.Empty(t, pending.retain(map[string][]int{"a": {0, 1}}))
	assert.Equal(t, rowSet{}, pending.pendingSet())
	assert.Equal(t, -1, pending.first())
}

// A refused write evicts the collection only while the cache still holds the
// entry it routed by: when another write or an invalidation has replaced it
// since, evicting again would only throw away a fresher describe.
func TestSplitFenceRefreshEvictsOnlyTheEntryItRoutedBy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	routed := &collectionInfo{CollID: 7, VChannels: []string{"a"}}
	fresher := &collectionInfo{CollID: 7, VChannels: []string{"a"}}
	for name, tc := range map[string]struct {
		current   *collectionInfo
		err       error
		evictions int
	}{
		"the routed entry is still cached":   {current: routed, evictions: 1},
		"another entry replaced it":          {current: fresher, evictions: 0},
		"the cache cannot say what it holds": {err: errors.New("describe failed"), evictions: 1},
		"the cache holds nothing":            {evictions: 1},
	} {
		t.Run(name, func(t *testing.T) {
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionInfo(mock.Anything, "", "", int64(7)).Return(tc.current, tc.err).Once()
			if tc.evictions > 0 {
				cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Return().Times(tc.evictions)
			}
			fence := newSplitFence()
			fence.markFenced("a", fencedErr("a"))
			fence.routedBy(&writeRoute{vchannels: routed.VChannels, info: routed})
			retryAgain, err := fence.refresh(ctx, cache, 7, nil)
			assert.True(t, retryAgain)
			assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
			assert.Nil(t, fence.routedInfo, "a refresh consumes the recorded route")
		})
	}
	t.Run("without a recorded route it evicts", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Return().Once()
		fence := newSplitFence()
		_, _ = fence.refresh(ctx, cache, 7, nil)
	})
}

// Many writers refused together: once one of them has refreshed the entry,
// the others route by the fresh one and do not evict it.
func TestConcurrentRefusedWritesShareOneRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	stale := &collectionInfo{CollID: 7, VChannels: []string{"a"}}
	current := stale
	evictions := 0
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionInfo(mock.Anything, "", "", int64(7)).RunAndReturn(
		func(context.Context, string, string, int64) (*collectionInfo, error) { return current, nil })
	cache.EXPECT().RefreshCollectionByID(mock.Anything, int64(7)).Run(func(context.Context, int64) {
		evictions++
		// The next lookup describes again.
		current = &collectionInfo{CollID: 7, VChannels: []string{"a"}}
	}).Return()

	const writers = 8
	fences := make([]*splitFence, writers)
	for i := range fences {
		fences[i] = newSplitFence()
		fences[i].markFenced("a", fencedErr("a"))
		fences[i].routedBy(&writeRoute{vchannels: stale.VChannels, info: stale})
	}
	for _, fence := range fences {
		_, _ = fence.refresh(ctx, cache, 7, nil)
	}
	assert.Equal(t, 1, evictions, "%d writers refused by the same entry evict it once", writers)
}
