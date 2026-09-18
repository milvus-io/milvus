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

package proxy

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
)

func fencedErr(vchannel string) error {
	return status.NewShardFenced(vchannel, 0, 0)
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

func TestPendingRowsHoldsBackRowsRoutedToAFencedVChannel(t *testing.T) {
	fence := newSplitFence()
	fence.markFenced("a")
	pending := newPendingRows(4, fence)

	got := pending.retain(map[string][]int{"a": {0, 1}, "b": {2, 3}})
	assert.Equal(t, map[string][]int{"b": {2, 3}}, got)
	assert.False(t, pending.done())

	pending.settle([]int{2, 3})
	assert.Equal(t, rowSet{0: {}, 1: {}}, pending.pendingSet())
	pending.settle([]int{0, 1})
	assert.True(t, pending.done())
}

func TestPendingRowsDropsMessagesToAFencedVChannel(t *testing.T) {
	fence := newSplitFence()
	fence.markFenced("a")
	pending := newPendingRows(3, fence)
	msgs := fenceTestMessages(t, "a", "b", "c")

	kept, offsets := pending.dropFenced(msgs, [][]int{{0}, {1}})
	require.Len(t, kept, 2)
	assert.Equal(t, "b", kept[0].VChannel())
	assert.Equal(t, "c", kept[1].VChannel())
	assert.Equal(t, [][]int{{1}, nil}, offsets, "a message with no recorded offsets keeps none")
}

// The helpers are also called outside a retry loop, with no pending rows.
func TestPendingRowsIsNilSafe(t *testing.T) {
	var pending *pendingRows
	all := map[string][]int{"a": {0}}
	assert.Equal(t, all, pending.retain(all))
	assert.Nil(t, pending.pendingSet())
	msgs := fenceTestMessages(t, "a")
	kept, offsets := pending.dropFenced(msgs, [][]int{{0}})
	assert.Equal(t, msgs, kept)
	assert.Equal(t, [][]int{{0}}, offsets)

	var fence *splitFence
	assert.False(t, fence.isFenced("a"))
}

// settle returns the offsets of every message that landed, records every
// vchannel that refused with SHARD_FENCED, and reports the first other error.
func TestSplitFenceSettleClassifiesEveryMessage(t *testing.T) {
	fence := newSplitFence()
	msgs := fenceTestMessages(t, "a", "b", "c", "d")
	boom := errors.New("boom")

	durable, err := fence.settle(appendResponses(nil, fencedErr("b"), boom, nil), msgs, [][]int{{0, 1}, {2}, {3}, {4}})
	assert.ErrorIs(t, err, boom)
	assert.ElementsMatch(t, []int{0, 1, 4}, durable)
	assert.True(t, fence.isFenced("b"))
	assert.False(t, fence.isFenced("c"), "an error that is not a fence does not fence")
	assert.EqualValues(t, 13, fence.maxTimeTick)
	assert.ErrorIs(t, fence.refusal, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(fence.refusal))
}

// A message with no response is not proven durable, so its rows stay pending.
func TestSplitFenceSettleTreatsAMissingResponseAsNotLanded(t *testing.T) {
	fence := newSplitFence()
	durable, err := fence.settle(appendResponses(nil), fenceTestMessages(t, "a", "b"), [][]int{{0}, {1}})
	assert.NoError(t, err)
	assert.Equal(t, []int{0}, durable)
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
	cache.EXPECT().RemoveCollectionsByID(mock.Anything, int64(7)).Return(nil).Times(3)

	fence := newSplitFence()
	retryAgain, err := fence.refresh(ctx, cache, 7, nil)
	assert.True(t, retryAgain)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable, "no refusal yet: rows left unplaced")

	fence.markFenced("a")
	retryAgain, err = fence.refresh(ctx, cache, 7, nil)
	assert.True(t, retryAgain)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.Contains(t, err.Error(), "a is fenced")

	cause := errors.New("cause")
	retryAgain, err = fence.refresh(ctx, cache, 7, cause)
	assert.True(t, retryAgain)
	assert.Same(t, cause, err)
}

// A request with no deadline stops re-routing after
// proxy.shardSplit.maxFenceRetryWait, measured from its first refresh.
func TestSplitFenceRefreshCapsARequestWithNoDeadline(t *testing.T) {
	old := Params.ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue("50ms")
	t.Cleanup(func() { Params.ProxyCfg.ShardSplitMaxFenceRetryWait.SwapTempValue(old) })
	cache := NewMockCache(t)
	cache.EXPECT().RemoveCollectionsByID(mock.Anything, int64(7)).Return(nil).Once()

	fence := newSplitFence()
	fence.markFenced("a")
	retryAgain, err := fence.refresh(context.Background(), cache, 7, nil)
	require.True(t, retryAgain)
	require.Error(t, err)

	time.Sleep(60 * time.Millisecond)
	retryAgain, err = fence.refresh(context.Background(), cache, 7, nil)
	assert.False(t, retryAgain, "the wait is spent")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestShardFencedErrorIsRecognised(t *testing.T) {
	assert.True(t, status.AsStreamingError(fencedErr("a")).IsShardFenced())
	assert.False(t, status.AsStreamingError(errors.New("x")).IsShardFenced())
}

// A message an idempotency window answered as a duplicate settles the offsets
// the window answered for, with its own.
func TestSplitFenceSettleCountsTheOffsetsAWindowAnsweredFor(t *testing.T) {
	answer, err := anypb.New(streamingmessage.NewIdempotentInsertResult([]uint32{5, 7}, int64IDs(50, 70)))
	require.NoError(t, err)
	resp := appendResponses(nil)
	resp.Responses[0].AppendResult.Extra = answer

	durable, err := newSplitFence().settle(resp, fenceTestMessages(t, "a"), [][]int{{0}})
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{0, 5, 7}, durable)
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
	_, err = fence.settle(resp, fenceTestMessages(t, "a"), [][]int{{0}})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestSplitFenceUnprobedListsEveryFencedVChannelOnce(t *testing.T) {
	fence := newSplitFence()
	fence.markFenced("b")
	fence.markFenced("c")
	assert.Equal(t, []string{"a", "b", "c"}, fence.unprobed([]string{"c", "a"}))
	fence.markProbed("b")
	assert.Equal(t, []string{"a", "c"}, fence.unprobed([]string{"c", "a"}))

	pending := newPendingRows(0, fence)
	assert.Equal(t, -1, pending.first())
	pending = newPendingRows(3, fence)
	pending.settle([]int{0})
	assert.Equal(t, 1, pending.first())
}
