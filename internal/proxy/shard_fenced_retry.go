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
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// A shard split fences its source vchannel, and the fence is final: there is no
// abort after it, so a vchannel that refused a write with SHARD_FENCED never
// takes one again. The keys it owned belong to the split's targets once the
// split's routing commit lands -- some time AFTER the fence, when the broadcast
// is acknowledged -- and the proxy learns the new routing by re-describing the
// collection (design doc §3.3).
//
// A write that spans several shards is not all-or-nothing. AppendMessages hands
// the messages of each vchannel to that vchannel's producer and commits them
// independently, so when a split fences ONE shard the request's other shards
// commit while that one is refused, and re-sending the whole request would
// write the committed rows twice. The retry unit is therefore the ROW: the
// messages of one vchannel are appended or refused together, so the rows of a
// refused message are precisely the rows still to write.
//
// The same holds for tombstones, and for the same reason. A delete is NOT
// idempotent under the WAL's time ticks: the messages of one vchannel in one
// append commit in one transaction and take the commit's tick, and a tombstone
// removes a row only when its tick is greater than the row's. A tombstone
// re-sent to a vchannel that already committed it lands with a later tick and
// deletes whatever was inserted there in between -- including the row the same
// upsert just wrote, whose insert shared the first tombstone's tick. Deletes are
// therefore settled per message exactly like inserts.
//
// Each retry evicts the collection from the proxy's cache, so the next attempt
// routes against a fresh DescribeCollection. A refresh that lands before the
// routing commit still names the fenced source; rows routed there are held back
// rather than sent to be refused again, and wait for the next refresh.
//
// There is no attempt cap. The fence is final and the split's routing commit is
// re-driven by the broadcaster until it lands, so the write keeps refreshing,
// with capped exponential backoff, until the request's deadline. A request with
// no deadline stops after proxy.shardSplit.maxFenceRetryWait from the first
// refusal. Either way it then fails with a retriable ServiceUnavailable. A
// request that fails this way may have PARTIALLY landed -- the rows and
// tombstones of every message that committed stay committed.

const (
	shardFencedRetryInitialBackoff = 200 * time.Millisecond
	shardFencedRetryMaxBackoff     = 2 * time.Second
)

// shardFencedRetryOptions are the retry options of every fence-retrying write:
// unbounded attempts, bounded by the request's context (and by
// splitFence.refresh for a context with no deadline), with backoff doubling
// from shardFencedRetryInitialBackoff up to shardFencedRetryMaxBackoff.
func shardFencedRetryOptions() []retry.Option {
	return []retry.Option{
		retry.Attempts(0),
		retry.Sleep(shardFencedRetryInitialBackoff),
		retry.MaxSleepTime(shardFencedRetryMaxBackoff),
	}
}

// rowSet is a set of row offsets.
type rowSet map[int]struct{}

func newRowSet(offsets []int) rowSet {
	set := make(rowSet, len(offsets))
	for _, offset := range offsets {
		set[offset] = struct{}{}
	}
	return set
}

// retain narrows a routing result to the rows in the set, dropping any channel
// left with nothing to write. A nil rowSet retains everything.
func (s rowSet) retain(channel2RowOffsets map[string][]int) map[string][]int {
	if s == nil {
		return channel2RowOffsets
	}
	retained := make(map[string][]int, len(channel2RowOffsets))
	for channel, offsets := range channel2RowOffsets {
		kept := make([]int, 0, len(offsets))
		for _, offset := range offsets {
			if _, ok := s[offset]; ok {
				kept = append(kept, offset)
			}
		}
		if len(kept) > 0 {
			retained[channel] = kept
		}
	}
	return retained
}

func (s rowSet) remove(offsets []int) {
	for _, offset := range offsets {
		delete(s, offset)
	}
}

// allRowOffsets is every row of a request.
func allRowOffsets(numRows int) []int {
	offsets := make([]int, numRows)
	for i := range offsets {
		offsets[i] = i
	}
	return offsets
}

// pendingRows is the part of a write -- the rows of an insert, or the primary
// keys of a delete -- that is not durable yet. The repack helpers route every
// row and then keep only what this still has to place.
type pendingRows struct {
	rows  rowSet
	fence *splitFence
}

func newPendingRows(numRows int, fence *splitFence) *pendingRows {
	return &pendingRows{rows: newRowSet(allRowOffsets(numRows)), fence: fence}
}

// retain narrows a routing result to the rows still pending, and holds back the
// rows routed to a vchannel a fence already refused. A nil pendingRows retains
// everything: the repack helpers are also called outside a write loop.
func (p *pendingRows) retain(channel2RowOffsets map[string][]int) map[string][]int {
	if p == nil {
		return channel2RowOffsets
	}
	retained := p.rows.retain(channel2RowOffsets)
	for channel := range retained {
		if p.fence.isFenced(channel) {
			delete(retained, channel)
		}
	}
	return retained
}

// settle drops the rows that are durable after one append (see
// splitFence.settle).
func (p *pendingRows) settle(durable []int) {
	p.rows.remove(durable)
}

// dropFenced removes the messages addressed to a vchannel a fence already
// refused, keeping their offsets pending. msgs and offsets are parallel.
func (p *pendingRows) dropFenced(msgs []message.MutableMessage, offsets [][]int) ([]message.MutableMessage, [][]int) {
	if p == nil {
		return msgs, offsets
	}
	keptMsgs := make([]message.MutableMessage, 0, len(msgs))
	keptOffsets := make([][]int, 0, len(msgs))
	for i, msg := range msgs {
		if p.fence.isFenced(msg.VChannel()) {
			continue
		}
		keptMsgs = append(keptMsgs, msg)
		var rows []int
		if i < len(offsets) {
			rows = offsets[i]
		}
		keptOffsets = append(keptOffsets, rows)
	}
	return keptMsgs, keptOffsets
}

// pendingSet is the set of offsets still to place, or nil -- place everything --
// for a write outside a retry loop.
func (p *pendingRows) pendingSet() rowSet {
	if p == nil {
		return nil
	}
	return p.rows
}

// first is the smallest pending offset, or -1 when nothing is pending.
func (p *pendingRows) first() int {
	first := -1
	for offset := range p.rows {
		if first < 0 || offset < first {
			first = offset
		}
	}
	return first
}

func (p *pendingRows) done() bool {
	return len(p.rows) == 0
}

// splitFence is what one write has learned from its appends: which vchannels
// refused it with SHARD_FENCED, and the highest time tick any append reached.
type splitFence struct {
	fenced map[string]struct{}
	// refusal is the retriable error a write reports if its budget runs out.
	refusal     error
	maxTimeTick uint64
	// firstRefresh is when the write first had to refresh, which is where the
	// wait of a request with no deadline is measured from.
	firstRefresh time.Time
	// probed lists the fenced vchannels whose idempotency window already
	// answered for this write's key (see probeFencedWindows).
	probed map[string]struct{}
}

func newSplitFence() *splitFence {
	return &splitFence{fenced: make(map[string]struct{}), probed: make(map[string]struct{})}
}

// unprobed returns, sorted, the fenced vchannels -- the ones the route lists
// and the ones this write learned -- whose window has not answered yet.
func (f *splitFence) unprobed(listed []string) []string {
	candidates := make(map[string]struct{}, len(listed)+len(f.fenced))
	for _, vchannel := range listed {
		candidates[vchannel] = struct{}{}
	}
	for vchannel := range f.fenced {
		candidates[vchannel] = struct{}{}
	}
	out := make([]string, 0, len(candidates))
	for vchannel := range candidates {
		if _, ok := f.probed[vchannel]; !ok {
			out = append(out, vchannel)
		}
	}
	sort.Strings(out)
	return out
}

func (f *splitFence) markProbed(vchannel string) {
	f.probed[vchannel] = struct{}{}
}

func (f *splitFence) isFenced(vchannel string) bool {
	if f == nil {
		return false
	}
	_, ok := f.fenced[vchannel]
	return ok
}

// markFenced records a vchannel that refused a write with SHARD_FENCED.
func (f *splitFence) markFenced(vchannel string) {
	f.fenced[vchannel] = struct{}{}
	// Retriable and a System error: the request is valid, the proxy's routing
	// is one commit behind.
	f.refusal = merr.WrapErrServiceUnavailableMsg("vchannel %s is fenced by a shard split and the new owner of its keys is not visible yet", vchannel)
}

func (f *splitFence) observe(resp streaming.AppendResponses) {
	f.maxTimeTick = max(f.maxTimeTick, resp.MaxTimeTick())
}

// settle reads one append's responses. It returns the offsets that are durable
// now, and the first error that is not a fence.
//
// A message that landed makes its own offsets durable. A message an
// idempotency window answered as a duplicate was not appended again: the
// window answers with the offsets its key's first append wrote on that
// vchannel, and those are durable too. The message's own offsets are settled
// with them, because re-sending them under the same key to the same vchannel
// would only be answered the same way.
//
// An error that is not a fence is not ours to retry -- the caller fails the
// request rather than replaying rows that may already be durable. A message
// with no response does not count as landed: nothing proves it durable, so its
// rows stay pending.
func (f *splitFence) settle(resp streaming.AppendResponses, msgs []message.MutableMessage, offsets [][]int) ([]int, error) {
	f.observe(resp)
	var durable []int
	var fatal error
	for i, msg := range msgs {
		if i >= len(resp.Responses) {
			continue
		}
		if err := resp.Responses[i].Error; err != nil {
			if !status.AsStreamingError(err).IsShardFenced() {
				if fatal == nil {
					fatal = err
				}
				continue
			}
			f.markFenced(msg.VChannel())
			continue
		}
		if i < len(offsets) {
			durable = append(durable, offsets[i]...)
		}
		answered, _, err := duplicateAnsweredOffsets(resp.Responses[i])
		if err != nil {
			if fatal == nil {
				fatal = err
			}
			continue
		}
		durable = append(durable, answered...)
	}
	return durable, fatal
}

// duplicateAnsweredOffsets returns the row offsets an idempotency window
// answered for, and whether the response is such an answer at all; a fresh
// append carries no answer.
func duplicateAnsweredOffsets(resp streaming.AppendResponse) ([]int, bool, error) {
	if resp.AppendResult == nil || resp.AppendResult.Extra == nil {
		return nil, false, nil
	}
	extra := &messagespb.IdempotentInsertResult{}
	if !resp.AppendResult.Extra.MessageIs(extra) {
		return nil, false, nil
	}
	if err := resp.AppendResult.GetExtra(extra); err != nil {
		return nil, false, merr.WrapErrServiceInternalErr(err, "decode the idempotent insert result of a duplicate append")
	}
	offsets := make([]int, 0, len(extra.GetRowOffsets()))
	for _, offset := range extra.GetRowOffsets() {
		offsets = append(offsets, int(offset))
	}
	return offsets, true, nil
}

// retryPreparation decides what a write does with an error met before its
// append -- reading the routing, or repacking -- on a retry, when rows of an
// earlier attempt have landed. A transient failure (a retriable Milvus error, or
// one that carries no Milvus code at all, as a transport error does) backs off
// and refreshes like a refusal, within the same deadline: failing then would
// fail a request that is already partly written. A failure no retry can cure,
// and a context that ended, end the request.
func (f *splitFence) retryPreparation(ctx context.Context, cache Cache, collectionID int64, err error) (bool, error) {
	if merr.IsCanceledOrTimeout(err) || (merr.IsMilvusError(err) && !merr.IsRetryableErr(err)) {
		return false, err
	}
	mlog.RatedWarn(ctx, 1, "preparing a retry after a shard split fence failed, backing off",
		mlog.FieldCollectionID(collectionID), mlog.Err(err))
	return f.refresh(ctx, cache, collectionID, err)
}

// refresh evicts the collection from the proxy's cache, so the next attempt
// re-describes it, and asks retry.Handle for that attempt. cause is what the
// write reports if its deadline ends the retry; it defaults to the latest
// refusal.
//
// A request with no deadline would retry forever if the routing commit never
// became visible, so its wait is capped by proxy.shardSplit.maxFenceRetryWait,
// measured from its first refresh.
func (f *splitFence) refresh(ctx context.Context, cache Cache, collectionID int64, cause error) (bool, error) {
	if cause == nil {
		cause = f.refusal
	}
	if cause == nil {
		cause = merr.WrapErrServiceUnavailableMsg("the write left rows unplaced after a shard split fence")
	}
	now := time.Now()
	if f.firstRefresh.IsZero() {
		f.firstRefresh = now
	}
	if _, ok := ctx.Deadline(); !ok {
		if maxWait := Params.ProxyCfg.ShardSplitMaxFenceRetryWait.GetAsDurationByParse(); now.Sub(f.firstRefresh) >= maxWait {
			mlog.Warn(ctx, "write without a deadline gave up waiting for a shard split's routing commit",
				mlog.FieldCollectionID(collectionID), mlog.Duration("maxWait", maxWait), mlog.Err(cause))
			return false, cause
		}
	}
	cache.RemoveCollectionsByID(ctx, collectionID)
	mlog.RatedInfo(ctx, 1, "write not fully placed after a shard split fence, refreshing the routing and retrying",
		mlog.FieldCollectionID(collectionID), mlog.Err(cause))
	return true, cause
}
