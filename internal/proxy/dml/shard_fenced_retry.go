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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// A shard split fences its source vchannel S with one WAL message. From then on
// the StreamingNode of S refuses DML with the retriable SHARD_FENCED error, and
// the keys S owned belong to the targets of the split. A proxy learns the new
// routing when RootCoord has applied the split -- some time AFTER the fence,
// when the broadcast is acknowledged -- by describing the collection again
// (design doc 20261109-shard_split_on_queryview.md, 8.1).
//
// A write that spans several shards is not all-or-nothing. AppendMessages hands
// the messages of each vchannel to the producer of that vchannel and commits
// them independently, so when a split fences ONE shard the other shards of the
// request commit while that one is refused, and sending the whole request again
// would write the committed rows twice. The retry unit is therefore the ROW:
// the messages of one vchannel are appended or refused together, so the rows of
// a refused message are precisely the rows still to write.
//
// The same holds for tombstones, and for the same reason. A delete is NOT
// idempotent under the time ticks of the WAL: the messages of one vchannel in
// one append commit in one transaction and take the tick of the commit, and a
// tombstone removes a row only when its tick is greater than the tick of the
// row. A tombstone sent again to a vchannel that already committed it lands
// with a later tick and deletes whatever was inserted there in between --
// including the row the same upsert just wrote, whose insert shared the tick of
// the first tombstone. Deletes are therefore settled per message exactly like
// inserts.
//
// Each retry evicts the collection from the cache of the proxy, so the next
// attempt routes against a fresh DescribeCollection. A refresh that lands
// before RootCoord has applied the split still names the fenced source, and the
// rows routed there are sent to it and refused again. Nothing is remembered
// about a vchannel that refused: a refusal is a statement about that moment,
// not about the vchannel. The fence of a split source never ends, but a target
// of a split may refuse DML for a short time before it opens (design doc 8.2),
// and it must take the rows as soon as it does. So every attempt sends every
// pending row to the vchannel the current routing gives it, after a back-off.
//
// The retry is bounded in time, not in attempts: it ends at the deadline of the
// request, and at proxy.shardSplit.maxFenceRetryWait from the first refusal,
// whichever comes first. The write then fails with a retriable
// ServiceUnavailable. A request that fails this way may have PARTIALLY landed:
// the rows and tombstones of every message that committed stay committed.

const (
	shardFencedRetryInitialBackoff = 200 * time.Millisecond
	shardFencedRetryMaxBackoff     = 2 * time.Second
)

// shardFencedRetryOptions are the retry options of every write that retries
// refused rows: no attempt limit, bounded by the context of the request and by
// splitFence.refresh, with a back-off doubling from
// shardFencedRetryInitialBackoff up to shardFencedRetryMaxBackoff.
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
//
// A write starts with every row pending and nothing settled, and that state
// costs nothing: rows stays nil, retain hands the routing back unchanged and
// pendingSet is nil, which the repack helpers read as "every row". An append
// that answers every message without an error settles the whole write at
// once (see splitFence.settle). The per-row set is built only when something
// has to be told apart row by row -- a refusal, an error, an idempotency
// window's answer under placement checks -- so a write no split touches does
// the work it did before splits existed.
type pendingRows struct {
	// rows is the set of offsets still pending once materialized; nil before,
	// when every offset below numRows is pending, or after settleAll.
	rows    rowSet
	numRows int
	// settledAll is set when one append made the whole write durable before
	// rows was ever built.
	settledAll bool
	// keyed is set for a keyed insert, which records in placedOn, once rows is
	// built, the vchannel holding each offset that is durable: the one that
	// took it, or the one whose idempotency window answered for it. Every
	// other write never gets a window's answer.
	keyed    bool
	placedOn map[int]string
	// checkPlacement is set once the write routes against a collection a split
	// has touched, or meets a fence: from then on a window's answer that does
	// not match where the rows went is refused (see settleAnswered). Until
	// then the idempotent-write contract of an untouched collection holds.
	checkPlacement bool
}

func newPendingRows(numRows int) *pendingRows {
	return &pendingRows{numRows: numRows}
}

// newKeyedPendingRows is newPendingRows for a keyed insert, which checks every
// answer of an idempotency window against where its rows went.
func newKeyedPendingRows(numRows int) *pendingRows {
	p := newPendingRows(numRows)
	p.keyed = true
	return p
}

// untouched reports whether nothing has been settled yet and the per-row set
// has not been built: every row is pending.
func (p *pendingRows) untouched() bool {
	return p.rows == nil && !p.settledAll
}

// materialize builds the per-row set from the untouched state. It is a no-op
// once built, and after settleAll.
func (p *pendingRows) materialize() {
	if !p.untouched() {
		return
	}
	p.rows = newRowSet(allRowOffsets(p.numRows))
	if p.keyed {
		p.placedOn = make(map[int]string, p.numRows)
	}
}

// settleAll makes the whole write durable at once. Only an untouched write
// can be settled this way: there is no row set to empty.
func (p *pendingRows) settleAll() {
	p.settledAll = true
}

// landed is how many rows are durable.
func (p *pendingRows) landed() int {
	switch {
	case p.settledAll:
		return p.numRows
	case p.rows == nil:
		return 0
	default:
		return p.numRows - len(p.rows)
	}
}

// retain narrows a routing result to the rows still pending. A nil pendingRows
// retains everything: the repack helpers are also called outside a write loop.
// So does an untouched one, without looking at a row; a write settled whole
// retains nothing.
func (p *pendingRows) retain(channel2RowOffsets map[string][]int) map[string][]int {
	if p == nil {
		return channel2RowOffsets
	}
	return p.pendingSet().retain(channel2RowOffsets)
}

// checkPlacementFrom makes every later window answer be checked against where
// this write placed its rows (see settleAnswered). A keyed insert calls it
// once it routes against a collection a split has touched, or meets a fence.
func (p *pendingRows) checkPlacementFrom() {
	p.checkPlacement = true
}

// settleLanded settles the offsets of a message vchannel took. Only pending
// offsets are ever sent, so under placement checks an offset that is no
// longer pending was placed elsewhere by an answer of the same append: it is
// now durable twice.
func (p *pendingRows) settleLanded(vchannel string, offsets []int) error {
	p.materialize()
	for _, offset := range offsets {
		if _, ok := p.rows[offset]; !ok && p.checkPlacement {
			return merr.WrapErrServiceInternalMsg(
				"row %d was written on vchannel %s after the idempotency window of %s answered for it", offset, vchannel, p.placedOn[offset])
		}
		p.place(vchannel, offset)
	}
	return nil
}

func (p *pendingRows) place(vchannel string, offset int) {
	delete(p.rows, offset)
	if p.placedOn != nil {
		p.placedOn[offset] = vchannel
	}
}

// settleAnswered settles the offsets the idempotency window of vchannel
// answered for: the offsets the key's first append wrote there. carried are
// the offsets the answered message addressed to vchannel.
//
// On a collection no split has touched, and before this write met a fence,
// this is the idempotent-write contract as it was before splits: the message
// is answered, so its own offsets are settled with the answered ones, and a
// key reused with a same-size but different payload returns the first
// request's result (20260604-idempotent_write.md). An answered offset
// outside the request is left to the result merge, which reports the reused
// key.
//
// Once rows may have been re-routed by a fence -- by this request or by the
// earlier request the key comes from -- the answered offsets, and only those,
// are durable, and the answer must match where the rows went:
//   - an answered offset outside the request, or a carried offset the answer
//     leaves out, is a key reused with a different payload: an identical
//     retry sends each vchannel exactly the offsets the first request wrote
//     there, since auto ids stay pinned to residues (repinPendingAutoIDs).
//     Input error. Keeping such a row pending would only loop: the window
//     gives the same answer every time;
//   - an answer for a row this write placed on another vchannel, or never
//     placed, is an inconsistency of the placement across the fence: settling
//     it would call durable a row never written here, or write one twice.
//     System error.
func (p *pendingRows) settleAnswered(vchannel string, answered, carried []int) error {
	p.materialize()
	if !p.checkPlacement {
		for _, offset := range carried {
			p.place(vchannel, offset)
		}
		for _, offset := range answered {
			if offset >= 0 && offset < p.numRows {
				p.place(vchannel, offset)
			}
		}
		return nil
	}
	for _, offset := range answered {
		if offset < 0 || offset >= p.numRows {
			return errKeyReusedWithAnotherPayload(vchannel, "answered for row %d of a %d-row insert", offset, p.numRows)
		}
	}
	answeredSet := newRowSet(answered)
	for _, offset := range carried {
		if _, ok := answeredSet[offset]; !ok {
			return errKeyReusedWithAnotherPayload(vchannel, "does not answer for row %d this insert routes there", offset)
		}
	}
	for _, offset := range answered {
		if _, ok := p.rows[offset]; ok {
			p.place(vchannel, offset)
			continue
		}
		if p.placedOn == nil || p.placedOn[offset] != vchannel {
			return merr.WrapErrServiceInternalMsg(
				"the idempotency window of vchannel %s answered for row %d, which this insert placed on %q", vchannel, offset, p.placedOn[offset])
		}
	}
	return nil
}

func errKeyReusedWithAnotherPayload(vchannel string, format string, args ...any) error {
	return merr.WrapErrParameterInvalidMsg(
		"idempotency key was reused with a different payload: the idempotency window of vchannel %s %s",
		vchannel, fmt.Sprintf(format, args...))
}

// pendingSet is the set of offsets still to place, or nil -- place everything --
// for a write outside a retry loop and for an untouched one. A write settled
// whole has an empty set: the other half of an upsert may still be retried.
func (p *pendingRows) pendingSet() rowSet {
	if p == nil {
		return nil
	}
	if p.settledAll {
		return rowSet{}
	}
	return p.rows
}

// sortedOffsets returns the pending offsets in ascending order.
func (p *pendingRows) sortedOffsets() []int {
	if p.done() {
		return []int{}
	}
	if p.rows == nil {
		return allRowOffsets(p.numRows)
	}
	offsets := make([]int, 0, len(p.rows))
	for offset := range p.rows {
		offsets = append(offsets, offset)
	}
	sort.Ints(offsets)
	return offsets
}

// first is the smallest pending offset, or -1 when nothing is pending.
func (p *pendingRows) first() int {
	if p.done() {
		return -1
	}
	if p.rows == nil {
		return 0
	}
	first := -1
	for offset := range p.rows {
		if first < 0 || offset < first {
			first = offset
		}
	}
	return first
}

func (p *pendingRows) done() bool {
	if p.rows == nil {
		return p.settledAll || p.numRows == 0
	}
	return len(p.rows) == 0
}

// isShardRefusal reports whether a vchannel refused a message in a way that
// leaves nothing written and that a later attempt, routed again, can get past.
// Today that is the fence of a split source. A target that refuses DML until it
// opens must answer with an error this function accepts; nothing else in the
// retry distinguishes one refusal from another.
func isShardRefusal(err error) bool {
	if err == nil {
		return false
	}
	return status.AsStreamingError(err).IsShardFenced()
}

// splitFence is what one write has learned from its appends: which vchannels
// refused it, and the highest time tick any append reached.
type splitFence struct {
	// fenced lists the vchannels that refused this write at least once. It is
	// not used to route: see the comment at the top of this file.
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
	// staleProbes lists the vchannels whose probe answered as a vchannel that
	// no longer serves the collection, and got the refresh that normally
	// delists it (see retryProbeAnswer).
	staleProbes map[string]struct{}
	// strict is set by routedBy once an attempt routes by a residue table or
	// follows a refusal: from then on a message the append did not answer is
	// a failure, not a landed message (see settle).
	strict bool
	// A non-schema append error need not prove that shard wrote nothing.
	// A sibling schema mismatch must not invite whole-request SDK replay.
	ambiguousAppend bool
	// routedInfo is the cache entry the latest attempt routed by, until the
	// refresh that follows it (see refresh).
	routedInfo *collectionInfo
}

// routedBy records the route an attempt is about to append with.
func (f *splitFence) routedBy(route *writeRoute) {
	if route.split() || f.refused() {
		f.strict = true
	}
	f.routedInfo = route.info
}

func newSplitFence() *splitFence {
	return &splitFence{
		fenced:      make(map[string]struct{}),
		probed:      make(map[string]struct{}),
		staleProbes: make(map[string]struct{}),
	}
}

// unprobed returns, sorted, the split sources whose idempotency window has not
// answered yet: the sources the route names that may hold a pending key
// (named, with the row each probe carries, by writeRoute.sourcesToProbe), and
// the vchannels that refused this write and that the route does not list as a
// shard any more, which carry the first pending row. A vchannel that refused
// and is still a shard of the route is not a candidate: it may be a shard that
// opens later, and a probe it accepts would be written there.
func (f *splitFence) unprobed(route *writeRoute, sources map[string]int, firstPending int) ([]string, map[string]int) {
	candidates := make(map[string]int, len(sources)+len(f.fenced))
	for vchannel, row := range sources {
		candidates[vchannel] = row
	}
	for vchannel := range f.fenced {
		if _, ok := candidates[vchannel]; !ok && !route.lists(vchannel) {
			candidates[vchannel] = firstPending
		}
	}
	out := make([]string, 0, len(candidates))
	for vchannel := range candidates {
		if _, ok := f.probed[vchannel]; !ok {
			out = append(out, vchannel)
		}
	}
	sort.Strings(out)
	return out, candidates
}

func (f *splitFence) markProbed(vchannel string) {
	f.probed[vchannel] = struct{}{}
}

// refused reports whether any vchannel refused this write.
func (f *splitFence) refused() bool {
	return len(f.fenced) > 0
}

// markFenced records a vchannel that refused a write.
func (f *splitFence) markFenced(vchannel string, cause error) {
	f.fenced[vchannel] = struct{}{}
	// Retriable and a System error: the request is valid, the routing of the
	// proxy is behind the shards.
	f.refusal = merr.WrapErrServiceUnavailableErr(cause,
		"vchannel %s refused the write during a shard split and the shard that takes its keys is not writable yet", vchannel)
}

func (f *splitFence) observe(resp streaming.AppendResponses) {
	f.maxTimeTick = max(f.maxTimeTick, resp.MaxTimeTick())
}

// settle reads the responses of one append into pending, and returns the first
// error that is not a refusal (see isShardRefusal).
//
// A message that carries no error has landed and makes its own offsets durable,
// which is how the write path read an append before splits existed. A message
// with no response at all is read the same way on a collection that was never
// split and before any refusal, as master reads it; after that it fails the
// write (see splitFence.strict). The real accessor answers every message, so
// only a test double can make the difference. A message
// an idempotency window answered as a duplicate was not appended again: the
// window answers with the offsets the first append of its key wrote on that
// vchannel, and exactly those are durable (see pendingRows.settleAnswered).
//
// An error that is not a refusal is not ours to retry: the caller fails the
// request rather than replaying rows that may already be durable.
func (f *splitFence) settle(resp streaming.AppendResponses, msgs []message.MutableMessage, offsets [][]int, pending *pendingRows) error {
	f.observe(resp)
	if pending.untouched() && f.answeredWhole(resp, msgs, offsets, pending) {
		pending.settleAll()
		return nil
	}
	pending.materialize()
	var fatal error
	fail := func(err error) {
		if !status.AsStreamingError(err).IsSchemaVersionMismatch() {
			f.ambiguousAppend = true
		}
		if fatal == nil {
			fatal = err
		}
	}
	for i, msg := range msgs {
		var answer streaming.AppendResponse
		if i < len(resp.Responses) {
			answer = resp.Responses[i]
		} else if f.strict {
			// The accessor answers every message (types.NewAppendResponseN):
			// a message with no response has an unknown outcome. Once rows are
			// routed by a residue table or re-routed after a refusal, reading
			// it as landed would call a row durable that may never have been
			// written, and nothing would send it again.
			fail(merr.WrapErrServiceInternalMsg(
				"the append answered %d of %d messages; the message to vchannel %s has no response", len(resp.Responses), len(msgs), msg.VChannel()))
			break
		}
		if err := answer.Error; err != nil {
			if !isShardRefusal(err) {
				fail(err)
				continue
			}
			f.markFenced(msg.VChannel(), err)
			continue
		}
		var own []int
		if i < len(offsets) {
			own = offsets[i]
		}
		answered, duplicate, err := duplicateAnsweredOffsets(answer)
		if err != nil {
			fail(err)
			continue
		}
		if duplicate {
			err = pending.settleAnswered(msg.VChannel(), answered, own)
		} else {
			err = pending.settleLanded(msg.VChannel(), own)
		}
		if err != nil {
			fail(err)
		}
	}
	return fatal
}

// answeredWhole reports whether one append made an untouched write durable as
// a whole, so that settle need not look at a row: every message has a
// response with no error, the messages carry every row of the write, and no
// response is an idempotency window's answer that has to be checked against
// where the rows went. Anything else is settled row by row. It is O(messages).
func (f *splitFence) answeredWhole(resp streaming.AppendResponses, msgs []message.MutableMessage, offsets [][]int, pending *pendingRows) bool {
	if len(resp.Responses) < len(msgs) && f.strict {
		return false
	}
	if len(offsets) != len(msgs) {
		return false
	}
	carried := 0
	for i := range msgs {
		carried += len(offsets[i])
		if i >= len(resp.Responses) {
			continue
		}
		answer := resp.Responses[i]
		if answer.Error != nil {
			return false
		}
		if answer.AppendResult != nil && answer.AppendResult.Extra != nil {
			// A duplicate answer, or an extra settle must decode: settled one
			// by one. Decoding an answer that does not need checking costs a
			// row set only on a retried keyed insert.
			return false
		}
	}
	return carried == pending.numRows
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
// earlier attempt have landed, and with a failed answer to a keyed insert's
// probe on any attempt, since a probe writes nothing. A transient failure (a retriable Milvus error, or
// one that carries no Milvus code at all, as a transport error does) backs off
// and refreshes like a refusal, within the same deadline: failing then would
// fail a request that is already partly written. A failure no retry can cure,
// and a context that ended, end the request.
func (f *splitFence) retryPreparation(ctx context.Context, cache Cache, collectionID int64, err error) (bool, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The ambient context has already ended -- whatever err itself looks
		// like. A plain transport or library error commonly surfaces right as a
		// request's deadline lapses, not as context.DeadlineExceeded itself, so
		// this is not folded into the merr.IsCanceledOrTimeout(err) check below.
		// There is no context left to retry into, so end the request here
		// rather than falling through to refresh(), which would evict the cache
		// and ask for another attempt no context remains to make. Classify err
		// exactly as refresh() classifies its cause (an uncoded failure becomes
		// a retriable ServiceUnavailable), and fold ctxErr into the message with
		// merr.Wrapf so the reason the request stopped here -- not just err's
		// own text -- is still visible, never silently dropped.
		return false, classifyUncodedCause(merr.Wrapf(err, "shard split fence retry ended: ambient context %s", ctxErr))
	}
	if merr.IsCanceledOrTimeout(err) || (merr.IsMilvusError(err) && !merr.IsRetryableErr(err)) {
		return false, err
	}
	mlog.RatedWarn(ctx, 1, "preparing a retry after a shard split fence failed, backing off",
		mlog.FieldCollectionID(collectionID), mlog.Err(err))
	return f.refresh(ctx, cache, collectionID, err)
}

// writtenPart names one part of a write -- the rows of an insert, the
// tombstones of a delete -- for the error of a request that ended partly
// applied (see splitFence.finalError).
type writtenPart struct {
	what    string
	pending *pendingRows
}

// finalError is the error a write reports when its retry loop ends with err.
// Before splits, and on a collection no split touches before any refusal, it
// is err as it was. Otherwise it adds what the client needs to act on it:
//
//   - when the context of the request ended while the write was waiting for
//     the shards of a split, retry.Handle returns the last refusal rather than
//     the context's error; the context's reason is logged and added to the
//     message, and the refusal's code is kept;
//   - when some rows or tombstones are already durable, the message says the
//     request was partially applied and how much of it. The code is kept: the
//     error stays retriable, exactly as a multi-vchannel append that failed on
//     one vchannel is on master. A client that resends the whole request
//     writes the landed rows again (an unkeyed insert) or re-applies the
//     landed tombstones at a later tick (a delete); the message is what lets
//     it decide.
func (f *splitFence) finalError(ctx context.Context, collectionID int64, err error, parts ...writtenPart) error {
	schemaMismatch := errors.Is(err, merr.ErrCollectionSchemaMismatch)
	if err == nil || (!f.strict && !schemaMismatch) {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.IsAny(err, context.Canceled, context.DeadlineExceeded) &&
		!strings.Contains(err.Error(), ctxErr.Error()) {
		mlog.Warn(ctx, "a write waiting for the shards of a split ended with its context",
			mlog.FieldCollectionID(collectionID), mlog.Err(ctxErr), mlog.NamedError("lastRefusal", err))
		err = merr.Wrapf(err, "the write stopped waiting for the shards of a split because its context ended (%s)", ctxErr)
	}
	landed := false
	written := make([]string, 0, len(parts))
	for _, part := range parts {
		if part.pending == nil {
			continue
		}
		if part.pending.landed() > 0 {
			landed = true
		}
		written = append(written, fmt.Sprintf("%d of %d %s", part.pending.landed(), part.pending.numRows, part.what))
	}
	if schemaMismatch && (landed || f.ambiguousAppend) {
		err = merr.WrapErrCollectionMutationIncompleteErr(err)
	}
	if landed {
		err = merr.Wrapf(err, "the request was partially applied: %s are durable and stay written", strings.Join(written, " and "))
	}
	return err
}

// evictIfRoutedEntryIsCached drops the collection from the proxy's cache so
// that the next attempt describes it again -- unless the cache already holds
// another entry than the one the refused attempt routed by. Then some other
// write, or an invalidation, has refreshed it since that attempt read it, and
// evicting again would only throw away a fresher describe. N writers refused
// together thus cause about one describe per back-off interval instead of N.
// An entry that is replaced but still stale is refused again on the next
// attempt, which then evicts it, so the write still makes progress.
//
// Without a recorded route -- the attempt failed before it appended -- the
// collection is evicted as before. Only this collection, and without draining
// the proxy's in-flight fills: every attempt of every refused write gets here.
func (f *splitFence) evictIfRoutedEntryIsCached(ctx context.Context, cache Cache, collectionID int64) {
	routed := f.routedInfo
	f.routedInfo = nil
	if routed != nil {
		current, err := cache.GetCollectionInfo(ctx, "", "", collectionID)
		if err == nil && current != nil && current != routed {
			mlog.RatedDebug(ctx, 1, "the routing a refused write used was refreshed already; not evicting it again",
				mlog.FieldCollectionID(collectionID))
			return
		}
	}
	cache.RefreshCollectionByID(ctx, collectionID)
}

// classifyUncodedCause wraps cause as a retriable ServiceUnavailable when it
// carries no Milvus code of its own -- a transport error, for instance -- and
// returns an already-coded cause exactly as it is. Shared by refresh (the
// normal backoff path) and retryPreparation's already-ended-context exit, so
// every place a write's failure can reach the caller unretried classifies it
// the same way instead of one of them leaking a raw, uncoded error.
func classifyUncodedCause(cause error) error {
	if cause == nil || merr.IsMilvusError(cause) {
		return cause
	}
	return merr.WrapErrServiceUnavailableErr(cause, "shard split fence retry met an uncoded failure")
}

// refresh drops the collection from the proxy's cache, so the next attempt
// re-describes it, and asks retry.Handle for that attempt. cause is what the
// write reports if its deadline ends the retry; it defaults to the latest
// refusal.
//
// The wait is capped by proxy.shardSplit.maxFenceRetryWait, measured from the
// first refresh, also for a request with a deadline: a write that waits holds a
// slot of the DML task pool of the proxy, and a deadline far away must not let
// a split that does not complete hold those slots.
func (f *splitFence) refresh(ctx context.Context, cache Cache, collectionID int64, cause error) (bool, error) {
	if cause == nil {
		cause = f.refusal
	}
	if cause == nil {
		cause = merr.WrapErrServiceUnavailableMsg("the write left rows unplaced after a shard split fence")
	}
	// A cause with no Milvus code at all -- a transport error, as
	// retryPreparation's doc describes -- must still fail the request as a
	// retriable ServiceUnavailable if the deadline ends the retry before
	// another attempt runs (see the comment atop this file). Classifying here,
	// once, covers both places that can surface cause unwrapped: this
	// function's own no-deadline timeout below, and retry.Handle returning it
	// verbatim when the context's deadline is too close for another sleep.
	// retryPreparation's own already-ended-context exit uses the same
	// classifyUncodedCause so it reports a failure the same way.
	cause = classifyUncodedCause(cause)
	now := time.Now()
	if f.firstRefresh.IsZero() {
		f.firstRefresh = now
	}
	if maxWait := paramtable.Get().ProxyCfg.ShardSplitMaxFenceRetryWait.GetAsDurationByParse(); now.Sub(f.firstRefresh) >= maxWait {
		mlog.Warn(ctx, "write gave up waiting for the shards of a split to take its rows",
			mlog.FieldCollectionID(collectionID), mlog.Duration("maxWait", maxWait), mlog.Err(cause))
		return false, cause
	}
	f.evictIfRoutedEntryIsCached(ctx, cache, collectionID)
	mlog.RatedInfo(ctx, 1, "write not fully placed after a shard split fence, refreshing the routing and retrying",
		mlog.FieldCollectionID(collectionID), mlog.Err(cause))
	return true, cause
}
