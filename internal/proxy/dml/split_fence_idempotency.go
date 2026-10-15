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

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A keyed insert and a shard split (design doc §7 and §11, N-4).
//
// Deduplication covers exactly the inserts a client keyed explicitly: an insert
// with no `idempotency-key` is not deduplicated at all, by a split or without
// one (20260604-idempotent_write.md -- there is no global and no per-collection
// enable switch, and the proxy never derives a key from the payload). An
// unkeyed insert whose response is lost while a split is in flight and which
// the client retries therefore writes its rows twice, exactly as it would with
// no split in flight: unkeyed writes are at-least-once, across a fence as
// anywhere else. Everything below is about a KEYED insert.
//
// The idempotency window is per vchannel. A keyed insert whose first attempt
// landed on a shard that a split then fenced, and whose response was lost, is
// retried by the client; with the refreshed routing the retry would go to the
// split's targets, whose windows never saw the key, and write the rows twice.
//
// The source still knows, and that rests on three upstream facts:
//
//   - NO message type invalidates a window. The window of a vchannel is dropped
//     only when its WAL closes (idempotency_interceptor.go Close), and DDL --
//     DropCollection, TruncateCollection, DropPartition included -- neither
//     clears it nor filters the retained WALSummary records. Retained history is
//     bounded only by streaming.idempotency.maxBytesPerWindow per vchannel and
//     streaming.summary.maxBytesPerPChannel per pchannel, and is rebuilt from
//     the summary at recovery (recovery_stream.go buildIdempotencySnapshots), a
//     read failure failing the WAL open rather than yielding a partial window.
//     So the window of a fenced source survives every DDL and every restart.
//   - Nothing is written to a fenced vchannel after the fence, so its window is
//     frozen at what it held when the fence landed.
//   - The fence itself is persisted, by the shard split's record on the source's
//     VChannelMeta and by SplitFenceTimeTick / SplitFenceTaskID on
//     moduleapi.VChannelWritePathRecoveryState. A streamingnode that restarts
//     mid-split rebuilds the DoAppend gate, so a fenced vchannel keeps refusing
//     writes instead of coming back writable -- which is what lets the "listed
//     as fenced but took the probe" branch below stay an internal error rather
//     than a transient one.
//
// The streamingnode's interceptor chain consults the window BEFORE the shard
// interceptor refuses a fenced write (idempotency -> redo -> lock -> replicate
// -> timetick -> shard). A single keyed insert addressed to a fenced vchannel is
// therefore answered one of two ways:
//
//   - the window holds the key: a duplicate, answered with the row offsets and
//     ids the key's first append wrote on that vchannel;
//   - it does not: the append becomes the key's owner, the shard interceptor
//     refuses it with SHARD_FENCED, and the failed owner releases the key, so
//     nothing is written and the window is left as it was.
//
// So before a keyed insert places a row anywhere, it asks every fenced
// vchannel it knows of -- the shards the collection lists as Splitting, and any
// that refused it during this request -- with a one-row probe carrying the key.
// A duplicate answer settles exactly the offsets the window returns, whatever
// row the probe carried, and merges the first attempt's ids into the result.
// The answer names its own offsets, so the probe needs no lineage: asking a
// fenced vchannel that never saw the key costs a refusal and changes nothing.
//
// The probe must be a single message. A keyed append of several messages to one
// vchannel is a transaction whose key rides only on its commit, and the fence
// refuses the transaction's bodies before the commit reaches the window, so its
// SHARD_FENCED says nothing about the key.
//
// Coverage ends at adoption, and the limiting factor is the PROXY's routing
// view, not the window's retention: the source's window keeps answering for as
// long as the summary retains it (4 GB per pchannel by default), but once the
// adoption delists the source the proxy no longer learns its name from the
// collection's meta and never asks again, so a retry of a write acked on it is
// written again on its targets. That residual hole is accepted (design doc §11,
// N-4); the window's retention bound is not what closes it.

// probeAnswerError is an answer to a probe that is neither a duplicate nor the
// fence: a transport failure, a streamingnode fail-over, or a source the
// split's adoption already retired and that no longer knows the collection.
// Whatever it is, the probe wrote nothing, so unlike a failure preparing an
// ordinary first attempt it never fails the request by itself: the write backs
// off and refreshes its routing, as a retry does (see
// splitFence.retryPreparation), and only a failure no retry can cure ends it.
type probeAnswerError struct {
	vchannel string
	cause    error
}

func (e *probeAnswerError) Error() string {
	return fmt.Sprintf("idempotency probe of vchannel %s: %s", e.vchannel, e.cause)
}

func (e *probeAnswerError) Unwrap() error {
	return e.cause
}

// probeAnswerRemedy is what a failed probe answer calls for.
type probeAnswerRemedy int

const (
	// probeAnswerTransient is a failure of the moment -- a transport error, a
	// streamingnode fail-over or shutdown, throttling: back off and ask again,
	// within the request's deadline.
	probeAnswerTransient probeAnswerRemedy = iota
	// probeAnswerStaleRoute is a vchannel that no longer serves the
	// collection, as a source the split's adoption retired answers: a refresh
	// of the routing, which then no longer lists it, cures it.
	probeAnswerStaleRoute
	// probeAnswerPermanent is an answer no refresh or retry changes.
	probeAnswerPermanent
)

// classifyProbeAnswer tells what a failed probe answer calls for.
func classifyProbeAnswer(err error) probeAnswerRemedy {
	if merr.IsMilvusError(err) {
		if merr.IsRetryableErr(err) {
			return probeAnswerTransient
		}
		return probeAnswerPermanent
	}
	switch status.AsStreamingError(err).Code {
	case streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE:
		// The redo interceptor's answer for a vchannel whose collection
		// registration is gone.
		return probeAnswerStaleRoute
	case streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
		streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED,
		streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM,
		streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN,
		streamingpb.StreamingCode_STREAMING_CODE_INNER,
		streamingpb.StreamingCode_STREAMING_CODE_RESOURCE_ACQUIRED,
		streamingpb.StreamingCode_STREAMING_CODE_RATE_LIMIT_REJECTED,
		streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN:
		// UNKNOWN is also what an error with no streaming code -- a transport
		// failure, an ended context -- reads as.
		//
		// RATE_LIMIT_REJECTED is also the WAL's recovery-tail reject mode
		// (wal_adaptor.go, streaming.walRecovery.tail.highWatermark): it is
		// decided AHEAD of the interceptor chain, so a refused probe never
		// reached the window and asking again is sound. It can persist for
		// minutes under a large backlog, which is the one answer that can
		// exhaust proxy.shardSplit.maxFenceRetryWait on a healthy split.
		return probeAnswerTransient
	default:
		return probeAnswerPermanent
	}
}

// retryProbeAnswer decides what a keyed insert does with a failed probe
// answer. The probe wrote nothing, so a failure a refresh or a later attempt
// can cure backs off and refreshes like a retry, on any attempt (see
// splitFence.retryPreparation). A stale route is worth one refresh per
// vchannel: when the refreshed routing still lists the vchannel and it
// answers the same way, no refresh will help. Anything else ends the request
// with the answer itself, classified as any failed append is.
func (f *splitFence) retryProbeAnswer(ctx context.Context, cache Cache, collectionID int64, answer *probeAnswerError) (bool, error) {
	switch classifyProbeAnswer(answer.cause) {
	case probeAnswerPermanent:
		return false, answer.cause
	case probeAnswerStaleRoute:
		if _, again := f.staleProbes[answer.vchannel]; again {
			return false, answer.cause
		}
		f.staleProbes[answer.vchannel] = struct{}{}
	}
	return f.retryPreparation(ctx, cache, collectionID, answer.cause)
}

// probeFencedWindows asks the idempotency window of every fenced vchannel not
// asked yet for this insert's key, and settles the rows they answer for. It
// returns the first error merging an answer into the result (a key reused with
// a different payload) -- also when it fails -- and the first error that fails
// the request.
func (it *InsertTask) probeFencedWindows(
	ctx context.Context,
	route *writeRoute,
	fence *splitFence,
	pending *pendingRows,
	idempotency *insertIdempotencyDecoration,
	ez *message.CipherConfig,
) (mergeErr error, err error) {
	vchannels := fence.unprobed(route.fenced)
	if len(vchannels) == 0 || pending.done() {
		return nil, nil
	}
	row := pending.first()
	partitionID, partitionName, err := it.partitionOfRow(ctx, row)
	if err != nil {
		return nil, err
	}
	probes := make([]message.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		msgs, err := buildSingleInsertMessageForStreamingService(partitionID, partitionName, []int{row}, vchannel,
			it.insertMsg, ez, it.schemaVersion, nil, idempotency)
		if err != nil {
			return nil, err
		}
		probes = append(probes, msgs...)
	}

	resp := streaming.WAL().AppendMessagesWithOptions(ctx, probes, streaming.AppendOption{IdempotencyKey: it.idempotencyKey})
	fence.observe(resp)
	for i, probe := range probes {
		if i >= len(resp.Responses) {
			return mergeErr, merr.WrapErrServiceInternalMsg("no response to the idempotency probe of vchannel %s", probe.VChannel())
		}
		answer := resp.Responses[i]
		if answer.Error != nil {
			if !status.AsStreamingError(answer.Error).IsShardFenced() {
				return mergeErr, &probeAnswerError{vchannel: probe.VChannel(), cause: answer.Error}
			}
			// The window does not hold the key, and never will: the vchannel
			// takes no write any more.
			fence.markFenced(probe.VChannel())
			fence.markProbed(probe.VChannel())
			continue
		}
		fence.markProbed(probe.VChannel())
		offsets, duplicate, err := duplicateAnsweredOffsets(answer)
		if err != nil {
			return mergeErr, err
		}
		if !duplicate {
			// The probe was appended: the vchannel took a write although the
			// collection lists it as fenced. A Splitting shard is fenced from the
			// fence on and never unfenced, so this is a Milvus bug; placing the
			// other rows on the targets could write them next to data the
			// source still takes. Fail the request instead.
			mlog.Error(ctx, "idempotency probe of a vchannel listed as fenced was appended",
				mlog.FieldVChannel(probe.VChannel()), mlog.Int("row", row))
			return mergeErr, merr.WrapErrServiceInternalMsg(
				"vchannel %s is listed as fenced by a shard split but took a write", probe.VChannel())
		}
		mlog.RatedInfo(ctx, 1, "a fenced vchannel answered a keyed insert from its idempotency window",
			mlog.FieldVChannel(probe.VChannel()), mlog.Int("rows", len(offsets)))
		if err := pending.settleAnswered(probe.VChannel(), offsets, nil); err != nil {
			return mergeErr, err
		}
		// Merge each answer as it is settled: a later probe of this batch may
		// fail, and the rows settled here are never asked for again, so the
		// ids reported for them must be the stored ones from now on.
		if err := mergeDuplicateInsertResults(it.result, streaming.AppendResponses{
			Responses: []streaming.AppendResponse{answer},
		}); err != nil && mergeErr == nil {
			mergeErr = err
		}
	}
	return mergeErr, nil
}

// partitionOfRow returns the partition a row of this insert is written to.
func (it *InsertTask) partitionOfRow(ctx context.Context, row int) (int64, string, error) {
	metaCache := it.GetMetaCache()
	dbName, collectionName := it.insertMsg.GetDbName(), it.insertMsg.GetCollectionName()
	partitionName := it.insertMsg.GetPartitionName()
	if it.partitionKeys != nil {
		partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, metaCache, dbName, collectionName)
		if err != nil {
			return 0, "", err
		}
		hashValues, err := typeutil.HashKey2Partitions(it.partitionKeys, partitionNames)
		if err != nil {
			return 0, "", err
		}
		if row < 0 || row >= len(hashValues) {
			return 0, "", merr.WrapErrServiceInternalMsg("row %d has no partition key among %d", row, len(hashValues))
		}
		partitionName = partitionNames[hashValues[row]]
	}
	partitionID, err := metaCache.GetPartitionID(ctx, dbName, collectionName, partitionName)
	if err != nil {
		return 0, "", err
	}
	return partitionID, partitionName, nil
}
