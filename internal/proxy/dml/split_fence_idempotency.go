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
	"regexp"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A keyed insert and a shard split.
//
// Deduplication covers exactly the inserts a client keyed explicitly: an insert
// with no `idempotency-key` is not deduplicated at all, by a split or without
// one (20260604-idempotent_write.md). An unkeyed insert whose response is lost
// while a split is in flight and which the client retries therefore writes its
// rows twice, exactly as it would with no split in flight: unkeyed writes are
// at-least-once, across a fence as anywhere else. Everything below is about a
// KEYED insert.
//
// The idempotency window is per vchannel. A keyed insert whose first attempt
// landed on a shard that a split then fenced, and whose response was lost, is
// retried by the client; routed by the new table the retry goes to the targets
// of the split, whose windows never saw the key, and would write the rows
// twice.
//
// The source still knows. No message type invalidates a window; nothing is
// written to a fenced vchannel after the fence, so its window is frozen at what
// it held when the fence landed; and the fence is persisted, so a StreamingNode
// that restarts keeps refusing DML on the source.
//
// The interceptor chain of the StreamingNode consults the window BEFORE the
// shard interceptor refuses a fenced write (idempotency -> redo -> lock ->
// replicate -> timetick -> shard, walmanager/manager_impl.go). A single keyed
// insert addressed to a fenced vchannel is therefore answered one of two ways:
//
//   - the window holds the key: a duplicate, answered with the row offsets and
//     ids the first append of the key wrote on that vchannel;
//   - it does not: the append becomes the owner of the key, the shard
//     interceptor refuses it with SHARD_FENCED, and the failed owner releases
//     the key, so nothing is written and the window is left as it was.
//
// So before a keyed insert places a row on the targets, it asks every split
// source it knows of with a one-row probe carrying the key. A duplicate answer
// settles exactly the offsets the window returns, whatever row the probe
// carried, and merges the ids of the first attempt into the result.
//
// The probe must be a single message. A keyed append of several messages to one
// vchannel is a transaction whose key rides only on its commit, and the fence
// refuses the bodies of the transaction before the commit reaches the window,
// so its SHARD_FENCED says nothing about the key.
//
// Which vchannels are split sources. Two ways, both used:
//
//   - The describe of the collection names them. RootCoord reports every
//     split source of the collection as a shard info in state ShardSplitting,
//     next to the shards that take DML and outside the vchannel list
//     (metacache.SplitRouting.Sources); a chain of splits lists every source of
//     the chain. Such a source never takes part in the routing. When its shard
//     info names the residues it owned, it is asked only for a pending key of
//     those residues (writeRoute.sourcesToProbe); otherwise always. This is
//     what covers a retry through a proxy that already routes by the new
//     table: it never addresses the source for a write, but asks it.
//   - A vchannel that refused this write and that the refreshed routing does
//     not list any more is a split source (a vchannel leaves the routing only
//     by being split). This covers a proxy whose describe does not name the
//     source yet. A vchannel that refused and is still listed is sent its rows
//     again instead; it may be a target that has not opened yet, and a probe it
//     accepted would write the probe row there.
//
// A source whose StreamingNode no longer knows the collection -- retired on
// this cluster, or its collection state gone -- has no window left either, and
// counts as asked and not found (see isCollectionGone).
//
// Cost. While RootCoord lists a source, every keyed insert into the collection
// whose keys the source may hold sends it one one-row probe more, in an append
// of its own before its rows. Unkeyed inserts, deletes and upserts never ask.

// probeAnswerError is an answer to a probe that is neither a duplicate, nor the
// fence, nor a source that no longer knows the collection: a transport
// failure, a streamingnode fail-over, another unrecoverable answer.
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
	// probeAnswerStaleRoute is any other unrecoverable answer: the refreshed
	// routing may stop naming the vchannel. One more attempt is made; when it
	// answers the same way the request ends.
	probeAnswerStaleRoute
	// probeAnswerPermanent is an answer no refresh or retry changes.
	probeAnswerPermanent
	// probeAnswerSourceGone is the answer of a source whose StreamingNode no
	// longer knows the collection -- it was retired, or the collection was
	// dropped: its window is gone with it, so the probe counts as asked and
	// not found (see isCollectionGone).
	probeAnswerSourceGone
)

// collectionNotFoundCause matches the causes the StreamingNode gives an insert
// to a vchannel whose pchannel does not know the collection (master, the shard
// interceptor and the redo interceptor): "collection %d not found", "fail to
// wait growing segment ready, collection not found", "fail to assign segment,
// collection not found". All come from shards.ErrCollectionNotFound and are
// wrapped as STREAMING_CODE_UNRECOVERABLE with the text alone; no code tells
// them apart from the other unrecoverable answers.
var collectionNotFoundCause = regexp.MustCompile(`\bcollection (\d+ )?not found\b`)

// isCollectionGone reports whether a probe answer says that the vchannel's
// StreamingNode does not know the collection at all.
//
// It cannot hide an outage. The idempotency interceptor runs before the shard
// interceptor, so a source that still holds the key answers from its window
// whatever else it knows. A StreamingNode that is down, moving, or recovering
// answers with a transport error, CHANNEL_NOT_EXIST, ON_SHUTDOWN or
// UNMATCHED_CHANNEL_TERM, which stay transient; it accepts appends only once
// its shard manager is recovered, and a collection a source served is in that
// recovery until the collection is dropped there. A partition that is not
// found is not this answer either.
func isCollectionGone(err error) bool {
	se := status.AsStreamingError(err)
	return se.Code == streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE &&
		collectionNotFoundCause.MatchString(se.Cause)
}

// classifyProbeAnswer tells what a failed probe answer calls for.
func classifyProbeAnswer(err error) probeAnswerRemedy {
	if merr.IsMilvusError(err) {
		if merr.IsRetryableErr(err) {
			return probeAnswerTransient
		}
		return probeAnswerPermanent
	}
	if isCollectionGone(err) {
		return probeAnswerSourceGone
	}
	switch status.AsStreamingError(err).Code {
	case streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE:
		// Not a collection that is gone (isCollectionGone is asked first):
		// a partition that is gone, a schema the source cannot apply, ...
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
// splitFence.retryPreparation). A stale route is worth one more attempt per
// vchannel: when it answers the same way again, no retry will help. Anything
// else ends the request with the answer, coded (see probeFailure).
func (f *splitFence) retryProbeAnswer(ctx context.Context, cache Cache, collectionID int64, answer *probeAnswerError) (bool, error) {
	switch classifyProbeAnswer(answer.cause) {
	case probeAnswerPermanent:
		return false, probeFailure(answer)
	case probeAnswerStaleRoute:
		if _, again := f.staleProbes[answer.vchannel]; again {
			return false, probeFailure(answer)
		}
		f.staleProbes[answer.vchannel] = struct{}{}
	}
	return f.retryPreparation(ctx, cache, collectionID, answer.cause)
}

// probeFailure is the error a probe answer no retry cures ends the request
// with. A Milvus error keeps its code. A StreamingError carries none, and the
// client would see it as Code 65535: it is a failure inside Milvus -- the
// probe is built by the proxy, and the source is a vchannel of the collection
// -- so it becomes ServiceInternal, not retriable. The chain is kept, so the
// write path still maps a schema version mismatch to
// ErrCollectionSchemaMismatch, as it does for an append.
func probeFailure(answer *probeAnswerError) error {
	if merr.IsMilvusError(answer.cause) {
		return answer.cause
	}
	return merr.WrapErrServiceInternalErr(answer, "a keyed insert could not ask a split source whether it holds its key")
}

// probeFencedWindows asks the idempotency window of every split source not
// asked yet (see splitFence.unprobed) for this insert's key, and settles the rows they answer for. It
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
	if pending.done() {
		return nil, nil
	}
	sources, err := route.sourcesToProbe(it.result.GetIDs(), pending)
	if err != nil {
		return nil, err
	}
	vchannels, rows := fence.unprobed(route, sources, pending.first())
	if len(vchannels) == 0 {
		return nil, nil
	}
	probes := make([]message.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		row := rows[vchannel]
		partitionID, partitionName, err := it.partitionOfRow(ctx, row)
		if err != nil {
			return nil, err
		}
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
			switch {
			case isShardRefusal(answer.Error):
				// The window does not hold the key, and never will: the
				// vchannel takes no write any more.
			case classifyProbeAnswer(answer.Error) == probeAnswerSourceGone:
				// The source no longer serves the collection, so its window
				// is gone too: asked, and not found.
				mlog.RatedInfo(ctx, 1, "a split source no longer knows the collection; its window holds nothing",
					mlog.FieldVChannel(probe.VChannel()), mlog.Err(answer.Error))
			default:
				return mergeErr, &probeAnswerError{vchannel: probe.VChannel(), cause: answer.Error}
			}
			fence.markProbed(probe.VChannel())
			continue
		}
		fence.markProbed(probe.VChannel())
		offsets, duplicate, err := duplicateAnsweredOffsets(answer)
		if err != nil {
			return mergeErr, err
		}
		if !duplicate {
			// The probe was appended: a split source -- named by the describe,
			// or a vchannel that refused this write and that the routing does
			// not list any more -- took a row. The fence of a split source is
			// never lifted, so this is a Milvus bug. The probe row
			// is now on a vchannel that does not own it; fail the request
			// instead of placing the other rows as if nothing happened.
			mlog.Error(ctx, "idempotency probe of a split source was appended",
				mlog.FieldVChannel(probe.VChannel()), mlog.Int("row", rows[probe.VChannel()]))
			return mergeErr, merr.WrapErrServiceInternalMsg(
				"vchannel %s is a split source but took a write", probe.VChannel())
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
