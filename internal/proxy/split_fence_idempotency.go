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

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A keyed insert and a shard split (design doc §7 and §11, N-4).
//
// The idempotency window is per vchannel. A keyed insert whose first attempt
// landed on a shard that a split then fenced, and whose response was lost, is
// retried by the client; with the refreshed routing the retry would go to the
// split's targets, whose windows never saw the key, and write the rows twice.
//
// The source still knows. The fence does not invalidate its window
// (InvalidatesIdempotencyWindow lists only DropCollection, TruncateCollection
// and DropPartition), nothing is written to a fenced vchannel afterwards, so the
// window is frozen, and the streamingnode's interceptor chain consults the
// window BEFORE the shard interceptor refuses a fenced write (idempotency ->
// redo -> lock -> replicate -> timetick -> shard). A single keyed insert
// addressed to a fenced vchannel is therefore answered one of two ways:
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
// Coverage ends at adoption: once the source is delisted, the proxy no longer
// learns its name from the collection's meta, and a retry of a write acked on
// it is written again on its targets.

// probeFencedWindows asks the idempotency window of every fenced vchannel not
// asked yet for this insert's key, and settles the rows they answer for. It
// returns the first error merging an answer into the result (a key reused with
// a different payload), and the first error that fails the request.
func (it *insertTask) probeFencedWindows(
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
			return nil, merr.WrapErrServiceInternalMsg("no response to the idempotency probe of vchannel %s", probe.VChannel())
		}
		answer := resp.Responses[i]
		if answer.Error != nil {
			if !status.AsStreamingError(answer.Error).IsShardFenced() {
				return nil, answer.Error
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
			return nil, err
		}
		if !duplicate {
			// The probe was appended: the vchannel took a write although the
			// collection lists it as fenced. A Splitting shard is fenced from the
			// fence on and never unfenced, so this is a Milvus bug; placing the
			// other rows on the targets could write them next to data the
			// source still takes. Fail the request instead.
			mlog.Error(ctx, "idempotency probe of a vchannel listed as fenced was appended",
				mlog.FieldVChannel(probe.VChannel()), mlog.Int("row", row))
			return nil, merr.WrapErrServiceInternalMsg(
				"vchannel %s is listed as fenced by a shard split but took a write", probe.VChannel())
		}
		mlog.RatedInfo(ctx, 1, "a fenced vchannel answered a keyed insert from its idempotency window",
			mlog.FieldVChannel(probe.VChannel()), mlog.Int("rows", len(offsets)))
		pending.settle(offsets)
	}
	if err := mergeDuplicateInsertResults(it.result, resp); err != nil {
		mergeErr = err
	}
	return mergeErr, nil
}

// partitionOfRow returns the partition a row of this insert is written to.
func (it *insertTask) partitionOfRow(ctx context.Context, row int) (int64, string, error) {
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
