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
	"slices"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	streamingmessage "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// These tests state the contract of the write path against a shard split as
// the design has it (20261109-shard_split_on_queryview.md, 8.1): the routing
// table of a split collection lists only the shards that take DML, a key goes
// to the shard that owns hash(pk) mod modulus, and a collection that was never
// split is written exactly as before.

// At modulus 2 a key belongs to the target that owns the residue of its hash,
// for an int64 and for a varchar primary key.
func TestPKChannelIndexesAtModulusTwo(t *testing.T) {
	_, post := oneShardSplit()
	owners := []string{splitTarget0, splitTarget1}

	ints := make([]int64, 512)
	strs := make([]string, 512)
	for i := range ints {
		ints[i] = int64(i*104729 - 20000)
		strs[i] = fmt.Sprintf("key-%d", i*7)
	}
	indexes, err := pkChannelIndexes(post.SplitRouting.Table, int64IDs(ints...), post.VChannels)
	require.NoError(t, err)
	seen := map[string]int{}
	for i, pk := range ints {
		hash, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		assert.Equal(t, owners[hash%2], post.VChannels[indexes[i]], "pk %d", pk)
		seen[post.VChannels[indexes[i]]]++
	}
	assert.Len(t, seen, 2, "both targets take keys")

	indexes, err = pkChannelIndexes(post.SplitRouting.Table, strIDs(strs...), post.VChannels)
	require.NoError(t, err)
	for i, pk := range strs {
		assert.Equal(t, owners[typeutil.HashString2Uint32(pk)%2], post.VChannels[indexes[i]], "pk %s", pk)
	}
}

// A table with an unsplit shard next to a split pair: the keys of the unsplit
// shard stay where the legacy modulo put them, and the keys of the source go to
// the target that owns their residue at the doubled modulus.
func TestPKChannelIndexesWithAnUnsplitShardNextToASplitPair(t *testing.T) {
	pre, post := twoShardSplit()
	pks := make([]int64, 1024)
	for i := range pks {
		pks[i] = int64(i*7919 + 3)
	}
	before, err := pkChannelIndexes(nil, int64IDs(pks...), pre.VChannels)
	require.NoError(t, err)
	after, err := pkChannelIndexes(post.SplitRouting.Table, int64IDs(pks...), post.VChannels)
	require.NoError(t, err)

	moved := map[string]int{}
	for i, pk := range pks {
		was, is := pre.VChannels[before[i]], post.VChannels[after[i]]
		hash, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		switch hash % 4 {
		case 0:
			assert.Equal(t, splitSource, was)
			assert.Equal(t, splitTarget0, is, "pk %d", pk)
		case 2:
			assert.Equal(t, splitSource, was)
			assert.Equal(t, splitTarget1, is, "pk %d", pk)
		default:
			assert.Equal(t, splitSibling, was)
			assert.Equal(t, splitSibling, is, "no key of the unsplit shard moves, pk %d", pk)
		}
		moved[is]++
	}
	assert.Len(t, moved, 3)
}

// Both halves of an upserted row stay together across a refusal: the insert
// and the tombstone of a key land on the same shard, the one that owns the key
// after the split, in one transaction and so at one time tick. Nothing of the
// row is left on any other shard.
func TestUpsertHalvesOfARowStayTogetherAcrossARefusal(t *testing.T) {
	for name, staleRefreshes := range map[string]int{"the new routing is visible at once": 0, "the new routing is visible after two refreshes": 2} {
		t.Run(name, func(t *testing.T) {
			useSingleMessageRepack(t)
			pre, post := twoShardSplit()
			f := newSplitFenceFixture(t, pre, post)
			f.staleRefreshes = staleRefreshes
			wal := installSplitFenceTestWAL(t, splitSource)
			pks := seqPKs(48)
			task := f.upsertTask(t, pks, pks)

			require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

			sawSibling, sawTarget := false, false
			for _, pk := range pks {
				owner := splitOwner(t, post, pk)
				sawSibling = sawSibling || owner == splitSibling
				sawTarget = sawTarget || owner != splitSibling
				for _, vchannel := range []string{splitSource, splitSibling, splitTarget0, splitTarget1} {
					if vchannel != owner {
						assert.Empty(t, wal.insertTicks[vchannel][pk], "insert of pk %d on %s", pk, vchannel)
						assert.Empty(t, wal.tombstoneTicks[vchannel][pk], "tombstone of pk %d on %s", pk, vchannel)
					}
				}
				require.Len(t, wal.insertTicks[owner][pk], 1, "pk %d is inserted once", pk)
				require.Len(t, wal.tombstoneTicks[owner][pk], 1, "pk %d is tombstoned once", pk)
				assert.Equal(t, wal.insertTicks[owner][pk][0], wal.tombstoneTicks[owner][pk][0], "both halves of pk %d share a time tick", pk)
			}
			require.True(t, sawSibling && sawTarget, "the batch spans a shard that committed and a shard that refused")
			wal.assertLiveOnTheirOwner(t, post, pks)
			wal.assertNoAppendToTheSourceAfterItRefused(t)
			assert.Equal(t, staleRefreshes+1, f.evictions)
		})
	}
}

// An upsert whose retry first fails to pack its messages, for a reason that
// passes, and then succeeds reports success: the error the failed attempt left
// in the result does not stay behind.
func TestUpsertThatSucceedsAfterAFailedRetryReportsSuccess(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.onEvict = func(eviction int) {
		switch eviction {
		case 1:
			f.partitionErr = merr.WrapErrServiceUnavailable("partition lookup unavailable")
		case 2:
			f.partitionErr = nil
		}
	}
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)
	task := f.upsertTask(t, pks, pks)
	task.result.Status = merr.Success()

	require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

	assert.Equal(t, 2, f.evictions, "the refusal, then the failed pack")
	assert.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 101)
	assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
}

// A target of a split may refuse DML for a while before it opens. Its rows are
// sent to it again, land once when it opens, and the rows another shard took
// in the meantime are not sent again. Nothing else is written to the target
// that refused: a keyed insert does not probe a vchannel that is still a shard.
func TestWritesWaitForATargetThatRefusesBeforeItOpens(t *testing.T) {
	type write struct {
		run    func(t *testing.T, f *splitFenceFixture, pks []int64) error
		verify func(t *testing.T, wal *splitFenceTestWAL, post *collectionInfo, pks []int64)
	}
	rowsOnce := func(firstRowID int64) func(*testing.T, *splitFenceTestWAL, *collectionInfo, []int64) {
		return func(t *testing.T, wal *splitFenceTestWAL, post *collectionInfo, pks []int64) {
			assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, firstRowID)
		}
	}
	tombstonesOnce := func(t *testing.T, wal *splitFenceTestWAL, post *collectionInfo, pks []int64) {
		assertTombstonesLandedOnTheirOwner(t, wal, post, pks)
		for vchannel, deleted := range wal.deletedPKs {
			seen := map[int64]int{}
			for _, pk := range deleted {
				seen[pk]++
				assert.Equal(t, 1, seen[pk], "tombstone of pk %d was sent twice to %s", pk, vchannel)
			}
		}
	}
	for name, w := range map[string]write{
		"insert": {
			run: func(t *testing.T, f *splitFenceFixture, pks []int64) error {
				task := f.insertTask(pks)
				err := task.Execute(context.Background())
				require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
				return err
			},
			verify: rowsOnce(1),
		},
		"keyed insert": {
			run: func(t *testing.T, f *splitFenceFixture, pks []int64) error {
				task := f.keyedInsertTask(pks, "key")
				err := task.Execute(context.Background())
				require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
				return err
			},
			verify: rowsOnce(1),
		},
		"delete": {
			run: func(t *testing.T, f *splitFenceFixture, pks []int64) error {
				return f.deleteTask(pks).Execute(context.Background())
			},
			verify: tombstonesOnce,
		},
		"upsert": {
			run: func(t *testing.T, f *splitFenceFixture, pks []int64) error {
				return f.upsertTask(t, pks, pks).appendUpsertAttempt(context.Background(), nil)
			},
			verify: func(t *testing.T, wal *splitFenceTestWAL, post *collectionInfo, pks []int64) {
				rowsOnce(101)(t, wal, post, pks)
				tombstonesOnce(t, wal, post, pks)
				wal.assertLiveOnTheirOwner(t, post, pks)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			useSingleMessageRepack(t)
			_, post := twoShardSplit()
			// The proxy routes by the new table from the start.
			f := newSplitFenceFixture(t, post, post)
			wal := installSplitFenceTestWAL(t, splitTarget0)
			f.onEvict = func(eviction int) {
				if eviction == 2 {
					delete(wal.fenced, splitTarget0)
				}
			}
			pks := seqPKs(48)

			require.NoError(t, w.run(t, f, pks))

			require.Len(t, wal.batches, 3, "the target refused twice, then took its rows")
			assert.Contains(t, wal.vchannelsOf(0), splitTarget0)
			assert.Equal(t, []string{splitTarget0}, wal.vchannelsOf(1), "only the refused rows are sent again")
			assert.Equal(t, []string{splitTarget0}, wal.vchannelsOf(2))
			assert.Equal(t, 2, f.evictions)
			w.verify(t, wal, post, pks)
		})
	}
}

// A keyed insert that the proxy sends to the source as a transaction, with the
// routing from before the split, after its first attempt landed there and the
// response was lost. The fence refuses the bodies of the transaction before its
// commit reaches the idempotency window, so the refusal says nothing about the
// key. The proxy learns the source from the refusal; once the refreshed routing
// does not list it any more, it asks its window with a single message, and the
// rows are not written again on the targets.
func TestKeyedInsertRefusedAsATransactionIsAnsweredByTheWindowOfTheSource(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"_default_0": 300, "_default_1": 301}, nil).Maybe()
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(16)
	keyed := func() *InsertTask {
		task := f.keyedInsertTask(pks, "key")
		task.partitionKeys = partialUpdateCASPKFieldData(pks)
		return task
	}

	first := keyed()
	require.NoError(t, first.Execute(context.Background()))
	require.True(t, merr.Ok(first.result.GetStatus()), first.result.GetStatus().GetReason())
	require.Len(t, wal.batches, 1)
	require.Greater(t, len(wal.batches[0]), 1, "the insert is a transaction of one message per partition")
	require.Len(t, wal.insertedRowIDs[splitSource], len(pks))

	// The split fences the source. The proxy still routes by the old table.
	wal.fenced[splitSource] = struct{}{}

	retry := keyed()
	require.NoError(t, retry.Execute(context.Background()))
	require.True(t, merr.Ok(retry.result.GetStatus()), retry.result.GetStatus().GetReason())

	require.Len(t, wal.batches, 3, "the first attempt, the refused transaction, the probe")
	assert.Greater(t, len(wal.batches[1]), 1, "the retry was refused as a transaction")
	require.Len(t, wal.batches[2], 1, "the probe is a single message")
	assert.Equal(t, splitSource, wal.batches[2][0].VChannel())
	assert.Equal(t, 1, f.evictions)
	assert.Empty(t, wal.insertedRowIDs[splitTarget0], "the retry wrote nothing on a target")
	assert.Empty(t, wal.insertedRowIDs[splitTarget1], "the retry wrote nothing on a target")
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
}

// A keyed insert that landed on the source before the fence, and whose
// response was lost, is retried by the client through a proxy that already
// routes by the new table. That proxy never addresses the source for a write,
// but the describe names it as a split source (a shard info in state
// ShardSplitting), so the insert asks the source's window before it writes a
// target: the source answers for every row, and nothing is written twice.
func TestKeyedInsertRetriedThroughAProxyThatAlreadyRoutesByTheTargetsIsAnsweredByTheSource(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := withLineage(oneShardSplit())
	require.Len(t, post.SplitRouting.Sources, 1)
	require.NotContains(t, post.VChannels, splitSource, "the source takes no part in the routing")
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(8)

	first := runKeyedInsert(t, f, pks, "key", 0)
	require.Len(t, wal.insertedRowIDs[splitSource], len(pks))

	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := runKeyedInsert(t, f, pks, "key", 0)
	require.Len(t, wal.batches, 2, "the retry sent only the probe")
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(1))
	assert.Empty(t, wal.insertedRowIDs[splitTarget0], "the rows are not written again")
	assert.Empty(t, wal.insertedRowIDs[splitTarget1], "the rows are not written again")
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
	assert.Zero(t, f.evictions)
}

// A keyed insert new to the collection asks the listed source too; the source,
// fenced, holds no such key, and the rows go to the targets after one probe.
func TestKeyedInsertNewToASplitCollectionAsksTheSourceOnce(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := withLineage(oneShardSplit())
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(8)

	runKeyedInsert(t, f, pks, "key", 0)
	require.Len(t, wal.batches, 2, "one probe, then the rows")
	assert.Equal(t, []string{splitSource}, wal.vchannelsOf(0))
	assertRowsLandedOnceOnTheirOwner(t, wal, post, pks, 1)
}

// An unkeyed insert has no window to ask: a listed source costs it nothing.
func TestUnkeyedInsertDoesNotAskTheSplitSources(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := withLineage(oneShardSplit())
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(8)
	task := f.insertTask(pks)

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	require.Len(t, wal.batches, 1)
	assert.NotContains(t, wal.vchannelsOf(0), splitSource)
}

// A partial update that a shard split refuses on one shard after another shard
// committed it. Sending the whole request again is safe only when every
// operation is a REPLACE; a relative operation would be applied twice on the
// shard that committed, so the request ends as a conflict the client must not
// retry blindly.
func TestPartialUpdateRefusedOnOneShardAfterAnotherCommitted(t *testing.T) {
	both := func() map[string]*messagespb.PartialUpdateCAS {
		return map[string]*messagespb.PartialUpdateCAS{
			splitSource:  {ReadTs: 10, ObservedPchannelTerm: 1},
			splitSibling: {ReadTs: 10, ObservedPchannelTerm: 1},
		}
	}
	newTask := func(t *testing.T) (*UpsertTask, *splitFenceFixture, *splitFenceTestWAL) {
		useSingleMessageRepack(t)
		pre, post := twoShardSplit()
		f := newSplitFenceFixture(t, pre, post)
		wal := installSplitFenceTestWAL(t, splitSource)
		pks := seqPKs(16)
		task := f.upsertTask(t, pks, pks)
		task.req.PartialUpdate = true
		task.partialUpdateCASGroups = both()
		return task, f, wal
	}

	t.Run("replace only", func(t *testing.T) {
		task, f, wal := newTask(t)
		err := task.appendUpsertAttempt(context.Background(), nil)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		assert.True(t, merr.IsRetryableErr(err))
		assert.NotEmpty(t, wal.insertedRowIDs[splitSibling], "the sibling committed")
		assert.Len(t, wal.batches, 1, "a partial update is not routed again")
		assert.Equal(t, 1, f.evictions)
	})
	t.Run("a relative operation", func(t *testing.T) {
		task, f, wal := newTask(t)
		task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{FieldName: "name", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND}}
		err := task.appendUpsertAttempt(context.Background(), nil)
		assert.ErrorIs(t, err, merr.ErrCollectionPartialUpdateConflict)
		assert.False(t, merr.IsRetryableErr(err))
		assert.NotEmpty(t, wal.insertedRowIDs[splitSibling], "the sibling committed")
		assert.Len(t, wal.batches, 1)
		assert.Equal(t, 1, f.evictions)
	})
	t.Run("a relative operation that committed nowhere", func(t *testing.T) {
		task, _, _ := newTask(t)
		task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{FieldName: "name", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND}}
		err := task.partialUpdateFenceRefusal(context.Background(), appendResponses(fencedErr(splitSource), fencedErr(splitSource)))
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		assert.True(t, merr.IsRetryableErr(err))
	})
}

// assertNamespaceRefusal checks the refusal of a write by namespace on a split
// collection: unsupported, a System error, not retriable.
func assertNamespaceRefusal(t *testing.T, err error) {
	t.Helper()
	assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
	assert.False(t, merr.IsRetryableErr(err))
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

// A collection that shards by namespace must not be split. If its metadata
// says it was, a write that places rows by namespace is refused instead of
// being placed by the position of the namespace in the channel list.
func TestNamespacePlacementIsRefusedOnASplitCollection(t *testing.T) {
	_, post := twoShardSplit()
	table := post.SplitRouting.Table
	namespace := "ns"

	t.Run("delete", func(t *testing.T) {
		patch := mockey.Mock(dql.NamespaceShardingChannelID).Return(uint32(0), true, nil).Build()
		defer patch.UnPatch()
		_, _, _, err := repackPendingDeleteMsgs(context.Background(), table, int64IDs(1, 2), nil, post.VChannels,
			allocator.NewLocalAllocator(1, 100), 10, 1, "c", 2, "p", "db", &namespace, &schemapb.CollectionSchema{})
		assertNamespaceRefusal(t, err)

		// Never split: the namespace decides, as before.
		result, _, rows, err := repackPendingDeleteMsgs(context.Background(), nil, int64IDs(1, 2), nil, post.VChannels,
			allocator.NewLocalAllocator(1, 100), 10, 1, "c", 2, "p", "db", &namespace, &schemapb.CollectionSchema{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, rows)
		assert.Len(t, result, 1)
	})
	t.Run("partial update proof", func(t *testing.T) {
		patch := mockey.Mock(dql.NamespaceShardingChannelID).Return(uint32(1), true, nil).Build()
		defer patch.UnPatch()
		task := &UpsertTask{req: &milvuspb.UpsertRequest{}, schema: &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{}}}
		_, err := task.partialUpdateCASChannelIndexes(table, int64IDs(1, 2), post.VChannels)
		assertNamespaceRefusal(t, err)

		indexes, err := task.partialUpdateCASChannelIndexes(nil, int64IDs(1, 2), post.VChannels)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1, 1}, indexes)
	})
	t.Run("insert", func(t *testing.T) {
		patch := mockey.Mock(dql.NamespacePartitionKeyModeEnabled).Return(true).Build()
		defer patch.UnPatch()
		insertMsg := newInt64VarCharInsertMsgForRepackTest(splitFenceTestCollectionID, "a", "b")
		require.NotNil(t, insertMsg.Namespace)
		_, _, err := repackInsertDataWithPartitionKeyForStreamingService(context.Background(), NewMockCache(t), post.VChannels, table,
			insertMsg, nil, nil, nil, &schemapb.CollectionSchema{}, 0, nil, nil, nil)
		assertNamespaceRefusal(t, err)
	})
}

// A collection that was never split is written as it always was: one append,
// no refresh of the cache, every row and tombstone on the vchannel the legacy
// modulo gives it, and the messages are the ones the repack helpers build with
// no table and no pending set.
func TestWritesToANeverSplitCollectionAreUnchanged(t *testing.T) {
	vchannels := []string{splitSource, splitSibling, splitTarget0}
	info := splitCollectionInfo(splitFenceTestCollectionID, 0, vchannels)
	require.Nil(t, info.SplitRouting)
	pks := seqPKs(64)
	legacy, err := typeutil.HashPK2Channels(int64IDs(pks...), vchannels)
	require.NoError(t, err)
	legacyOwner := func(pk int64) string { return vchannels[legacy[pk-1]] }

	payloads := func(msgs []streamingmessage.MutableMessage) map[string][]byte {
		out := make(map[string][]byte, len(msgs))
		for _, msg := range msgs {
			_, dup := out[msg.VChannel()]
			require.False(t, dup, "one message per vchannel")
			out[msg.VChannel()] = msg.Payload()
		}
		return out
	}

	t.Run("insert", func(t *testing.T) {
		useSingleMessageRepack(t)
		f := newSplitFenceFixture(t, info, info)
		wal := installSplitFenceTestWAL(t)
		task := f.insertTask(pks)
		reference := f.insertTask(pks)
		want, _, err := repackInsertDataForStreamingService(context.Background(), f.cache, vchannels, nil,
			reference.insertMsg, reference.result, nil, reference.schemaVersion, nil, reference.idempotentInsertDecoration(), nil)
		require.NoError(t, err)

		require.NoError(t, task.Execute(context.Background()))
		require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())

		require.Len(t, wal.batches, 1)
		assert.Zero(t, f.evictions)
		assert.Equal(t, payloads(want), payloads(wal.batches[0]))
		// The row with id i carries the primary key pks[i-1], which is i.
		for vchannel, rowIDs := range wal.insertedRowIDs {
			for _, rowID := range rowIDs {
				assert.Equal(t, legacyOwner(rowID), vchannel, "pk %d", rowID)
			}
		}
		assertEveryRowLandedOnce(t, wal, len(pks))
		assert.Equal(t, wal.tick, task.result.GetTimestamp())
	})

	t.Run("delete", func(t *testing.T) {
		f := newSplitFenceFixture(t, info, info)
		wal := installSplitFenceTestWAL(t)
		task := f.deleteTask(pks)
		reference := f.deleteTask(pks)
		wantMsgs, wantRows, err := repackDeleteMsgByHash(context.Background(), nil, int64IDs(pks...), vchannels,
			reference.idAllocator, reference.ts, reference.collectionID, reference.req.GetCollectionName(),
			reference.partitionID, reference.req.GetPartitionName(), reference.req.GetDbName(), nil, nil)
		require.NoError(t, err)

		require.NoError(t, task.Execute(context.Background()))

		require.Len(t, wal.batches, 1)
		assert.Zero(t, f.evictions)
		assert.EqualValues(t, wantRows, task.count)
		assert.Equal(t, wal.tick, task.sessionTS)
		sent := 0
		for _, msg := range wal.batches[0] {
			body := streamingmessage.MustAsMutableDeleteMessageV1(msg).MustBody()
			index := typeutil.NewSet[string](vchannels...)
			require.True(t, index.Contain(msg.VChannel()))
			for _, pk := range body.GetPrimaryKeys().GetIntId().GetData() {
				assert.Equal(t, legacyOwner(pk), msg.VChannel(), "tombstone of pk %d", pk)
				sent++
			}
		}
		assert.Equal(t, len(pks), sent)
		for key, msgs := range wantMsgs {
			var got []int64
			for _, msg := range wal.batches[0] {
				if msg.VChannel() == vchannels[key] {
					got = append(got, streamingmessage.MustAsMutableDeleteMessageV1(msg).MustBody().GetPrimaryKeys().GetIntId().GetData()...)
				}
			}
			var want []int64
			for _, msg := range msgs {
				want = append(want, msg.GetPrimaryKeys().GetIntId().GetData()...)
			}
			assert.Equal(t, want, got)
		}
	})

	t.Run("upsert", func(t *testing.T) {
		useSingleMessageRepack(t)
		f := newSplitFenceFixture(t, info, info)
		wal := installSplitFenceTestWAL(t)
		task := f.upsertTask(t, pks, pks)

		require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))

		require.Len(t, wal.batches, 1)
		assert.Zero(t, f.evictions)
		for _, pk := range pks {
			owner := legacyOwner(pk)
			require.Len(t, wal.insertTicks[owner][pk], 1, "pk %d", pk)
			require.Len(t, wal.tombstoneTicks[owner][pk], 1, "pk %d", pk)
			assert.Equal(t, wal.insertTicks[owner][pk][0], wal.tombstoneTicks[owner][pk][0])
		}
		assert.Equal(t, wal.tick, task.result.GetTimestamp())
	})

	t.Run("partial update checks no proof against the route", func(t *testing.T) {
		useSingleMessageRepack(t)
		f := newSplitFenceFixture(t, info, info)
		wal := installSplitFenceTestWAL(t)
		task := f.upsertTask(t, pks, pks)
		task.req.PartialUpdate = true
		// Proofs for every shard, as PreExecute takes them. Nothing compares
		// them with the route of a collection that was never split.
		task.partialUpdateCASGroups = map[string]*messagespb.PartialUpdateCAS{}
		for _, vchannel := range vchannels {
			task.partialUpdateCASGroups[vchannel] = &messagespb.PartialUpdateCAS{ReadTs: 10, ObservedPchannelTerm: 1}
		}
		patch := mockey.Mock((*UpsertTask).checkPartialUpdateProofsCoverRoute).To(func(*UpsertTask, context.Context, *writeRoute) error {
			t.Error("the proofs of a never-split collection were checked against the route")
			return nil
		}).Build()
		defer patch.UnPatch()

		require.NoError(t, task.appendUpsertAttempt(context.Background(), nil))
		require.Len(t, wal.batches, 1)
		assert.Zero(t, f.evictions)
	})
}

// chainSplitInfo is a collection whose single shard S was split into T1 (owns
// residue 0 of 2) and T2 (owns 1), and T1 later into U1 (0 of 4) and U2 (2 of
// 4); T2 is expressed again as {1, 3}. Both S and T1 are listed as split
// sources. withResidues makes the shard infos of the sources carry the
// residues they owned, at the current modulus.
const (
	chainS  = "by-dev-rootcoord-dml_0_1001v0"
	chainT1 = "by-dev-rootcoord-dml_1_1001v1"
	chainT2 = "by-dev-rootcoord-dml_2_1001v2"
	chainU1 = "by-dev-rootcoord-dml_3_1001v3"
	chainU2 = "by-dev-rootcoord-dml_4_1001v4"
)

func chainSplitInfos(withResidues bool) (afterFirst, afterSecond *collectionInfo) {
	afterFirst = splitCollectionInfo(splitFenceTestCollectionID, 2, []string{chainT1, chainT2},
		splitShardInfo(schemapb.ShardState_ShardNormal, chainT1, 0),
		splitShardInfo(schemapb.ShardState_ShardNormal, chainT2, 1),
		splitShardInfo(schemapb.ShardState_ShardSplitting, chainS),
	)
	sourceS := splitShardInfo(schemapb.ShardState_ShardSplitting, chainS)
	sourceT1 := splitShardInfo(schemapb.ShardState_ShardSplitting, chainT1)
	if withResidues {
		sourceS = splitShardInfo(schemapb.ShardState_ShardSplitting, chainS, 0, 1, 2, 3)
		sourceT1 = splitShardInfo(schemapb.ShardState_ShardSplitting, chainT1, 0, 2)
	}
	afterSecond = splitCollectionInfo(splitFenceTestCollectionID, 4, []string{chainT2, chainU1, chainU2},
		sourceS,
		splitShardInfo(schemapb.ShardState_ShardNormal, chainT2, 1, 3),
		sourceT1,
		splitShardInfo(schemapb.ShardState_ShardNormal, chainU1, 0),
		splitShardInfo(schemapb.ShardState_ShardNormal, chainU2, 2),
	)
	return afterFirst, afterSecond
}

// pksOfResidues returns n int64 keys whose residue modulo 4 is in residues.
func pksOfResidues(t *testing.T, n int, residues ...uint64) []int64 {
	t.Helper()
	var pks []int64
	for pk := int64(1); len(pks) < n; pk++ {
		r, err := routing.PKResidueInt64(pk, 4)
		require.NoError(t, err)
		if slices.Contains(residues, r) {
			pks = append(pks, pk)
		}
	}
	return pks
}

// A source that names its residues is asked only for keys it can hold; one
// that names none is asked for every key. The probe of a narrowed source
// carries a row of its own residues.
func TestSourcesToProbeNarrowsByResidue(t *testing.T) {
	_, narrowed := chainSplitInfos(true)
	_, unnarrowed := chainSplitInfos(false)
	route := func(info *collectionInfo) *writeRoute {
		return newSplitWriteRoute(info.VChannels, info.SplitRouting)
	}
	// Keys of T2 (residues 1, 3) were never on T1.
	t2Keys := pksOfResidues(t, 4, 1, 3)
	got, err := route(narrowed).sourcesToProbe(int64IDs(t2Keys...), newPendingRows(len(t2Keys)))
	require.NoError(t, err)
	assert.Equal(t, map[string]int{chainS: 0}, got)

	got, err = route(unnarrowed).sourcesToProbe(int64IDs(t2Keys...), newPendingRows(len(t2Keys)))
	require.NoError(t, err)
	assert.Equal(t, map[string]int{chainS: 0, chainT1: 0}, got, "without residues every source is asked")

	// A batch whose first key is T2's and whose third is U2's: T1 is asked
	// with the row of residue 2.
	mixed := append(append([]int64{}, t2Keys[:2]...), pksOfResidues(t, 1, 2)...)
	got, err = route(narrowed).sourcesToProbe(int64IDs(mixed...), newPendingRows(len(mixed)))
	require.NoError(t, err)
	assert.Equal(t, map[string]int{chainS: 0, chainT1: 2}, got)

	// Only pending rows count.
	pending := newPendingRows(len(mixed))
	require.NoError(t, pending.settleLanded(chainU2, []int{2}))
	got, err = route(narrowed).sourcesToProbe(int64IDs(mixed...), pending)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{chainS: 0}, got)

	t.Run("nothing to ask", func(t *testing.T) {
		got, err := (&writeRoute{}).sourcesToProbe(int64IDs(1), newPendingRows(1))
		require.NoError(t, err)
		assert.Empty(t, got)
		done := newPendingRows(1)
		done.settleAll()
		got, err = route(narrowed).sourcesToProbe(int64IDs(1), done)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
	t.Run("malformed", func(t *testing.T) {
		noTable := &writeRoute{sources: route(narrowed).sources}
		_, err := noTable.sourcesToProbe(int64IDs(1), newPendingRows(1))
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		_, err = route(narrowed).sourcesToProbe(int64IDs(1), newPendingRows(2))
		assert.ErrorIs(t, err, merr.ErrServiceInternal, "a pending row with no key")
	})
}

// A chain of splits. A keyed insert landed on T1 and T2 between the two splits,
// its response was lost, T1 was then split too, and the client retries through
// a proxy that routes by U1, U2 and T2. S and T1 are both listed as sources:
// T1's window answers for the rows it took, T2 (still a shard) for its own,
// and S, whose window never saw the key, is asked and answers nothing. No row
// is written twice. With residues on the sources, the same holds.
func TestKeyedInsertRetriedAcrossAChainOfSplitsIsAnsweredByEverySourceOfTheChain(t *testing.T) {
	for _, withResidues := range []bool{false, true} {
		t.Run(fmt.Sprintf("residues %v", withResidues), func(t *testing.T) {
			useSingleMessageRepack(t)
			afterFirst, afterSecond := chainSplitInfos(withResidues)
			f := newSplitFenceFixture(t, afterFirst, afterSecond)
			wal := installSplitFenceTestWAL(t, chainS)
			pks := seqPKs(32)

			first := runKeyedInsert(t, f, pks, "key", 0)
			require.Len(t, wal.batches, 2, "S is asked, then the rows land on T1 and T2")
			require.NotEmpty(t, wal.insertedRowIDs[chainT1])
			require.NotEmpty(t, wal.insertedRowIDs[chainT2])

			wal.fenced[chainT1] = struct{}{}
			f.committed = true
			retry := runKeyedInsert(t, f, pks, "key", 0)
			assert.Empty(t, wal.insertedRowIDs[chainU1], "nothing is written again")
			assert.Empty(t, wal.insertedRowIDs[chainU2], "nothing is written again")
			assertEveryRowLandedOnce(t, wal, len(pks))
			assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
			probed := map[string]bool{}
			for batch := range wal.batches[2:] {
				for _, vchannel := range wal.vchannelsOf(batch + 2) {
					probed[vchannel] = true
				}
			}
			assert.True(t, probed[chainS], "S is asked")
			assert.True(t, probed[chainT1], "T1 is asked")
		})
	}
}
