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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// N-4: a keyed insert whose first attempt landed on the source before the
// fence, and whose response was lost, is retried by the client after the fence.
// The retry must be answered from the source's idempotency window, not written
// again on the targets whose windows never saw the key.

// runKeyedInsert executes one client attempt of a keyed insert of pks, whose
// ids are pks shifted by idBase (an auto-id collection re-draws them on every
// attempt).
func runKeyedInsert(t *testing.T, f *splitFenceFixture, pks []int64, key string, idBase int64) *insertTask {
	t.Helper()
	task := f.keyedInsertTask(pks, key)
	ids := task.result.GetIDs().GetIntId().GetData()
	for i := range ids {
		ids[i] += idBase
	}
	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	return task
}

func assertEveryRowLandedOnce(t *testing.T, w *splitFenceTestWAL, numRows int) {
	t.Helper()
	landed := make(map[int64]int)
	for _, rowIDs := range w.insertedRowIDs {
		for _, rowID := range rowIDs {
			landed[rowID]++
		}
	}
	require.Len(t, landed, numRows, "every row lands")
	for rowID, times := range landed {
		assert.Equal(t, 1, times, "row %d landed more than once", rowID)
	}
}

func TestKeyedInsertRetriedAfterTheFenceIsAnsweredByTheSource(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(32)

	first := runKeyedInsert(t, f, pks, "key", 0)
	require.Len(t, wal.insertedRowIDs[splitSource], len(pks), "the first attempt landed on the source; its response is lost")

	// The split fences the source and its routing commit becomes visible.
	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := runKeyedInsert(t, f, pks, "key", 0)
	assert.Empty(t, wal.insertedRowIDs[splitTarget0], "the retry wrote nothing on a target")
	assert.Empty(t, wal.insertedRowIDs[splitTarget1], "the retry wrote nothing on a target")
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
	assert.Equal(t, first.result.GetTimestamp(), retry.result.GetTimestamp(), "a duplicate answers with the first append's tick")
}

// The retry of an auto-id insert re-draws its ids; the source answers with the
// ids its first attempt wrote.
func TestKeyedAutoIDInsertRetriedAfterTheFenceGetsTheFirstIDs(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(16)

	first := runKeyedInsert(t, f, pks, "auto-key", 1000)
	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := runKeyedInsert(t, f, pks, "auto-key", 5000)
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
	assertEveryRowLandedOnce(t, wal, len(pks))
}

// A batch that landed on the source and on an untouched sibling: the source
// answers for its rows, the sibling for its own, and no target is written.
func TestKeyedInsertRetriedAfterTheFenceSpanningShards(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(32)

	first := runKeyedInsert(t, f, pks, "key", 0)
	require.NotEmpty(t, wal.insertedRowIDs[splitSource])
	require.NotEmpty(t, wal.insertedRowIDs[splitSibling])
	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := runKeyedInsert(t, f, pks, "key", 0)
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
}

// The fence refused the first attempt, which the proxy then re-routed to the
// targets; the retry of that attempt is answered by the targets, and the
// source, whose window never took the key, answers nothing.
func TestKeyedInsertRetriedAfterItWasReroutedIsAnsweredByTheTargets(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(32)

	first := runKeyedInsert(t, f, pks, "key", 0)
	require.Empty(t, wal.insertedRowIDs[splitSource])
	require.NotEmpty(t, wal.insertedRowIDs[splitTarget0])
	require.NotEmpty(t, wal.insertedRowIDs[splitTarget1])

	retry := runKeyedInsert(t, f, pks, "key", 0)
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
}

// A retry that reaches the proxy while the routing commit is not visible yet
// still routes to the source, which answers from its window as well.
func TestKeyedInsertRetriedBeforeTheRoutingCommitIsVisible(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(16)

	first := runKeyedInsert(t, f, pks, "key", 0)
	wal.fenced[splitSource] = struct{}{}

	retry := runKeyedInsert(t, f, pks, "key", 0)
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
	assert.Zero(t, f.evictions, "the source answered every row")
}

// A partition-key collection: the probe carries its row into the partition the
// row hashes to, and the source answers for every row.
func TestKeyedPartitionKeyInsertRetriedAfterTheFenceIsAnsweredByTheSource(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"_default_0": 300, "_default_1": 301}, nil).Maybe()
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(16)
	keyed := func() *insertTask {
		task := f.keyedInsertTask(pks, "key")
		task.partitionKeys = partialUpdateCASPKFieldData(pks)
		return task
	}

	first := keyed()
	require.NoError(t, first.Execute(context.Background()))
	require.True(t, merr.Ok(first.result.GetStatus()), first.result.GetStatus().GetReason())
	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := keyed()
	require.NoError(t, retry.Execute(context.Background()))
	require.True(t, merr.Ok(retry.result.GetStatus()), retry.result.GetStatus().GetReason())
	assertEveryRowLandedOnce(t, wal, len(pks))
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
}

// A probe that cannot resolve its row's partition fails the request before
// anything is written.
func TestKeyedInsertFailsWhenTheProbeCannotResolveItsPartition(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	f.cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("partitions unavailable")).Maybe()
	wal := installSplitFenceTestWAL(t, splitSource)
	task := f.keyedInsertTask(seqPKs(4), "key")
	task.partitionKeys = partialUpdateCASPKFieldData(seqPKs(4))

	require.NoError(t, task.Execute(context.Background()))
	assert.Contains(t, task.result.GetStatus().GetReason(), "partitions unavailable")
	assert.Empty(t, wal.batches)
}

// An error from the fenced vchannel that is not the fence fails the request:
// nothing tells whether the window holds the key.
func TestKeyedInsertFailsOnAProbeErrorThatIsNotTheFence(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = errors.New("boom")
	task := f.keyedInsertTask(seqPKs(4), "key")

	require.NoError(t, task.Execute(context.Background()))
	assert.Contains(t, task.result.GetStatus().GetReason(), "boom")
	require.Len(t, wal.batches, 1, "only the probe was sent")
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
}

// A vchannel the collection lists as fenced but that takes the probe breaks
// the invariant the probe relies on (Splitting implies fenced): the request
// fails as a System error, and no other row is placed.
func TestKeyedInsertFailsWhenAFencedVChannelTakesItsProbe(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t)
	task := f.keyedInsertTask(seqPKs(8), "key")

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), st.GetCode(), st.GetReason())
	assert.False(t, st.GetRetriable())
	assert.Len(t, wal.batches, 1, "only the probe was sent")
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
}

// A window answer that does not line up with the request -- a key reused with
// a payload of another shape -- is reported as the input error it is.
func TestKeyedInsertReportsAProbeAnswerForADifferentPayload(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t, splitSource)
	wal.windows[splitSource] = map[string]*splitFenceTestWindowEntry{
		"key": {tick: 7, result: idempotentResult([]uint32{100}, 1)},
	}
	task := f.keyedInsertTask(seqPKs(4), "key")

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrParameterInvalid), st.GetCode(), st.GetReason())
}

// drawAutoIDs draws n auto ids the way an idempotent auto-id insert does for
// route, from the id space starting at base.
func drawAutoIDs(t *testing.T, n int, base int64, route *writeRoute) []int64 {
	t.Helper()
	ids := make([]int64, n)
	next := base
	for i := range ids {
		ids[i] = next
		next++
	}
	alloc := func(count uint32) (int64, int64, error) {
		begin := next
		next += int64(count)
		return begin, next, nil
	}
	require.NoError(t, reassignAutoIDByResidue(ids, schemapb.DataType_Int64, route.modulus(), 0, alloc))
	return ids
}

// An idempotent auto-id insert acked before a split's routing is visible, and
// retried by the client after it with re-drawn ids: the untouched sibling keeps
// exactly the offsets it holds, the source answers for its own, and no row
// lands twice.
func TestKeyedAutoIDInsertRetriedAcrossASplitKeepsEveryOffsetOnItsShard(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t)
	const n = 12

	first := f.keyedInsertTask(drawAutoIDs(t, n, 1000, legacyWriteRoute(pre.VChannels)), "auto-key")
	require.NoError(t, first.Execute(context.Background()))
	require.NotEmpty(t, wal.insertedRowIDs[splitSource])
	require.NotEmpty(t, wal.insertedRowIDs[splitSibling])

	wal.fenced[splitSource] = struct{}{}
	f.committed = true

	retry := f.keyedInsertTask(drawAutoIDs(t, n, 50000, newSplitWriteRoute(post.VChannels, post.SplitRouting)), "auto-key")
	require.NoError(t, retry.Execute(context.Background()))
	require.True(t, merr.Ok(retry.result.GetStatus()), retry.result.GetStatus().GetReason())
	assertEveryRowLandedOnce(t, wal, n)
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), retry.result.GetIDs().GetIntId().GetData())
}
