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
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// N-4: a keyed insert whose first attempt landed on the source before the
// fence, and whose response was lost, is retried by the client after the fence.
// The retry must be answered from the source's idempotency window, not written
// again on the targets whose windows never saw the key.

// runKeyedInsert executes one client attempt of a keyed insert of pks, whose
// ids are pks shifted by idBase (an auto-id collection re-draws them on every
// attempt).
func runKeyedInsert(t *testing.T, f *splitFenceFixture, pks []int64, key string, idBase int64) *InsertTask {
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
	keyed := func() *InsertTask {
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
// anything is written. The probe runs before the first append of the attempt,
// same as reading the routing or repacking, so the failure ends the request
// through prepareFailed and Execute reports it directly.
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

	assert.ErrorContains(t, task.Execute(context.Background()), "partitions unavailable")
	assert.Contains(t, task.result.GetStatus().GetReason(), "partitions unavailable")
	assert.Empty(t, wal.batches)
}

// AV-L2-M1: a probe writes nothing, whatever it is answered, so an answer that
// is neither a duplicate nor the fence -- here the source, already retired by
// the split's adoption, no longer knows the collection -- backs off and
// refreshes the routing even on the first attempt, instead of failing a keyed
// insert that has written nothing. The refreshed routing no longer lists the
// source, and the rows go to the targets.
func TestKeyedInsertBacksOffOnAProbeFailureOnItsFirstAttempt(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	adopted := splitCollectionInfo(splitFenceTestCollectionID, 2, []string{splitTarget0, splitTarget1},
		splitShardInfo(schemapb.ShardState_ShardNormal, splitTarget0, 0),
		splitShardInfo(schemapb.ShardState_ShardNormal, splitTarget1, 1),
	)
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = status.NewUnrecoverableError("fail to wait growing segment ready, %s", merr.WrapErrCollectionNotFound(splitFenceTestCollectionID))
	f.onEvict = func(int) { f.post = adopted }
	pks := seqPKs(8)
	task := f.keyedInsertTask(pks, "key")

	require.NoError(t, task.Execute(context.Background()))
	require.True(t, merr.Ok(task.result.GetStatus()), task.result.GetStatus().GetReason())
	assert.Equal(t, 1, f.evictions)
	assertEveryRowLandedOnce(t, wal, len(pks))
	assertRowsLandedOnceOnTheirOwner(t, wal, adopted, pks, 1)
}

// N-3: a probe answer no routing refresh can cure fails the request at once,
// with the class of the error -- not after the deadline as a retriable
// ServiceUnavailable.
func TestKeyedInsertFailsFastOnAProbeAnswerNoRefreshCures(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = status.NewInvalidArgument("the probe is malformed")
	task := f.keyedInsertTask(seqPKs(4), "key")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, task.Execute(ctx))
	st := task.result.GetStatus()
	assert.False(t, merr.Ok(st))
	assert.NotEqual(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.Contains(t, st.GetReason(), "the probe is malformed")
	assert.Zero(t, f.evictions)
	assert.Len(t, wal.batches, 1, "only the probe was sent")
}

// N-3: an unrecoverable answer -- a source that no longer knows the
// collection -- is worth one refresh, which normally delists it (AV-L2-M1).
// If the refreshed routing still lists it and it answers the same way, no
// further refresh will help: the request fails with that answer.
func TestKeyedInsertStopsRefreshingWhenTheRefreshedRouteStillListsTheFailingSource(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = status.NewUnrecoverableError("fail to wait growing segment ready, %s", merr.WrapErrCollectionNotFound(splitFenceTestCollectionID))
	task := f.keyedInsertTask(seqPKs(4), "key")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, task.Execute(ctx))
	st := task.result.GetStatus()
	assert.False(t, merr.Ok(st))
	assert.NotEqual(t, merr.Code(merr.ErrServiceUnavailable), st.GetCode(), st.GetReason())
	assert.Contains(t, st.GetReason(), "fail to wait growing segment ready")
	assert.Equal(t, 1, f.evictions)
	assert.Len(t, wal.batches, 2, "the probe, and the probe after the refresh")
}

func TestClassifyProbeAnswer(t *testing.T) {
	cases := []struct {
		err  error
		want probeAnswerRemedy
	}{
		{errors.New("connection reset"), probeAnswerTransient},
		{context.Canceled, probeAnswerTransient},
		{merr.WrapErrServiceUnavailableMsg("busy"), probeAnswerTransient},
		{merr.WrapErrParameterInvalidMsg("bad"), probeAnswerPermanent},
		{status.NewUnrecoverableError("gone"), probeAnswerStaleRoute},
		{status.NewOnShutdownError("bye"), probeAnswerTransient},
		{status.NewChannelNotExist("v"), probeAnswerTransient},
		{status.NewInvalidArgument("bad"), probeAnswerPermanent},
		{status.NewSchemaVersionMismatch("old"), probeAnswerPermanent},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, classifyProbeAnswer(c.err), "%v", c.err)
	}
}

func TestProbeAnswerErrorKeepsItsCause(t *testing.T) {
	cause := merr.WrapErrServiceUnavailableMsg("node down")
	err := &probeAnswerError{vchannel: splitSource, cause: cause}
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.Contains(t, err.Error(), splitSource)
}

// A probe answer no retry can cure -- a non-retriable Milvus error -- still
// fails the request at once, with nothing but the probe sent.
func TestKeyedInsertFailsOnAProbeErrorNoRetryCures(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := oneShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	f.committed = true
	wal := installSplitFenceTestWAL(t)
	wal.failing[splitSource] = merr.WrapErrParameterInvalidMsg("boom")
	task := f.keyedInsertTask(seqPKs(4), "key")

	require.NoError(t, task.Execute(context.Background()))
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrParameterInvalid), st.GetCode(), st.GetReason())
	assert.Contains(t, st.GetReason(), "boom")
	require.Len(t, wal.batches, 1, "only the probe was sent")
	assert.Zero(t, f.evictions)
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

	err := task.Execute(context.Background())
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err), err)
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), st.GetCode(), st.GetReason())
	assert.False(t, st.GetRetriable())
	assert.Len(t, wal.batches, 1, "only the probe was sent")
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
}

// A window answer that does not line up with the request -- a key reused with
// a payload of another shape -- is reported as the input error it is, before
// any row is placed under the reused key.
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

	err := task.Execute(context.Background())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	st := task.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrParameterInvalid), st.GetCode(), st.GetReason())
	assert.Empty(t, wal.insertedRowIDs[splitTarget0])
	assert.Empty(t, wal.insertedRowIDs[splitTarget1])
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
	require.NoError(t, stabilizeAutoIDs(ids, route, alloc))
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

// autoIDKeyedInsertTask is an idempotent auto-id insert under key: its row ids,
// its primary keys and its result ids are all ids, and the row at offset i
// carries the payload "row-i".
func (f *splitFenceFixture) autoIDKeyedInsertTask(ids []int64, key string) *InsertTask {
	task := f.keyedInsertTask(ids, key)
	task.insertMsg.RowIDs = slices.Clone(ids)
	task.insertMsg.FieldsData[0].GetScalars().GetLongData().Data = slices.Clone(ids)
	task.stableAutoIDPrimary = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true}
	task.idAllocator = &allocator.IDAllocator{}
	return task
}

// mockIDAllocator makes every IDAllocator hand out fresh ids from base up.
func mockIDAllocator(t *testing.T, base int64) {
	next := base
	patch := mockey.Mock((*allocator.IDAllocator).Alloc).To(func(_ *allocator.IDAllocator, count uint32) (int64, int64, error) {
		begin := next
		next += int64(count)
		return begin, next, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

// idsWithResidues returns, counting up from base, one id per wanted residue
// modulo modulus.
func idsWithResidues(t *testing.T, base int64, modulus uint64, residues ...uint64) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(residues))
	next := base
	for _, want := range residues {
		for ; ; next++ {
			got, err := routing.PKResidues(int64IDs(next), modulus)
			require.NoError(t, err)
			if got[0] == want {
				ids = append(ids, next)
				next++
				break
			}
		}
	}
	return ids
}

// assertEveryOffsetLandedOnceUnder checks that the row of every offset landed
// exactly once, and under the id the result reports for that offset.
func assertEveryOffsetLandedOnceUnder(t *testing.T, w *splitFenceTestWAL, ids []int64) {
	t.Helper()
	landed := make(map[string][]int64)
	for _, rows := range w.insertedRows {
		for _, row := range rows {
			landed[row.payload] = append(landed[row.payload], row.pk)
		}
	}
	require.Len(t, landed, len(ids), "every offset lands, and nothing else does")
	for offset, id := range ids {
		assert.Equal(t, []int64{id}, landed[fmt.Sprintf("row-%d", offset)], "offset %d", offset)
	}
}

// CZ-F1: an idempotent auto-id insert pins offset i to the owner of residue
// i % M. The first request here is refused by the fence and re-routes the
// source's rows within the request; one target commits them and the other
// fails, so the request fails having landed in part. The client's retry
// buckets by i % M after the split, and must find on the committed target
// exactly the offsets it carries there: otherwise the window's answer settles
// offsets that were never written and the retry rewrites others.
func TestKeyedAutoIDInsertRetriedAfterAPartlyLandedRerouteLandsEveryOffsetOnce(t *testing.T) {
	useSingleMessageRepack(t)
	pre, post := twoShardSplit()
	f := newSplitFenceFixture(t, pre, post)
	wal := installSplitFenceTestWAL(t, splitSource)
	mockIDAllocator(t, 1<<40)
	ctx := context.Background()

	// Before the split offset i sits on shard i % 2, the source holding the
	// even ones. Their ids hash so that a re-route by id sends offsets {0, 2}
	// to the target owning residue 0 of 4 and {4, 6} to the one owning 2 --
	// where a retry bucketing by i % 4 sends {0, 4} and {2, 6}.
	wal.failing[splitTarget1] = errors.New("the request's deadline ran out")
	first := f.autoIDKeyedInsertTask(idsWithResidues(t, 1000, 4, 0, 1, 0, 3, 2, 1, 2, 3), "auto-key")
	require.NoError(t, first.Execute(ctx))
	require.False(t, merr.Ok(first.result.GetStatus()), "the first request fails on the target owning residue 2")
	require.NotEmpty(t, wal.insertedRows[splitTarget0], "the first request landed on the target owning residue 0")
	require.Empty(t, wal.insertedRows[splitTarget1])

	delete(wal.failing, splitTarget1)
	retry := f.autoIDKeyedInsertTask(drawAutoIDs(t, 8, 50000, newSplitWriteRoute(post.VChannels, post.SplitRouting)), "auto-key")
	require.NoError(t, retry.Execute(ctx))
	require.True(t, merr.Ok(retry.result.GetStatus()), retry.result.GetStatus().GetReason())
	assertEveryOffsetLandedOnceUnder(t, wal, retry.result.GetIDs().GetIntId().GetData())
}

// repinTestTask is an idempotent auto-id insert of ids whose primary field is
// of dataType, with no primary column yet.
func repinTestTask(dataType schemapb.DataType, ids []int64) *InsertTask {
	primary := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: dataType, IsPrimaryKey: true, AutoID: true}
	fieldData, err := autoGenPrimaryFieldData(primary, slices.Clone(ids))
	if err != nil {
		panic(err)
	}
	resultIDs, err := parsePrimaryFieldData2IDs(fieldData)
	if err != nil {
		panic(err)
	}
	return &InsertTask{
		insertMsg: &BaseInsertTask{InsertRequest: &msgpb.InsertRequest{
			NumRows: uint64(len(ids)),
			RowIDs:  slices.Clone(ids),
		}},
		result:              &milvuspb.MutationResult{IDs: resultIDs},
		stableAutoIDPrimary: primary,
		idAllocator:         &allocator.IDAllocator{},
	}
}

// A pending row whose id routes elsewhere than the owner of its offset's
// residue gets a new id -- in its row ids, its primary column and the result --
// while a settled row and a well-placed pending one keep theirs.
func TestRepinPendingAutoIDsMovesOnlyTheMisplacedPendingRows(t *testing.T) {
	_, post := twoShardSplit()
	route := newSplitWriteRoute(post.VChannels, post.SplitRouting)
	placement := newAutoIDPlacement(route)
	mockIDAllocator(t, 1<<40)
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(dataType.String(), func(t *testing.T) {
			// Offsets 0 and 4 belong to residue 0 of 4; the ids here sit on
			// residue 2 for offset 0 and on residue 0 for offset 4.
			ids := idsWithResidues(t, 1000, 4, 2, 1, 2, 3, 0, 1, 2, 3)
			if dataType == schemapb.DataType_VarChar {
				// A varchar key hashes its text, so draw ids until the residues
				// of their decimal strings match.
				ids = varCharIDsWithResidues(t, 1000, 4, 2, 1, 2, 3, 0, 1, 2, 3)
			}
			task := repinTestTask(dataType, ids)
			pending := newKeyedPendingRows(8, newSplitFence())
			require.NoError(t, pending.settleLanded(splitSibling, []int{1, 3}))

			require.NoError(t, task.repinPendingAutoIDs(route, pending))
			rowIDs := task.insertMsg.GetRowIDs()
			assert.NotEqual(t, ids[0], rowIDs[0], "a misplaced pending row is re-drawn")
			for _, offset := range []int{1, 3, 4, 5, 7} {
				assert.Equal(t, ids[offset], rowIDs[offset], "offset %d keeps its id", offset)
			}
			owners, err := autoIDCandidateOwners(rowIDs, dataType, placement)
			require.NoError(t, err)
			for offset, owner := range owners {
				assert.Equal(t, placement.ownerOf[uint64(offset)%placement.modulus], owner, "offset %d", offset)
			}
			primaryField := task.insertMsg.GetFieldsData()[0]
			resultIDs, err := parsePrimaryFieldData2IDs(primaryField)
			require.NoError(t, err)
			assert.Equal(t, resultIDs, task.result.GetIDs(), "the primary column and the result agree")
		})
	}
}

func varCharIDsWithResidues(t *testing.T, base int64, modulus uint64, residues ...uint64) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(residues))
	next := base
	for _, want := range residues {
		for ; ; next++ {
			got, err := routing.PKResidues(&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{fmt.Sprint(next)}}}}, modulus)
			require.NoError(t, err)
			if got[0] == want {
				ids = append(ids, next)
				next++
				break
			}
		}
	}
	return ids
}

func TestRepinPendingAutoIDsLeavesAWellPlacedOrUnpinnedInsertAlone(t *testing.T) {
	_, post := twoShardSplit()
	route := newSplitWriteRoute(post.VChannels, post.SplitRouting)
	ids := idsWithResidues(t, 1000, 4, 0, 1, 2, 3)

	placed := repinTestTask(schemapb.DataType_Int64, ids)
	placed.idAllocator = nil
	require.NoError(t, placed.repinPendingAutoIDs(route, newKeyedPendingRows(4, newSplitFence())))
	assert.Equal(t, ids, placed.insertMsg.GetRowIDs(), "nothing is drawn when every row is where it belongs")

	unpinned := repinTestTask(schemapb.DataType_Int64, []int64{7, 7, 7, 7})
	unpinned.stableAutoIDPrimary = nil
	require.NoError(t, unpinned.repinPendingAutoIDs(route, newKeyedPendingRows(4, newSplitFence())))
	assert.Equal(t, []int64{7, 7, 7, 7}, unpinned.insertMsg.GetRowIDs())

	oneShard := repinTestTask(schemapb.DataType_Int64, []int64{7, 7})
	require.NoError(t, oneShard.repinPendingAutoIDs(legacyWriteRoute([]string{splitSource}), newKeyedPendingRows(2, newSplitFence())))
	assert.Equal(t, []int64{7, 7}, oneShard.insertMsg.GetRowIDs())
}

func TestRepinPendingAutoIDsFailures(t *testing.T) {
	_, post := twoShardSplit()
	route := newSplitWriteRoute(post.VChannels, post.SplitRouting)
	misplaced := idsWithResidues(t, 1000, 4, 2, 1)

	short := repinTestTask(schemapb.DataType_Int64, misplaced)
	assert.ErrorIs(t, short.repinPendingAutoIDs(route, newKeyedPendingRows(3, newSplitFence())), merr.ErrServiceInternal)

	noAllocator := repinTestTask(schemapb.DataType_Int64, misplaced)
	noAllocator.idAllocator = nil
	assert.ErrorIs(t, noAllocator.repinPendingAutoIDs(route, newKeyedPendingRows(2, newSplitFence())), merr.ErrServiceInternal)

	patch := mockey.Mock((*allocator.IDAllocator).Alloc).Return(int64(0), int64(0), errors.New("allocator down")).Build()
	defer patch.UnPatch()
	failing := repinTestTask(schemapb.DataType_Int64, misplaced)
	assert.ErrorContains(t, failing.repinPendingAutoIDs(route, newKeyedPendingRows(2, newSplitFence())), "allocator down")

	unsupported := repinTestTask(schemapb.DataType_Int64, misplaced)
	unsupported.stableAutoIDPrimary = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Float}
	assert.Error(t, unsupported.repinPendingAutoIDs(route, newKeyedPendingRows(2, newSplitFence())))
}

// pksRoutingOtherwise returns n primary keys, counting up from base, whose
// shards under info differ from those of pks at the same offsets somewhere.
func pksRoutingOtherwise(t *testing.T, info *collectionInfo, pks []int64, base int64) []int64 {
	t.Helper()
	other := make([]int64, len(pks))
	differs := false
	for i := range other {
		other[i] = base + int64(i)
		if splitOwner(t, info, other[i]) != splitOwner(t, info, pks[i]) {
			differs = true
		}
	}
	require.True(t, differs)
	return other
}

// N-2: on a collection no split has touched, reusing a key with a same-size
// but different payload keeps the idempotent-write contract of master
// (20260604-idempotent_write.md): the first request's result, and no new row.
func TestKeyReusedWithAnotherPayloadOnANeverSplitCollectionReturnsTheFirstResult(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := twoShardSplit()
	f := newSplitFenceFixture(t, pre, pre)
	wal := installSplitFenceTestWAL(t)
	pks := seqPKs(8)

	first := runKeyedInsert(t, f, pks, "key", 1000)
	landed := len(wal.insertedRowIDs[splitSource]) + len(wal.insertedRowIDs[splitSibling])

	reused := f.keyedInsertTask(pksRoutingOtherwise(t, pre, pks, 100), "key")
	require.NoError(t, reused.Execute(context.Background()))
	require.True(t, merr.Ok(reused.result.GetStatus()), reused.result.GetStatus().GetReason())
	assert.Equal(t, first.result.GetIDs().GetIntId().GetData(), reused.result.GetIDs().GetIntId().GetData())
	assert.Equal(t, landed, len(wal.insertedRowIDs[splitSource])+len(wal.insertedRowIDs[splitSibling]), "no new row is written")
}

// N-2: on a split collection the placement guards stand, and a key reused
// with a payload that routes otherwise is reported as the input error it is.
func TestKeyReusedWithAnotherPayloadOnASplitCollectionIsAnInputError(t *testing.T) {
	useSingleMessageRepack(t)
	_, post := twoShardSplit()
	f := newSplitFenceFixture(t, post, post)
	installSplitFenceTestWAL(t, splitSource)
	pks := seqPKs(8)

	runKeyedInsert(t, f, pks, "key", 1000)

	reused := f.keyedInsertTask(pksRoutingOtherwise(t, post, pks, 100), "key")
	require.NoError(t, reused.Execute(context.Background()))
	st := reused.result.GetStatus()
	assert.Equal(t, merr.Code(merr.ErrParameterInvalid), st.GetCode(), st.GetReason())
}

// N-4: when a later probe of the same batch fails, the answers already
// settled are merged into the result before the request goes on or fails, so
// the ids reported for those rows are the ids stored.
func TestProbeMergesEachAnswerBeforeALaterProbeFails(t *testing.T) {
	useSingleMessageRepack(t)
	pre, _ := oneShardSplit()
	f := newSplitFenceFixture(t, pre, pre)
	wal := installSplitFenceTestWAL(t)
	wal.windows[splitSource] = map[string]*splitFenceTestWindowEntry{
		"key": {tick: 7, result: idempotentResult([]uint32{0, 1}, 500, 501)},
	}
	wal.failing[splitSibling] = errors.New("transient")
	task := f.keyedInsertTask(seqPKs(4), "key")
	fence := newSplitFence()
	pending := newKeyedPendingRows(4, fence)
	route := &writeRoute{fenced: []string{splitSource, splitSibling}}

	_, err := task.probeFencedWindows(context.Background(), route, fence, pending, task.idempotentInsertDecoration(), nil)
	require.Error(t, err)
	assert.Equal(t, []int64{500, 501, 3, 4}, task.result.GetIDs().GetIntId().GetData())
	assert.Equal(t, rowSet{2: {}, 3: {}}, pending.pendingSet())
}

// N-5: while the routing is the one the ids were pinned against, no pending
// row can be misplaced, so repin does not even hash them; once it re-pins
// against another routing, that one becomes the reference.
func TestRepinPendingAutoIDsSkipsTheRoutingItPinnedAgainst(t *testing.T) {
	_, post := twoShardSplit()
	route := newSplitWriteRoute(post.VChannels, post.SplitRouting)
	mockIDAllocator(t, 1<<40)
	misplaced := idsWithResidues(t, 1000, 4, 2, 1)

	pinned := repinTestTask(schemapb.DataType_Int64, misplaced)
	pinned.stableAutoIDRouteKey = autoIDRouteKey(route)
	require.NoError(t, pinned.repinPendingAutoIDs(route, newKeyedPendingRows(2, newSplitFence())))
	assert.Equal(t, misplaced, pinned.insertMsg.GetRowIDs(), "the routing it pinned against is not re-checked")

	moved := repinTestTask(schemapb.DataType_Int64, misplaced)
	moved.stableAutoIDRouteKey = autoIDRouteKey(legacyWriteRoute([]string{splitSource, splitSibling}))
	require.NoError(t, moved.repinPendingAutoIDs(route, newKeyedPendingRows(2, newSplitFence())))
	assert.NotEqual(t, misplaced[0], moved.insertMsg.GetRowIDs()[0])
	assert.Equal(t, autoIDRouteKey(route), moved.stableAutoIDRouteKey)
	assert.NotEqual(t, autoIDRouteKey(route), autoIDRouteKey(legacyWriteRoute([]string{splitSource, splitSibling})))
}
