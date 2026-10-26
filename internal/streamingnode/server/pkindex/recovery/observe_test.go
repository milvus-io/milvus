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

package recovery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// testPKFieldID is the primary key field of testMeta.
const testPKFieldID int64 = 100

// at gives a mutable message the time tick tt and a message id from tt, as
// the WAL does at the append.
func at(msg message.MutableMessage, tt uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tt))
	return msg.WithTimeTick(tt).WithLastConfirmed(id).IntoImmutableMessage(id)
}

// newInsert builds an insert of pks into segment segmentID of vchannel.
func newInsert(vchannel string, segmentID int64, pks ...int64) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{{
				PartitionId:       2,
				Rows:              uint64(len(pks)),
				BinarySize:        1024,
				SegmentAssignment: &message.SegmentAssignment{SegmentId: segmentID},
			}},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: 1,
			PartitionID:  2,
			NumRows:      uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{{
				Type:    schemapb.DataType_Int64,
				FieldId: testPKFieldID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			}},
		}).
		MustBuildMutable()
}

// newVarCharInsert builds an insert of varchar pks into segment segmentID of
// vchannel.
func newVarCharInsert(vchannel string, segmentID int64, pks ...string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{{
				PartitionId:       2,
				Rows:              uint64(len(pks)),
				BinarySize:        1024,
				SegmentAssignment: &message.SegmentAssignment{SegmentId: segmentID},
			}},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: 1,
			PartitionID:  2,
			NumRows:      uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{{
				Type:    schemapb.DataType_VarChar,
				FieldId: testPKFieldID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: pks}},
				}},
			}},
		}).
		MustBuildMutable()
}

// newDelete builds a delete of pks of vchannel.
func newDelete(vchannel string, pks ...int64) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: uint64(len(pks))}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  2,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			NumRows:      int64(len(pks)),
			Timestamps:   make([]uint64, len(pks)),
		}).
		MustBuildMutable()
}

func insertMsg(vchannel string, tt uint64, segmentID int64, pks ...int64) message.ImmutableMessage {
	return at(newInsert(vchannel, segmentID, pks...), tt)
}

func deleteMsg(vchannel string, tt uint64, pks ...int64) message.ImmutableMessage {
	return at(newDelete(vchannel, pks...), tt)
}

func timeTickMsg(tt uint64) message.ImmutableMessage {
	return at(message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_TimeTick, Timestamp: tt}}).
		WithAllVChannel().
		MustBuildMutable(), tt)
}

// txnMsg assembles a transaction message of vchannel with bodies, committed
// at commitTT, as the WAL recovery delivers one.
func txnMsg(t *testing.T, vchannel string, commitTT uint64, txnID int64, bodies ...message.MutableMessage) message.ImmutableMessage {
	txnCtx := message.TxnContext{TxnID: message.TxnID(txnID), Keepalive: time.Second}
	beginID := walimplstest.NewTestMessageID(int64(commitTT) - 1)
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 1000}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(commitTT - 1).
		WithLastConfirmed(beginID).
		IntoImmutableMessage(beginID)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	for _, body := range bodies {
		builder.Add(at(body.WithTxnContext(txnCtx), commitTT-1))
	}
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(commitTT).
		WithLastConfirmed(beginID).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(commitTT)))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return txn
}

func createCollectionMsg(vchannel string, collectionID int64, tt uint64) message.ImmutableMessage {
	return at(message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: collectionID, PartitionIds: []int64{2}}).
		WithBody(&msgpb.CreateCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionID:   collectionID,
			CollectionName: "collection",
			CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: testPKFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			}},
		}).
		MustBuildMutable(), tt)
}

func dropCollectionMsg(vchannel string, collectionID int64, tt uint64) message.ImmutableMessage {
	return at(message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.DropCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			CollectionID:   collectionID,
			CollectionName: "collection",
		}).
		MustBuildMutable(), tt)
}

// withReplicate marks msg as replicated from another cluster.
func manualFlushMsg(vchannel string, tt uint64) message.ImmutableMessage {
	return at(message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable(), tt)
}

func flushAllMsg(pchannel string, tt uint64) message.ImmutableMessage {
	return at(message.NewFlushAllMessageBuilderV2().
		WithVChannel(funcutil.GetControlChannel(pchannel)).
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		MustBuildMutable(), tt)
}

func withReplicate(msg message.MutableMessage, tt uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tt))
	return at(msg.WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "source",
		MessageID:              id,
		LastConfirmedMessageID: id,
		TimeTick:               tt,
		VChannel:               msg.VChannel(),
	}), tt)
}

// present returns the keys of pks that the index of idx holds. The Index must
// be ready, else every key counts as present.
func present(t *testing.T, idx *index.Index, pks ...int64) []dedup.PK {
	keys := make([]dedup.PK, 0, len(pks))
	for _, pk := range pks {
		keys = append(keys, dedup.Int64PK(pk))
	}
	return presentPKs(t, idx, keys)
}

func presentPKs(t *testing.T, idx *index.Index, keys []dedup.PK) []dedup.PK {
	ctx := context.Background()
	require.True(t, idx.Ready())
	g, err := idx.Decider().Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: keys})
	require.NoError(t, err)
	defer g.NotAppended()
	return g.CompanionDeletePKs()
}

// segmentOf returns the segment of pk in the engine of idx, 0 when the
// engine has no record or a tombstone of pk.
func segmentOf(t *testing.T, idx *index.Index, pk int64) int64 {
	vs, err := idx.EngineForTest().MultiGet(context.Background(), [][]byte{dedup.Int64PK(pk).Encode()})
	require.NoError(t, err)
	if vs[0] == nil || codec.IsTombstone(vs[0]) {
		return 0
	}
	e, err := codec.DecodePKEntry(vs[0])
	require.NoError(t, err)
	return e.SegmentID
}

func recoverV1(t *testing.T, env *pkindex.FakeEnv, sched nodescheduler.Scheduler) *Recovery {
	ctx := context.Background()
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}, Seed: 10},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	return rec
}

func TestObserveReplaysLikeTheAppendPath(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())

	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1, 2, 3))
	rec.ObserveMessage(ctx, deleteMsg("v1", 12, 2))
	// An insert of a key that exists is a transaction in the WAL: its
	// companion delete, then the insert.
	rec.ObserveMessage(ctx, txnMsg(t, "v1", 13, 100, newDelete("v1", 3), newInsert("v1", 8, 3)))
	rec.ObserveMessage(ctx, txnMsg(t, "v1", 14, 101, newDelete("v1", 1), newInsert("v1", 9, 4)))
	rec.ObserveMessage(ctx, timeTickMsg(15))
	require.NoError(t, rec.Run(ctx))

	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	assert.Equal(t, []dedup.PK{dedup.Int64PK(3), dedup.Int64PK(4)}, present(t, idx, 1, 2, 3, 4))
	assert.Equal(t, int64(0), segmentOf(t, idx, 1))
	assert.Equal(t, int64(0), segmentOf(t, idx, 2))
	assert.Equal(t, int64(8), segmentOf(t, idx, 3), "the insert of the transaction replaces the segment")
	assert.Equal(t, int64(9), segmentOf(t, idx, 4))
	assert.Equal(t, uint64(15), idx.LastObservedForTest())
}

func TestObserveReplaysVarCharPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_VarChar, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })

	rec.ObserveMessage(ctx, at(newVarCharInsert("v1", 7, "a", "b"), 11))
	require.NoError(t, rec.Run(ctx))

	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	keys := []dedup.PK{dedup.VarCharPK("a"), dedup.VarCharPK("b"), dedup.VarCharPK("c")}
	assert.Equal(t, keys[:2], presentPKs(t, idx, keys))
}

func TestObserveIgnoresACreateCollectionOfAnOpenedVChannel(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	rec := recoverV1(t, env, env.Manager.Scheduler())
	m := env.Manager.GetForTest("p")
	idx := m.IndexForTest("v1")
	require.False(t, idx.Ready())

	rec.ObserveMessage(ctx, createCollectionMsg("v1", 1, 11))
	assert.Same(t, idx, m.IndexForTest("v1"))
	assert.False(t, idx.Ready(), "the index waits for its install")
	rec.mu.Lock()
	targets := len(rec.targets)
	rec.mu.Unlock()
	assert.Equal(t, 1, targets)
}

func TestObserveAppliesReplicatedMessagesAndSkipsTheControlChannel(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	control := "p_" + funcutil.ControlChannelSuffix

	rec.ObserveMessage(ctx, withReplicate(newInsert("v1", 7, 1), 11))
	rec.ObserveMessage(ctx, createCollectionMsg(control, 2, 12))
	rec.ObserveMessage(ctx, dropCollectionMsg(control, 2, 13))
	require.NoError(t, rec.Run(ctx))

	m := env.Manager.GetForTest("p")
	assert.Equal(t, []dedup.PK{dedup.Int64PK(1)}, present(t, m.IndexForTest("v1"), 1), "the index of a secondary cluster follows its WAL")
	assert.Nil(t, m.IndexForTest(control))
}

func TestObserveCreatesAndDropsCollections(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec, err := Recover(ctx, env.Manager, Param{PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}}})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	m := env.Manager.GetForTest("p")

	rec.ObserveMessage(ctx, createCollectionMsg("v2", 2, 11))
	idx := m.IndexForTest("v2")
	require.NotNil(t, idx)
	assert.True(t, idx.Ready(), "a collection created in this term has no manifest")
	rec.ObserveMessage(ctx, insertMsg("v2", 12, 7, 1))
	assert.Equal(t, []dedup.PK{dedup.Int64PK(1)}, present(t, idx, 1))

	after := rec.ObserveMessage(ctx, dropCollectionMsg("v2", 2, 13))
	require.NotNil(t, after)
	assert.NotNil(t, m.IndexForTest("v2"), "the caller closes the index after the observe")
	after(ctx)
	assert.Nil(t, m.IndexForTest("v2"))
	assert.Nil(t, rec.ObserveMessage(ctx, dropCollectionMsg("v3", 3, 14)), "never seen: nothing happens")
	assert.Nil(t, rec.ObserveMessage(ctx, insertMsg("v2", 15, 7, 1)), "dropped: nothing happens")
	require.NoError(t, rec.Run(ctx))
}

// TestObserveClosesADroppedIndexAfterTheWritesBeforeTheDrop checks that an
// insert that the observe reaches after the drop was appended still finds
// the engine open: the observe, not the interceptor, closes the index, in
// the work that ObserveMessage returns for the drop, and the writes after
// the drop are skipped without stopping the observe.
func TestObserveClosesADroppedIndexAfterTheWritesBeforeTheDrop(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	var fatals []error
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}, Seed: 10, OnFatal: func(err error) { fatals = append(fatals, err) }},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	require.NoError(t, rec.Run(ctx))
	m := env.Manager.GetForTest("p")
	idx := m.IndexForTest("v1")

	// the interceptor appended the insert and then the drop; the observe sees
	// the insert first and the index is still open
	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	assert.Equal(t, int64(7), segmentOf(t, idx, 1))
	after := rec.ObserveMessage(ctx, dropCollectionMsg("v1", 1, 12))
	require.NotNil(t, after)
	after(ctx)
	assert.Nil(t, m.IndexForTest("v1"))
	rec.ObserveMessage(ctx, insertMsg("v1", 13, 7, 2)) // after the drop: skipped
	rec.ObserveMessage(ctx, timeTickMsg(14))
	assert.Empty(t, fatals)
	rec.ObserveMessage(ctx, createCollectionMsg("v2", 2, 15))
	assert.NotNil(t, m.IndexForTest("v2"), "the observe did not stop")
}

// TestObserveAppliesATransactionInOneBatch checks rule A for a transaction:
// the in-flight entries of its keys leave the set only after one engine
// batch holds every tombstone and every put of the transaction.
func TestObserveAppliesATransactionInOneBatch(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, rec.Run(ctx))
	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	a := idx.Decider()

	// the interceptor decided the transaction: pk 1 is deleted, pk 2 is inserted, both in flight at 12
	g, err := a.Decide(ctx, dedup.DecideRequest{Kind: dedup.KindDelete, PKs: []dedup.PK{dedup.Int64PK(1)}, TxnID: 100})
	require.NoError(t, err)
	g.Appended(12)
	g, err = a.Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: []dedup.PK{dedup.Int64PK(2)}, TxnID: 100})
	require.NoError(t, err)
	g.Appended(12)
	g, err = a.Decide(ctx, dedup.DecideRequest{Kind: dedup.KindCommitTxn, TxnID: 100})
	require.NoError(t, err)
	g.Appended(12)
	require.Equal(t, 2, dedup.InflightLenForTest(a))

	batches := 0
	idx.OnWriteForTest(func(muts []engine.Mutation) {
		batches++
		assert.Equal(t, 2, dedup.InflightLenForTest(a), "no entry leaves the set before the batch is written")
		require.Len(t, muts, 2)
		assert.True(t, muts[0].Delete, "the tombstone comes first")
		assert.False(t, muts[1].Delete)
	})
	rec.ObserveMessage(ctx, txnMsg(t, "v1", 12, 100, newInsert("v1", 8, 2), newDelete("v1", 1)))
	assert.Equal(t, 1, batches, "one batch for the whole transaction")
	assert.Equal(t, 0, dedup.InflightLenForTest(a), "both entries left after the batch")
	assert.Equal(t, int64(8), segmentOf(t, idx, 2))
	assert.Equal(t, int64(0), segmentOf(t, idx, 1))
}

func TestObserveKeepsTheFirstErrorAndRunReturnsIt(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, env.Manager.GetForTest("p").IndexForTest("v1").EngineForTest().Close())

	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	rec.ObserveMessage(ctx, insertMsg("v1", 12, 7, 2)) // after the first error: ignored
	err := rec.Run(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed), "got %v", err)
	rec.ObserveMessage(ctx, timeTickMsg(13))
	assert.Equal(t, uint64(10), env.Manager.GetForTest("p").LastAcked(), "nothing moves after the error")
}

// TestObserveAppliesBeforeAndAfterRun checks that the replay before the
// barrier and the live messages after it go through the same apply.
func TestObserveAppliesBeforeAndAfterRun(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	require.NoError(t, rec.Run(ctx))

	rec.ObserveMessage(ctx, insertMsg("v1", 12, 8, 2))
	rec.ObserveMessage(ctx, timeTickMsg(13))
	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	assert.Equal(t, int64(7), segmentOf(t, idx, 1))
	assert.Equal(t, int64(8), segmentOf(t, idx, 2), "the live message is applied by the same observe")
	assert.Equal(t, uint64(13), idx.LastObservedForTest())
}

// TestObserveAppliesTheDeleteOfATransactionBeforeItsPut checks the apply
// order of a transaction that the index built for an insert hit: the body
// order is the insert then its companion delete, and the insert must win.
func TestObserveAppliesTheDeleteOfATransactionBeforeItsPut(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	rec.ObserveMessage(ctx, txnMsg(t, "v1", 12, 100, newInsert("v1", 8, 1), newDelete("v1", 1)))
	rec.ObserveMessage(ctx, txnMsg(t, "v1", 13, 101, newInsert("v1", 9, 2), newInsert("v1", 10, 3), newDelete("v1", 2, 3)))
	require.NoError(t, rec.Run(ctx))

	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	assert.Equal(t, int64(8), segmentOf(t, idx, 1), "the put of the transaction survives its delete")
	assert.Equal(t, int64(9), segmentOf(t, idx, 2), "each insert keeps its own segment")
	assert.Equal(t, int64(10), segmentOf(t, idx, 3))
}

// TestObserveRetiresTheInFlightEntriesOfTheWrite checks rule A of the
// in-flight set: the keys of a write leave the set after the engine write,
// and only the keys of writes at or below the observed time tick.
func TestObserveRetiresTheInFlightEntriesOfTheWrite(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, rec.Run(ctx))
	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	a := idx.Decider()

	// the interceptor decided two writes and the WAL accepted them at 11 and 12
	g, err := a.Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: []dedup.PK{dedup.Int64PK(1)}})
	require.NoError(t, err)
	g.Appended(11)
	g, err = a.Decide(ctx, dedup.DecideRequest{Kind: dedup.KindInsert, PKs: []dedup.PK{dedup.Int64PK(2)}})
	require.NoError(t, err)
	g.Appended(12)
	require.Equal(t, 2, dedup.InflightLenForTest(a))

	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	assert.Equal(t, 1, dedup.InflightLenForTest(a), "the insert at 11 is applied, the insert at 12 is still in flight")
	assert.Equal(t, int64(7), segmentOf(t, idx, 1))
	rec.ObserveMessage(ctx, insertMsg("v1", 12, 8, 2))
	assert.Equal(t, 0, dedup.InflightLenForTest(a))
	assert.Equal(t, int64(8), segmentOf(t, idx, 2))
}

// TestObserveWritesTheEngineBeforeItMovesTheObservedTimeTick checks the
// order a handover relies on: a write is in the engine before the TimeTick
// after it moves the observed time tick, and a write alone does not move it.
func TestObserveWritesTheEngineBeforeItMovesTheObservedTimeTick(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, rec.Run(ctx))
	idx := env.Manager.GetForTest("p").IndexForTest("v1")

	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	assert.Equal(t, uint64(10), idx.LastObservedForTest(), "a write does not move the observed time tick")
	assert.Equal(t, int64(7), segmentOf(t, idx, 1), "but it is in the engine")
	rec.ObserveMessage(ctx, timeTickMsg(12))
	assert.Equal(t, uint64(12), idx.LastObservedForTest())
	p, err := idx.PrepareForTest(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, uint64(12), p.Covered(), "the handover covers the observed time tick it read before the rotate")
}

// TestObserveFlushRequestsACoveringHandover checks that the index observes
// a ManualFlush on its vchannel and a FlushAll on every vchannel, so a
// handover covers the flush.
func TestObserveFlushRequestsACoveringHandover(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	rec.ObserveMessage(ctx, createCollectionMsg("v2", 2, 11))
	require.NoError(t, rec.Run(ctx))
	m := env.Manager.GetForTest("p")

	rec.ObserveMessage(ctx, manualFlushMsg("v1", 12))
	assert.Equal(t, uint64(12), m.FlushTTForTest("v1"))
	assert.Zero(t, m.FlushTTForTest("v2"))
	rec.ObserveMessage(ctx, flushAllMsg("p", 13))
	assert.Equal(t, uint64(13), m.FlushTTForTest("v1"))
	assert.Equal(t, uint64(13), m.FlushTTForTest("v2"))
}

// recoverV1WithFatal is recoverV1 with an OnFatal handler.
func recoverV1WithFatal(t *testing.T, env *pkindex.FakeEnv, onFatal func(error)) *Recovery {
	ctx := context.Background()
	rec, err := Recover(ctx, env.Manager, Param{
		PChannelConfig: pkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 1}, Seed: 10, OnFatal: onFatal},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Sched:          env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	return rec
}

// TestObserveStopsOnAClosedIndexAndKeepsTheObservedTimeTick checks that a
// closed index after Run stops the observe without a fatal, and that the
// observed time tick does not move past the write it skipped.
func TestObserveStopsOnAClosedIndexAndKeepsTheObservedTimeTick(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	var fatals []error
	rec := recoverV1WithFatal(t, env, func(err error) { fatals = append(fatals, err) })
	require.NoError(t, rec.Run(ctx))
	m := env.Manager.GetForTest("p")
	require.NoError(t, m.IndexForTest("v1").EngineForTest().Close())

	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	rec.ObserveMessage(ctx, timeTickMsg(12))
	assert.Equal(t, uint64(10), m.LastObserved(), "a later rotate must not cover the write the observe skipped")
	assert.Empty(t, fatals, "a closed index means the WAL is closing")
}

// TestObserveCallsOnFatalOnAnEngineError checks that an engine error after
// Run goes to OnFatal with its class, counts in the metric, and stops the
// observe.
func TestObserveCallsOnFatalOnAnEngineError(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	var fatals []error
	rec := recoverV1WithFatal(t, env, func(err error) { fatals = append(fatals, err) })
	require.NoError(t, rec.Run(ctx))
	m := env.Manager.GetForTest("p")
	idx := m.IndexForTest("v1")
	counter := metrics.StreamingNodePKIndexFatalTotal.WithLabelValues(paramtable.GetStringNodeID(), "io")
	before := testutil.ToFloat64(counter)

	idx.FailWritesForTest(pkroot.MarkIO(errors.New("disk"), "write"))
	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	require.Len(t, fatals, 1)
	assert.True(t, errors.Is(fatals[0], pkroot.ErrIO), "got %v", fatals[0])
	assert.Equal(t, before+1, testutil.ToFloat64(counter))

	idx.FailWritesForTest(nil)
	rec.ObserveMessage(ctx, insertMsg("v1", 12, 7, 2))
	rec.ObserveMessage(ctx, timeTickMsg(13))
	assert.Equal(t, int64(0), segmentOf(t, idx, 2), "the observe stopped")
	assert.Equal(t, uint64(10), m.LastObserved())
	assert.Len(t, fatals, 1, "one fatal per WAL")
}

// TestCloseWipesTheVChannelAfterACorruptedWrite checks the cold start after
// a corruption error: Close removes the local directory of the vchannel.
func TestCloseWipesTheVChannelAfterACorruptedWrite(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	env := pkindex.NewFakeEnv(t, root)
	var fatals []error
	rec := recoverV1WithFatal(t, env, func(err error) { fatals = append(fatals, err) })
	require.NoError(t, rec.Run(ctx))
	idx := env.Manager.GetForTest("p").IndexForTest("v1")
	dir := index.Dir(env.Manager.Root(), "v1")
	_, err := os.Stat(dir)
	require.NoError(t, err)

	idx.FailWritesForTest(errors.Mark(errors.New("checksum"), pkroot.ErrCorrupted))
	rec.ObserveMessage(ctx, insertMsg("v1", 11, 7, 1))
	require.Len(t, fatals, 1)
	assert.True(t, errors.Is(fatals[0], pkroot.ErrCorrupted))
	rec.Close(ctx)
	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err), "the local index of the vchannel is gone: %v", err)
}

// TestObservePanicsOnAMalformedBody checks that a persisted body that does
// not unmarshal ends the process, like MustBody in the other modules.
func TestObservePanicsOnAMalformedBody(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	good := insertMsg("v1", 11, 7, 1)
	bad := message.NewImmutableMesasge(good.MessageID(), []byte("not a protobuf"), good.Properties().ToRawMap())

	require.Panics(t, func() { rec.ObserveMessage(ctx, bad) })
}
