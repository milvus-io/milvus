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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
func present(t *testing.T, idx *index.Index, pks ...int64) []authority.PK {
	keys := make([]authority.PK, 0, len(pks))
	for _, pk := range pks {
		keys = append(keys, authority.Int64PK(pk))
	}
	return presentPKs(t, idx, keys)
}

func presentPKs(t *testing.T, idx *index.Index, keys []authority.PK) []authority.PK {
	ctx := context.Background()
	require.True(t, idx.Ready())
	g, err := idx.Authority().Decide(ctx, authority.DecideRequest{Kind: authority.KindDelete, VChannel: idx.VChannel(), PKs: keys})
	require.NoError(t, err)
	defer g.Discard(ctx, nil)
	return g.Decision().PresentPKs
}

// segmentOf returns the segment of pk in the engine of idx, 0 when the
// engine has no record or a tombstone of pk.
func segmentOf(t *testing.T, idx *index.Index, pk int64) int64 {
	vs, err := idx.EngineForTest().MultiGet(context.Background(), [][]byte{authority.Int64PK(pk).Encode()})
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
		Info:  types.PChannelInfo{Name: "p", Term: 1},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_Int64, false)},
		Seed:  10,
		Sched: sched,
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	return rec
}

func TestObserveReplaysLikeTheAppendPath(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())

	rec.Observe(ctx, insertMsg("v1", 11, 7, 1, 2, 3))
	rec.Observe(ctx, deleteMsg("v1", 12, 2))
	// An insert of a key that exists is a transaction in the WAL: its
	// companion delete, then the insert.
	rec.Observe(ctx, txnMsg(t, "v1", 13, 100, newDelete("v1", 3), newInsert("v1", 8, 3)))
	rec.Observe(ctx, txnMsg(t, "v1", 14, 101, newDelete("v1", 1), newInsert("v1", 9, 4)))
	rec.Observe(ctx, timeTickMsg(15))
	require.NoError(t, rec.Finish(ctx))

	idx := env.Manager.Get("p").IndexForTest("v1")
	assert.Equal(t, []authority.PK{authority.Int64PK(3), authority.Int64PK(4)}, present(t, idx, 1, 2, 3, 4))
	assert.Equal(t, int64(0), segmentOf(t, idx, 1))
	assert.Equal(t, int64(0), segmentOf(t, idx, 2))
	assert.Equal(t, int64(8), segmentOf(t, idx, 3), "the insert of the transaction replaces the segment")
	assert.Equal(t, int64(9), segmentOf(t, idx, 4))
	assert.Equal(t, uint64(15), idx.LastAppliedTimeTickForTest())
}

func TestObserveReplaysVarCharPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec, err := Recover(ctx, env.Manager, Param{
		Info:  types.PChannelInfo{Name: "p", Term: 1},
		Metas: map[string]*streamingpb.VChannelMeta{"v1": testMeta("v1", 1, schemapb.DataType_VarChar, false)},
		Sched: env.Manager.Scheduler(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })

	rec.Observe(ctx, at(newVarCharInsert("v1", 7, "a", "b"), 11))
	require.NoError(t, rec.Finish(ctx))

	idx := env.Manager.Get("p").IndexForTest("v1")
	keys := []authority.PK{authority.VarCharPK("a"), authority.VarCharPK("b"), authority.VarCharPK("c")}
	assert.Equal(t, keys[:2], presentPKs(t, idx, keys))
}

func TestObserveIgnoresACreateCollectionOfAnOpenedVChannel(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	rec := recoverV1(t, env, env.Manager.Scheduler())
	m := env.Manager.Get("p")
	idx := m.IndexForTest("v1")
	require.False(t, idx.Ready())

	rec.Observe(ctx, createCollectionMsg("v1", 1, 11))
	assert.Same(t, idx, m.IndexForTest("v1"))
	assert.False(t, idx.Ready(), "the index waits for its install")
	rec.mu.Lock()
	targets := len(rec.targets)
	rec.mu.Unlock()
	assert.Equal(t, 1, targets)
}

func TestObserveSkipsWritesTheManifestCovers(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	sched := nodescheduler.New(1)
	t.Cleanup(sched.Close)
	rec := recoverV1(t, env, sched)
	idx := env.Manager.Get("p").IndexForTest("v1")
	require.False(t, idx.Ready())

	rec.Observe(ctx, insertMsg("v1", 100, 7, 1))
	rec.Observe(ctx, insertMsg("v1", 101, 7, 2))
	assert.True(t, idx.Dirty(), "the write above the checkpoint is applied")
	require.NoError(t, rec.Finish(ctx))

	select {
	case <-idx.ReadyForTest():
	case <-time.After(10 * time.Second):
		t.Fatal("the install did not mark the index ready")
	}
	assert.Equal(t, []authority.PK{authority.Int64PK(2)}, present(t, idx, 1, 2))
}

func TestObserveSkipsReplicatedMessagesAndTheControlChannel(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	control := "p_" + funcutil.ControlChannelSuffix

	rec.Observe(ctx, withReplicate(newInsert("v1", 7, 1), 11))
	rec.Observe(ctx, createCollectionMsg(control, 2, 12))
	rec.Observe(ctx, dropCollectionMsg(control, 2, 13))
	require.NoError(t, rec.Finish(ctx))

	m := env.Manager.Get("p")
	assert.Empty(t, present(t, m.IndexForTest("v1"), 1))
	assert.Nil(t, m.IndexForTest(control))
}

func TestObserveCreatesAndDropsCollections(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec, err := Recover(ctx, env.Manager, Param{Info: types.PChannelInfo{Name: "p", Term: 1}, Sched: env.Manager.Scheduler()})
	require.NoError(t, err)
	t.Cleanup(func() { rec.Close(ctx) })
	m := env.Manager.Get("p")

	rec.Observe(ctx, createCollectionMsg("v2", 2, 11))
	idx := m.IndexForTest("v2")
	require.NotNil(t, idx)
	assert.True(t, idx.Ready(), "a collection created in this term has no manifest")
	rec.Observe(ctx, insertMsg("v2", 12, 7, 1))
	assert.Equal(t, []authority.PK{authority.Int64PK(1)}, present(t, idx, 1))

	rec.Observe(ctx, dropCollectionMsg("v2", 2, 13))
	assert.Nil(t, m.IndexForTest("v2"))
	rec.Observe(ctx, dropCollectionMsg("v3", 3, 14)) // never seen: nothing happens
	rec.Observe(ctx, insertMsg("v2", 15, 7, 1))      // dropped: nothing happens
	require.NoError(t, rec.Finish(ctx))
}

func TestObserveKeepsTheFirstErrorAndFinishReturnsIt(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, env.Manager.Get("p").IndexForTest("v1").EngineForTest().Close())

	rec.Observe(ctx, insertMsg("v1", 11, 7, 1))
	rec.Observe(ctx, insertMsg("v1", 12, 7, 2)) // after the first error: ignored
	err := rec.Finish(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed), "got %v", err)
	rec.Observe(ctx, timeTickMsg(13))
	assert.Equal(t, uint64(10), env.Manager.Get("p").LastAcked(), "nothing moves after the error")
}

func TestObserveAfterFinishIsIgnored(t *testing.T) {
	ctx := context.Background()
	env := pkindex.NewFakeEnv(t, t.TempDir())
	rec := recoverV1(t, env, env.Manager.Scheduler())
	require.NoError(t, rec.Finish(ctx))

	rec.Observe(ctx, insertMsg("v1", 11, 7, 1))
	rec.Observe(ctx, timeTickMsg(12))
	idx := env.Manager.Get("p").IndexForTest("v1")
	assert.Empty(t, present(t, idx, 1))
	assert.Equal(t, uint64(10), idx.LastAppliedTimeTickForTest())
}
