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

package txn

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const fakeTxnID = message.TxnID(7)

type fakeAppender struct {
	types    []message.MessageType
	appended []message.MutableMessage
	ids      []message.MessageID // nil for a failed call
	txnIDs   []message.TxnID     // 0 when the message carries no txn context
	failAt   int                 // fail the n-th call (0-based). -1 never
	failErr  error
	nextTT   uint64
}

func newFakeAppender() *fakeAppender {
	return &fakeAppender{failAt: -1, nextTT: 100}
}

func (f *fakeAppender) append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	idx := len(f.types)
	f.types = append(f.types, msg.MessageType())
	f.appended = append(f.appended, msg)
	if idx == f.failAt {
		f.txnIDs = append(f.txnIDs, 0)
		f.ids = append(f.ids, nil)
		return nil, f.failErr
	}
	f.nextTT++
	msg.WithTimeTick(f.nextTT).WithLastConfirmedUseMessageID()
	if msg.MessageType() == message.MessageTypeBeginTxn {
		msg.WithTxnContext(message.TxnContext{TxnID: fakeTxnID, Keepalive: time.Second})
	}
	if txnCtx := msg.TxnContext(); txnCtx != nil {
		f.txnIDs = append(f.txnIDs, txnCtx.TxnID)
	} else {
		f.txnIDs = append(f.txnIDs, 0)
	}
	// the timetick interceptor publishes the txn context of every appended message.
	if utility.GetExtraAppendResult(ctx) != nil {
		utility.ReplaceAppendResultTxnContext(ctx, msg.TxnContext())
	}
	id := walimplstest.NewTestMessageID(int64(f.nextTT))
	f.ids = append(f.ids, id)
	return id, nil
}

// immutableAt returns the appended form of the n-th message.
func (f *fakeAppender) immutableAt(idx int) message.ImmutableMessage {
	return f.appended[idx].IntoImmutableMessage(f.ids[idx])
}

// newTestAppendContext returns the context an append chain provides.
func newTestAppendContext() (context.Context, *utility.ExtraAppendResult) {
	result := &utility.ExtraAppendResult{}
	return utility.WithExtraAppendResult(context.Background(), result), result
}

func newTestDelete() message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			NumRows:      1,
			Timestamps:   []uint64{0},
		}).
		MustBuildMutable()
}

func newTestKeyedInsert(key message.IdempotencyKey) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1, BinarySize: 1024}},
		}).
		WithBody(&msgpb.InsertRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}}).
		WithIdempotencyKey(key).
		MustBuildMutable()
}

// assembleTestTxn builds the transaction message that a scanner assembles from
// the appended group. That is what an observer of the wal sees, and what the
// durable idempotency window is rebuilt from.
func assembleTestTxn(t *testing.T, f *fakeAppender) message.ImmutableTxnMessage {
	t.Helper()
	require.GreaterOrEqual(t, len(f.appended), 2)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(f.immutableAt(0)))
	for idx := 1; idx < len(f.appended)-1; idx++ {
		builder.Add(f.immutableAt(idx))
	}
	txnMsg, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(f.immutableAt(len(f.appended) - 1)))
	require.NoError(t, err)
	return txnMsg
}

func newTestCommit() message.MutableMessage {
	return message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{TxnID: 42, Keepalive: time.Second})
}

func newTestManualFlush() message.MutableMessage {
	return message.NewManualFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
}

func withTestReplicateHeader(msg message.MutableMessage) message.MutableMessage {
	return msg.WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "primary",
		MessageID:              walimplstest.NewTestMessageID(1),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
		TimeTick:               1,
		VChannel:               "v1",
	})
}

func requireInner(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.NotNil(t, streamingErr)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INNER, streamingErr.Code)
}

func TestAppendInTxnAutocommit(t *testing.T) {
	f := newFakeAppender()
	main := message.CreateTestEmptyInsertMesage(1, nil)
	ctx, appendResult := newTestAppendContext()

	result, err := AppendInTxn(ctx, f.append, main, []message.MutableMessage{newTestDelete()})
	require.NoError(t, err)

	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f.types)
	require.Equal(t, []message.TxnID{fakeTxnID, fakeTxnID, fakeTxnID, fakeTxnID}, f.txnIDs)

	// the caller's message is left untouched.
	require.Nil(t, main.TxnContext())
	// the client appended one autocommit message, so it must not see a transaction.
	require.Nil(t, appendResult.TxnCtx)

	require.Equal(t, message.MessageTypeInsert, result.Main.MessageType())
	require.Equal(t, fakeTxnID, result.Main.TxnContext().TxnID)
	require.Equal(t, message.MessageTypeCommitTxn, result.Commit.MessageType())
	require.Greater(t, result.Commit.TimeTick(), result.Main.TimeTick())
}

// TestAppendInTxnAutocommitCarriesIdempotencyKey checks that the self-built
// CommitTxn carries the key of main. The durable idempotency window reads the
// key of an assembled transaction from its CommitTxn, so a commit without the
// key makes a retry after a wal reopen write the rows a second time.
func TestAppendInTxnAutocommitCarriesIdempotencyKey(t *testing.T) {
	key := message.NewCollectionScopedIdempotencyKey(1, "key-1")
	f := newFakeAppender()
	main := newTestKeyedInsert(key)
	ctx, _ := newTestAppendContext()

	result, err := AppendInTxn(ctx, f.append, main, []message.MutableMessage{newTestDelete()})
	require.NoError(t, err)
	require.Equal(t, key, message.IdempotencyKeyOf(main))
	require.Equal(t, message.IdempotencyKeyOf(main), message.IdempotencyKeyOf(result.Commit))
	// the copy of main keeps the key, like every body of a client transaction.
	require.Equal(t, key, message.IdempotencyKeyOf(result.Main))

	require.Equal(t, key, message.IdempotencyKeyOf(assembleTestTxn(t, f).Commit()))
}

func TestAppendInTxnAutocommitWithoutIdempotencyKey(t *testing.T) {
	f := newFakeAppender()
	main := message.CreateTestEmptyInsertMesage(1, nil)
	ctx, _ := newTestAppendContext()

	result, err := AppendInTxn(ctx, f.append, main, []message.MutableMessage{newTestDelete()})
	require.NoError(t, err)
	require.Empty(t, message.IdempotencyKeyOf(result.Commit))
	require.Empty(t, message.IdempotencyKeyOf(assembleTestTxn(t, f).Commit()))
}

func TestAppendInTxnAutocommitKeepsBarrierTimeTick(t *testing.T) {
	f := newFakeAppender()
	main := message.CreateTestEmptyInsertMesage(1, nil).WithBarrierTimeTick(999)
	ctx, _ := newTestAppendContext()

	var beginBarrier uint64
	appendOp := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		if msg.MessageType() == message.MessageTypeBeginTxn {
			beginBarrier = msg.BarrierTimeTick()
		}
		return f.append(ctx, msg)
	}
	_, err := AppendInTxn(ctx, appendOp, main, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(999), beginBarrier, "the begin message must wait for the barrier of the main message")
}

func TestAppendInTxnAutocommitRollsBackOnFailure(t *testing.T) {
	boom := errors.New("boom")
	for _, c := range []struct {
		name     string
		failAt   int
		expected []message.MessageType
	}{
		{"begin fails without a session, nothing to roll back", 0, []message.MessageType{message.MessageTypeBeginTxn}},
		{"main fails", 1, []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeInsert, message.MessageTypeRollbackTxn}},
		{"extra fails", 2, []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeRollbackTxn}},
		{"commit fails", 3, []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeCommitTxn, message.MessageTypeRollbackTxn}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeAppender()
			f.failAt, f.failErr = c.failAt, boom
			main := message.CreateTestEmptyInsertMesage(1, nil)
			ctx, _ := newTestAppendContext()

			result, err := AppendInTxn(ctx, f.append, main, []message.MutableMessage{newTestDelete()})
			require.Nil(t, result)
			require.ErrorIs(t, err, boom)
			require.Equal(t, c.expected, f.types)
			require.Nil(t, main.TxnContext())
			if last := len(f.types) - 1; f.types[last] == message.MessageTypeRollbackTxn {
				require.Equal(t, fakeTxnID, f.txnIDs[last])
			}
		})
	}
}

func TestAppendInTxnAutocommitRollsBackWhenBeginSessionExists(t *testing.T) {
	boom := errors.New("boom")
	var types []message.MessageType
	appendOp := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		types = append(types, msg.MessageType())
		if msg.MessageType() == message.MessageTypeBeginTxn {
			// the session is created before the begin message is written, so it
			// outlives a failed append.
			msg.WithTxnContext(message.TxnContext{TxnID: fakeTxnID, Keepalive: time.Second})
			return nil, boom
		}
		return walimplstest.NewTestMessageID(1), nil
	}
	ctx, _ := newTestAppendContext()

	_, err := AppendInTxn(ctx, appendOp, message.CreateTestEmptyInsertMesage(1, nil), nil)
	require.ErrorIs(t, err, boom)
	require.Equal(t, []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeRollbackTxn}, types)
}

func TestAppendInTxnAutocommitTurnsTxnErrorIntoInner(t *testing.T) {
	f := newFakeAppender()
	f.failAt, f.failErr = 2, status.NewTransactionExpired("x")
	ctx, _ := newTestAppendContext()

	_, err := AppendInTxn(ctx, f.append, message.CreateTestEmptyInsertMesage(1, nil), []message.MutableMessage{newTestDelete()})
	// the client did not ask for a transaction, so the append stays retriable.
	requireInner(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeRollbackTxn,
	}, f.types)
}

// The txn manager refuses BeginTxn with a transaction error while it closes.
func TestAppendInTxnAutocommitTurnsRefusedBeginIntoInner(t *testing.T) {
	f := newFakeAppender()
	f.failAt, f.failErr = 0, status.NewTransactionExpired("manager closed")
	ctx, _ := newTestAppendContext()

	_, err := AppendInTxn(ctx, f.append, message.CreateTestEmptyInsertMesage(1, nil), nil)
	requireInner(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeBeginTxn}, f.types)
}

func TestAppendInTxnRollbackFailureKeepsOriginalError(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	appendOp := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		calls++
		switch msg.MessageType() {
		case message.MessageTypeBeginTxn:
			msg.WithTxnContext(message.TxnContext{TxnID: fakeTxnID, Keepalive: time.Second})
			return walimplstest.NewTestMessageID(1), nil
		case message.MessageTypeInsert:
			return nil, boom
		default:
			return nil, errors.New("rollback failed")
		}
	}
	ctx, _ := newTestAppendContext()
	_, err := AppendInTxn(ctx, appendOp, message.CreateTestEmptyInsertMesage(1, nil), nil)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 3, calls)
}

func TestAppendInTxnCommit(t *testing.T) {
	f := newFakeAppender()
	commit := newTestCommit()
	ctx, appendResult := newTestAppendContext()

	result, err := AppendInTxn(ctx, f.append, commit, []message.MutableMessage{newTestDelete()})
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f.types)
	require.Equal(t, []message.TxnID{42, 42}, f.txnIDs)
	require.Equal(t, message.MessageTypeCommitTxn, result.Main.MessageType())
	require.Equal(t, result.Main.MessageID(), result.Commit.MessageID())
	// the transaction belongs to the client, so it keeps its txn context.
	require.NotNil(t, appendResult.TxnCtx)
}

func TestAppendInTxnCommitExtraFailure(t *testing.T) {
	expired := status.NewTransactionExpired("x")
	f := newFakeAppender()
	f.failAt, f.failErr = 0, expired
	ctx, _ := newTestAppendContext()

	_, err := AppendInTxn(ctx, f.append, newTestCommit(), []message.MutableMessage{newTestDelete()})
	// the transaction belongs to the client, so it gets the original error.
	require.Equal(t, expired, err)
	// neither the commit nor a rollback is written.
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)
}

func TestAppendInTxnRejectsInvalidInput(t *testing.T) {
	insert := func() message.MutableMessage { return message.CreateTestEmptyInsertMesage(1, nil) }
	otherVChannel := message.NewDeleteMessageBuilderV1().WithVChannel("v2").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).WithBody(&msgpb.DeleteRequest{}).MustBuildMutable()

	for _, c := range []struct {
		name   string
		main   message.MutableMessage
		extras []message.MutableMessage
	}{
		{"nil main", nil, nil},
		{"main already in a transaction", insert().WithTxnContext(message.TxnContext{TxnID: 1}), nil},
		{"main is a begin message", message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), nil},
		{"main is not a dml message", newTestManualFlush(), nil},
		{"main is replicated", withTestReplicateHeader(insert()), nil},
		{"extra is nil", insert(), []message.MutableMessage{nil}},
		{"extra is not a dml message", insert(), []message.MutableMessage{newTestManualFlush()}},
		{"extra is replicated", insert(), []message.MutableMessage{withTestReplicateHeader(newTestDelete())}},
		{"extra already in a transaction", insert(), []message.MutableMessage{newTestDelete().WithTxnContext(message.TxnContext{TxnID: 1})}},
		{"extra of another vchannel", insert(), []message.MutableMessage{otherVChannel}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeAppender()
			_, err := AppendInTxn(context.Background(), f.append, c.main, c.extras)
			requireInner(t, err)
			require.Empty(t, f.types, "nothing may be appended when the input is invalid")
		})
	}
}
