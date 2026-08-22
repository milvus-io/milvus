package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newTestIdempotentCommittedInsertMessage(t *testing.T, vchannel string, key string, id int64) message.ImmutableMessage {
	t.Helper()
	return newTestIdempotentInsertMessage(t, vchannel, key, nil).
		WithTimeTick(uint64(id)).
		WithLastConfirmed(rmq.NewRmqID(id - 1)).
		IntoImmutableMessage(rmq.NewRmqID(id))
}

func newTestIdempotentInsertMessage(t *testing.T, vchannel string, key string, extra *messagespb.IdempotentInsertResult) message.MutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId: 1,
	}
	message.SetInsertHeaderIdempotentInsertResult(header, extra)
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{}).
		WithIdempotencyKey(key).
		MustBuildMutable()
}

// testReplicateHeader marks a message as replicated from another cluster.
func testReplicateHeader(msgID int64) *message.ReplicateHeader {
	return &message.ReplicateHeader{
		ClusterID:              "source-cluster",
		MessageID:              rmq.NewRmqID(msgID),
		LastConfirmedMessageID: rmq.NewRmqID(msgID - 1),
		TimeTick:               uint64(msgID),
		VChannel:               "v1",
	}
}

func TestSummaryRecordFromMessageWithIdempotency(t *testing.T) {
	extra := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 10}}},
		},
	}
	msg := newTestIdempotentInsertMessage(t, "v1", "key-1", extra).
		WithTimeTick(120).
		WithLastConfirmed(rmq.NewRmqID(119)).
		IntoImmutableMessage(rmq.NewRmqID(120))

	record, ok := newSummaryRecordFromMessage("p1", msg)
	require.True(t, ok)
	require.Equal(t, uint64(120), record.SourceTimeTick)
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageID).EQ(rmq.NewRmqID(120)))
	require.True(t, message.MustUnmarshalMessageID(record.LastConfirmedMessageID).EQ(rmq.NewRmqID(119)))
	require.Equal(t, "key-1", record.IdempotencyKey)
	require.Equal(t, []uint32{2, 0}, record.InsertResult.GetRowOffsets())
	require.Equal(t, []int64{11, 10}, record.InsertResult.GetIds().GetIntId().GetData())
}

func TestSummaryRecordFromTxnMessageWithIdempotency(t *testing.T) {
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(99)).
		IntoImmutableMessage(rmq.NewRmqID(100))
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	body1 := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-0"}}},
		},
	}).WithTxnContext(txnCtx).WithTimeTick(101).IntoImmutableMessage(rmq.NewRmqID(101))
	body2 := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-2", "pk-1"}}},
		},
	}).WithTxnContext(txnCtx).WithTimeTick(102).IntoImmutableMessage(rmq.NewRmqID(102))
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey("txn-key").
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(103).
		WithLastConfirmed(rmq.NewRmqID(102)).
		IntoImmutableMessage(rmq.NewRmqID(103))
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).Add(body1).Add(body2).Build(commitMsg)
	require.NoError(t, err)
	record, ok := newSummaryRecordFromMessage("p1", txnMsg)
	require.True(t, ok)
	require.Equal(t, "txn-key", record.IdempotencyKey)
	require.Equal(t, []uint32{0, 2, 1}, record.InsertResult.GetRowOffsets())
	require.Equal(t, []string{"pk-0", "pk-2", "pk-1"}, record.InsertResult.GetIds().GetStrId().GetData())
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageID).EQ(rmq.NewRmqID(103)))
	// A txn's record takes the commit message's position, so a duplicate commit
	// answers with the same one the first commit did.
	require.True(t, message.MustUnmarshalMessageID(record.LastConfirmedMessageID).EQ(rmq.NewRmqID(102)))
}

func TestSummaryRecordSkipsReplicatedIdempotencyKey(t *testing.T) {
	// A replicated insert preserves the SOURCE cluster's key AND insert result.
	// Both must be dropped: the local key history is independent of the source's,
	// and a source result could never be served as a local duplicate response.
	replicated := newTestIdempotentInsertMessage(t, "v1", "replicated-key", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).
		WithReplicateHeader(testReplicateHeader(5)).
		WithTimeTick(20).
		WithLastConfirmed(rmq.NewRmqID(19)).
		IntoImmutableMessage(rmq.NewRmqID(20))

	record, ok := newSummaryRecordFromMessage("p1", replicated)
	require.True(t, ok)
	require.Empty(t, record.IdempotencyKey)
	require.Nil(t, record.InsertResult)
}

func TestSummaryRecordSkipsReplicatedTxnCommitKey(t *testing.T) {
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(99)).
		IntoImmutableMessage(rmq.NewRmqID(100))
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	// The replicated body carries the SOURCE cluster's insert result in its
	// header, just like the commit carries the source's key.
	body := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).
		WithReplicateHeader(testReplicateHeader(101)).
		WithTxnContext(txnCtx).WithTimeTick(101).IntoImmutableMessage(rmq.NewRmqID(101))
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey("txn-key").
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithReplicateHeader(testReplicateHeader(102)).
		WithTimeTick(103).
		WithLastConfirmed(rmq.NewRmqID(102)).
		IntoImmutableMessage(rmq.NewRmqID(103))
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).Add(body).Build(commitMsg)
	require.NoError(t, err)

	record, ok := newSummaryRecordFromMessage("p1", txnMsg)
	require.True(t, ok)
	require.Empty(t, record.IdempotencyKey)
	require.Nil(t, record.InsertResult)
}

func TestSummaryRecordWithoutKeyCarriesNoInsertResult(t *testing.T) {
	extra := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-1"}}},
		},
	}
	msg := newTestIdempotentInsertMessage(t, "v1", "", extra).
		WithTimeTick(121).
		WithLastConfirmed(rmq.NewRmqID(120)).
		IntoImmutableMessage(rmq.NewRmqID(121))

	record, ok := newSummaryRecordFromMessage("p1", msg)
	require.True(t, ok)
	// An insert result without a key could never be served, so the record drops
	// it instead of carrying rows nothing will read. (The append path rejects a
	// result without a key in the first place; this pins the builder's own rule.)
	require.Empty(t, record.IdempotencyKey)
	require.Nil(t, record.InsertResult)
}

func TestSummaryRecordSizeIsDominatedByPrimaryKeys(t *testing.T) {
	// The byte budgets that bound the staging buffer and the dedup window are
	// spent almost entirely on primary keys, so the estimate has to track them.
	small := newTestSummaryRecord("key-1", 10, 1)
	large := newTestSummaryRecord("key-1", 10, make([]int64, 512)...)
	require.Greater(t, large.Size(), 8*small.Size())
}
