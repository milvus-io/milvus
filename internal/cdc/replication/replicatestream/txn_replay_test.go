package replicatestream

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTxnBodyCountSurvivesReconnectAndReconstruction(t *testing.T) {
	build := func(bodyCnt uint64) message.ImmutableTxnMessage {
		txnCtx := message.TxnContext{TxnID: 42, Keepalive: message.TxnKeepaliveInfinite}
		seal := func(msg message.MutableMessage, id int64) message.ImmutableMessage {
			return msg.WithTxnContext(txnCtx).WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(id))
		}
		begin := seal(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), 1)
		builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
		// Source TT order may differ from physical MessageID order.
		builder.Add(seal(message.CreateTestEmptyInsertMesage(1, nil), 3))
		builder.Add(seal(message.CreateTestEmptyInsertMesage(1, nil), 2))
		commit := seal(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: bodyCnt}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable(), 4)
		result, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		require.NoError(t, err)
		return result
	}
	type sentMessage struct {
		id    string
		count uint64
	}
	var sent []sentMessage
	failBody := true
	patch := mockey.Mock((*replicateStreamClient).sendMessage).To(func(_ *replicateStreamClient, msg message.ImmutableMessage) error {
		var count uint64
		if msg.MessageType() == message.MessageTypeCommitTxn {
			count = message.MustAsImmutableCommitTxnMessageV2(msg).Header().BodyCnt
		}
		sent = append(sent, sentMessage{msg.MessageID().Marshal(), count})
		if failBody && msg.MessageID().EQ(walimplstest.NewTestMessageID(2)) {
			return context.Canceled
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	client := &replicateStreamClient{}
	txn := build(0)
	require.ErrorIs(t, client.sendTxnMessage(txn), context.Canceled)
	require.Len(t, sent, 3, "do not send Commit after a failed body")
	partial := append([]sentMessage(nil), sent...)
	sent = nil
	failBody = false
	require.NoError(t, client.sendTxnMessage(txn))
	expected := []sentMessage{{"1", 0}, {"3", 0}, {"2", 0}, {"4", 0}}
	require.Equal(t, expected, sent)
	require.Zero(t, message.MustAsImmutableCommitTxnMessageV2(txn.Commit()).Header().BodyCnt, "legacy source Commit must stay unchanged")
	require.Equal(t, expected[:3], partial)
	sent = nil
	require.NoError(t, (&replicateStreamClient{}).sendTxnMessage(build(0)))
	require.Equal(t, expected, sent, "CDC restart must preserve the legacy zero count")
	sent = nil
	counted := build(2)
	require.NoError(t, client.sendTxnMessage(counted))
	expected[3].count = 2
	require.Equal(t, expected, sent)
	require.EqualValues(t, 2, message.MustAsImmutableCommitTxnMessageV2(counted.Commit()).Header().BodyCnt)
}
