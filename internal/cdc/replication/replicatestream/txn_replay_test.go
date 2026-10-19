package replicatestream

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTxnPredecessorsSurviveReconnectAndReconstruction(t *testing.T) {
	build := func() message.ImmutableTxnMessage {
		txnCtx := message.TxnContext{TxnID: 42, Keepalive: message.TxnKeepaliveInfinite}
		seal := func(msg message.MutableMessage, id int64) message.ImmutableMessage {
			return msg.WithTxnContext(txnCtx).WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(id))
		}
		begin := seal(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), 1)
		builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
		// Source TT order may differ from physical MessageID order.
		builder.Add(seal(message.CreateTestEmptyInsertMesage(1, nil), 3))
		builder.Add(seal(message.CreateTestEmptyInsertMesage(1, nil), 2))
		commit := seal(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable(), 4)
		result, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		require.NoError(t, err)
		return result
	}
	type link struct{ id, previous string }
	var sent []link
	failBody := true
	patch := mockey.Mock((*replicateStreamClient).sendMessageWithPrevious).To(func(_ *replicateStreamClient, msg message.ImmutableMessage, previous message.MessageID) error {
		sent = append(sent, link{msg.MessageID().Marshal(), previous.Marshal()})
		if failBody && msg.MessageID().EQ(walimplstest.NewTestMessageID(2)) {
			return context.Canceled
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	client := &replicateStreamClient{}
	txn := build()
	require.ErrorIs(t, client.sendTxnMessage(txn), context.Canceled)
	require.Len(t, sent, 3, "do not send Commit after a failed body")
	partial := append([]link(nil), sent...)
	sent = nil
	failBody = false
	require.NoError(t, client.sendTxnMessage(txn))
	expected := []link{{"1", "1"}, {"3", "1"}, {"2", "3"}, {"4", "2"}}
	require.Equal(t, expected, sent)
	require.Equal(t, expected[:3], partial)
	sent = nil
	require.NoError(t, (&replicateStreamClient{}).sendTxnMessage(build()))
	require.Equal(t, expected, sent, "CDC restart must reconstruct the same predecessor chain")
}
