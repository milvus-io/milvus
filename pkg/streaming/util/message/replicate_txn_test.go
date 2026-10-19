package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestReplicateTxnPredecessorRoundTrip(t *testing.T) {
	id := walimplstest.NewTestMessageID(10)
	source := message.NewBeginTxnMessageBuilderV2().
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		WithVChannel("p0_v0").MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).
		WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id)
	wire := message.WithReplicateTxnPrevious(source, id)
	require.False(t, source.Properties().Exist("_rtprev"), "CDC must not change its retained source transaction")
	replicated, err := message.NewReplicateMessage("source", wire)
	require.NoError(t, err)
	require.True(t, id.EQ(replicated.ReplicateHeader().PrevTxnMessageID))
	require.False(t, replicated.Properties().Exist("_rtprev"))
	persisted := replicated.WithTimeTick(200).WithLastConfirmed(id).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	reloaded := message.NewImmutableMesasge(persisted.MessageID(), persisted.Payload(), persisted.Properties().ToRawMap())
	require.True(t, id.EQ(reloaded.ReplicateHeader().PrevTxnMessageID))
	require.Equal(t, uint64(100), reloaded.ReplicateHeader().TimeTick)
	legacy, err := message.NewReplicateMessage("source", source.IntoImmutableMessageProto())
	require.NoError(t, err)
	require.Nil(t, legacy.ReplicateHeader().PrevTxnMessageID)
}
