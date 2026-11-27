package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTxnBodyCountWireRoundTrip(t *testing.T) {
	id := walimplstest.NewTestMessageID(10)
	source := message.MustAsImmutableCommitTxnMessageV2(message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("v1").MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).
		WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id))
	replicated := message.MustNewReplicateMessage("source", source.IntoImmutableMessageProto())
	persisted := replicated.WithTimeTick(200).WithLastConfirmed(id).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	reloaded := message.NewImmutableMesasge(persisted.MessageID(), persisted.Payload(), persisted.Properties().ToRawMap())
	require.EqualValues(t, 2, message.MustAsImmutableCommitTxnMessageV2(reloaded).Header().BodyCnt)
	require.EqualValues(t, 100, reloaded.ReplicateHeader().TimeTick)
}
