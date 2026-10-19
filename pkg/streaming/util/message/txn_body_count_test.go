package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTxnBodyCountWireCopy(t *testing.T) {
	id := walimplstest.NewTestMessageID(10)
	source := message.MustAsImmutableCommitTxnMessageV2(message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("v1").MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).
		WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id))
	copy := message.WithTxnBodyCount(source, 2)
	require.Zero(t, source.Header().BodyCnt, "must not mutate the cached source header")
	require.Zero(t, message.MustAsImmutableCommitTxnMessageV2(message.NewImmutableMesasge(id, source.Payload(), source.Properties().ToRawMap())).Header().BodyCnt)
	replicated := message.MustNewReplicateMessage("source", copy.IntoImmutableMessageProto())
	persisted := replicated.WithTimeTick(200).WithLastConfirmed(id).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	reloaded := message.NewImmutableMesasge(persisted.MessageID(), persisted.Payload(), persisted.Properties().ToRawMap())
	require.EqualValues(t, 2, message.MustAsImmutableCommitTxnMessageV2(reloaded).Header().BodyCnt)
	require.EqualValues(t, 100, reloaded.ReplicateHeader().TimeTick)
}

func TestTxnBuilderBodyCount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected uint64
		actual   int
		valid    bool
	}{
		{"complete", 2, 2, true},
		{"missing", 2, 1, false},
		{"extra", 1, 2, false},
		{"empty", 0, 0, true},
		{"legacy_nonempty", 0, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seal := func(msg message.MutableMessage, id int64) message.ImmutableMessage {
				return msg.WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}).
					WithTimeTick(uint64(id)).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(id))
			}
			begin := seal(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), 1)
			builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
			for i := 0; i < tc.actual; i++ {
				builder.Add(seal(message.CreateTestEmptyInsertMesage(1, nil), int64(i+2)))
			}
			commit := seal(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: tc.expected}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable(), 100)
			txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, tc.actual, txn.Size())
			} else {
				require.ErrorContains(t, err, "body count mismatch")
				require.Nil(t, txn)
				_, bodies := builder.Messages()
				require.Len(t, bodies, tc.actual, "rejected build must preserve the recovery state")
			}
		})
	}
}
