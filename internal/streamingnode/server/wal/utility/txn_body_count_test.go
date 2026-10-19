package utility

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTxnCountAfterPhysicalDedupAndRecovery(t *testing.T) {
	for _, expected := range []uint64{0, 1, 2} {
		for _, replicated := range []bool{false, true} {
			seal := func(msg message.MutableMessage, id int64, tt uint64) message.ImmutableMessage {
				if replicated {
					msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: walimplstest.NewTestMessageID(id), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 100, VChannel: "v1"})
				}
				return msg.WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}).
					WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(id))
			}
			begin := seal(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), 1, 1)
			body := seal(message.CreateTestEmptyInsertMesage(1, nil), 2, 2)
			// An ambiguous backend retry may persist twice with different local
			// MessageIDs but identical properties (including the allocated TT).
			duplicate := message.NewImmutableMesasge(walimplstest.NewTestMessageID(3), body.Payload(), body.Properties().ToRawMap())
			reorder := NewReOrderBuffer()
			_, err := reorder.Push(begin)
			require.NoError(t, err)
			_, err = reorder.Push(body)
			require.NoError(t, err)
			result, err := reorder.Push(duplicate)
			require.NoError(t, err)
			require.True(t, result.Dropped)
			buffer := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
			require.Empty(t, buffer.HandleImmutableMessages(reorder.PopUtilTimeTick(2), 2))
			buffer = buffer.Snapshot() // Resume from the durable logical bodies.
			commit := seal(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: expected}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable(), 4, 3)
			if expected == 2 {
				require.Panics(t, func() { buffer.HandleImmutableMessages([]message.ImmutableMessage{commit}, 3) })
				require.Len(t, buffer.GetUncommittedMessageBuilder(), 1, "integrity failure must not discard the builder")
			} else {
				out := buffer.HandleImmutableMessages([]message.ImmutableMessage{commit}, 3)
				require.Len(t, out, 1)
				require.Equal(t, 1, message.AsImmutableTxnMessage(out[0]).Size())
			}
		}
	}
}
