package replicates

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// Exercise the real replication admission, consumer buffer and recovery together.
// Connections deliberately overlap; only the complete source prefix may commit.
func TestLocalFlushReplayAndRecovery(t *testing.T) {
	for _, operation := range []string{"ManualFlush", "FlushAll", "CreateSnapshot"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			buffer := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
			checkpoint := &utility.ReplicateCheckpoint{ClusterID: "test2", PChannel: "test2-rootcoord-dml_0", MessageID: walimplstest.NewTestMessageID(0), TimeTick: 1}
			recoverManager := func() ReplicatesManager {
				rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
					ChannelInfo: types.PChannelInfo{Name: "test1-rootcoord-dml_0", Term: 1}, CurrentClusterID: "test1",
					InitialRecoverSnapshot: newReplicateRecoverySnapshot(newReplicateConfiguration("test2", "test1"), checkpoint, buffer.Snapshot()),
				})
				require.NoError(t, err)
				return rm
			}
			rm := recoverManager()
			makeTxn := func() []message.MutableMessage {
				source := newImmutableTxnMessageWithTxnID("test2", 100, 1)
				// Two bodies; their source IDs need not follow the local TT order.
				source = append(source[:2:2], source[1], source[2])
				ids := []int64{10, 30, 20, 40}
				out := make([]message.MutableMessage, len(source))
				for i, msg := range source {
					wire := msg.IntoImmutableMessageProto()
					wire.Id = walimplstest.NewTestMessageID(ids[i]).IntoProto()
					replicated := message.MustNewReplicateMessage("test2", wire)
					if replicated.MessageType() == message.MessageTypeCommitTxn {
						message.MustAsMutableCommitTxnMessageV2(replicated).OverwriteHeader(&message.CommitTxnMessageHeader{BodyCnt: 2})
					}
					replicated.OverwriteReplicateVChannel("test1-rootcoord-dml_0")
					out[i] = replicated
				}
				return out
			}
			var localID int64 = 1000
			appendMessage := func(msg message.MutableMessage) []message.ImmutableMessage {
				acker, err := rm.BeginReplicateMessage(ctx, msg)
				require.NoError(t, err)
				localID++
				persisted := msg.WithTimeTick(uint64(localID)).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(localID))
				acker.Ack(nil)
				return buffer.HandleImmutableMessages([]message.ImmutableMessage{persisted}, persisted.TimeTick())
			}
			reject := func(msg message.MutableMessage) {
				acker, err := rm.BeginReplicateMessage(ctx, msg)
				require.Nil(t, acker)
				require.True(t, status.AsStreamingError(err).IsTxnExpired(), "%v", err)
			}
			ignore := func(msg message.MutableMessage) {
				acker, err := rm.BeginReplicateMessage(ctx, msg)
				require.Nil(t, acker)
				require.True(t, status.AsStreamingError(err).IsIgnoredOperation(), "%v", err)
			}
			flush := func(vchannel string) message.MutableMessage {
				if operation == "FlushAll" {
					return message.NewFlushAllMessageBuilderV2().WithAllVChannel().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).WithUnreplicable().MustBuildMutable()
				}
				if operation == "CreateSnapshot" {
					return message.NewCreateSnapshotMessageBuilderV2().WithVChannel(vchannel).
						WithHeader(&message.CreateSnapshotMessageHeader{}).WithBody(&message.CreateSnapshotMessageBody{}).
						WithUnreplicable().MustBuildMutable()
				}
				return message.NewManualFlushMessageBuilderV2().WithVChannel(vchannel).WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).WithUnreplicable().MustBuildMutable()
			}
			first := makeTxn()
			require.Empty(t, appendMessage(first[0]))
			require.Empty(t, appendMessage(first[1]))
			// Failed Flush cannot discard an accepted prefix.
			acker, err := rm.BeginReplicateMessage(ctx, flush("test1-rootcoord-dml_0"))
			require.NoError(t, err)
			acker.Ack(context.Canceled)
			ignore(makeTxn()[1])
			if operation != "FlushAll" {
				require.Len(t, appendMessage(flush("other_vchannel")), 1)
				require.Len(t, buffer.GetUncommittedMessageBuilder(), 1)
				ignore(makeTxn()[1])
			}
			require.Len(t, appendMessage(flush("test1-rootcoord-dml_0")), 1)
			require.Empty(t, buffer.GetUncommittedMessageBuilder())
			require.Zero(t, buffer.Bytes())
			cp, err := rm.GetReplicateCheckpoint()
			require.NoError(t, err)
			require.EqualValues(t, 1, cp.TimeTick)
			rm = recoverManager() // Crash immediately after the durable Flush.
			reject(makeTxn()[2])
			reject(makeTxn()[3])
			replayed := makeTxn()
			require.Empty(t, appendMessage(replayed[0]))
			reject(makeTxn()[3]) // Old connection cannot commit an empty prefix.
			require.Empty(t, appendMessage(replayed[1]))
			rm = recoverManager() // Recover a partial replay, without a new epoch.
			ignore(makeTxn()[0])
			ignore(makeTxn()[1])
			reject(makeTxn()[3])
			require.Empty(t, appendMessage(makeTxn()[2])) // Either connection may complete the prefix.
			committed := appendMessage(makeTxn()[3])
			require.Len(t, committed, 1)
			require.Equal(t, message.MessageTypeTxn, committed[0].MessageType())
			require.Empty(t, buffer.GetUncommittedMessageBuilder())
			ignore(makeTxn()[3])
			cp, err = rm.GetReplicateCheckpoint()
			require.NoError(t, err)
			require.EqualValues(t, 100, cp.TimeTick)
		})
	}
}
