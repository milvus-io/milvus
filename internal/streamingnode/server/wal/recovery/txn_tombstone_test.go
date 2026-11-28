package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRecoveryRestoresCommitTombstones(t *testing.T) {
	for _, checkpoint := range []struct {
		name     string
		timeTick uint64
	}{
		{name: "uncheckpointed commit", timeTick: 1},
		{name: "checkpointed commit in replayed prefix", timeTick: 3},
	} {
		t.Run(checkpoint.name, func(t *testing.T) {
			resource.InitForTest(t)
			floor := walimplstest.NewTestMessageID(0)
			channel := types.PChannelInfo{Name: "txn-tombstone-recovery", Term: 1}
			storage := newRecoveryStorage(channel, &utility.WALCheckpoint{MessageID: floor, TimeTick: checkpoint.timeTick})
			defer storage.metrics.Close()
			txnCtx := message.TxnContext{TxnID: 7, Keepalive: time.Second}
			begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
				MustBuildMutable().WithTimeTick(2).WithLastConfirmed(floor).WithTxnContext(txnCtx).
				IntoImmutableMessage(walimplstest.NewTestMessageID(2))
			commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
				MustBuildMutable().WithTimeTick(3).WithLastConfirmed(floor).WithTxnContext(txnCtx).
				IntoImmutableMessage(walimplstest.NewTestMessageID(3))
			transaction, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).
				Build(message.MustAsImmutableCommitTxnMessageV2(commit))
			require.NoError(t, err)
			barrier := message.NewTimeTickMessageBuilderV1().WithVChannel("").
				WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).
				MustBuildMutable().WithTimeTick(4).WithLastConfirmed(floor).
				IntoImmutableMessage(walimplstest.NewTestMessageID(4))
			stream := &streamBuilder{channel: channel, histories: []message.ImmutableMessage{transaction, barrier}}
			snapshot, err := storage.recoverFromStream(context.Background(), stream, barrier)
			require.NoError(t, err)
			result := snapshot.TxnTombstones.Get(txnCtx.TxnID, "v1")
			require.NotNil(t, result)
			require.Equal(t, commit.MessageID(), result.MessageID)
			require.Equal(t, commit.TimeTick(), result.Result.TimeTick)
			require.Equal(t, commit.LastConfirmedMessageID(), result.Result.LastConfirmedMessageID)
			require.Equal(t, commit.TxnContext(), result.Result.TxnCtx)
		})
	}
}
