package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestBoundedRecoveryRestoresCommitTombstones(t *testing.T) {
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
			storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: floor, TimeTick: checkpoint.timeTick})
			defer storage.metrics.Close()
			defer storage.taskScheduler.Close()
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
			barrier := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
				WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).
				MustBuildMutable().WithTimeTick(4).WithLastConfirmed(floor).
				IntoImmutableMessage(walimplstest.NewTestMessageID(4))
			stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 2), closed: make(chan struct{})}
			stream.ch <- transaction
			stream.ch <- barrier
			defer stream.Close()
			snapshot, err := storage.runBoundedRecovery(context.Background(), &recordingRecoveryStreamBuilder{stream: stream}, barrier)
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
