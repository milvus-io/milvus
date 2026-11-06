package txn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTransactionVChannelMembership(t *testing.T) {
	for _, channel := range []string{"p_1v0", "p_vcchan", ""} {
		s := newTxnSession(channel, message.TxnContext{}, 10, nil)
		require.NoError(t, s.ValidateVChannelMembership(channel))
		require.Error(t, s.ValidateVChannelMembership(channel+"foreign"))
		require.Zero(t, s.inFlightCount)
		require.Zero(t, s.bodyCount)
		require.Equal(t, uint64(10), s.lastTimetick)
	}
}

func TestUpsertUsesCommonCommitCountContract(t *testing.T) {
	for _, count := range []uint64{0, 1, 2} {
		for _, effects := range []uint64{0, 1} {
			metrics := metricsutil.NewTxnMetrics("upsert-count")
			s := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: time.Second}, 10, metrics.BeginTxn())
			require.NoError(t, s.AddNewMessage(context.Background(), 11))
			s.RecordUpsertEffects(effects)
			s.AddNewMessageDoneAndKeepalive(11, message.MessageTypeUpsert)
			require.Equal(t, effects == 0, s.IsInsertOnly())
			cleaned := false
			s.RegisterCleanup(func() { cleaned = true }, 11)
			err := s.RequestCommitAndWait(context.Background(), 12, count)
			if count == 2 {
				require.Error(t, err)
				require.True(t, status.AsStreamingError(err).IsTxnUnavilable())
				require.True(t, cleaned)
			} else {
				require.NoError(t, err)
				s.CommitDone()
				require.True(t, cleaned)
			}
			metrics.Close()
		}
	}
}

func TestRecoveredUpsertInsertOnlyClassification(t *testing.T) {
	resource.InitForTest(t)
	for _, effects := range []bool{false, true} {
		ctx := message.TxnContext{TxnID: 70}
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(ctx).WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(10))
		header := &message.UpsertMessageHeader{CollectionId: 1}
		if effects {
			header.DeleteRows = 1
		}
		body := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTxnContext(ctx).WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(11))
		manager := NewTxnManager(types.PChannelInfo{Name: "test"}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{70: message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(body)})
		session, err := manager.GetSessionOfTxn(70)
		require.NoError(t, err)
		require.Equal(t, !effects, session.IsInsertOnly())
		manager.Close()
	}
}
