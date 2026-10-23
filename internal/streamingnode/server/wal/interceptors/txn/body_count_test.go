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

func TestCommitBodyCount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected uint64
		actual   int
		valid    bool
	}{
		{"complete", 2, 2, true},
		{"missing", 2, 1, false},
		{"upper_retry_duplicate", 1, 2, false},
		{"empty", 0, 0, false},
		{"legacy_nonempty", 0, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTxnSession("v1", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 1, metricsutil.NewTxnMetrics("body-count").BeginTxn())
			t.Cleanup(s.Cleanup)
			cleaned := false
			s.RegisterCleanup(func() { cleaned = true }, 1)
			for i := 0; i < tc.actual; i++ {
				require.NoError(t, s.AddNewMessage(context.Background(), 2))
				s.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
			}
			// An admitted but failed append is not a logical body.
			require.NoError(t, s.AddNewMessage(context.Background(), 3))
			s.AddNewMessageFail()
			err := s.RequestCommitAndWait(context.Background(), 4, tc.expected)
			if tc.valid {
				require.NoError(t, err)
				require.False(t, cleaned)
				s.CommitDone()
			} else {
				if tc.actual == 0 {
					require.ErrorContains(t, err, "cannot commit an empty transaction")
				} else {
					require.True(t, status.AsStreamingError(err).IsTxnExpired())
				}
				require.Equal(t, message.TxnStateRollbacked, s.State())
				require.True(t, s.IsExpiredOrDone(4), "must release LastConfirmedMessageID barrier")
				require.Error(t, s.AddNewMessage(context.Background(), 5), "late retry cannot revive rejected txn")
			}
			require.True(t, cleaned)
		})
	}
}

func TestCommitBodyCountWaitsForInflight(t *testing.T) {
	s := newTxnSession("v1", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 1, metricsutil.NewTxnMetrics("body-count-wait").BeginTxn())
	t.Cleanup(s.Cleanup)
	require.NoError(t, s.AddNewMessage(context.Background(), 2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.RequestCommitAndWait(ctx, 3, 1) }()
	require.Eventually(t, func() bool { return s.State() == message.TxnStateOnCommit }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Commit returned before body completion: %v", err)
	default:
	}
	s.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
	require.NoError(t, <-done)
	s.CommitDone()
}

func TestRecoveredSessionBodyCount(t *testing.T) {
	resource.InitForTest(t)
	begin := newImmutableBeginTxnMessageWithVChannel("v1", 1, 1, message.TxnKeepaliveInfinite)
	body := message.CreateTestEmptyInsertMesage(1, nil).WithTxnContext(*begin.TxnContext()).
		WithTimeTick(2).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	m := NewTxnManager(types.PChannelInfo{Name: "body-count-recover"}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{
		1: message.NewImmutableTxnMessageBuilder(begin).Add(body),
	})
	s, err := m.GetSessionOfTxn(1)
	require.NoError(t, err)
	require.NoError(t, s.AddNewMessage(context.Background(), 3))
	s.AddNewMessageDoneAndKeepalive(3, message.MessageTypeInsert)
	require.NoError(t, s.RequestCommitAndWait(context.Background(), 4, 2), "count includes recovered and newly appended bodies")
	s.CommitDone()
	m.CleanupTxnUntil(5)
	select {
	case <-m.RecoverDone():
	default:
		t.Fatal("completed recovered txn must release the recovery barrier")
	}
}
