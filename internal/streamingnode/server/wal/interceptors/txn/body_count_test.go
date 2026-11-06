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
		name       string
		expected   uint64
		actual     int
		valid      bool
		upsert     bool
		deleteRows uint64
	}{
		{"complete", 2, 2, true, false, 0},
		{"missing", 2, 1, false, false, 0},
		{"upper_retry_duplicate", 1, 2, false, false, 0},
		{"empty", 0, 0, false, false, 0},
		{"legacy_nonempty", 0, 2, true, false, 0},
		{"upsert_complete", 1, 1, true, true, 0},
		{"upsert_missing", 2, 1, false, true, 1},
		{"upsert_legacy_zero", 0, 1, true, true, 0},
		{"upsert_delete_effects", 1, 1, true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTxnSession("v1", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 1, metricsutil.NewTxnMetrics("body-count").BeginTxn())
			t.Cleanup(s.Cleanup)
			cleaned := false
			s.RegisterCleanup(func() { cleaned = true }, 1)
			for i := 0; i < tc.actual; i++ {
				require.NoError(t, s.AddNewMessage(context.Background(), 2))
				typ := message.MessageTypeInsert
				if tc.upsert {
					typ = message.MessageTypeUpsert
				}
				s.AddNewMessageDoneAndKeepalive(newTxnBodyMessage(2, typ, tc.deleteRows))
			}
			require.Equal(t, tc.actual > 0 && tc.deleteRows == 0, s.IsInsertOnly())
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
	s.AddNewMessageDoneAndKeepalive(newTxnBodyMessage(2, message.MessageTypeInsert, 0))
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
	s.AddNewMessageDoneAndKeepalive(newTxnBodyMessage(3, message.MessageTypeInsert, 0))
	require.NoError(t, s.RequestCommitAndWait(context.Background(), 4, 2), "count includes recovered and newly appended bodies")
	s.CommitDone()
	m.CleanupTxnUntil(5)
	select {
	case <-m.RecoverDone():
	default:
		t.Fatal("completed recovered txn must release the recovery barrier")
	}
}

func TestCommitClassificationWaitsForInflightBody(t *testing.T) {
	for _, tc := range []struct {
		name       string
		typ        message.MessageType
		deleteRows uint64
		insertOnly bool
	}{
		{"insert", message.MessageTypeInsert, 0, true},
		{"delete", message.MessageTypeDelete, 0, false},
		{"upsert-rows", message.MessageTypeUpsert, 0, true},
		{"upsert-delete", message.MessageTypeUpsert, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := metricsutil.NewTxnMetrics("classification-inflight")
			defer metrics.Close()
			session := newTxnSession("v", message.TxnContext{TxnID: 1, Keepalive: time.Second}, 10, metrics.BeginTxn())
			defer session.Cleanup()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			require.NoError(t, session.AddNewMessage(ctx, 11))
			type commitResult struct {
				err        error
				insertOnly bool
			}
			done := make(chan commitResult, 1)
			go func() {
				err := session.RequestCommitAndWait(ctx, 12, 1)
				done <- commitResult{err, session.IsInsertOnly()}
			}()
			require.Eventually(t, func() bool { return session.State() == message.TxnStateOnCommit }, time.Second, time.Millisecond)
			select {
			case result := <-done:
				t.Fatalf("commit completed before body classification: %v", result.err)
			default:
			}
			session.AddNewMessageDoneAndKeepalive(newTxnBodyMessage(11, tc.typ, tc.deleteRows))
			result := <-done
			require.NoError(t, result.err)
			require.Equal(t, tc.insertOnly, result.insertOnly, "classification must be visible to the awakened Commit waiter")
			session.CommitDone()
		})
	}
}
