package txn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestExpirationWaitsForAcceptedBodies(t *testing.T) {
	for _, succeed := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed body", true: "successful body"}[succeed], func(t *testing.T) {
			resource.InitForTest(t)
			m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
			s, err := m.BeginNewTxn(t.Context(), newBeginTxnMessage(1, time.Second))
			require.NoError(t, err)
			// A is admitted, but B reaches the expiration boundary before A
			// registers downstream cleanup or completes its WAL append.
			require.NoError(t, s.AddNewMessage(t.Context(), 2))
			expiration := s.expiredTimeTick()
			require.True(t, status.AsStreamingError(s.AddNewMessage(t.Context(), expiration)).IsTxnExpired())
			cleaned := 0
			require.NotPanics(t, func() { s.RegisterCleanup(func() { cleaned++ }, 2) })
			for _, ts := range []uint64{1, expiration} {
				m.CleanupTxnUntil(ts)
				require.Zero(t, cleaned, "accepted bodies still own the cleanup resources")
				require.False(t, s.IsExpiredOrDone(ts), "accepted bodies must retain the LCMID pin")
				active, err := m.GetSessionOfTxn(s.TxnContext().TxnID)
				require.NoError(t, err)
				require.Same(t, s, active)
			}
			require.True(t, status.AsStreamingError(s.RequestCommitAndWait(t.Context(), 2, 1)).IsTxnExpired())
			if succeed {
				s.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
			} else {
				s.AddNewMessageFail()
			}
			// The completion's keepalive must not revive the expired session.
			require.True(t, s.IsExpiredOrDone(2))
			m.CleanupTxnUntil(expiration)
			m.CleanupTxnUntil(expiration + 1)
			require.Equal(t, 1, cleaned)
			_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
			require.True(t, status.AsStreamingError(err).IsTxnExpired())
			require.NoError(t, m.GracefulClose(t.Context()))
		})
	}
}

func TestDefiniteRollbackRetriesBeforeAndAfterCleanup(t *testing.T) {
	for _, reason := range []string{"body mismatch", "commit rejection", "rollback", "scoped abort"} {
		t.Run(reason, func(t *testing.T) {
			resource.InitForTest(t)
			m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
			s, err := m.BeginNewTxn(t.Context(), newBeginTxnMessage(1, time.Second))
			require.NoError(t, err)
			require.NoError(t, s.AddNewMessage(t.Context(), 2))
			s.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
			switch reason {
			case "body mismatch":
				require.True(t, status.AsStreamingError(s.RequestCommitAndWait(t.Context(), 3, 2)).IsTxnExpired())
			case "commit rejection":
				require.NoError(t, s.RequestCommitAndWait(t.Context(), 3, 1))
				s.RejectCommit()
			case "rollback":
				require.NoError(t, s.RequestRollback(t.Context(), 3))
				s.RollbackDone()
			case "scoped abort":
				m.FailTxnAtVChannel("v1", 3)
			}
			for _, ts := range []uint64{3, s.expiredTimeTick()} {
				require.True(t, status.AsStreamingError(s.RequestCommitAndWait(t.Context(), ts, 1)).IsTxnExpired())
				require.True(t, status.AsStreamingError(s.AddNewMessage(t.Context(), ts)).IsTxnExpired())
				require.True(t, status.AsStreamingError(s.RequestRollback(t.Context(), ts)).IsTxnExpired())
			}
			m.CleanupTxnUntil(3)
			_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
			require.True(t, status.AsStreamingError(err).IsTxnExpired())
			require.NoError(t, m.GracefulClose(t.Context()))
		})
	}
}

func TestAbortedUnresolvedCommitIsNotRetryableThroughHeldSession(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
	s, err := m.BeginNewTxn(t.Context(), newBeginTxnMessage(1, time.Second))
	require.NoError(t, err)
	require.NoError(t, s.AddNewMessage(t.Context(), 2))
	s.AddNewMessageDoneAndKeepalive(2, message.MessageTypeInsert)
	require.NoError(t, s.RequestCommitAndWait(t.Context(), 3, 1))
	m.FailTxnAtVChannel("v1", 4)
	for _, ts := range []uint64{4, s.expiredTimeTick()} {
		require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE,
			status.AsStreamingError(s.RequestCommitAndWait(t.Context(), ts, 1)).Code)
		require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE,
			status.AsStreamingError(s.AddNewMessage(t.Context(), ts)).Code)
	}
	_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
	require.True(t, status.AsStreamingError(err).IsTxnNotFound())
	require.NoError(t, m.GracefulClose(t.Context()))
}
