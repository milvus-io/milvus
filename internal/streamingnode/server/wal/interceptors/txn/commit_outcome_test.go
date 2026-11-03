package txn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestCommitOutcomeDoesNotBecomeExpired(t *testing.T) {
	resource.InitForTest(t)
	for _, state := range []message.TxnState{message.TxnStateOnCommit, message.TxnStateCommitted} {
		t.Run(state.String(), func(t *testing.T) {
			m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
			s, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, time.Millisecond))
			require.NoError(t, err)
			require.NoError(t, s.RequestCommitAndWait(context.Background(), 0, 0))
			if state == message.TxnStateCommitted {
				s.CommitDone()
			}
			for _, ts := range []uint64{0, s.expiredTimeTick()} {
				err := s.AddNewMessage(context.Background(), ts)
				require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, status.AsStreamingError(err).Code)
				err = s.RequestCommitAndWait(context.Background(), ts, 0)
				require.Error(t, err)
				require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, status.AsStreamingError(err).Code)
				require.Equal(t, state, s.State())
			}
			m.CleanupTxnUntil(s.expiredTimeTick())
			_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
			require.ErrorContains(t, err, "not found")
			require.False(t, status.AsStreamingError(err).IsTxnExpired())
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		})
	}
}

func TestCommitExpiredAndNotFoundAreDistinct(t *testing.T) {
	resource.InitForTest(t)
	m := NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
	s, err := m.BeginNewTxn(context.Background(), newBeginTxnMessage(0, time.Millisecond))
	require.NoError(t, err)
	err = s.RequestCommitAndWait(context.Background(), s.expiredTimeTick(), 0)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	// Explicit expiration is sticky, and cannot be followed by a Commit.
	err = s.RequestCommitAndWait(context.Background(), 0, 0)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	m.CleanupTxnUntil(s.expiredTimeTick())
	_, err = m.GetSessionOfTxn(s.TxnContext().TxnID)
	require.ErrorContains(t, err, "not found")
	require.False(t, status.AsStreamingError(err).IsTxnExpired(), "absence does not establish an outcome")
}
