package partialupdate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// A CommitTxn that reached the WAL is resent when its response is lost. Its
// write-set record is already gone, so admission must leave the decision to
// TimeTick: the committed session answers INVALID_TRANSACTION_STATE, which the
// producer does not replay. TRANSACTION_EXPIRED or PARTIAL_UPDATE_RETRYABLE
// would make the producer or Proxy apply the same write a second time.
func TestPartialUpdateChainRetriedCommitAfterSuccessIsNotReplayable(t *testing.T) {
	for _, cas := range []bool{false, true} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			txnContext := prepareCommitRetryTxn(t, env, readTS, cas)
			newCommit := func() message.MutableMessage {
				if cas {
					return newChainTestCASCommit(t, txnContext)
				}
				return newCommitTxnMessage("v1", txnContext.TxnID, 0)
			}
			_, err := env.append(newCommit(), nil)
			require.NoError(t, err)
			require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
			require.Nil(t, env.partial.state.getTxn(txnContext.TxnID))

			// The response is lost and the producer resends the same CommitTxn.
			appended := false
			_, err = env.append(newCommit(), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return env.nextMessageID(), nil
			})
			require.Error(t, err)
			require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE,
				status.AsStreamingError(err).Code, "retried commit returned: %v", err)
			require.False(t, appended)
			session, err := env.txnManager.GetSessionOfTxn(txnContext.TxnID)
			require.NoError(t, err)
			require.Equal(t, message.TxnStateCommitted, session.State())
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10)
		})
	}
}

// prepareCommitRetryTxn appends Begin and one body for PK 10 with a lease long
// enough that the retried commit always reaches the committed session.
func prepareCommitRetryTxn(t *testing.T, env *partialUpdateChainTestEnv, readTS uint64, cas bool) message.TxnContext {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: (30 * time.Second).Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	result, err := env.append(begin, nil)
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)
	body := newGuardTestDelete(10)
	if cas {
		body = newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(readTS, 1))
	}
	_, err = env.append(body.WithTxnContext(*result.TxnCtx), nil)
	require.NoError(t, err)
	return *result.TxnCtx
}
