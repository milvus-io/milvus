package partialupdate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestExpiredAcceptedBodyRetainsWriteSetCleanup(t *testing.T) {
	for _, beforeRegistration := range []bool{false, true} {
		t.Run(map[bool]string{false: "during append", true: "before registration"}[beforeRegistration], func(t *testing.T) {
			resource.InitForTest(t)
			manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, nil)
			begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 1000}).
				WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(1)
			session, err := manager.BeginNewTxn(t.Context(), message.MustAsMutableBeginTxnMessageV2(begin))
			require.NoError(t, err)
			require.NoError(t, session.AddNewMessage(t.Context(), 2))
			expiration := tsoutil.AddPhysicalDurationOnTs(1, time.Second)
			expire := func() {
				require.True(t, status.AsStreamingError(session.AddNewMessage(t.Context(), expiration)).IsTxnExpired())
				manager.CleanupTxnUntil(1)
			}
			if beforeRegistration {
				expire()
			}
			state := newPartialUpdateState(time.Minute, 1<<20)
			interceptor := &appendInterceptor{enableCAS: true, state: state}
			body := newGuardTestDelete(10).WithTimeTick(2).WithTxnContext(session.TxnContext())
			require.NotPanics(t, func() {
				_, err = interceptor.DoAppend(txn.WithTxnSession(t.Context(), session), body,
					func(context.Context, message.MutableMessage) (message.MessageID, error) {
						if !beforeRegistration {
							expire()
						}
						return walimplstest.NewTestMessageID(2), nil
					})
			})
			require.NoError(t, err)
			pending := state.getTxn(session.TxnContext().TxnID)
			require.NotNil(t, pending)
			require.True(t, pending.cleanupRegistered, "successful append must not recreate an orphaned write set")
			require.Equal(t, 1, pending.pks.Len())
			session.AddNewMessageDoneAndKeepalive(2, message.MessageTypeDelete)
			manager.CleanupTxnUntil(expiration)
			require.Nil(t, state.getTxn(session.TxnContext().TxnID))
			require.NoError(t, manager.GracefulClose(t.Context()))
		})
	}
}
