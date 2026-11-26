package ack

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestRecoveredPinWaitsForAcceptedBodyAfterExpiration(t *testing.T) {
	resource.InitForTest(t)
	floor := walimplstest.NewTestMessageID(0)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(10).WithLastConfirmed(floor).IntoImmutableMessage(walimplstest.NewTestMessageID(10))
	manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{
		1: message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)),
	})
	session, err := manager.GetSessionOfTxn(1)
	require.NoError(t, err)
	last := newLastConfirmedManager(floor)
	later := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(15).WithLastConfirmed(floor).
		IntoImmutableMessage(walimplstest.NewTestMessageID(30))
	require.NoError(t, session.AddNewMessage(t.Context(), 20))
	expiration := tsoutil.AddPhysicalDurationOnTs(10, time.Second)
	require.True(t, status.AsStreamingError(session.AddNewMessage(t.Context(), expiration)).IsTxnExpired())
	last.AddConfirmedDetails(sortedDetails{
		&AckDetail{Message: begin, TxnSession: session, EndTimestamp: 11},
		&AckDetail{Message: later, EndTimestamp: 15},
	}, 19)
	require.True(t, floor.EQ(last.GetLastConfirmedMessageID()), "recovered Begin must remain pinned while the accepted body is in flight")
	manager.CleanupTxnUntil(19)
	select {
	case <-manager.RecoverDone():
		t.Fatal("recovery wait must not finish before the accepted body")
	default:
	}
	body := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(20).WithLastConfirmed(floor).
		WithTxnContext(session.TxnContext()).IntoImmutableMessage(walimplstest.NewTestMessageID(40))
	session.AddNewMessageDoneAndKeepalive(20, message.MessageTypeInsert)
	last.AddConfirmedDetails(sortedDetails{&AckDetail{Message: body, TxnSession: session, EndTimestamp: 21}}, 22)
	require.True(t, body.MessageID().EQ(last.GetLastConfirmedMessageID()), "completion must not renew a terminal transaction's pin")
	manager.CleanupTxnUntil(22)
	select {
	case <-manager.RecoverDone():
	default:
		t.Fatal("completed expired body must release recovery wait")
	}
	require.NoError(t, manager.GracefulClose(t.Context()))
}

func TestRecoveredAckEntriesFollowActualSessionLifecycle(t *testing.T) {
	for _, finish := range []string{"commit", "rollback", "expire", "scoped abort", "global abort"} {
		t.Run(finish, func(t *testing.T) {
			resource.InitForTest(t)
			initialTT := tsoutil.ComposeTS(1000, 0)
			floor := walimplstest.NewTestMessageID(0)
			builders := make(map[message.TxnID]*message.ImmutableTxnMessageBuilder)
			begins := make([]message.ImmutableMessage, 0, 2)
			for n, vchannel := range []string{"v1", "v2"} {
				lease := message.TxnKeepaliveInfinite
				if finish == "expire" && n == 0 {
					lease = time.Second
				}
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(vchannel).
					WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
					MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: message.TxnID(n + 1), Keepalive: lease}).
					WithTimeTick(initialTT).WithLastConfirmed(floor).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(n + 10)))
				builders[message.TxnID(n+1)] = message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
				begins = append(begins, begin)
			}
			manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, builders)
			defer manager.FailTxnAtVChannel("", 1)
			first, err := manager.GetSessionOfTxn(1)
			require.NoError(t, err)
			second, err := manager.GetSessionOfTxn(2)
			require.NoError(t, err)
			metrics := metricsutil.NewTimeTickMetrics(t.Name())
			defer metrics.Close()
			ackManager := NewAckManager(initialTT+1, floor, metrics,
				&AckDetail{Message: begins[0], TxnSession: first, EndTimestamp: initialTT + 1},
				&AckDetail{Message: begins[1], TxnSession: second, EndTimestamp: initialTT + 1})
			later := message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(initialTT + 2).WithLastConfirmed(floor).
				IntoImmutableMessage(walimplstest.NewTestMessageID(20))
			last := ackManager.lastConfirmedManager
			last.AddConfirmedDetails(sortedDetails{&AckDetail{Message: later, EndTimestamp: initialTT + 2}}, initialTT+3)
			require.True(t, floor.EQ(last.GetLastConfirmedMessageID()))
			finishTT := initialTT + 4
			switch finish {
			case "commit":
				require.NoError(t, first.AddNewMessage(context.Background(), finishTT))
				first.AddNewMessageDoneAndKeepalive(finishTT, message.MessageTypeInsert)
				require.NoError(t, first.RequestCommitAndWait(context.Background(), finishTT, 1))
				first.CommitDone()
			case "rollback":
				require.NoError(t, first.RequestRollback(context.Background(), finishTT))
				first.RollbackDone()
			case "expire":
				finishTT = tsoutil.AddPhysicalDurationOnTs(initialTT, time.Second)
			case "scoped abort":
				manager.FailTxnAtVChannel("v1", 1)
			case "global abort":
				manager.FailTxnAtVChannel("", 1)
			}
			last.AddConfirmedDetails(nil, finishTT)
			if finish == "global abort" {
				require.True(t, later.MessageID().EQ(last.GetLastConfirmedMessageID()))
			} else {
				require.True(t, begins[0].MessageID().EQ(last.GetLastConfirmedMessageID()))
				require.False(t, second.IsExpiredOrDone(finishTT), "other recovered txn must still pin")
				manager.FailTxnAtVChannel("v2", 1)
				last.AddConfirmedDetails(nil, finishTT+1)
				require.True(t, later.MessageID().EQ(last.GetLastConfirmedMessageID()))
			}
			require.Empty(t, ackManager.acknowledgedDetails, "recovery must not republish historical ACKs")
		})
	}
}
