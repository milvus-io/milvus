package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func recoveryTestMessage(t *testing.T, kind message.MessageType, tt uint64) message.MutableMessage {
	t.Helper()
	var msg message.MutableMessage
	var err error
	switch kind {
	case message.MessageTypeTimeTick:
		msg, err = message.NewTimeTickMessageBuilderV1().WithVChannel("").WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).BuildMutable()
	case message.MessageTypeRecoveryBarrier:
		msg, err = message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).BuildMutable()
	case message.MessageTypeBeginTxn:
		msg, err = message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).BuildMutable()
	case message.MessageTypeInsert:
		msg, err = message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).BuildMutable()
	case message.MessageTypeCommitTxn:
		msg, err = message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).BuildMutable()
	case message.MessageTypeRollbackTxn:
		msg, err = message.NewRollbackTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).BuildMutable()
	default:
		t.Fatalf("unsupported message type: %s", kind)
	}
	require.NoError(t, err)
	if kind != message.MessageTypeTimeTick && kind != message.MessageTypeRecoveryBarrier {
		msg = msg.WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite})
	}
	return msg.WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(0))
}

func newRecoveryTestWAL(t *testing.T) walimpls.WALImpls {
	t.Helper()
	opener, err := registry.MustGetBuilder(message.WALNameTest).Build()
	require.NoError(t, err)
	t.Cleanup(opener.Close)
	l, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: types.PChannelInfo{
		Name: "recovery-" + funcutil.GenRandomStr(), Term: 1, AccessMode: types.AccessModeRW,
	}})
	require.NoError(t, err)
	t.Cleanup(l.Close)
	return l
}

func appendRecoveryTestMessage(t *testing.T, l walimpls.WALImpls, kind message.MessageType, tt uint64) message.ImmutableMessage {
	t.Helper()
	msg := recoveryTestMessage(t, kind, tt)
	// The test WAL occasionally rejects an append before writing it. Retry only
	// that injected failure; every successful append still has one physical record.
	var id message.MessageID
	var err error
	for attempt := 0; attempt < 100; attempt++ {
		id, err = l.Append(context.Background(), msg)
		if err == nil {
			break
		}
		require.EqualError(t, err, "random error")
	}
	require.NoError(t, err)
	return msg.IntoImmutableMessage(id)
}

func receiveRecoveryTestMessage(t *testing.T, stream recovery.RecoveryStream) message.ImmutableMessage {
	t.Helper()
	select {
	case msg, ok := <-stream.Chan():
		require.True(t, ok, "recovery stream closed")
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery stream")
		return nil
	}
}

func TestRecoveryScannerPreservesPartialTransaction(t *testing.T) {
	for _, tc := range []struct {
		name            string
		rollback, evict bool
	}{
		{name: "commit"}, {name: "rollback", rollback: true}, {name: "evicted barrier", evict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resource.InitForTest(t)
			l := newRecoveryTestWAL(t)
			historical := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 1)
			appendRecoveryTestMessage(t, l, message.MessageTypeBeginTxn, 2)
			body := appendRecoveryTestMessage(t, l, message.MessageTypeInsert, 3)
			appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 4)
			barrier := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 5)
			capacity := 1024 * 1024
			if tc.evict {
				capacity = 1
			}
			buffer := wab.NewWriteAheadBuffer(l.Channel().Name, mlog.With(), capacity, time.Hour, barrier)
			t.Cleanup(buffer.Close)
			liveReady := make(chan struct{})
			ro := adaptImplsToROWAL(l, func() {})
			t.Cleanup(ro.Close)
			stream := newRecoveryStreamBuilder(ro, buffer, liveReady).Build(recovery.BuildRecoveryStreamParam{
				StartCheckpoint: historical.MessageID(), RecoveryBarrier: barrier,
			})
			t.Cleanup(func() { _ = stream.Close() })
			for _, tt := range []uint64{1, 4, 5} {
				require.Equal(t, tt, receiveRecoveryTestMessage(t, stream).TimeTick())
			}
			snapshot := stream.TxnBuffer()
			require.NotNil(t, snapshot)
			builders := snapshot.GetUncommittedMessageBuilder()
			require.Len(t, builders, 1)
			begin, bodies := builders[1].Messages()
			require.Equal(t, uint64(2), begin.TimeTick())
			require.Len(t, bodies, 1)
			require.True(t, body.MessageID().EQ(bodies[0].MessageID()))

			// The writer can confirm more data while recovery is paused. With a tiny
			// WAB this evicts the handoff position, requiring continued durable catchup.
			if tc.evict {
				after := appendRecoveryTestMessage(t, l, message.MessageTypeInsert, 6)
				tt := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 7)
				buffer.Append([]message.ImmutableMessage{after}, tt)
				_, err := buffer.ReadFromExclusiveTimeTick(context.Background(), barrier.TimeTick())
				require.ErrorIs(t, err, wab.ErrEvicted)
			}
			select {
			case <-stream.Chan():
				t.Fatal("scanner consumed past the startup gate")
			default:
			}
			close(liveReady)
			if tc.evict {
				require.Equal(t, uint64(7), receiveRecoveryTestMessage(t, stream).TimeTick())
			}

			// In the normal case Commit/Rollback is available only in WAB. No
			// durable TimeTick after the target barrier is needed to switch.
			kind := message.MessageTypeCommitTxn
			if tc.rollback {
				kind = message.MessageTypeRollbackTxn
			}
			after := recoveryTestMessage(t, message.MessageTypeInsert, 8).IntoImmutableMessage(walimplstest.NewTestMessageID(100))
			finish := recoveryTestMessage(t, kind, 9).IntoImmutableMessage(walimplstest.NewTestMessageID(101))
			tt := recoveryTestMessage(t, message.MessageTypeTimeTick, 10).IntoImmutableMessage(walimplstest.NewTestMessageID(102))
			if tc.evict {
				// If the tail reader has already opened, evict it again. Both catchup
				// paths must retain the same partial transaction until its durable commit.
				after = appendRecoveryTestMessage(t, l, message.MessageTypeInsert, 8)
				finish = appendRecoveryTestMessage(t, l, kind, 9)
				tt = appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 10)
			}
			buffer.Append([]message.ImmutableMessage{after, finish}, tt)
			if !tc.rollback {
				txn := receiveRecoveryTestMessage(t, stream)
				require.Equal(t, message.MessageTypeTxn, txn.MessageType())
				want := 2
				if tc.evict {
					want++
				}
				require.Equal(t, want, txn.(message.ImmutableTxnMessage).Size())
			}
			require.Equal(t, uint64(10), receiveRecoveryTestMessage(t, stream).TimeTick())
			// A live Build/Commit consumes builders and rewrites body slice entries;
			// the startup snapshot must still contain the original unfinished state.
			begin, bodies = builders[1].Messages()
			require.Equal(t, uint64(2), begin.TimeTick())
			require.Len(t, bodies, 1)
			require.Equal(t, uint64(3), bodies[0].TimeTick())
		})
	}
}

func TestRecoveryScannerCloseWhilePaused(t *testing.T) {
	for _, phase := range []string{"before barrier", "at barrier", "empty WAB"} {
		t.Run(phase, func(t *testing.T) {
			resource.InitForTest(t)
			l := newRecoveryTestWAL(t)
			barrier := recoveryTestMessage(t, message.MessageTypeRecoveryBarrier, 5).IntoImmutableMessage(walimplstest.NewTestMessageID(0))
			if phase != "before barrier" {
				barrier = appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 5)
			}
			buffer := wab.NewWriteAheadBuffer(l.Channel().Name, mlog.With(), 1024, time.Hour, barrier)
			t.Cleanup(buffer.Close)
			ro := adaptImplsToROWAL(l, func() {})
			t.Cleanup(ro.Close)
			ready := make(chan struct{})
			stream := newRecoveryStreamBuilder(ro, buffer, ready).Build(recovery.BuildRecoveryStreamParam{
				StartCheckpoint: barrier.MessageID(), RecoveryBarrier: barrier,
			})
			if phase != "before barrier" {
				require.Equal(t, uint64(5), receiveRecoveryTestMessage(t, stream).TimeTick())
			}
			if phase == "empty WAB" {
				close(ready)
			}
			done := make(chan struct{})
			go func() { _ = stream.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("closing recovery scanner blocked")
			}
		})
	}
}
