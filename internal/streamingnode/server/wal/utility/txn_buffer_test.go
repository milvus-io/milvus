package utility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var idAllocator = typeutil.NewIDAllocator()

func TestFlushAbortsScopedTransactions(t *testing.T) {
	for _, origin := range []string{"local", "historical", "replicated"} {
		t.Run(origin, func(t *testing.T) {
			b := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
			for i, vchannel := range []string{"v1", "v2"} {
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(vchannel).
					WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
					MustBuildMutable().WithTimeTick(uint64(i + 1)).WithLastConfirmedUseMessageID().
					WithTxnContext(message.TxnContext{TxnID: message.TxnID(i + 1), Keepalive: message.TxnKeepaliveInfinite}).
					IntoImmutableMessage(walimplstest.NewTestMessageID(int64(i + 1)))
				b.HandleImmutableMessages([]message.ImmutableMessage{begin}, begin.TimeTick())
			}
			flush := func(msg message.MutableMessage, ts uint64) {
				if origin == "replicated" {
					msg = msg.WithReplicateHeader(&message.ReplicateHeader{
						ClusterID: "source", VChannel: msg.VChannel(), TimeTick: ts,
						MessageID:              walimplstest.NewTestMessageID(int64(ts)),
						LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
					})
				}
				immutable := msg.WithTimeTick(ts).WithLastConfirmedUseMessageID().
					IntoImmutableMessage(walimplstest.NewTestMessageID(int64(ts)))
				require.Equal(t, []message.ImmutableMessage{immutable}, b.HandleImmutableMessages([]message.ImmutableMessage{immutable}, ts))
			}
			manualFlush := func(vchannel string) message.MutableMessage {
				builder := message.NewManualFlushMessageBuilderV2().WithVChannel(vchannel).
					WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{})
				if origin == "local" {
					builder.WithUnreplicable()
				}
				return builder.MustBuildMutable()
			}

			bytesBefore := b.Bytes()
			flush(manualFlush("p1_vcchan"), 3)
			require.Len(t, b.builders, 2, "control-channel flush must not abort data transactions")
			require.Equal(t, bytesBefore, b.Bytes())

			flush(manualFlush("v1"), 4)
			require.NotContains(t, b.builders, message.TxnID(1))
			require.Contains(t, b.builders, message.TxnID(2))
			require.Less(t, b.Bytes(), bytesBefore)
			require.Len(t, b.Snapshot().builders, 1, "recovery must not revive the aborted prefix")

			// Legacy FlushAll records may carry the PChannel name without a scope flag.
			all := message.NewFlushAllMessageBuilderV2().WithVChannel("p1").
				WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{})
			if origin == "local" {
				all.WithUnreplicable()
			}
			flush(all.MustBuildMutable(), 5)
			require.Empty(t, b.builders)
			require.Zero(t, b.Bytes())
		})
	}
}

func TestTxnBuffer(t *testing.T) {
	b := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.ComposeTSByTime(time.Now())

	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, nil, baseTso),
		newInsertMessage(t, nil, baseTso),
		newInsertMessage(t, nil, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 3)

	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, nil, baseTso),
		newInsertMessage(t, &message.TxnContext{
			TxnID:     1,
			Keepalive: time.Second,
		}, baseTso),
		newInsertMessage(t, nil, baseTso),
		newRollbackMessage(t, &message.TxnContext{
			TxnID:     1,
			Keepalive: time.Second,
		}, baseTso),
		newCommitMessage(t, &message.TxnContext{
			TxnID:     2,
			Keepalive: time.Second,
		}, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 2)

	// Test successful commit
	txnCtx := &message.TxnContext{
		TxnID:     1,
		Keepalive: 201 * time.Millisecond,
	}
	createUnCommitted := func() {
		msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
			newBeginMessage(t, txnCtx, baseTso),
		}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
		assert.Len(t, msgs, 0)

		msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
			newInsertMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 100*time.Millisecond)),
		}, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond))
		assert.Len(t, msgs, 0)

		msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
			newInsertMessage(t, nil, tsoutil.AddPhysicalDurationOnTs(baseTso, 250*time.Millisecond)),
			newInsertMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 300*time.Millisecond)),
		}, tsoutil.AddPhysicalDurationOnTs(baseTso, 400*time.Millisecond))
		// non txn message should be passed.
		assert.Len(t, msgs, 1)
	}
	createUnCommitted()
	assert.Len(t, b.GetUncommittedMessageBuilder(), 1)
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newCommitMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 500*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 600*time.Millisecond))
	assert.Len(t, msgs, 1)
	assert.Len(t, b.builders, 0)

	// Test rollback
	txnCtx.TxnID = 2
	createUnCommitted()
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newRollbackMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 500*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 600*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 0)

	// Test expired txn
	createUnCommitted()
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{}, tsoutil.AddPhysicalDurationOnTs(baseTso, 500*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{}, tsoutil.AddPhysicalDurationOnTs(baseTso, 501*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 0)
}

func newInsertMessage(t *testing.T, txnCtx *message.TxnContext, ts uint64) message.ImmutableMessage {
	msg, err := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	if txnCtx != nil {
		msg = msg.WithTxnContext(*txnCtx)
	}
	return msg.WithTimeTick(ts).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(idAllocator.Allocate()))
}

func newBeginMessage(t *testing.T, txnCtx *message.TxnContext, ts uint64) message.ImmutableMessage {
	msg, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	return msg.WithTimeTick(ts).
		WithLastConfirmedUseMessageID().
		WithTxnContext(*txnCtx).
		IntoImmutableMessage(walimplstest.NewTestMessageID(idAllocator.Allocate()))
}

func newCommitMessage(t *testing.T, txnCtx *message.TxnContext, ts uint64) message.ImmutableMessage {
	msg, err := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	return msg.WithTimeTick(ts).
		WithLastConfirmedUseMessageID().
		WithTxnContext(*txnCtx).
		IntoImmutableMessage(walimplstest.NewTestMessageID(idAllocator.Allocate()))
}

func newRollbackMessage(t *testing.T, txnCtx *message.TxnContext, ts uint64) message.ImmutableMessage {
	msg, err := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	return msg.WithTimeTick(ts).
		WithLastConfirmedUseMessageID().
		WithTxnContext(*txnCtx).
		IntoImmutableMessage(walimplstest.NewTestMessageID(idAllocator.Allocate()))
}

func newAlterReplicateConfigMessage(t *testing.T, forcePromote bool, ignore bool, ts uint64) message.ImmutableMessage {
	msg, err := message.NewAlterReplicateConfigMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ForcePromote: forcePromote,
			Ignore:       ignore,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	return msg.WithTimeTick(ts).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(idAllocator.Allocate()))
}

func TestForcePromoteRollsBackUncommittedTxn(t *testing.T) {
	b := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.ComposeTSByTime(time.Now())

	// Create uncommitted transaction
	txnCtx := &message.TxnContext{
		TxnID:     1,
		Keepalive: time.Hour,
	}

	// Start a transaction
	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newBeginMessage(t, txnCtx, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)

	// Add body message
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 100*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)

	// Send force promote message (not ignored) - should rollback all uncommitted transactions
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newAlterReplicateConfigMessage(t, true, false, tsoutil.AddPhysicalDurationOnTs(baseTso, 300*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 400*time.Millisecond))

	// Force promote message should be returned (it's not a txn message)
	assert.Len(t, msgs, 1)
	// All uncommitted transactions should be rolled back
	assert.Len(t, b.builders, 0)
}

func TestForcePromoteIgnored_DoesNotRollback(t *testing.T) {
	b := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.ComposeTSByTime(time.Now())

	// Create uncommitted transaction
	txnCtx := &message.TxnContext{
		TxnID:     1,
		Keepalive: time.Hour,
	}

	// Start a transaction
	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newBeginMessage(t, txnCtx, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)

	// Add body message
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, txnCtx, tsoutil.AddPhysicalDurationOnTs(baseTso, 100*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)

	// Send force promote message WITH ignore=true - should NOT rollback transactions
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newAlterReplicateConfigMessage(t, true, true, tsoutil.AddPhysicalDurationOnTs(baseTso, 300*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 400*time.Millisecond))

	// Message should be returned
	assert.Len(t, msgs, 1)
	// Transaction should still exist (not rolled back because ignore=true)
	assert.Len(t, b.builders, 1)
}

func TestNonForcePromoteAlterReplicateConfigAborts(t *testing.T) {
	b := NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.ComposeTSByTime(time.Now())

	// Create uncommitted transaction
	txnCtx := &message.TxnContext{
		TxnID:     1,
		Keepalive: time.Hour,
	}

	// Start a transaction
	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newBeginMessage(t, txnCtx, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 1)

	// Ordinary topology changes share the exclusive transaction boundary.
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newAlterReplicateConfigMessage(t, false, false, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 300*time.Millisecond))

	// Message should be returned
	assert.Len(t, msgs, 1)
	// Transaction is aborted even without ForcePromote.
	assert.Empty(t, b.builders)
}

func TestTxnBufferSnapshotIsIndependent(t *testing.T) {
	metrics := metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()
	defer metrics.Close()
	buffer := NewTxnBuffer(mlog.With(), metrics)
	txn := &message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	buffer.HandleImmutableMessages([]message.ImmutableMessage{
		newBeginMessage(t, txn, 1), newInsertMessage(t, txn, 2),
	}, 3)
	snapshot := buffer.Snapshot()
	assert.Equal(t, buffer.Bytes(), snapshot.Bytes())
	assert.NotSame(t, buffer.builders[1], snapshot.builders[1])
	// Build mutates the builder and body slice, even with no later body append.
	committed := buffer.HandleImmutableMessages([]message.ImmutableMessage{newCommitMessage(t, txn, 4)}, 4)
	assert.Len(t, committed, 1)
	assert.Empty(t, buffer.builders)
	begin, body := snapshot.builders[1].Messages()
	assert.Equal(t, uint64(1), begin.TimeTick())
	assert.Len(t, body, 1)
	assert.Equal(t, uint64(2), body[0].TimeTick())
	snapshot.HandleImmutableMessages([]message.ImmutableMessage{newRollbackMessage(t, txn, 5)}, 5)
	assert.Empty(t, snapshot.builders)
	assert.Zero(t, snapshot.Bytes())
}
