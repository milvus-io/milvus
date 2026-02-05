package utility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var idAllocator = typeutil.NewIDAllocator()

func TestTxnBuffer(t *testing.T) {
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.GetCurrentTime()

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

func TestRollbackAllUncommittedTxn(t *testing.T) {
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.GetCurrentTime()

	// Create uncommitted transactions
	txnCtx1 := &message.TxnContext{
		TxnID:     1,
		Keepalive: time.Hour, // Long keepalive so it doesn't expire
	}
	txnCtx2 := &message.TxnContext{
		TxnID:     2,
		Keepalive: time.Hour,
	}

	// Start two transactions
	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newBeginMessage(t, txnCtx1, baseTso),
		newBeginMessage(t, txnCtx2, baseTso),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 2)

	// Add some body messages
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, txnCtx1, tsoutil.AddPhysicalDurationOnTs(baseTso, 100*time.Millisecond)),
		newInsertMessage(t, txnCtx2, tsoutil.AddPhysicalDurationOnTs(baseTso, 100*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond))
	assert.Len(t, msgs, 0)
	assert.Len(t, b.builders, 2)

	// Verify bytes are tracked
	assert.Greater(t, b.Bytes(), 0)

	// Rollback all uncommitted transactions
	b.RollbackAllUncommittedTxn()

	// Verify all transactions are rolled back
	assert.Len(t, b.builders, 0)
	assert.Equal(t, 0, b.Bytes())
}

func TestRollbackAllUncommittedTxn_Empty(t *testing.T) {
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	// Rollback on empty buffer should be a no-op
	b.RollbackAllUncommittedTxn()
	assert.Len(t, b.builders, 0)
	assert.Equal(t, 0, b.Bytes())
}

func TestForcePromoteRollsBackUncommittedTxn(t *testing.T) {
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.GetCurrentTime()

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
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.GetCurrentTime()

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

func TestNonForcePromoteAlterReplicateConfig_DoesNotRollback(t *testing.T) {
	b := NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())

	baseTso := tsoutil.GetCurrentTime()

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

	// Send AlterReplicateConfig message WITHOUT force promote - should NOT rollback
	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newAlterReplicateConfigMessage(t, false, false, tsoutil.AddPhysicalDurationOnTs(baseTso, 200*time.Millisecond)),
	}, tsoutil.AddPhysicalDurationOnTs(baseTso, 300*time.Millisecond))

	// Message should be returned
	assert.Len(t, msgs, 1)
	// Transaction should still exist (not force promote)
	assert.Len(t, b.builders, 1)
}
