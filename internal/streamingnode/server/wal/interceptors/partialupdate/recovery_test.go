package partialupdate

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPartialUpdateRecoveryLocksDurablePrefixAndNewSuffix(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	txnContext := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	_, err := env.append(newGuardTestDelete(20).WithTxnContext(txnContext), nil)
	require.NoError(t, err)
	readTS := env.allocateReadTS(t)
	_, err = env.append(newCommitTxnMessage("v1", 1, 0), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		for _, pk := range []int64{10, 20} {
			key := pkLockKey{vchannel: "v1", kind: primarykey.KindInt64, int64Value: pk}
			require.False(t, testPKMutex(t, env.partial.state.rowLocks, key).TryLock(), "durable prefix and suffix must remain locked during Commit WAL I/O")
			require.True(t, testPKMutex(t, env.partial.state.rowLocks, key).TryRLock(), "ordinary recovery must use shared row locks")
			testPKMutex(t, env.partial.state.rowLocks, key).RUnlock()
		}
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
	require.Nil(t, env.partial.state.getTxn(1))
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
}

func TestPartialUpdateRecoveryReplicatedCASKeepsCommittedSourceWrites(t *testing.T) {
	txnContext := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	body := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(5, 999)).
		WithReplicateHeader(newChainTestReplicateHeader(11, 11))
	builder := recoveredWriteSetBuilder(t, txnContext, 10, body)
	env := newPartialUpdateChainTestEnvWithBuilders(t, map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: builder})
	_, err := env.append(newGuardTestDelete(20).WithTxnContext(txnContext).
		WithReplicateHeader(newChainTestReplicateHeader(12, 12)), nil)
	require.NoError(t, err)
	readTS := env.allocateReadTS(t)
	commit := newCASCommitTxnMessage(t, "v1", 1, 0).WithReplicateHeader(newChainTestReplicateHeader(13, 13))
	_, err = env.append(commit, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		// Replicated messages are serialized by the replicate interceptor and
		// take no row locks.
		for _, pk := range []int64{10, 20} {
			key := pkLockKey{vchannel: "v1", kind: primarykey.KindInt64, int64Value: pk}
			mutex := testPKMutex(t, env.partial.state.rowLocks, key)
			if !mutex.TryLock() {
				return nil, errors.Errorf("replicated commit took the row lock of PK %d", pk)
			}
			mutex.Unlock()
		}
		return env.nextMessageID(), nil
	})
	require.NoError(t, err, "a secondary must preserve the source's CAS result rather than validate the source term locally")
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
}

func TestPartialUpdateRecoveryCleanupPreservesOriginalLease(t *testing.T) {
	txnContext := message.TxnContext{TxnID: 1, Keepalive: 100 * time.Millisecond}
	beginTS := tsoutil.ComposeTSByTime(time.Now())
	builder := recoveredWriteSetBuilder(t, txnContext, beginTS, newGuardTestDelete(10))
	env := newPartialUpdateChainTestEnvWithBuilders(t, map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: builder})
	_, bodies := builder.Messages()
	lastBodyTS := bodies[0].TimeTick()
	env.txnManager.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(lastBodyTS, 99*time.Millisecond))
	require.NotNil(t, env.partial.state.getTxn(1))
	env.txnManager.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(lastBodyTS, 100*time.Millisecond))
	require.Nil(t, env.partial.state.getTxn(1), "snapshot recovery must not renew the old lease")
	_, err := env.txnManager.GetSessionOfTxn(1)
	requireTransactionExpired(t, err)
}

func TestPartialUpdateRecoveryUnsupportedBodyOnlyRejectsItsTransaction(t *testing.T) {
	bad := recoveredWriteSetBuilder(t, message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 10,
		corruptMessageBody(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})))
	good := recoveredWriteSetBuilder(t, message.TxnContext{TxnID: 2, Keepalive: message.TxnKeepaliveInfinite}, 20,
		newGuardTestDelete(20))
	env := newPartialUpdateChainTestEnvWithBuilders(t, map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: bad, 2: good})
	called := false
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return env.nextMessageID(), nil
	})
	requireTransactionExpired(t, err)
	require.False(t, called, "a new-format body with missing PKs must not commit unguarded")
	_, err = env.append(newCommitTxnMessage("v1", 2, 0), nil)
	require.NoError(t, err)
	require.Nil(t, env.partial.state.getTxn(1))
	require.Nil(t, env.partial.state.getTxn(2))
}

func TestPartialUpdateRecoveryPayloadUsesBoundedWALOpenContext(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txnContext := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	builder := recoveredWriteSetBuilder(t, txnContext, 10)
	body := message.MustAsImmutableDeleteMessageV1(newGuardTestDelete(10).WithTxnContext(txnContext).
		WithTimeTick(11).IntoImmutableMessage(walimplstest.NewTestMessageID(11)))
	readStarted := false
	builder.Add(&blockedRecoveryDelete{
		ImmutableDeleteMessageV1: body,
		read: func(bodyCtx context.Context) (*message.DeleteRequest, error) {
			readStarted = true
			deadline, ok := bodyCtx.Deadline()
			require.True(t, ok, "recovery decryption must not retry without a deadline")
			require.LessOrEqual(t, time.Until(deadline), recoveryPKDecodeTimeout)
			cancel()
			<-bodyCtx.Done()
			return nil, bodyCtx.Err()
		},
	})
	builders := map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: builder}
	manager := txn.NewTxnManager(types.PChannelInfo{Name: "p1", Term: 1}, builders)
	t.Cleanup(func() {
		manager.FailTxnAtVChannel("")
		require.NoError(t, manager.GracefulClose(context.Background()))
	})
	txnBuffer := utility.NewTxnBuffer(nil, nil)
	txnBuffer.GetUncommittedMessageBuilder()[1] = builder
	i := (&interceptorBuilder{}).Build(&interceptors.InterceptorBuildParam{
		Context:                ctx,
		TxnManager:             manager,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{TxnBuffer: txnBuffer},
	}).(*appendInterceptor)
	t.Cleanup(i.Close)
	require.True(t, readStarted)
	_, err := i.state.validateCommit(newCommitTxnMessage("v1", 1, 20), 1)
	requireTransactionExpired(t, err)
	_, durableBodies := builder.Messages()
	require.Len(t, durableBodies, 1, "an optional PK decode failure must retain durable body data")
	manager.FailTxnAtVChannel("v1")
	require.Nil(t, i.state.getTxn(1), "a failed recovery still needs its session cleanup callback")
}

type blockedRecoveryDelete struct {
	message.ImmutableDeleteMessageV1
	read func(context.Context) (*message.DeleteRequest, error)
}

func (m *blockedRecoveryDelete) Body(ctx context.Context) (*message.DeleteRequest, error) {
	return m.read(ctx)
}

func TestPartialUpdateLegacyInsertRemainsWritableWithoutPKSchema(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	env.partial.pkDescriptorGetter = &staticPrimaryKeyDescriptorGetter{err: shards.ErrCollectionSchemaNotFound}
	legacy := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	insert := message.MustAsMutableInsertMessageV1(legacy)
	header := insert.Header()
	header.SchemaVersion = nil
	insert.OverwriteHeader(header)
	// A legacy append remains writable even while a local CAS owns this PK.
	release := acquireTestRowLock(t, env.partial.state.rowLocks, "v1", primarykey.Keys{Int64Values: []int64{10}}, true)
	done := make(chan error, 1)
	go func() { _, err := env.append(legacy, nil); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		release()
		t.Fatal("legacy schema-less insert acquired a PK lock")
	}
	release()
	_, err := env.append(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}), nil)
	require.Error(t, err, "versioned inserts must still require exact PK extraction")
}

func TestPartialUpdateLegacyTxnPublishesKnownPKsAndFence(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	env.partial.pkDescriptorGetter = &staticPrimaryKeyDescriptorGetter{err: shards.ErrCollectionSchemaNotFound}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
	result, err := env.append(begin, nil)
	require.NoError(t, err)
	legacy := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	insert := message.MustAsMutableInsertMessageV1(legacy)
	header := insert.Header()
	header.SchemaVersion = nil
	insert.OverwriteHeader(header)
	_, err = env.append(legacy.WithTxnContext(*result.TxnCtx), nil)
	require.NoError(t, err)
	_, err = env.append(newGuardTestDelete(20).WithTxnContext(*result.TxnCtx), nil)
	require.NoError(t, err)
	readTS := env.allocateReadTS(t)
	_, err = env.append(newCommitTxnMessage("v1", result.TxnCtx.TxnID, 0), nil)
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
	requirePartialUpdateRetryable(t, env.partial.state.fences.Verify("v1", 10, readTS))
}

func recoveredWriteSetBuilder(t *testing.T, txnContext message.TxnContext, beginTS uint64, bodies ...message.MutableMessage) *message.ImmutableTxnMessageBuilder {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(beginTS)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin.IntoImmutableMessage(walimplstest.NewTestMessageID(int64(beginTS)))))
	for idx, body := range bodies {
		ts := beginTS + uint64(idx) + 1
		builder.Add(body.WithTxnContext(txnContext).WithTimeTick(ts).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(ts))))
	}
	return builder
}

func requireTransactionExpired(t *testing.T, err error) {
	t.Helper()
	requirePartialUpdateCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED)
}
