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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
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
			if err := testPKSharedError(env.partial.state.rowLocks, "v1", pk); err != nil {
				return nil, errors.Wrap(err, "durable prefix and suffix must remain share-locked during Commit WAL I/O")
			}
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
			if inspectTestPK(env.partial.state.rowLocks, "v1", pk) != (testPKLock{}) {
				return nil, errors.Errorf("replicated commit took the row lock of PK %d", pk)
			}
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
			require.LessOrEqual(t, time.Until(deadline), paramtable.Get().StreamingCfg.PartialUpdateRecoveryDecodeTimeout.GetAsDurationByParse())
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

func TestPartialUpdateRecoveryZeroDecodeTimeoutRejectsRecoveredCommit(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().StreamingCfg.PartialUpdateRecoveryDecodeTimeout.Key
	require.NoError(t, paramtable.Get().Save(key, "0s"))
	defer paramtable.Get().Reset(key)
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	state := env.partial.state.getTxn(1)
	require.NotNil(t, state)
	require.False(t, state.observedBegin)
	appended := false
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return env.nextMessageID(), nil
	})
	requireTransactionExpired(t, err)
	require.False(t, appended, "a commit without a rebuilt write set must not reach the WAL")
}

type blockedRecoveryDelete struct {
	message.ImmutableDeleteMessageV1
	read func(context.Context) (*message.DeleteRequest, error)
}

func (m *blockedRecoveryDelete) Body(ctx context.Context) (*message.DeleteRequest, error) {
	return m.read(ctx)
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
