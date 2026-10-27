package partialupdate

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPartialUpdateRecoveryRestoresPrefixAndAddsSuffixWithoutLosingBody(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	state := env.partial.state.getTxn(1)
	require.NotNil(t, state)
	require.True(t, state.observedBegin)
	require.True(t, state.recovered)
	require.Equal(t, []int64{10}, state.pks.Int64Values)
	readTS := env.allocateReadTS(t)
	ctx := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	var suffix, commit message.ImmutableMessage
	_, err := env.append(newGuardTestDelete(20).WithTxnContext(ctx), func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		id := env.nextMessageID()
		suffix = msg.IntoImmutableMessage(id)
		return id, nil
	})
	require.NoError(t, err)
	_, err = env.append(newCommitTxnMessage("v1", 1, 0), func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		for _, pk := range []int64{10, 20} {
			if err := testPKSharedError(env.partial.state.rowLocks, "v1", pk); err != nil {
				return nil, errors.Wrap(err, "ordinary commit must share-lock the recovered prefix and live suffix")
			}
		}
		id := env.nextMessageID()
		commit = msg.IntoImmutableMessage(id)
		return id, nil
	})
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
	require.Nil(t, env.partial.state.getTxn(1))
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)

	// The recovered consumer buffer retains the persisted prefix. Appending the
	// suffix and commit emits both rows at one commit timestamp.
	buffer := env.buildParam.InitialRecoverSnapshot.TxnBuffer
	completed := buffer.HandleImmutableMessages([]message.ImmutableMessage{suffix, commit}, commit.TimeTick())
	require.Len(t, completed, 1)
	committed := message.AsImmutableTxnMessage(completed[0])
	require.NotNil(t, committed)
	require.Equal(t, 2, committed.Size())
	var pks []int64
	require.NoError(t, committed.RangeOver(func(msg message.ImmutableMessage) error {
		require.Equal(t, commit.TimeTick(), msg.TimeTick())
		pks = append(pks, message.MustAsImmutableDeleteMessageV1(msg).MustBody().GetPrimaryKeys().GetIntId().GetData()...)
		return nil
	}))
	require.Equal(t, []int64{10, 20}, pks)
}

func TestPartialUpdateRecoveryRegistersLeaseCleanup(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	state := env.partial.state.getTxn(1)
	require.True(t, state.cleanupRegistered)
	env.txnManager.CleanupTxnUntil(math.MaxUint64)
	require.Nil(t, env.partial.state.getTxn(1))
	_, err := env.txnManager.GetSessionOfTxn(1)
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10)
}

func TestPartialUpdateRecoveryPreservesExistingFiniteLease(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "live"
		beginTS := tsoutil.ComposeTSByTime(time.Now())
		if expired {
			name = "expired"
			beginTS = tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute))
		}
		t.Run(name, func(t *testing.T) {
			ctx := message.TxnContext{TxnID: 1, Keepalive: 10 * time.Second}
			builders := recoveredRowLockBuildersWithContext(t, ctx, beginTS, newGuardTestDelete(10))
			env := newPartialUpdateChainTestEnvWithBuilders(t, builders)
			session, err := env.txnManager.GetSessionOfTxn(1)
			require.NoError(t, err)
			require.Equal(t, ctx, session.TxnContext())
			called := false
			_, err = env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				called = true
				return env.nextMessageID(), nil
			})
			if expired {
				require.True(t, status.AsStreamingError(err).IsTxnExpired())
				require.False(t, called, "recovery must not renew an expired transaction")
				env.txnManager.CleanupTxnUntil(math.MaxUint64)
				require.Nil(t, env.partial.state.getTxn(1))
			} else {
				require.NoError(t, err)
				require.True(t, called)
				require.Equal(t, message.TxnStateCommitted, session.State())
			}
		})
	}
}

// Replicated messages are serialized and deduplicated by the replicate
// interceptor and skip the row-lock hook. A recovered replicated CAS keeps the
// source-proof bypass and still publishes its PK versions.
func TestPartialUpdateRecoveryReplicatedCASTakesNoRowLock(t *testing.T) {
	body := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(5, 99)).
		WithReplicateHeader(newChainTestReplicateHeader(11, 11))
	env := newPartialUpdateChainTestEnvWithBuilders(t, recoveredRowLockBuilders(t, body))
	readTS := env.allocateReadTS(t)
	state := env.partial.state.getTxn(1)
	require.True(t, state.recovered)
	require.True(t, state.observedBegin)
	called := false
	commit := newCASCommitTxnMessage(t, "v1", 1, 0).
		WithReplicateHeader(newChainTestReplicateHeader(12, 12))
	_, err := env.append(commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		if inspectTestPK(env.partial.state.rowLocks, "v1", int64(10)) != (testPKLock{}) {
			return nil, errors.New("replicated commit took a row lock")
		}
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	require.True(t, called, "secondary incorrectly rejected the primary's committed CAS proof")
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.Nil(t, env.partial.state.getTxn(1))
}

// Supported paths always give ShardManager the collection schema, so a
// version-less insert without one fails closed instead of skipping row locks.
func TestPartialUpdateVersionlessInsertWithoutSchemaFailsClosed(t *testing.T) {
	for _, mode := range []string{"auto_commit", "transaction", "recovered_transaction"} {
		t.Run(mode, func(t *testing.T) {
			body := withoutSchemaVersion(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}))
			var builders map[message.TxnID]*message.ImmutableTxnMessageBuilder
			if mode == "recovered_transaction" {
				builders = recoveredRowLockBuilders(t, body)
			}
			env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t, builders, &staticPrimaryKeyDescriptorGetter{
				err: shards.ErrCollectionSchemaNotFound,
			})
			readTS := env.allocateReadTS(t)
			appended := false
			appendOp := func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return env.nextMessageID(), nil
			}
			switch mode {
			case "auto_commit":
				_, err := env.append(body, appendOp)
				requireUnrecoverable(t, err)
				require.False(t, appended)
			case "transaction":
				result, err := env.append(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
					WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), nil)
				require.NoError(t, err)
				_, err = env.append(body.WithTxnContext(*result.TxnCtx), appendOp)
				requireUnrecoverable(t, err)
				require.False(t, appended)
				// The rejected body never reached the WAL and left no write set.
				_, err = env.append(newCommitTxnMessage("v1", result.TxnCtx.TxnID, 0), nil)
				require.NoError(t, err)
			case "recovered_transaction":
				// The durable body's PK descriptor cannot be resolved, so the
				// transaction is incomplete and its commit is rejected before
				// the WAL append.
				_, err := env.append(newCommitTxnMessage("v1", 1, 0), appendOp)
				require.Error(t, err)
				require.True(t, status.AsStreamingError(err).IsTxnExpired())
				require.True(t, txn.IsCommitAdmissionRejected(err))
				require.False(t, appended)
			}
			require.NoError(t, env.partial.state.fences.Verify("v1", 10, readTS))
			require.False(t, env.hasPKVersionAfter("v1", 10, readTS))
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10)
		})
	}
}

func TestPartialUpdateNewInsertWithoutSchemaRejectsBeforeWAL(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t, nil, &staticPrimaryKeyDescriptorGetter{
		err: shards.ErrCollectionSchemaNotFound,
	})
	appended := false
	_, err := env.append(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return env.nextMessageID(), nil
	})
	require.Error(t, err)
	require.False(t, appended)
}

func TestPartialUpdateRecoveryMalformedNewInsertDoesNotBlockWALOpen(t *testing.T) {
	body := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t, recoveredRowLockBuilders(t, body), &staticPrimaryKeyDescriptorGetter{
		err: shards.ErrCollectionSchemaNotFound,
	})
	state := env.partial.state.getTxn(1)
	require.NotNil(t, state)
	require.False(t, state.observedBegin)
	appended := false
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return env.nextMessageID(), nil
	})
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.False(t, appended)
}

func TestPartialUpdateRecoveryPayloadDecodeRespectsWALOpenCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decoded := false
	patch := mockey.Mock(decodeDeleteBody).To(func(decodeCtx context.Context, msg message.BasicMessage) (*message.DeleteRequest, error) {
		if _, immutable := msg.(message.ImmutableMessage); immutable {
			decoded = true
			deadline, ok := decodeCtx.Deadline()
			require.True(t, ok, "recovery payload decode must have a deadline")
			require.Positive(t, time.Until(deadline))
			require.LessOrEqual(t, time.Until(deadline), paramtable.Get().StreamingCfg.PartialUpdateRecoveryDecodeTimeout.GetAsDurationByParse())
			// Simulate WAL-open cancellation while a KMS payload decode waits.
			cancel()
			select {
			case <-decodeCtx.Done():
				return nil, decodeCtx.Err()
			default:
				t.Fatal("recovery decode discarded WAL-open cancellation")
			}
		}
		return utility.DecodeDeleteBody(decodeCtx, msg.(message.MutableMessage))
	}).Build()
	defer patch.UnPatch()
	getter := &staticPrimaryKeyDescriptorGetter{
		descriptor: shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64},
	}
	env := newPartialUpdateChainTestEnvWithBuildersGetterAndContext(ctx, t,
		recoveredRowLockBuilders(t, newGuardTestDelete(10)), getter)
	require.True(t, decoded)
	state := env.partial.state.getTxn(1)
	require.NotNil(t, state)
	require.False(t, state.observedBegin)
	appended := false
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return env.nextMessageID(), nil
	})
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.False(t, appended)
	// A failed optional recovery decode does not prevent unrelated writes.
	_, err = env.append(newGuardTestDelete(20), nil)
	require.NoError(t, err)
}

// A recovered transaction derives the PKs of version-less and stale-version
// inserts from the current schema and locks all of them.
func TestPartialUpdateRecoveryLocksPKsRegardlessOfSchemaVersion(t *testing.T) {
	versionless := withoutSchemaVersion(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(20)}))
	versioned := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t,
		recoveredRowLockBuilders(t, versionless, versioned), currentSchemaDescriptorGetter{})
	readTS := env.allocateReadTS(t)
	state := env.partial.state.getTxn(1)
	require.True(t, state.observedBegin)
	require.ElementsMatch(t, []int64{10, 20}, state.pks.Int64Values)
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		for _, pk := range []int64{10, 20} {
			if err := testPKSharedError(env.partial.state.rowLocks, "v1", pk); err != nil {
				return nil, errors.Wrap(err, "every recovered insert row must be share-locked")
			}
		}
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
	require.NoError(t, env.partial.state.fences.Verify("v1", 10, readTS))
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
}

func recoveredRowLockBuilders(t *testing.T, bodies ...message.MutableMessage) map[message.TxnID]*message.ImmutableTxnMessageBuilder {
	t.Helper()
	ctx := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	return recoveredRowLockBuildersWithContext(t, ctx, 10, bodies...)
}

func recoveredRowLockBuildersWithContext(t *testing.T, ctx message.TxnContext, beginTS uint64, bodies ...message.MutableMessage) map[message.TxnID]*message.ImmutableTxnMessageBuilder {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(ctx).WithTimeTick(beginTS).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	for idx, body := range bodies {
		builder.Add(body.WithTxnContext(ctx).WithTimeTick(beginTS + uint64(1+idx)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(11 + idx))))
	}
	return map[message.TxnID]*message.ImmutableTxnMessageBuilder{1: builder}
}
