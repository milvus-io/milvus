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
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPartialUpdateRecoveryRestoresPrefixAndAddsSuffixWithoutLosingBody(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	state := env.partial.state.getTxn(1)
	require.NotNil(t, state)
	require.True(t, state.observedBegin)
	require.True(t, state.recovered)
	require.Equal(t, []int64{10}, state.pks.int64Values)
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
			key := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: pk}
			require.False(t, testPKMutex(t, env.partial.state.rowLocks, key).TryLock(), "commit did not lock recovered prefix and live suffix")
			require.True(t, testPKMutex(t, env.partial.state.rowLocks, key).TryRLock(), "ordinary commit unexpectedly took an exclusive PK lock")
			testPKMutex(t, env.partial.state.rowLocks, key).RUnlock()
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
		key := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10}
		mutex := testPKMutex(t, env.partial.state.rowLocks, key)
		if !mutex.TryLock() {
			return nil, errors.New("replicated commit took a row lock")
		}
		mutex.Unlock()
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	require.True(t, called, "secondary incorrectly rejected the primary's committed CAS proof")
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.Nil(t, env.partial.state.getTxn(1))
}

func TestPartialUpdateLegacySchemaLessInsertPreservesWriteWithoutRowLock(t *testing.T) {
	for _, mode := range []string{"auto_commit", "transaction", "recovered_transaction"} {
		t.Run(mode, func(t *testing.T) {
			body := legacySchemaLessInsert(t, 10)
			var builders map[message.TxnID]*message.ImmutableTxnMessageBuilder
			if mode == "recovered_transaction" {
				builders = recoveredRowLockBuilders(t, body)
			}
			env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t, builders, &staticPrimaryKeyDescriptorGetter{
				err: shards.ErrCollectionSchemaNotFound,
			})
			key := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10}
			// An exclusive same-PK holder cannot block an allowed old-schema write.
			testPKMutex(t, env.partial.state.rowLocks, key).Lock()
			var once bool
			unlock := func() {
				if !once {
					once = true
					testPKMutex(t, env.partial.state.rowLocks, key).Unlock()
				}
			}
			defer unlock()
			if mode == "transaction" {
				result, err := env.append(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
					WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), nil)
				require.NoError(t, err)
				body = body.WithTxnContext(*result.TxnCtx)
			}
			readTS := env.allocateReadTS(t)
			appended := make(chan error, 1)
			go func() {
				var err error
				if mode != "recovered_transaction" {
					_, err = env.append(body, nil)
				}
				if err == nil && mode != "auto_commit" {
					txnID := message.TxnID(1)
					if body.TxnContext() != nil {
						txnID = body.TxnContext().TxnID
					}
					_, err = env.append(newCommitTxnMessage("v1", txnID, 0), nil)
				}
				appended <- err
			}()
			select {
			case err := <-appended:
				require.NoError(t, err)
			case <-time.After(time.Second):
				unlock()
				<-appended
				t.Fatal("legacy schema-less insert attempted to acquire a row lock")
			}
			requirePartialUpdateRetryable(t, env.partial.state.fences.Verify("v1", 10, readTS))
			unlock()
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
	patch := mockey.Mock(message.DecodePayload).To(func(decodeCtx context.Context, msg message.BasicMessage) ([]byte, error) {
		if _, immutable := msg.(message.ImmutableMessage); immutable {
			decoded = true
			deadline, ok := decodeCtx.Deadline()
			require.True(t, ok, "recovery payload decode must have a deadline")
			require.Positive(t, time.Until(deadline))
			require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
			// Simulate WAL-open cancellation while a KMS payload decode waits.
			cancel()
			select {
			case <-decodeCtx.Done():
				return nil, decodeCtx.Err()
			default:
				t.Fatal("recovery decode discarded WAL-open cancellation")
			}
		}
		return msg.Payload(), nil
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

func TestPartialUpdateRecoveryMixedLegacyAndNewInsertLocksOnlyKnownPKs(t *testing.T) {
	legacy := legacySchemaLessInsert(t, 20)
	modern := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	env := newPartialUpdateChainTestEnvWithBuildersAndGetter(t,
		recoveredRowLockBuilders(t, legacy, modern), knownAndLegacyDescriptorGetter{})
	readTS := env.allocateReadTS(t)
	state := env.partial.state.getTxn(1)
	require.True(t, state.observedBegin)
	require.Equal(t, []int64{10}, state.pks.int64Values)
	require.EqualValues(t, 10, state.fenceCollection)
	_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		known := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10}
		unknown := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 20}
		require.False(t, testPKMutex(t, env.partial.state.rowLocks, known).TryLock())
		require.True(t, testPKMutex(t, env.partial.state.rowLocks, known).TryRLock())
		testPKMutex(t, env.partial.state.rowLocks, known).RUnlock()
		require.True(t, testPKMutex(t, env.partial.state.rowLocks, unknown).TryLock(), "schema-less legacy row unexpectedly acquired a row lock")
		testPKMutex(t, env.partial.state.rowLocks, unknown).Unlock()
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.False(t, env.hasPKVersionAfter("v1", 20, readTS))
	requirePartialUpdateRetryable(t, env.partial.state.fences.Verify("v1", 10, readTS))
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
}

type knownAndLegacyDescriptorGetter struct{}

func (knownAndLegacyDescriptorGetter) GetPrimaryKeyDescriptor(_ int64, schemaVersion int32) (shards.PrimaryKeyDescriptor, error) {
	if schemaVersion == latestCollectionSchemaVersion {
		return shards.PrimaryKeyDescriptor{}, shards.ErrCollectionSchemaNotFound
	}
	return shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64}, nil
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

func legacySchemaLessInsert(t *testing.T, pks ...int64) message.MutableMessage {
	t.Helper()
	msg := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(pks...)})
	insert := message.MustAsMutableInsertMessageV1(msg)
	header := insert.Header()
	header.SchemaVersion = nil
	insert.OverwriteHeader(header)
	return msg
}
