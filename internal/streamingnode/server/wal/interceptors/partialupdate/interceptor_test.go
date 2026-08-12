package partialupdate

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	lockinterceptor "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	streamingtimetick "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestPartialUpdateInterceptorBuildsIndependentPerWALState(t *testing.T) {
	budget := newVersionByteBudget(2 * estimatedVersionEntryFixedBytes)
	builder := &interceptorBuilder{versionIndexBudget: budget}
	first := builder.Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: "p1", Term: 1},
	}).(*appendInterceptor)
	second := builder.Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: "p1", Term: 2},
	}).(*appendInterceptor)

	require.NotSame(t, first.state, second.state)
	require.Same(t, first.state.pkVersions.budget, second.state.pkVersions.budget)
	require.EqualValues(t, 1, first.state.channel.Term)
	require.EqualValues(t, 2, second.state.channel.Term)

	first.state.pkVersions.UpdateAllTyped("v1", primaryKeys{
		kind:        primaryKeyKindInt64,
		int64Values: []int64{10},
	}, 100)
	longPK := strings.Repeat("x", int(estimatedVersionEntryFixedBytes))
	second.state.pkVersions.UpdateAllTyped("v2", primaryKeys{
		kind:         primaryKeyKindString,
		stringValues: []string{longPK},
	}, 101)
	require.Equal(t, estimatedVersionEntryFixedBytes, budget.used.Load())
	require.LessOrEqual(t, budget.used.Load(), budget.limit)
	requirePartialUpdateRetryable(t, second.state.pkVersions.VerifyTyped(
		"v2",
		primaryKeys{},
		101,
		102,
	))

	first.Close()
	require.Zero(t, budget.used.Load())
	second.Close()
	require.Zero(t, budget.used.Load())
}

func TestPartialUpdateChainSerializesConcurrentCASCommits(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	firstTxn := env.prepareCASTxn(t, readTS, 10)
	secondTxn := env.prepareCASTxn(t, readTS, 10)
	firstCommit := newChainTestCASCommit(t, firstTxn)
	secondCommit := newChainTestCASCommit(t, secondTxn)

	firstCommitAtWAL := make(chan struct{})
	releaseFirstCommit := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := env.append(
			firstCommit,
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				close(firstCommitAtWAL)
				<-releaseFirstCommit
				return env.nextMessageID(), nil
			},
		)
		firstDone <- err
	}()
	<-firstCommitAtWAL

	var commitAppendCount atomic.Int32
	commitAppendCount.Store(1)
	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		_, err := env.append(
			secondCommit,
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				commitAppendCount.Add(1)
				return env.nextMessageID(), nil
			},
		)
		secondDone <- err
	}()
	<-secondStarted

	select {
	case err := <-secondDone:
		close(releaseFirstCommit)
		require.NoError(t, <-firstDone)
		t.Fatalf("second CAS completed before the first commit published its write: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirstCommit)
	require.NoError(t, <-firstDone)
	requirePartialUpdateRetryable(t, <-secondDone)
	require.EqualValues(t, 1, commitAppendCount.Load())
}

func TestPartialUpdateChainCASWaitsForOrdinaryWritePublication(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	casTxn := env.prepareCASTxn(t, readTS, 10)
	commit := newChainTestCASCommit(t, casTxn)

	writerAtWAL := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(
			newDeleteMessage(&schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			}),
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				close(writerAtWAL)
				<-releaseWriter
				return env.nextMessageID(), nil
			},
		)
		writerDone <- err
	}()
	<-writerAtWAL

	commitReachedWAL := make(chan struct{}, 1)
	commitDone := make(chan error, 1)
	go func() {
		_, err := env.append(
			commit,
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				commitReachedWAL <- struct{}{}
				return env.nextMessageID(), nil
			},
		)
		commitDone <- err
	}()

	select {
	case err := <-commitDone:
		close(releaseWriter)
		require.NoError(t, <-writerDone)
		t.Fatalf("CAS completed before the ordinary writer published its PK: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseWriter)
	require.NoError(t, <-writerDone)
	requirePartialUpdateRetryable(t, <-commitDone)
	select {
	case <-commitReachedWAL:
		t.Fatal("conflicting CAS CommitTxn reached the WAL")
	default:
	}
}

func TestPartialUpdateChainPublishesCASBeforeNextOrdinaryWriter(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	casTxn := env.prepareCASTxn(t, readTS, 10)
	commit := newChainTestCASCommit(t, casTxn)

	commitAtWAL := make(chan struct{})
	releaseCommit := make(chan struct{})
	commitDone := make(chan error, 1)
	go func() {
		_, err := env.append(
			commit,
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				close(commitAtWAL)
				<-releaseCommit
				return env.nextMessageID(), nil
			},
		)
		commitDone <- err
	}()
	<-commitAtWAL

	writerAtWAL := make(chan bool, 1)
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(
			newDeleteMessage(&schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			}),
			func(context.Context, message.MutableMessage) (message.MessageID, error) {
				writerAtWAL <- env.hasPKVersionAfter("v1", int64(10), readTS)
				return env.nextMessageID(), nil
			},
		)
		writerDone <- err
	}()

	select {
	case <-writerAtWAL:
		close(releaseCommit)
		require.NoError(t, <-commitDone)
		t.Fatal("ordinary writer reached the WAL while the CAS commit held the vchannel lock")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseCommit)
	require.NoError(t, <-commitDone)
	require.True(t, <-writerAtWAL, "CAS PK version was not published before the next writer acquired the vchannel lock")
	require.NoError(t, <-writerDone)
}

func TestPartialUpdateChainExpirationCleansPendingTxn(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	txnContext := env.prepareCASTxn(t, readTS, 10)
	require.NotNil(t, env.partial.state.getTxn(txnContext.TxnID))

	env.txnManager.CleanupTxnUntil(math.MaxUint64)
	require.Nil(t, env.partial.state.getTxn(txnContext.TxnID))
}

func TestPartialUpdateChainRecoveredOrdinaryTxnKeepsCommitSemantics(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	readTS := env.allocateReadTS(t)

	_, err := env.append(newCommitTxnMessage("v1", 1, 0), nil)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, env.partial.state.incompleteTxnFences.Verify("v1", readTS))

	session, err := env.txnManager.GetSessionOfTxn(1)
	require.NoError(t, err)
	require.Equal(t, message.TxnStateCommitted, session.State())
}

func TestPartialUpdateChainRecoveredLocalCASReturnsRetry(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, true)
	commit := newCASCommitTxnMessage(t, "v1", 1, 0)

	_, err := env.append(commit, nil)
	requirePartialUpdateRetryable(t, err)
	require.True(t, txn.IsCommitAdmissionRejected(err))

	session, err := env.txnManager.GetSessionOfTxn(1)
	require.NoError(t, err)
	require.Equal(t, message.TxnStateRollbacked, session.State())
}

func TestPartialUpdateInterceptorRecordsTxnWriteAndMetaAfterAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	meta := validCASMeta(100, 1)
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, meta).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)
	txnState := interceptor.state.getTxn(1)
	require.Equal(t, primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, txnState.pks)
	require.True(t, proto.Equal(meta, txnState.meta))
	require.EqualValues(t, 10, txnState.collectionID)
	require.EqualValues(t, 1, txnState.schemaVersion)
	require.True(t, txnState.casScopeSet)
}

func TestPartialUpdateInterceptorCollectsPKsAcrossInsertChunks(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	meta := validCASMeta(100, 1)

	for idx, pks := range [][]int64{{10}, {20, 30}} {
		msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(pks...)}, meta).
			WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
			WithTimeTick(uint64(120 + idx))
		_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
		require.NoError(t, err)
	}

	txnState := interceptor.state.getTxn(1)
	require.Equal(t, primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10, 20, 30}}, txnState.pks)
	require.True(t, proto.Equal(meta, txnState.meta))
}

func TestPartialUpdateInterceptorPublishesConcurrentTxnBodies(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "replicated"}[replicated], func(t *testing.T) {
			interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
			txnContext := message.TxnContext{TxnID: 1, Keepalive: time.Second}
			interceptor.state.recordTxnBegin(txnContext.TxnID)
			bodies := make([]message.MutableMessage, 0, 3)
			for idx, pk := range []int64{10, 20, 30} {
				body := newDeleteMessage(&schemapb.IDs{
					IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}},
				}).WithTxnContext(txnContext).WithTimeTick(uint64(120 + idx))
				if replicated {
					body = body.WithReplicateHeader(newChainTestReplicateHeader(int64(idx+1), body.TimeTick()))
				}
				bodies = append(bodies, body)
			}

			var wg sync.WaitGroup
			errs := make(chan error, len(bodies))
			for _, body := range bodies {
				wg.Add(1)
				go func(body message.MutableMessage) {
					defer wg.Done()
					_, err := interceptor.DoAppend(context.Background(), body, appendOK)
					errs <- err
				}(body)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}

			commit := newCommitTxnMessage("v1", txnContext.TxnID, 130)
			if replicated {
				commit = commit.WithReplicateHeader(newChainTestReplicateHeader(4, commit.TimeTick()))
			}
			_, err := interceptor.DoAppend(context.Background(), commit, appendOK)
			require.NoError(t, err)
			for _, pk := range []int64{10, 20, 30} {
				requirePartialUpdateRetryable(t, interceptor.state.pkVersions.Verify("v1", []any{pk}, 129, 131))
			}
		})
	}
}

func TestPartialUpdateInterceptorCommitsCAS(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	commit := newCASCommitTxnMessage(t, "v1", 1, 130)

	_, err := interceptor.DoAppend(context.Background(), commit, appendOK)
	require.NoError(t, err)
	require.Nil(t, interceptor.state.getTxn(1))
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 129, 131))
}

func TestPartialUpdateInterceptorRejectsCommitWithoutTxnContext(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()

	_, err := interceptor.DoAppend(context.Background(), commit, appendOK)
	requireUnrecoverable(t, err)
}

func TestPartialUpdateInterceptorRejectsCASConflictBeforeAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	interceptor.state.pkVersions.UpdateAll("v1", []any{int64(10)}, 125)
	called := false

	_, err := interceptor.DoAppend(context.Background(), newCASCommitTxnMessage(t, "v1", 1, 130), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return walimplstest.NewTestMessageID(1), nil
	})

	requirePartialUpdateRetryable(t, err)
	require.False(t, called)
	require.Nil(t, interceptor.state.getTxn(1))
}

func TestPartialUpdateInterceptorCommitAppendErrorDoesNotPublish(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	expectedErr := errors.New("append failed")

	_, err := interceptor.DoAppend(context.Background(), newCASCommitTxnMessage(t, "v1", 1, 130), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, interceptor.state.getTxn(1))
	require.NoError(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 129, 131))
}

func TestPartialUpdateInterceptorRejectsMissingCommitMarker(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	called := false

	_, err := interceptor.DoAppend(context.Background(), newCommitTxnMessage("v1", 1, 130), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return walimplstest.NewTestMessageID(1), nil
	})

	requireUnrecoverable(t, err)
	require.False(t, called)
}

func TestPartialUpdateInterceptorPublishesOrdinaryTxn(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	interceptor.state.recordTxnBegin(1)
	body := newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
	}).WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), body, appendOK)
	require.NoError(t, err)

	_, err = interceptor.DoAppend(context.Background(), newCommitTxnMessage("v1", 1, 130), appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 129, 131))
}

func TestPartialUpdateInterceptorRecoveredOrdinaryTxnUsesVChannelFence(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	body := newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
	}).WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), body, appendOK)
	require.NoError(t, err)
	require.False(t, interceptor.state.getTxn(1).observedBegin)

	_, err = interceptor.DoAppend(context.Background(), newCommitTxnMessage("v1", 1, 130), appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.incompleteTxnFences.Verify("v1", 129))
	require.NoError(t, interceptor.state.incompleteTxnFences.Verify("v1", 130))
}

func TestPartialUpdateInterceptorRecoveredLocalCASRetries(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(100, 1)).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)

	called := false
	_, err = interceptor.DoAppend(
		context.Background(),
		newCASCommitTxnMessage(t, "v1", 1, 130),
		func(context.Context, message.MutableMessage) (message.MessageID, error) {
			called = true
			return walimplstest.NewTestMessageID(1), nil
		},
	)
	requirePartialUpdateRetryable(t, err)
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.False(t, called)
}

func TestPartialUpdateInterceptorRecoveredReplicatedCASUsesVChannelFence(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 2})
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(100, 1)).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)
	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)

	commit := newCASCommitTxnMessage(t, "v1", 1, 130).
		WithReplicateHeader(newChainTestReplicateHeader(2, 130))
	_, err = interceptor.DoAppend(context.Background(), commit, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.incompleteTxnFences.Verify("v1", 129))
}

func TestPartialUpdateInterceptorReplicatedCASBypassesSourceTerm(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 2})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	commit := newCASCommitTxnMessage(t, "v1", 1, 130).WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "primary",
		MessageID:              walimplstest.NewTestMessageID(10),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(9),
		TimeTick:               130,
		VChannel:               "v1",
	})

	_, err := interceptor.DoAppend(context.Background(), commit, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 129, 131))
}

func TestPartialUpdateInterceptorRollbackCleansTxnState(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	appendCASTxnBody(t, interceptor, 1, validCASMeta(100, 1), 120, 10)
	rollback := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(130)

	_, err := interceptor.DoAppend(context.Background(), rollback, appendOK)
	require.NoError(t, err)
	require.Nil(t, interceptor.state.getTxn(1))
}

func TestPartialUpdateInterceptorRejectsMalformedCASBeforeAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := withRawPartialUpdateCAS(
		newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}).
			WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
			WithTimeTick(120),
		"bad-cas",
	)
	called := false

	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return walimplstest.NewTestMessageID(1), nil
	})

	requireUnrecoverable(t, err)
	require.False(t, called)
}

func TestPartialUpdateInterceptorRejectsNonTransactionalCAS(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(100, 1)).WithTimeTick(120)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	requireUnrecoverable(t, err)
}

func TestPartialUpdateInterceptorRejectsCASWithMissingPKField(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := newCASInsertMessage(t, nil, validCASMeta(100, 1)).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	requireUnrecoverable(t, err)
}

func TestPartialUpdateInterceptorRejectsCASWithoutExplicitSchemaVersion(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(100, 1)).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)
	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	header := insertMsg.Header()
	header.SchemaVersion = nil
	insertMsg.OverwriteHeader(header)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	requireUnrecoverable(t, err)
}

func TestPartialUpdateInterceptorRejectsMalformedDeleteBody(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := corruptMessageBody(newDeleteMessage(&schemapb.IDs{})).WithTimeTick(120)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	requireUnrecoverable(t, err)
}

func TestPartialUpdateInterceptorUpdatesNonTxnDeletePKAfterAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	msg := newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
	}).WithTimeTick(120)

	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 119, 121))
}

func TestPartialUpdateInterceptorTimeTickAdvancesPKVersionRetention(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	now := time.Now()
	commitTS := tsoutil.ComposeTSByTime(now)
	advanceTS := tsoutil.ComposeTSByTime(now.Add(31 * time.Second))
	interceptor.state.pkVersions.UpdateAll("v1", []any{int64(10)}, commitTS)

	msg := streamingtimetick.NewTimeTickMsg(advanceTS, nil, 1, false)
	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)

	size, withinCapacity := snapshotPKVersionIndex(interceptor.state.pkVersions)
	require.True(t, withinCapacity)
	require.Zero(t, size)
}

func TestPartialUpdateInterceptorCollectionFences(t *testing.T) {
	t.Run("truncate", func(t *testing.T) {
		interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
		_, err := interceptor.DoAppend(context.Background(), newTruncateCollectionMessage(10).WithTimeTick(120), appendOK)
		require.NoError(t, err)
		requirePartialUpdateRetryable(t, interceptor.state.fences.Verify("v1", 10, 119))
	})
	t.Run("drop collection", func(t *testing.T) {
		interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
		interceptor.state.pkVersions.UpdateAll("v1", []any{int64(10)}, 110)
		interceptor.state.pkVersions.UpdateAll("v2", []any{int64(20)}, 110)
		interceptor.state.incompleteTxnFences.Update("v1", 110)
		interceptor.state.incompleteTxnFences.Update("v2", 110)
		interceptor.state.fences.Update("v1", 10, 110)
		interceptor.state.fences.Update("v2", 20, 110)
		_, err := interceptor.DoAppend(context.Background(), newDropCollectionMessage(10).WithTimeTick(120), appendOK)
		require.NoError(t, err)

		_, loaded := interceptor.state.pkVersions.channels.Load("v1")
		require.False(t, loaded)
		_, loaded = interceptor.state.pkVersions.channels.Load("v2")
		require.True(t, loaded)
		require.Equal(t, versionIndexBudgetForEntries(1), interceptor.state.pkVersions.budget.used.Load())
		requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v2", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{20}}, 109, 121))
		require.NoError(t, interceptor.state.incompleteTxnFences.Verify("v1", 109))
		requirePartialUpdateRetryable(t, interceptor.state.incompleteTxnFences.Verify("v2", 109))
		require.NoError(t, interceptor.state.fences.Verify("v1", 10, 109))
		requirePartialUpdateRetryable(t, interceptor.state.fences.Verify("v2", 20, 109))
	})
}

func TestPartialUpdateInterceptorKeepsStateWhenDropCollectionAppendFails(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	interceptor.state.pkVersions.UpdateAll("v1", []any{int64(10)}, 110)
	interceptor.state.incompleteTxnFences.Update("v1", 110)
	interceptor.state.fences.Update("v1", 10, 110)
	expectedErr := errors.New("append failed")

	_, err := interceptor.DoAppend(context.Background(), newDropCollectionMessage(10).WithTimeTick(120), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	_, loaded := interceptor.state.pkVersions.channels.Load("v1")
	require.True(t, loaded)
	require.Equal(t, versionIndexBudgetForEntries(1), interceptor.state.pkVersions.budget.used.Load())
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 109, 121))
	requirePartialUpdateRetryable(t, interceptor.state.incompleteTxnFences.Verify("v1", 109))
	requirePartialUpdateRetryable(t, interceptor.state.fences.Verify("v1", 10, 109))
}

func TestPartialUpdateInterceptorRejectsEmptyDropCollectionBeforeAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	called := false

	_, err := interceptor.DoAppend(context.Background(), newDropCollectionMessage(0).WithTimeTick(120), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return walimplstest.NewTestMessageID(1), nil
	})

	requireUnrecoverable(t, err)
	require.False(t, called)
}

func TestPartialUpdateInterceptorExtractsOrdinaryInsertPKUsingDescriptor(t *testing.T) {
	schemaManager := &partialUpdateSchemaManagerTarget{}
	descriptor := shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_Int64}
	m := mockey.Mock((*partialUpdateSchemaManagerTarget).GetPrimaryKeyDescriptor).Return(descriptor, nil).Build()
	defer m.UnPatch()
	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:  types.PChannelInfo{Name: "p1", Term: 1},
		ShardManager: schemaManager,
	}).(*appendInterceptor)

	_, err := interceptor.DoAppend(
		context.Background(),
		newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}).WithTimeTick(120),
		appendOK,
	)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.pkVersions.VerifyTyped("v1", primaryKeys{kind: primaryKeyKindInt64, int64Values: []int64{10}}, 119, 121))
}

func TestPartialUpdateInterceptorFallsBackToFenceWithoutSchema(t *testing.T) {
	schemaManager := &partialUpdateSchemaManagerTarget{}
	m := mockey.Mock((*partialUpdateSchemaManagerTarget).GetPrimaryKeyDescriptor).
		Return(shards.PrimaryKeyDescriptor{}, shards.ErrCollectionSchemaNotFound).
		Build()
	defer m.UnPatch()
	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:  types.PChannelInfo{Name: "p1", Term: 1},
		ShardManager: schemaManager,
	}).(*appendInterceptor)
	msg := newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)})
	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	header := insertMsg.Header()
	header.SchemaVersion = nil
	insertMsg.OverwriteHeader(header)

	_, err := interceptor.DoAppend(context.Background(), msg.WithTimeTick(120), appendOK)
	require.NoError(t, err)
	requirePartialUpdateRetryable(t, interceptor.state.fences.Verify("v1", 10, 119))
}

func TestPartialUpdateInterceptorPreservesSchemaVersionMismatch(t *testing.T) {
	schemaManager := &partialUpdateSchemaManagerTarget{}
	m := mockey.Mock((*partialUpdateSchemaManagerTarget).GetPrimaryKeyDescriptor).
		Return(shards.PrimaryKeyDescriptor{}, shards.ErrCollectionSchemaVersionNotMatch).
		Build()
	defer m.UnPatch()
	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:  types.PChannelInfo{Name: "p1", Term: 1},
		ShardManager: schemaManager,
	}).(*appendInterceptor)

	_, err := interceptor.DoAppend(
		context.Background(),
		newInsertMessage([]*schemapb.FieldData{int64PKFieldData(10)}).WithTimeTick(120),
		appendOK,
	)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsSchemaVersionMismatch())
}

func TestPartialUpdateInterceptorRejectsTxnCASAttemptMismatchBeforeAppend(t *testing.T) {
	interceptor := newTestAppendInterceptor(types.PChannelInfo{Name: "p1", Term: 1})
	require.NoError(t, interceptor.state.recordTxnCAS(1, validCASMeta(100, 1), casInsertScope{
		collectionID:  10,
		schemaVersion: 1,
	}))
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(20)}, validCASMeta(101, 1)).
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: time.Second}).
		WithTimeTick(120)
	called := false

	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		called = true
		return walimplstest.NewTestMessageID(1), nil
	})

	requireUnrecoverable(t, err)
	require.False(t, called)
}

type partialUpdateChainTestEnv struct {
	chain        interceptors.InterceptorWithReady
	partial      *appendInterceptor
	txnManager   *txn.TxnManager
	writeMetrics *metricsutil.WriteMetrics
	nextID       atomic.Int64
}

func newPartialUpdateChainTestEnv(t *testing.T) *partialUpdateChainTestEnv {
	return newPartialUpdateChainTestEnvWithBuilders(t, nil)
}

func newPartialUpdateChainTestEnvWithRecoveredTxn(t *testing.T, cas bool) *partialUpdateChainTestEnv {
	t.Helper()
	txnContext := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext).
		WithTimeTick(10)
	beginID := walimplstest.NewTestMessageID(10)
	builder := message.NewImmutableTxnMessageBuilder(
		message.MustAsImmutableBeginTxnMessageV2(begin.IntoImmutableMessage(beginID)),
	)

	var body message.MutableMessage
	if cas {
		body = newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(5, 1))
	} else {
		body = newDeleteMessage(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
		})
	}
	body = body.WithTxnContext(txnContext).WithTimeTick(11)
	builder.Add(body.IntoImmutableMessage(walimplstest.NewTestMessageID(11)))
	return newPartialUpdateChainTestEnvWithBuilders(t, map[message.TxnID]*message.ImmutableTxnMessageBuilder{
		txnContext.TxnID: builder,
	})
}

func newPartialUpdateChainTestEnvWithBuilders(
	t *testing.T,
	builders map[message.TxnID]*message.ImmutableTxnMessageBuilder,
) *partialUpdateChainTestEnv {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)

	channel := types.PChannelInfo{Name: "p1", Term: 1}
	lastConfirmed := walimplstest.NewTestMessageID(0)
	lastTimeTick := streamingtimetick.NewTimeTickMsg(1, lastConfirmed, 0, true).
		IntoImmutableMessage(lastConfirmed)
	txnManager := txn.NewTxnManager(channel, builders)
	if len(builders) == 0 {
		<-txnManager.RecoverDone()
	}
	param := &interceptors.InterceptorBuildParam{
		ChannelInfo:            channel,
		LastTimeTickMessage:    lastTimeTick,
		LastConfirmedMessageID: lastConfirmed,
		MVCCManager:            mvcc.NewMVCCManager(lastTimeTick.TimeTick()),
		TxnManager:             txnManager,
		ShardManager: &partialUpdateSchemaManagerTarget{
			primaryKeyDescriptorGetter: &staticPrimaryKeyDescriptorGetter{
				descriptor: shards.PrimaryKeyDescriptor{
					FieldID:  100,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	partial := NewInterceptorBuilder().Build(param).(*appendInterceptor)
	chain := interceptors.NewChainedInterceptor(
		lockinterceptor.NewInterceptorBuilder().Build(param),
		streamingtimetick.NewInterceptorBuilder().Build(param),
		partial,
	)
	writeMetrics := metricsutil.NewWriteMetrics(channel, message.WALNameRocksmq)
	t.Cleanup(writeMetrics.Close)
	t.Cleanup(chain.Close)
	return &partialUpdateChainTestEnv{
		chain:        chain,
		partial:      partial,
		txnManager:   txnManager,
		writeMetrics: writeMetrics,
	}
}

func (e *partialUpdateChainTestEnv) append(
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (*utility.ExtraAppendResult, error) {
	if appendOp == nil {
		appendOp = func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return e.nextMessageID(), nil
		}
	}
	extra := &utility.ExtraAppendResult{}
	ctx := utility.WithExtraAppendResult(context.Background(), extra)
	ctx = utility.WithAppendMetricsContext(ctx, e.writeMetrics.StartAppend(msg))
	_, err := e.chain.DoAppend(ctx, msg, appendOp)
	return extra, err
}

func (e *partialUpdateChainTestEnv) allocateReadTS(t *testing.T) uint64 {
	t.Helper()
	readTS, err := resource.Resource().TSOAllocator().Allocate(context.Background())
	require.NoError(t, err)
	return readTS
}

func (e *partialUpdateChainTestEnv) prepareCASTxn(t *testing.T, readTS uint64, pk int64) message.TxnContext {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	result, err := e.append(begin, nil)
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)

	body := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(pk)}, validCASMeta(readTS, 1)).
		WithTxnContext(*result.TxnCtx)
	_, err = e.append(body, nil)
	require.NoError(t, err)
	return *result.TxnCtx
}

func (e *partialUpdateChainTestEnv) nextMessageID() message.MessageID {
	return walimplstest.NewTestMessageID(e.nextID.Add(1))
}

func (e *partialUpdateChainTestEnv) hasPKVersionAfter(vchannel string, pk int64, readTS uint64) bool {
	channel := e.partial.state.pkVersions.channel(vchannel)
	channel.mu.Lock()
	defer channel.mu.Unlock()
	entry := channel.int64Versions[pk]
	return entry != nil && entry.commitTS > readTS
}

func newChainTestCASCommit(t *testing.T, txnContext message.TxnContext) message.MutableMessage {
	t.Helper()
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext)
	require.NoError(t, message.MarkPartialUpdateCASCommit(commit))
	return commit
}

func newChainTestReplicateHeader(id int64, timetick uint64) *message.ReplicateHeader {
	msgID := walimplstest.NewTestMessageID(id)
	return &message.ReplicateHeader{
		ClusterID:              "primary",
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               timetick,
		VChannel:               "v1",
	}
}

func appendCASTxnBody(
	t *testing.T,
	interceptor *appendInterceptor,
	txnID message.TxnID,
	meta *messagespb.PartialUpdateCAS,
	timetick uint64,
	pks ...int64,
) {
	t.Helper()
	interceptor.state.recordTxnBegin(txnID)
	msg := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(pks...)}, meta).
		WithTxnContext(message.TxnContext{TxnID: txnID, Keepalive: time.Second}).
		WithTimeTick(timetick)
	_, err := interceptor.DoAppend(context.Background(), msg, appendOK)
	require.NoError(t, err)
}

func newTestAppendInterceptor(channel types.PChannelInfo) *appendInterceptor {
	state := newPartialUpdateState(30*time.Second, versionIndexBudgetForEntries(100))
	state.channel = channel
	return &appendInterceptor{
		state: state,
		pkDescriptorGetter: &staticPrimaryKeyDescriptorGetter{
			descriptor: shards.PrimaryKeyDescriptor{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		},
	}
}

func appendOK(context.Context, message.MutableMessage) (message.MessageID, error) {
	return walimplstest.NewTestMessageID(1), nil
}

func newCASInsertMessage(
	t *testing.T,
	fields []*schemapb.FieldData,
	meta *messagespb.PartialUpdateCAS,
) message.MutableMessage {
	t.Helper()
	schemaVersion := int32(1)
	builder := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId:  10,
			SchemaVersion: &schemaVersion,
		}).
		WithBody(&msgpb.InsertRequest{FieldsData: fields})
	require.NoError(t, builder.AddPartialUpdateCAS(meta))
	return builder.MustBuildMutable()
}

type partialUpdateSchemaManagerTarget struct {
	shards.ShardManager
	primaryKeyDescriptorGetter
}

func withRawPartialUpdateCAS(msg message.MutableMessage, raw string) message.MutableMessage {
	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	body := insertMsg.MustBody()
	if body.Base == nil {
		body.Base = &commonpb.MsgBase{}
	}
	if body.Base.Properties == nil {
		body.Base.Properties = make(map[string]string)
	}
	body.Base.Properties["_puc"] = raw
	insertMsg.OverwriteBody(body)

	props := make(map[string]string, len(msg.Properties().ToRawMap())+1)
	for key, value := range msg.Properties().ToRawMap() {
		props[key] = value
	}
	props["_puc"] = ""
	return message.NewMutableMessageBeforeAppend(msg.Payload(), props)
}
