package idempotency

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	idempotencyutils "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newInterceptor(config WindowConfig) *idempotencyInterceptor {
	return newInterceptorWithEnabled(true, config)
}

func newInterceptorWithEnabled(enabled bool, config WindowConfig) *idempotencyInterceptor {
	config.Enabled = enabled
	return newIdempotencyInterceptor(config)
}

func TestInterceptorDuplicateDoesNotAppend(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	msg := newIdempotentInsertMessage(t, "v1", "key-1")

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	appendCount := 0
	firstID := newTestMessageID(10)
	firstLastConfirmed := newTestMessageID(9)
	msgID, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, firstLastConfirmed)
		return firstID, nil
	})
	require.NoError(t, err)
	require.True(t, firstID.EQ(msgID))
	require.Equal(t, 1, appendCount)

	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	duplicateID, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(11), nil
	})
	require.NoError(t, err)
	require.True(t, firstID.EQ(duplicateID))
	require.Equal(t, 1, appendCount)

	extra := utility.GetExtraAppendResult(ctx)
	require.Equal(t, uint64(100), extra.TimeTick)
	require.True(t, firstLastConfirmed.EQ(extra.LastConfirmedMessageID))
}

// withTestReplicateHeader marks msg as replicated from another cluster, the way
// the replicate stream server does before appending to the local WAL.
func withTestReplicateHeader(msg message.MutableMessage) message.MutableMessage {
	return msg.WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "source-cluster",
		MessageID:              newTestMessageID(1),
		LastConfirmedMessageID: newTestMessageID(1),
		TimeTick:               1,
		VChannel:               "v1",
	})
}

func TestInterceptorBypassesReplicatedInsert(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})

	// Seed the window: a native owner append records key-1.
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	appendCount := 0
	_, err := interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "key-1"), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, appendCount)

	// A replicated insert carrying the SAME key must bypass the window and reach
	// the WAL: the key belongs to the source cluster's history, and deduplicating
	// it locally would silently drop the replicated write.
	replicatedID := newTestMessageID(11)
	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msgID, err := interceptor.DoAppend(ctx, withTestReplicateHeader(newIdempotentInsertMessage(t, "v1", "key-1")), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return replicatedID, nil
	})
	require.NoError(t, err)
	require.True(t, replicatedID.EQ(msgID))
	require.Equal(t, 2, appendCount)

	// A replicated insert with a fresh key must leave no trace in the window: a
	// later native insert reusing that key is still the owner and appends.
	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, withTestReplicateHeader(newIdempotentInsertMessage(t, "v1", "key-2")), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(12), nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, appendCount)

	nativeID := newTestMessageID(13)
	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msgID, err = interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "key-2"), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 200)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(12))
		return nativeID, nil
	})
	require.NoError(t, err)
	require.True(t, nativeID.EQ(msgID))
	require.Equal(t, 4, appendCount)
}

func TestInterceptorBypassesReplicatedTxnCommit(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}

	newCommit := func() message.MutableMessage {
		return message.NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().
			WithTxnContext(txnCtx)
	}

	appendCount := 0
	append := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(int64(10 + appendCount)), nil
	}

	// Native commit seeds the window with txn-key ...
	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newCommit(), append)
	require.NoError(t, err)
	require.Equal(t, 1, appendCount)

	// ... and a duplicate native commit short-circuits (sanity check).
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newCommit(), append)
	require.NoError(t, err)
	require.Equal(t, 1, appendCount)

	// A replicated commit with the same key must bypass the window and append.
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), withTestReplicateHeader(newCommit()), append)
	require.NoError(t, err)
	require.Equal(t, 2, appendCount)
}

func TestInterceptorDuplicateTxnCommitRollsBackRetriedTxn(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	ownerTxn := message.TxnContext{TxnID: 1, Keepalive: 10}
	retryTxn := message.TxnContext{TxnID: 2, Keepalive: 10}

	newCommit := func(txnCtx message.TxnContext) message.MutableMessage {
		return message.NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().
			WithTxnContext(txnCtx)
	}
	newBody := func(txnCtx message.TxnContext, id int64) message.MutableMessage {
		return newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}},
			},
		}).WithTxnContext(txnCtx)
	}

	var appended []message.MutableMessage
	appendWithTT := func(tt uint64) func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = append(appended, msg)
			utility.ReplaceAppendResultTimeTick(ctx, tt)
			utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
			return newTestMessageID(int64(10 + len(appended))), nil
		}
	}

	// Owner txn commits the key.
	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newBody(ownerTxn, 100), appendWithTT(100))
	require.NoError(t, err)
	ownerID, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newCommit(ownerTxn), appendWithTT(100))
	require.NoError(t, err)
	require.Len(t, appended, 2)

	// Client retry: a fresh txnID re-appends the body, then its commit hits the
	// duplicate path. The interceptor must roll back the retried txn through the
	// inner chain, reclaim its body buffer, and return the owner's result. Model
	// the txn manager: the owner's txn was closed by its commit, the retried txn
	// session is still open.
	interceptor.txnActive = func(txnID message.TxnID) bool { return txnID == retryTxn.TxnID }
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newBody(retryTxn, 100), appendWithTT(999))
	require.NoError(t, err)
	require.Len(t, appended, 3)

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	retryCommit := newCommit(retryTxn)
	msgID, err := interceptor.DoAppend(ctx, retryCommit, appendWithTT(999))
	require.NoError(t, err)
	require.True(t, ownerID.EQ(msgID))

	require.Len(t, appended, 4)
	rollback := appended[3]
	require.Equal(t, message.MessageTypeRollbackTxn, rollback.MessageType())
	require.Equal(t, message.TxnID(2), rollback.TxnContext().TxnID)
	require.Equal(t, "v1", rollback.VChannel())

	// The duplicate response carries the owner's commit facts, not the rollback's.
	extra := utility.GetExtraAppendResult(ctx)
	require.Equal(t, uint64(100), extra.TimeTick)
	require.Nil(t, extra.TxnCtx)

	// The retried txn's insert-result buffer is reclaimed immediately.
	require.Nil(t, interceptor.txnInsertResultBuffers.Build(retryCommit))

	// When the txn session is already resolved (e.g. a concurrent duplicate that
	// shares the owner's txnID), no rollback is synthesized.
	interceptor.txnActive = func(message.TxnID) bool { return false }
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), newCommit(message.TxnContext{TxnID: 3, Keepalive: 10}), appendWithTT(999))
	require.NoError(t, err)
	require.Len(t, appended, 4)
}

func TestInterceptorDuplicateReturnsInsertIDs(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	insertResult := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{1, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 100}}},
		},
	}
	msg := newIdempotentInsertMessageWithInsertResult(t, "v1", "key-1", insertResult)

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	firstID := newTestMessageID(10)
	_, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return firstID, nil
	})
	require.NoError(t, err)

	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return newTestMessageID(11), nil
	})
	require.NoError(t, err)

	// The duplicate path decodes the stored entry, so the extra is a distinct
	// instance with identical content, not the original header pointer.
	extra := utility.GetExtraAppendResult(ctx).Extra.(*messagespb.IdempotentInsertResult)
	require.True(t, proto.Equal(insertResult, extra))
	require.Equal(t, []uint32{1, 0}, extra.GetRowOffsets())
	require.Equal(t, []int64{101, 100}, extra.GetIds().GetIntId().GetData())
}

func TestInterceptorWaitsForInflightOwnerResult(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	ownerMsg := newIdempotentInsertMessage(t, "v1", "key-wait")
	waiterMsg := newIdempotentInsertMessage(t, "v1", "key-wait")

	ownerStarted := make(chan struct{})
	releaseOwner := make(chan struct{})
	ownerDone := make(chan appendResult, 1)
	waiterDone := make(chan appendResult, 1)
	appendCount := atomic.Int32{}

	go func() {
		ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		msgID, err := interceptor.DoAppend(ctx, ownerMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendCount.Add(1)
			close(ownerStarted)
			<-releaseOwner
			utility.ReplaceAppendResultTimeTick(ctx, 100)
			utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
			return newTestMessageID(10), nil
		})
		ownerDone <- appendResult{id: msgID, err: err}
	}()

	select {
	case <-ownerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("owner append did not start")
	}

	go func() {
		ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		msgID, err := interceptor.DoAppend(ctx, waiterMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendCount.Add(1)
			return newTestMessageID(11), nil
		})
		waiterDone <- appendResult{id: msgID, err: err}
	}()

	select {
	case result := <-waiterDone:
		t.Fatalf("waiter returned before owner completed: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseOwner)
	ownerResult := <-ownerDone
	waiterResult := <-waiterDone
	require.NoError(t, ownerResult.err)
	require.NoError(t, waiterResult.err)
	require.True(t, ownerResult.id.EQ(waiterResult.id))
	require.Equal(t, int32(1), appendCount.Load())
}

func TestInterceptorOwnerAppendFailureAllowsRetry(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	appendErr := errors.New("append failed")
	appendCount := 0

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "key-retry"), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return nil, appendErr
	})
	require.ErrorIs(t, err, appendErr)
	require.Equal(t, 0, interceptor.window("v1").Len())
	require.Equal(t, 0, interceptor.window("v1").InflightLen())

	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msgID, err := interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "key-retry"), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.True(t, newTestMessageID(10).EQ(msgID))
	require.Equal(t, 2, appendCount)
}

func TestInterceptorIgnoresTxnBody(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	msg := newIdempotentInsertMessage(t, "v1", "key-1").
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: 10})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	appendCount := 0
	_, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, appendCount)
	require.Equal(t, 0, interceptor.window("v1").Len())
}

func TestInterceptorPassesThroughTxnBodyWithoutIdempotencyKey(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	msg := newIdempotentInsertMessage(t, "v1", "").
		WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: 10})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	appendCount := 0
	msgID, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.True(t, newTestMessageID(10).EQ(msgID))
	require.Equal(t, 1, appendCount)
	require.Nil(t, interceptor.txnInsertResultBuffers.Build(msg))
}

func TestInterceptorTxnCommitAssemblesInsertResultsFromBody(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	body1 := newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).WithTxnContext(txnCtx)
	body2 := newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{102, 101}}},
		},
	}).WithTxnContext(txnCtx)
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)

	appendCount := 0
	append := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(int64(10 + appendCount)), nil
	}

	_, err := interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), body1, append)
	require.NoError(t, err)
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), body2, append)
	require.NoError(t, err)
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, commit, append)
	require.NoError(t, err)
	require.Equal(t, 3, appendCount)

	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, commit, append)
	require.NoError(t, err)
	require.Equal(t, 3, appendCount)
	extra := utility.GetExtraAppendResult(ctx).Extra.(*messagespb.IdempotentInsertResult)
	require.Equal(t, []uint32{0, 2, 1}, extra.GetRowOffsets())
	require.Equal(t, []int64{100, 102, 101}, extra.GetIds().GetIntId().GetData())
}

func TestInterceptorTxnCommitAppendFailureClearsBuffer(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	body := newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).WithTxnContext(txnCtx)
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, body, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.NotNil(t, interceptor.txnInsertResultBuffers.Build(body))

	appendErr := errors.New("commit append failed")
	ctx = utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err = interceptor.DoAppend(ctx, commit, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return nil, appendErr
	})
	require.ErrorIs(t, err, appendErr)

	// The owner failed to append the commit; the buffered body results must be
	// dropped so the txn buffer does not leak, and the window slot must be
	// released so a retry can re-own the key.
	require.Nil(t, interceptor.txnInsertResultBuffers.Build(body))
	require.Equal(t, 0, interceptor.window("v1").InflightLen())
	require.Equal(t, 0, interceptor.window("v1").Len())
}

func TestInterceptorConcurrentDuplicateTxnCommitKeepsInsertIDs(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	body := newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}}},
	}).WithTxnContext(txnCtx)
	newCommit := func() message.MutableMessage {
		return message.NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().
			WithTxnContext(txnCtx)
	}

	// Buffer the txn body insert result that the commit must assemble.
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, body, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)

	ownerCommit := newCommit()
	waiterCommit := newCommit()
	ownerStarted := make(chan struct{})
	releaseOwner := make(chan struct{})
	ownerDone := make(chan appendResult, 1)
	waiterDone := make(chan appendResult, 1)
	waiterCtx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})

	go func() {
		c := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		msgID, err := interceptor.DoAppend(c, ownerCommit, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			close(ownerStarted)
			<-releaseOwner
			utility.ReplaceAppendResultTimeTick(ctx, 100)
			utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
			return newTestMessageID(11), nil
		})
		ownerDone <- appendResult{id: msgID, err: err}
	}()

	select {
	case <-ownerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("owner commit append did not start")
	}

	go func() {
		msgID, err := interceptor.DoAppend(waiterCtx, waiterCommit, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return newTestMessageID(99), nil
		})
		waiterDone <- appendResult{id: msgID, err: err}
	}()

	// The duplicate waiter must block on the owner, not return early.
	select {
	case result := <-waiterDone:
		t.Fatalf("waiter returned before owner completed: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	// While the owner is still in-flight (its Build already captured the buffered
	// result, its defer Remove has not yet run), a concurrent duplicate waiter must
	// NOT clear the shared txn buffer the owner depends on. If it does, a different
	// interleaving (waiter Remove before owner Build) would make the owner store a
	// WindowEntry with no insert IDs.
	require.NotNil(t, interceptor.txnInsertResultBuffers.Build(ownerCommit),
		"duplicate waiter must not remove the in-flight owner's txn insert-result buffer")

	close(releaseOwner)
	ownerResult := <-ownerDone
	waiterResult := <-waiterDone
	require.NoError(t, ownerResult.err)
	require.NoError(t, waiterResult.err)
	require.True(t, ownerResult.id.EQ(waiterResult.id))

	// Both the owner and the duplicate must report the assembled insert IDs.
	extra := utility.GetExtraAppendResult(waiterCtx).Extra.(*messagespb.IdempotentInsertResult)
	require.Equal(t, []uint32{0}, extra.GetRowOffsets())
	require.Equal(t, []int64{100}, extra.GetIds().GetIntId().GetData())
}

func TestFillDuplicateResultClearsStaleExtra(t *testing.T) {
	stale := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{9}}
	extra := &utility.ExtraAppendResult{Extra: stale}
	ctx := utility.WithExtraAppendResult(context.Background(), extra)

	// A keyed duplicate that committed without an idempotent insert payload.
	entry := &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 100,
		MessageId:      message.MustMarshalMessageID(newTestMessageID(10)),
	}
	msgID, err := fillDuplicateResult(ctx, entry)
	require.NoError(t, err)
	require.True(t, newTestMessageID(10).EQ(msgID))
	require.Equal(t, uint64(100), extra.TimeTick)
	require.Nil(t, extra.Extra, "stale extra must be cleared when the duplicate carries no insert result")
}

func TestTxnInsertResultBufferReclaimsUntrackedTxn(t *testing.T) {
	active := map[message.TxnID]bool{1: true}
	// No MVCC timetick provider: infinite-keepalive txns can never be reclaimed
	// by timetick expiry, so liveness is the only thing that can reclaim them.
	buffers := idempotencyutils.NewTxnInsertResultBuffers(nil, func(txnID message.TxnID) bool {
		return active[txnID]
	})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	body := newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}}},
	}).WithTxnContext(txnCtx)

	buffers.Add(body, &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}}},
	}, 0)
	require.NotNil(t, buffers.Build(body))

	// The txn session vanished (e.g. RollbackAllInFlightTransactions on failover)
	// without a commit/rollback message reaching the interceptor.
	active[1] = false
	require.Nil(t, buffers.Build(body))
}

func TestInterceptorClearsExpiredTxnInsertResults(t *testing.T) {
	baseTimeTick := tsoutil.ComposeTS(1000, 0)
	keepalive := 10 * time.Millisecond
	mvccManager := mvcc.NewMVCCManager(baseTimeTick)
	interceptor := newIdempotencyInterceptorWithParam(WindowConfig{Enabled: true}, &interceptors.InterceptorBuildParam{
		MVCCManager: mvccManager,
	})
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: keepalive}
	body := newIdempotentInsertMessageWithInsertResult(t, "v1", "txn-key", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).WithTxnContext(txnCtx)

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, body, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, baseTimeTick)
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.NotNil(t, interceptor.txnInsertResultBuffers.Build(body))

	expiredTimeTick := tsoutil.AddPhysicalDurationOnTs(baseTimeTick, keepalive)
	mvccManager.UpdateMVCC(newIdempotentInsertMessage(t, "v1", "").WithTimeTick(expiredTimeTick))
	require.Nil(t, interceptor.txnInsertResultBuffers.Build(body))
}

func TestInterceptorConfigDisabledPassThrough(t *testing.T) {
	interceptor := newInterceptorWithEnabled(false, WindowConfig{})
	msg := newIdempotentInsertMessage(t, "v1", "key-1")

	appendCount := 0
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(int64(10 + appendCount)), nil
	})
	require.NoError(t, err)

	_, err = interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		return newTestMessageID(int64(10 + appendCount)), nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, appendCount)
	require.Equal(t, 0, interceptor.window("v1").Len())
}

func TestInterceptorRejectsLongIdempotencyProperties(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{MaxKeyLength: 4})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, newIdempotentInsertMessage(t, "v1", "too-long"), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return newTestMessageID(10), nil
	})
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT, status.AsStreamingError(err).Code)
	require.Contains(t, err.Error(), "idempotency key length")
}

func TestInterceptorMalformedIdempotencyErrors(t *testing.T) {
	interceptor := newInterceptor(WindowConfig{})
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})

	_, err := interceptor.DoAppend(ctx, newIdempotentInsertMessageWithInsertResult(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
		},
	}), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return newTestMessageID(10), nil
	})
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT, status.AsStreamingError(err).Code)
	require.Contains(t, err.Error(), "idempotency insert result header requires idempotency key")

	msg := newIdempotentInsertMessageWithInsertResult(t, "v1", "key", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0, 1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
		},
	})
	_, err = interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return newTestMessageID(10), nil
	})
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT, status.AsStreamingError(err).Code)
	require.Contains(t, err.Error(), "malformed idempotency insert result header")
}

func TestInterceptorIdempotencyMetrics(t *testing.T) {
	paramtable.Init()
	interceptor := newInterceptor(WindowConfig{MinEntries: 0, MaxEntries: 1})
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})

	msg := newIdempotentInsertMessage(t, "metrics-v1", "key-1")
	nodeID := paramtable.GetStringNodeID()
	vchannel := msg.VChannel()
	entryGauge := metrics.WALIdempotencyWindowEntries.WithLabelValues(nodeID, vchannel)
	inflightGauge := metrics.WALIdempotencyWindowInflight.WithLabelValues(nodeID, vchannel)
	duplicateCounter := metrics.WALIdempotencyDuplicateTotal.WithLabelValues(nodeID, vchannel)
	evictionCounter := metrics.WALIdempotencyEvictionTotal.WithLabelValues(nodeID, vchannel)
	duplicateBefore := testutil.ToFloat64(duplicateCounter)
	evictionBefore := testutil.ToFloat64(evictionCounter)

	_, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(entryGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(inflightGauge))

	_, err = interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return newTestMessageID(11), nil
	})
	require.NoError(t, err)
	require.Equal(t, duplicateBefore+1, testutil.ToFloat64(duplicateCounter))

	next := newIdempotentInsertMessage(t, "metrics-v1", "key-2")
	_, err = interceptor.DoAppend(ctx, next, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 110)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(10))
		return newTestMessageID(11), nil
	})
	require.NoError(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(entryGauge))
	require.Equal(t, evictionBefore+1, testutil.ToFloat64(evictionCounter))
}

func TestInterceptorCloseDeletesWindowMetrics(t *testing.T) {
	paramtable.Init()
	interceptor := newInterceptor(WindowConfig{})
	msg := newIdempotentInsertMessage(t, "metrics-teardown-v1", "key-teardown")
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := interceptor.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		utility.ReplaceAppendResultTimeTick(ctx, 100)
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(9))
		return newTestMessageID(10), nil
	})
	require.NoError(t, err)

	before := testutil.CollectAndCount(metrics.WALIdempotencyWindowEntries)
	require.Positive(t, before)
	interceptor.Close()
	after := testutil.CollectAndCount(metrics.WALIdempotencyWindowEntries)
	require.Equal(t, before-1, after, "interceptor Close must delete the vchannel's window metric series")
}

func TestInterceptorOrderShortCircuitsDownstreamOnDuplicate(t *testing.T) {
	idempotencyInterceptor := newInterceptor(WindowConfig{})
	redo := &recordingAppendInterceptor{}
	timetick := &recordingAppendInterceptor{}
	shard := &recordingAppendInterceptor{}
	chain := interceptors.NewChainedInterceptor(idempotencyInterceptor, redo, timetick, shard)
	defer chain.Close()
	<-chain.Ready()

	appendCount := 0
	finalAppend := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendCount++
		msgID := newTestMessageID(int64(10 + appendCount))
		utility.ReplaceAppendResultTimeTick(ctx, uint64(100+appendCount))
		utility.ReplaceAppendResultLastConfirmedMessageID(ctx, newTestMessageID(int64(9+appendCount)))
		return msgID, nil
	}

	msg := newIdempotentInsertMessage(t, "v1", "key-1")
	ctx := newAppendTestContext(msg)
	firstID, err := chain.DoAppend(ctx, msg, finalAppend)
	require.NoError(t, err)
	require.True(t, newTestMessageID(11).EQ(firstID))
	require.Equal(t, 1, appendCount)
	require.Equal(t, 1, redo.calls)
	require.Equal(t, 1, timetick.calls)
	require.Equal(t, 1, shard.calls)

	ctx = newAppendTestContext(msg)
	duplicateID, err := chain.DoAppend(ctx, msg, finalAppend)
	require.NoError(t, err)
	require.True(t, firstID.EQ(duplicateID))
	require.Equal(t, 1, appendCount)
	require.Equal(t, 1, redo.calls)
	require.Equal(t, 1, timetick.calls)
	require.Equal(t, 1, shard.calls)

	passThrough := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DeleteRequest{CollectionID: 1}).
		MustBuildMutable()
	ctx = newAppendTestContext(passThrough)
	_, err = chain.DoAppend(ctx, passThrough, finalAppend)
	require.NoError(t, err)
	require.Equal(t, 2, appendCount)
	require.Equal(t, 2, redo.calls)
	require.Equal(t, 2, timetick.calls)
	require.Equal(t, 2, shard.calls)
}

func newAppendTestContext(msg message.MutableMessage) context.Context {
	mw := metricsutil.NewWriteMetrics(types.PChannelInfo{Name: msg.PChannel()}, message.WALNameRocksmq)
	ctx := utility.WithAppendMetricsContext(context.Background(), mw.StartAppend(msg))
	return utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
}

func newIdempotentInsertMessage(t *testing.T, vchannel string, key string) message.MutableMessage {
	t.Helper()
	return newIdempotentInsertMessageWithInsertResult(t, vchannel, key, nil)
}

func newIdempotentInsertMessageWithInsertResult(t *testing.T, vchannel string, key string, extra *messagespb.IdempotentInsertResult) message.MutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId:   1,
		IdempotencyKey: proto.String(key),
	}
	message.SetInsertHeaderIdempotentInsertResult(header, extra)
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		MustBuildMutable()
}

var registerTestMessageIDOnce sync.Once

func newTestMessageID(id int64) message.MessageID {
	registerTestMessageIDOnce.Do(func() {
		message.RegisterMessageIDUnmsarshaler(message.WALNameTest, func(data string) (message.MessageID, error) {
			id, err := strconv.ParseInt(data, 10, 64)
			if err != nil {
				return nil, err
			}
			return testMessageID(id), nil
		})
	})
	return testMessageID(id)
}

type recordingAppendInterceptor struct {
	calls int
}

func (i *recordingAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	i.calls++
	return append(ctx, msg)
}

func (i *recordingAppendInterceptor) Close() {}

type appendResult struct {
	id  message.MessageID
	err error
}

type testMessageID int64

func (id testMessageID) WALName() message.WALName {
	return message.WALNameTest
}

func (id testMessageID) LT(other message.MessageID) bool {
	return id < other.(testMessageID)
}

func (id testMessageID) LTE(other message.MessageID) bool {
	return id <= other.(testMessageID)
}

func (id testMessageID) EQ(other message.MessageID) bool {
	return id == other.(testMessageID)
}

func (id testMessageID) Marshal() string {
	return strconv.FormatInt(int64(id), 10)
}

func (id testMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      id.Marshal(),
		WALName: commonpb.WALName(id.WALName()),
	}
}

func (id testMessageID) String() string {
	return id.Marshal()
}
