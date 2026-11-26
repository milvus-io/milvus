package adaptor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type preparationTestInterceptor struct{ interceptors.Interceptor }

func (*preparationTestInterceptor) DoAppend(context.Context, message.MutableMessage, interceptors.Append) (message.MessageID, error) {
	panic("unpatched")
}

type preparationTestRunner struct{ function.FunctionRunner }

func (*preparationTestRunner) Close() { panic("unpatched") }

type preparationTestShardManager struct{ shards.ShardManager }

func (*preparationTestShardManager) WaitUntilGrowingSegmentReady(shards.PartitionUniqueKey) (<-chan struct{}, error) {
	panic("unpatched")
}

func installPreparationTestInterceptor(t *testing.T, w *walAdaptorImpl) function.FunctionRunnerManager {
	t.Helper()
	mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
	manager := function.NewFunctionRunnerManager()
	schema := &schemapb.CollectionSchema{
		Version: 1, FileResourceIds: []int64{7},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{Id: 1, Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}},
	}
	require.NoError(t, manager.Alloc(1, "v1", schema))
	mockey.Mock((*preparationTestInterceptor).DoAppend).To(func(_ *preparationTestInterceptor, ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
		// This reduced chain omits the TimeTick interceptor. Populate the
		// message property as well as the mocked backend's AppendResult.
		msg.WithTimeTick(1)
		if msg.MessageType() == message.MessageTypeInsert {
			_, err := manager.Materialize(function.WithDeferredPreparation(ctx), 1, "v1", function.LatestFunctionRunnerVersion, message.MustAsMutableInsertMessageV1(msg))
			if err != nil {
				return nil, err
			}
		}
		return appendOp(ctx, msg)
	}).Build()
	// The existing chain contains a real lock interceptor. Invoke the
	// materializer inside that lock so a blocked preparation would stall DDL.
	ready := make(chan struct{})
	close(ready)
	mockey.Mock((*preparationTestShardManager).WaitUntilGrowingSegmentReady).
		Return((<-chan struct{})(ready), nil).Build()
	redoInterceptor := redo.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: &preparationTestShardManager{},
	})
	rm, err := replicates.RecoverReplicateManager(&replicates.ReplicateManagerRecoverParam{
		ChannelInfo: w.Channel(), CurrentClusterID: "local", InitialRecoverSnapshot: &recovery.RecoverySnapshot{},
	})
	require.NoError(t, err)
	param := &interceptors.InterceptorBuildParam{ReplicateManager: rm}
	idempotencyInterceptor := idempotency.NewInterceptorBuilder().Build(param)
	t.Cleanup(redoInterceptor.Close)
	t.Cleanup(idempotencyInterceptor.Close)
	w.interceptorBuildResult.Interceptor = interceptors.NewChainedInterceptor(
		idempotencyInterceptor, redoInterceptor, w.interceptorBuildResult.Interceptor, replicate.NewInterceptorBuilder().Build(param), &preparationTestInterceptor{})
	return manager
}

func newPreparationTestInsert(key message.IdempotencyKey) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().WithVChannel("v1").
		WithIdempotencyKey(key).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}},
		}).
		WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
}

type preparationWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *preparationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestRedoPreparationDifferentRequestsCancelIndependently(t *testing.T) {
	mockey.PatchConvey("distinct executing requests share preparation independently", t, func() {
		w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Error("canceled preparation must not persist")
			return nil, nil
		}))
		manager := installPreparationTestInterceptor(t, w)
		defer manager.Close()
		started, stopped := make(chan struct{}), make(chan struct{})
		var workerCtx context.Context
		prepare := mockey.Mock(fileresource.Acquire).To(func(ctx context.Context, _ []int64, _ ...string) (*fileresource.Resources, error) {
			workerCtx = ctx
			close(started)
			defer close(stopped)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
		var original func(*function.PreparationRequiredError, context.Context) error
		var waits atomic.Int32
		secondJoined := make(chan struct{})
		mockey.Mock((*function.PreparationRequiredError).Prepare).Origin(&original).
			To(func(p *function.PreparationRequiredError, ctx context.Context) error {
				if waits.Add(1) == 2 {
					// FlightGroup reads Done after registering this waiter.
					ctx = &preparationWaitContext{Context: ctx, waiting: secondJoined}
				}
				return original(p, ctx)
			}).Build()
		firstCtx, cancelFirst := context.WithCancel(t.Context())
		defer cancelFirst()
		secondCtx, cancelSecond := context.WithCancel(t.Context())
		defer cancelSecond()
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := w.Append(firstCtx, newPreparationTestInsert("first-request"))
			firstDone <- err
		}()
		waitFenceTestSignal(t, started)
		go func() {
			_, err := w.Append(secondCtx, newPreparationTestInsert("second-request"))
			secondDone <- err
		}()
		waitFenceTestSignal(t, secondJoined)
		cancelFirst()
		require.ErrorIs(t, waitFenceTestError(t, firstDone), context.Canceled)
		require.NoError(t, workerCtx.Err(), "the second request still owns the shared load")
		cancelSecond()
		require.ErrorIs(t, waitFenceTestError(t, secondDone), context.Canceled)
		waitFenceTestSignal(t, stopped)
		require.Equal(t, 1, prepare.Times())
	})
}

func TestAppendPreparationReleasesWALLockAndHonorsCancellation(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		mockey.PatchConvey("preparation is outside the WAL lock and follows caller/WAL cancellation", t, func() {
			var persisted atomic.Int32
			w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				return finishFenceTestAppend(ctx, int64(persisted.Add(1))), nil
			}))
			manager := installPreparationTestInterceptor(t, w)
			defer manager.Close()
			started, stopped := make(chan struct{}), make(chan struct{})
			mockey.Mock(fileresource.Acquire).To(func(ctx context.Context, _ []int64, _ ...string) (*fileresource.Resources, error) {
				close(started)
				defer close(stopped)
				<-ctx.Done()
				return nil, ctx.Err()
			}).Build()
			build := mockey.Mock(function.BuildEmbeddingRunner).Return(nil, merr.ErrServiceInternal).Build()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := w.Append(ctx, newPreparationTestInsert("preparation-test"))
				done <- err
			}()
			waitFenceTestSignal(t, started)
			ddlDone := make(chan error, 1)
			go func() {
				_, err := w.Append(t.Context(), message.NewManualFlushMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
					WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable())
				ddlDone <- err
			}()
			require.NoError(t, waitFenceTestError(t, ddlDone), "same-vchannel exclusive DDL must proceed while resource I/O waits")
			if shutdown {
				w.availableCancel()
			} else {
				cancel()
			}
			require.ErrorIs(t, waitFenceTestError(t, done), context.Canceled)
			waitFenceTestSignal(t, stopped)
			require.EqualValues(t, 1, persisted.Load(), "only the DDL was persisted")
			require.Zero(t, build.Times())
		})
	}
}

func TestAppendRechecksRoleAfterPreparation(t *testing.T) {
	mockey.PatchConvey("demotion can finish while owner and duplicate wait", t, func() {
		var persisted atomic.Int32
		w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			persisted.Add(1)
			return finishFenceTestAppend(ctx, 1), nil
		}))
		manager := installPreparationTestInterceptor(t, w)
		defer manager.Close()
		started, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		defer release()
		mockey.Mock(fileresource.Acquire).To(func(context.Context, []int64, ...string) (*fileresource.Resources, error) {
			close(started)
			<-resume
			return &fileresource.Resources{}, nil
		}).Build()
		mockey.Mock(function.BuildEmbeddingRunner).Return(&preparationTestRunner{}, nil).Build()
		mockey.Mock((*preparationTestRunner).Close).Return().Build()
		waiterJoined := make(chan struct{})
		var original func(*idempotency.PendingEntry, context.Context, message.MutableMessage) idempotency.PendingResult
		mockey.Mock((*idempotency.PendingEntry).Wait).Origin(&original).To(func(p *idempotency.PendingEntry, ctx context.Context, msg message.MutableMessage) idempotency.PendingResult {
			close(waiterJoined)
			return original(p, ctx, msg)
		}).Build()
		result := make(chan error, 2)
		insert := func() {
			_, err := w.Append(t.Context(), newPreparationTestInsert("same-key"))
			result <- err
		}
		go insert()
		waitFenceTestSignal(t, started)
		go insert()
		waitFenceTestSignal(t, waiterJoined)
		config := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "local", Pchannels: []string{w.Channel().Name}},
				{ClusterId: "peer", Pchannels: []string{"peer-channel"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "peer", TargetClusterId: "local"}},
		}
		ddl := make(chan error, 1)
		go func() {
			_, err := w.Append(t.Context(), message.NewAlterReplicateConfigMessageBuilderV2().WithAllVChannel().
				WithHeader(&message.AlterReplicateConfigMessageHeader{ReplicateConfiguration: config}).
				WithBody(&message.AlterReplicateConfigMessageBody{}).MustBuildMutable())
			ddl <- err
		}()
		require.NoError(t, waitFenceTestError(t, ddl))
		release()
		for range 2 {
			err := waitFenceTestError(t, result)
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsReplicateViolation())
		}
		require.EqualValues(t, 1, persisted.Load())
	})
}

func TestAppendResumesAfterPreparationWithoutCancelingPersistence(t *testing.T) {
	mockey.PatchConvey("successful preparation resumes append with the WAL-owned context", t, func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		persisted := 0
		w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(appendCtx context.Context, _ message.MutableMessage) (message.MessageID, error) {
			cancel()
			require.NoError(t, appendCtx.Err(), "accepted WAL mutation must survive caller cancellation")
			persisted++
			return finishFenceTestAppend(appendCtx, 1), nil
		}))
		manager := installPreparationTestInterceptor(t, w)
		defer manager.Close()
		prepare := mockey.Mock(fileresource.Acquire).Return(&fileresource.Resources{}, nil).Build()
		build := mockey.Mock(function.BuildEmbeddingRunner).Return(&preparationTestRunner{}, nil).Build()
		mockey.Mock((*preparationTestRunner).Close).Return().Build()
		mockey.Mock(function.FillFunctionFields).Return(false, nil).Build()
		_, err := w.Append(ctx, newPreparationTestInsert("preparation-test"))
		require.NoError(t, err)
		require.Equal(t, 1, persisted)
		require.Equal(t, 1, prepare.Times())
		require.Equal(t, 1, build.Times())
		// The outer idempotency interceptor answers a cached result without
		// entering redo or consulting live runner state.
		manager.Release(1, "v1")
		_, err = w.Append(t.Context(), newPreparationTestInsert("preparation-test"))
		require.NoError(t, err)
		require.Equal(t, 1, persisted)
		require.Equal(t, 1, prepare.Times())
	})
}

func TestAppendPreparationFailureKeepsTerminalPolicy(t *testing.T) {
	mockey.PatchConvey("permanent preparation failures never reach persistence", t, func() {
		w := newFenceTestWAL(t, newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Error("failed preparation must not persist")
			return nil, nil
		}))
		manager := installPreparationTestInterceptor(t, w)
		defer manager.Close()
		prepare := mockey.Mock(fileresource.Acquire).Return(nil, merr.ErrIoPermissionDenied).Build()
		_, err := w.Append(t.Context(), newPreparationTestInsert("preparation-test"))
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		require.Equal(t, 1, prepare.Times())
	})
}
