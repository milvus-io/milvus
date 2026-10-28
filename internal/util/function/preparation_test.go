package function

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
)

func TestDeferredMaterializationPreparesWithoutChangingBody(t *testing.T) {
	mockey.PatchConvey("defer initialization and lease only ready runners", t, func() {
		manager := newFunctionRunnerManager()
		defer manager.Close()
		mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
		prepare := mockey.Mock(fileresource.Acquire).Return(&fileresource.Resources{}, nil).Build()
		build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
			return newTestFunctionRunner(schema, fn)
		}).Build()
		schema := newSchemaWithAddedFunction(newBM25SignatureTestSchema())
		schema.FileResourceIds = []int64{7}
		require.NoError(t, manager.Alloc(1, "v1", schema))
		msg := newTestInsertMessage(newBM25InsertRequest("message"))
		ctx := WithDeferredPreparation(t.Context())
		for i := 0; i < len(schema.GetFunctions()); i++ {
			changed, err := manager.Materialize(ctx, 1, "v1", schema.GetVersion(), msg)
			var preparation *PreparationRequiredError
			require.ErrorAs(t, err, &preparation)
			require.False(t, changed)
			require.Zero(t, msg.overwriteCalls.Load())
			require.Equal(t, i, prepare.Times(), "no preparation inside materialization")
			require.Equal(t, i, build.Times())
			// All leases acquired before finding the unready runner must have
			// been released before the caller starts preparing another runner.
			entry := manager.getEntry(1)
			for _, runner := range entry.runners {
				locked := runner.mu.TryLock()
				if locked {
					runner.mu.Unlock()
				}
				require.True(t, locked)
			}
			require.NoError(t, preparation.Prepare(t.Context()))
		}
		changed, err := manager.Materialize(ctx, 1, "v1", schema.GetVersion(), msg)
		require.NoError(t, err)
		require.True(t, changed)
		require.EqualValues(t, 1, msg.overwriteCalls.Load())
		changed, err = manager.Materialize(ctx, 1, "v1", schema.GetVersion(), msg)
		require.NoError(t, err)
		require.False(t, changed, "already materialized messages do not prepare again")
		require.Equal(t, len(schema.GetFunctions()), prepare.Times())
	})
}

func TestDeferredMaterializationResolvesSchemaAfterPreparation(t *testing.T) {
	mockey.PatchConvey("a removed runner cannot bind a retry to the old schema", t, func() {
		manager := newFunctionRunnerManager()
		defer manager.Close()
		mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
		prepare := mockey.Mock(fileresource.Acquire).Return(&fileresource.Resources{}, nil).Build()
		mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
			return newTestFunctionRunner(schema, fn)
		}).Build()
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		require.NoError(t, manager.Alloc(1, "v1", schema))
		ctx := WithDeferredPreparation(t.Context())
		msg := newTestInsertMessage(newBM25InsertRequest("message"))
		_, err := manager.Materialize(ctx, 1, "v1", LatestFunctionRunnerVersion, msg)
		var oldPreparation *PreparationRequiredError
		require.ErrorAs(t, err, &oldPreparation)

		newSchema := newSchemaWithChangedOutput(schema)
		require.NoError(t, manager.Update(1, "v1", newSchema))
		// Finish asynchronous GC deterministically before using the old signal.
		oldPreparation.entry.Close()
		require.NoError(t, oldPreparation.Prepare(t.Context()))
		require.Zero(t, prepare.Times())
		_, err = manager.Materialize(ctx, 1, "v1", LatestFunctionRunnerVersion, msg)
		var newPreparation *PreparationRequiredError
		require.ErrorAs(t, err, &newPreparation)
		require.NotSame(t, oldPreparation.entry, newPreparation.entry)
		require.NoError(t, newPreparation.Prepare(t.Context()))
		changed, err := manager.Materialize(ctx, 1, "v1", LatestFunctionRunnerVersion, msg)
		require.NoError(t, err)
		require.True(t, changed)
		require.True(t, HasAllFieldDataByID(msg.MustBody().GetFieldsData(), []int64{104}))
		_, err = manager.Materialize(ctx, 1, "v1", schema.GetVersion(), newTestInsertMessage(newBM25InsertRequest("old version")))
		require.ErrorContains(t, err, "schema version mismatch", "explicit schema versions remain strict after preparation")
	})
}

func TestDeferredPreparationPreservesLastWaiterCancellation(t *testing.T) {
	mockey.PatchConvey("forwarded preparation errors have independent caller waits", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		_, _, err := entry.getReadyRunner()
		var preparation *PreparationRequiredError
		require.ErrorAs(t, err, &preparation)
		entered := make(chan context.Context, 1)
		stopped := make(chan struct{})
		mockey.Mock(fileresource.Acquire).To(func(ctx context.Context, _ []int64, _ ...string) (*fileresource.Resources, error) {
			defer close(stopped)
			entered <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
		firstCtx, cancelFirst := context.WithCancel(t.Context())
		defer cancelFirst()
		secondCtx, cancelSecond := context.WithCancel(t.Context())
		defer cancelSecond()
		secondWaitCtx := &runnerWaitContext{Context: secondCtx, waiting: make(chan struct{})}
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { firstDone <- preparation.Prepare(firstCtx) }()
		workerCtx := receiveRunnerResult(t, entered)
		go func() { secondDone <- preparation.Prepare(secondWaitCtx) }()
		receiveRunnerResult(t, secondWaitCtx.waiting)
		cancelFirst()
		require.ErrorIs(t, receiveRunnerResult(t, firstDone), context.Canceled)
		require.NoError(t, workerCtx.Err())
		cancelSecond()
		require.ErrorIs(t, receiveRunnerResult(t, secondDone), context.Canceled)
		receiveRunnerResult(t, stopped)
	})
}
