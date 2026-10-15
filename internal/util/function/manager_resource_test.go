package function

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
)

func TestRunnerPreparationSurvivesInitCreatorCancellation(t *testing.T) {
	mockey.PatchConvey("only the creator's wait is cancelled", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		entered := make(chan context.Context, 1)
		release := make(chan struct{})
		prepare := mockey.Mock(fileresource.Prepare).To(func(ctx context.Context) error {
			entered <- ctx
			<-release
			return ctx.Err()
		}).Build()
		build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
			return newTestFunctionRunner(schema, fn)
		}).Build()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		firstDone := make(chan error, 1)
		go func() { firstDone <- entry.ensureRunner(ctx) }()
		var prepareCtx context.Context
		select {
		case prepareCtx = <-entered:
		case <-ctx.Done():
			t.Fatal("initialization did not start")
		}
		entry.mu.RLock()
		init := entry.init
		entry.mu.RUnlock()
		require.NotNil(t, init)
		cancel()
		require.ErrorIs(t, <-firstDone, context.Canceled)
		_, hasDeadline := prepareCtx.Deadline()
		require.False(t, hasDeadline, "the shared initializer must not inherit a request deadline")
		prepareErr := prepareCtx.Err()
		close(release)
		waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		require.NoError(t, entry.waitInit(waitCtx, init))
		require.NoError(t, prepareErr)
		require.True(t, entry.isReady())
		require.Equal(t, 1, prepare.Times())
		require.Equal(t, 1, build.Times())
	})
}
