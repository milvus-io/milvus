package function

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRunnerPreparationSurvivesInitCreatorCancellation(t *testing.T) {
	mockey.PatchConvey("only the creator's wait is canceled", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		entered := make(chan context.Context, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releasePreparation := func() { releaseOnce.Do(func() { close(release) }) }
		defer releasePreparation()
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
		releasePreparation()
		waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		require.NoError(t, entry.waitInit(waitCtx, init))
		require.NoError(t, prepareErr)
		require.True(t, entry.isReady())
		require.Equal(t, 1, prepare.Times())
		require.Equal(t, 1, build.Times())
	})
}

func TestRunnerPreparationIsNotRequiredForDefaultAnalyzer(t *testing.T) {
	for _, scenario := range []string{"default BM25", "default MinHash", "resource IDs", "explicit config without IDs", "multi analyzer without IDs", "MinHash without enable_analyzer"} {
		mockey.PatchConvey(scenario, t, func() {
			schema := newBM25SignatureTestSchema()
			switch scenario {
			case "default MinHash":
				schema.Functions[0].Type = schemapb.FunctionType_MinHash
			case "resource IDs":
				schema.FileResourceIds = []int64{7}
			case "explicit config without IDs", "MinHash without enable_analyzer":
				schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: analyzerParams, Value: `{"tokenizer":{"type":"jieba","dict":["missing_dictionary"]}}`}}
				if scenario == "MinHash without enable_analyzer" {
					schema.Functions[0].Type = schemapb.FunctionType_MinHash
				}
			case "multi analyzer without IDs":
				schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: multiAnalyzerParams, Value: `{"analyzers":{}}`}}
			}
			prepare := mockey.Mock(fileresource.Prepare).Return(merr.ErrIoFailed).Build()
			build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
				return newTestFunctionRunner(schema, fn)
			}).Build()
			entry := newFunctionRunnerEntry(schema, schema.Functions[0])
			defer entry.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := entry.ensureRunner(ctx)
			if scenario == "default BM25" || scenario == "default MinHash" {
				require.NoError(t, err, "unrelated broken resources must not block a default analyzer")
				require.Zero(t, prepare.Times())
				require.Equal(t, 1, build.Times())
			} else {
				require.ErrorIs(t, err, merr.ErrIoFailed)
				require.Equal(t, 1, prepare.Times())
				require.Zero(t, build.Times(), "a required preparation failure must not be ignored")
			}
		})
	}
}

func TestDefaultFieldAnalyzerSurvivesUnrelatedResourceFailure(t *testing.T) {
	mockey.PatchConvey("ordinary default field needs no file inventory", t, func() {
		prepare := mockey.Mock(fileresource.Prepare).Return(merr.ErrIoFailed).Build()
		manager := newFunctionRunnerManager()
		defer manager.Close()
		schema := newBM25SignatureTestSchema()
		schema.Functions = nil
		schema.Fields[1].TypeParams = append(schema.Fields[1].TypeParams,
			&commonpb.KeyValuePair{Key: "enable_analyzer", Value: "true"})
		require.NoError(t, manager.Alloc(1, "v1", schema))
		ok, err := manager.RunWithAnalyzer(context.Background(), 1, "v1", 101, func(analyzer Analyzer) error {
			tokens, err := analyzer.BatchAnalyze(false, false, []string{"hello world"})
			require.Len(t, tokens, 1)
			return err
		})
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, prepare.Times())
	})
}
