package function

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Signal after the caller has joined the initialization flight, without relying
// on goroutine scheduling or exposing FlightGroup's private waiter bookkeeping.
type runnerWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *runnerWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func receiveRunnerResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runner initialization")
		var zero T
		return zero
	}
}

func TestRunnerPreparationFollowsRequestCancellation(t *testing.T) {
	mockey.PatchConvey("only the final caller cancels shared runner preparation", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		entered := make(chan context.Context, 1)
		stopped := make(chan struct{})
		prepare := mockey.Mock(fileresource.Acquire).To(func(ctx context.Context, _ []int64, _ ...string) (*fileresource.Resources, error) {
			defer close(stopped)
			entered <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
		build := mockey.Mock(BuildEmbeddingRunner).Return(nil, nil).Build()
		firstCtx, firstCancel := context.WithCancel(t.Context())
		defer firstCancel()
		secondCtx, secondCancel := context.WithCancel(t.Context())
		defer secondCancel()
		secondWaitCtx := &runnerWaitContext{Context: secondCtx, waiting: make(chan struct{})}
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { firstDone <- entry.ensureRunner(firstCtx) }()
		workerCtx := receiveRunnerResult(t, entered)
		go func() { secondDone <- entry.ensureRunner(secondWaitCtx) }()
		receiveRunnerResult(t, secondWaitCtx.waiting)
		firstCancel()
		require.ErrorIs(t, receiveRunnerResult(t, firstDone), context.Canceled)
		require.NoError(t, workerCtx.Err())
		secondCancel()
		require.ErrorIs(t, receiveRunnerResult(t, secondDone), context.Canceled)
		receiveRunnerResult(t, stopped)
		require.ErrorIs(t, workerCtx.Err(), context.Canceled)
		require.False(t, entry.isReady())
		require.Equal(t, 1, prepare.Times())
		require.Zero(t, build.Times())
	})
}

func TestRunnerJoinsNativeBuildWithoutPreparingAgain(t *testing.T) {
	for _, cancelBeforeJoin := range []bool{false, true} {
		mockey.PatchConvey("native construction survives cancellation and is shared", t, func() {
			schema := newBM25SignatureTestSchema()
			schema.FileResourceIds = []int64{7}
			entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
			defer entry.Close()
			var prepareCalls atomic.Int32
			prepare := mockey.Mock(fileresource.Acquire).To(func(context.Context, []int64, ...string) (*fileresource.Resources, error) {
				if prepareCalls.Add(1) > 1 {
					return nil, merr.ErrIoPermissionDenied // A new refresh would fail.
				}
				return &fileresource.Resources{}, nil
			}).Build()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
				close(entered)
				<-release
				return newTestFunctionRunner(schema, fn)
			}).Build()
			waitEntered, waitExited := make(chan struct{}, 2), make(chan struct{}, 2)
			var originalWait func(*functionRunnerEntry, context.Context, *functionRunnerInit) error
			mockey.Mock((*functionRunnerEntry).waitInit).Origin(&originalWait).To(func(e *functionRunnerEntry, ctx context.Context, init *functionRunnerInit) error {
				waitEntered <- struct{}{}
				defer func() { waitExited <- struct{}{} }()
				return originalWait(e, ctx, init)
			}).Build()
			firstCtx, firstCancel := context.WithCancel(t.Context())
			defer firstCancel()
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			go func() { firstDone <- entry.ensureRunner(firstCtx) }()
			receiveRunnerResult(t, entered)
			receiveRunnerResult(t, waitEntered)
			if cancelBeforeJoin {
				firstCancel()
				require.ErrorIs(t, receiveRunnerResult(t, firstDone), context.Canceled)
			}
			secondCtx := &runnerWaitContext{Context: t.Context(), waiting: make(chan struct{})}
			go func() { secondDone <- entry.ensureRunner(secondCtx) }()
			receiveRunnerResult(t, secondCtx.waiting)
			if cancelBeforeJoin {
				// The replacement flight must actually rejoin the existing native
				// init while the build is blocked, not merely observe it ready later.
				receiveRunnerResult(t, waitEntered)
			}
			if !cancelBeforeJoin {
				firstCancel()
				require.ErrorIs(t, receiveRunnerResult(t, firstDone), context.Canceled)
			}
			unblock()
			require.NoError(t, receiveRunnerResult(t, secondDone))
			receiveRunnerResult(t, waitExited)
			if cancelBeforeJoin {
				receiveRunnerResult(t, waitExited)
			}
			require.NoError(t, entry.ensureRunner(t.Context()))
			require.Equal(t, 1, prepare.Times())
			require.Equal(t, 1, build.Times())
		})
	}
}

func TestRunnerSharesPreparationThroughNativeTransition(t *testing.T) {
	mockey.PatchConvey("callers arriving on either side of native admission share preparation", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		prepareEntered, releasePrepare := make(chan struct{}), make(chan struct{})
		buildEntered, releaseBuild := make(chan struct{}), make(chan struct{})
		var prepareOnce, buildOnce sync.Once
		unblockPrepare := func() { prepareOnce.Do(func() { close(releasePrepare) }) }
		unblockBuild := func() { buildOnce.Do(func() { close(releaseBuild) }) }
		defer unblockPrepare()
		defer unblockBuild()
		prepare := mockey.Mock(fileresource.Acquire).To(func(ctx context.Context, _ []int64, _ ...string) (*fileresource.Resources, error) {
			close(prepareEntered)
			select {
			case <-releasePrepare:
				return &fileresource.Resources{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Build()
		build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
			close(buildEntered)
			<-releaseBuild
			return newTestFunctionRunner(schema, fn)
		}).Build()
		results := make(chan error, 3)
		go func() { results <- entry.ensureRunner(t.Context()) }()
		receiveRunnerResult(t, prepareEntered)
		before := &runnerWaitContext{Context: t.Context(), waiting: make(chan struct{})}
		go func() { results <- entry.ensureRunner(before) }()
		receiveRunnerResult(t, before.waiting)
		unblockPrepare()
		receiveRunnerResult(t, buildEntered)
		after := &runnerWaitContext{Context: t.Context(), waiting: make(chan struct{})}
		go func() { results <- entry.ensureRunner(after) }()
		receiveRunnerResult(t, after.waiting)
		unblockBuild()
		for i := 0; i < 3; i++ {
			require.NoError(t, receiveRunnerResult(t, results))
		}
		require.Equal(t, 1, prepare.Times())
		require.Equal(t, 1, build.Times())
	})
}

func TestCanceledRunnerPreparationDoesNotAdmitNativeBuild(t *testing.T) {
	mockey.PatchConvey("cancellation after preparation cannot publish a native init", t, func() {
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
		defer entry.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mockey.Mock(fileresource.Acquire).To(func(context.Context, []int64, ...string) (*fileresource.Resources, error) {
			cancel()
			return &fileresource.Resources{}, nil
		}).Build()
		build := mockey.Mock(BuildEmbeddingRunner).Return(nil, nil).Build()
		require.ErrorIs(t, entry.initialize(ctx), context.Canceled)
		require.Nil(t, entry.init)
		require.Zero(t, build.Times())
	})
}

func TestPreparationErrorPreservesCause(t *testing.T) {
	for _, cause := range []error{merr.ErrServiceUnavailable, context.DeadlineExceeded, merr.WrapErrIoKeyNotFound("dictionary"), merr.ErrIoPermissionDenied} {
		mockey.PatchConvey(cause.Error(), t, func() {
			mockey.Mock(fileresource.Acquire).Return(nil, cause).Build()
			schema := newBM25SignatureTestSchema()
			schema.FileResourceIds = []int64{7}
			entry := newFunctionRunnerEntry(schema, schema.GetFunctions()[0])
			defer entry.Close()
			err := entry.ensureRunner(t.Context())
			require.ErrorIs(t, err, cause)
			require.Equal(t, merr.Code(cause), merr.Code(err))
		})
	}
}

func TestPullRunnerRegistrationDoesNotPrepareResources(t *testing.T) {
	mockey.PatchConvey("pull preparation is owned by requests rather than prewarming", t, func() {
		mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
		prepare := mockey.Mock(fileresource.Acquire).Return(nil, merr.ErrServiceUnavailable).Build()
		build := mockey.Mock(BuildEmbeddingRunner).Return(nil, nil).Build()
		manager := newFunctionRunnerManager()
		defer manager.Close()
		schema := newBM25SignatureTestSchema()
		schema.FileResourceIds = []int64{7}
		require.NoError(t, manager.Alloc(1, "v1", schema))
		schema.Version++
		require.NoError(t, manager.Update(1, "v1", schema))
		require.Zero(t, prepare.Times())
		require.Zero(t, build.Times())
		_, err := manager.RunWithRunner(t.Context(), 1, "v1", 102, func(FunctionRunner) error { return nil })
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Equal(t, 1, prepare.Times())
		require.Zero(t, build.Times())
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
			prepare := mockey.Mock(fileresource.Acquire).Return(nil, merr.ErrIoFailed).Build()
			build := mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
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
		prepare := mockey.Mock(fileresource.Acquire).Return(nil, merr.ErrIoFailed).Build()
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

func TestRunnerPreparationUsesActualAnalyzerConfigs(t *testing.T) {
	const remoteParams = `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`
	for _, tc := range []struct {
		name   string
		kind   schemapb.FunctionType
		key    string
		params string
	}{
		{"BM25", schemapb.FunctionType_BM25, analyzerParams, remoteParams},
		{"MinHash", schemapb.FunctionType_MinHash, analyzerParams, remoteParams},
		{"multi analyzer", schemapb.FunctionType_BM25, multiAnalyzerParams, `{"by_field":"language","analyzers":{"default":` + remoteParams + `}}`},
	} {
		mockey.PatchConvey(tc.name, t, func() {
			schema := newBM25SignatureTestSchema()
			// Older schemas and MinHash can have no FileResourceIds. The
			// runtime must still resolve the names in the actual config.
			schema.Functions[0].Type = tc.kind
			schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: tc.key, Value: tc.params}}
			prepare := mockey.Mock(fileresource.Acquire).To(func(_ context.Context, _ []int64, params ...string) (*fileresource.Resources, error) {
				require.Equal(t, []string{tc.params}, params)
				return &fileresource.Resources{}, nil
			}).Build()
			mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
				return newTestFunctionRunner(schema, fn)
			}).Build()
			entry := newFunctionRunnerEntry(schema, schema.Functions[0])
			defer entry.Close()
			require.NoError(t, entry.ensureRunner(t.Context()))
			require.Equal(t, 1, prepare.Times())
		})
	}
}

func TestResourceFreeRunnerDoesNotFetchUnrelatedCollectionResources(t *testing.T) {
	mockey.PatchConvey("collection IDs and explicit config do not force a resource fetch", t, func() {
		previous := fileresource.GlobalFileManager
		fileresource.GlobalFileManager = &fileresource.BaseManager{}
		defer func() {
			fileresource.SetResourceFetcher(nil, nil)
			fileresource.GlobalFileManager = previous
		}()
		fileresource.SetResourceFetcher(nil, func(context.Context, []int64, []string) ([]*internalpb.FileResourceInfo, error) {
			t.Error("this runner does not reference any remote resource")
			return nil, merr.ErrIoPermissionDenied
		})
		mockey.Mock(BuildEmbeddingRunner).To(func(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, _ ...string) (FunctionRunner, error) {
			return newTestFunctionRunner(schema, fn)
		}).Build()
		for _, params := range []string{`{}`, `{"tokenizer":"standard"}`, `{"filter":[{"stop_words_file":{"type":"local","path":"local.txt"}}]}`} {
			schema := newBM25SignatureTestSchema()
			schema.FileResourceIds = []int64{7} // Used by some other field.
			schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: analyzerParams, Value: params}}
			entry := newFunctionRunnerEntry(schema, schema.Functions[0])
			require.NoError(t, entry.ensureRunner(t.Context()))
			entry.Close()
		}
	})
}

func TestRunnerSignatureIncludesResourceBindings(t *testing.T) {
	schema := newBM25SignatureTestSchema()
	schema.FileResourceIds = []int64{7, 8}
	first, _, _, err := embeddingFunctionMetadata(schema, schema.Functions[0])
	require.NoError(t, err)
	schema.FileResourceIds = []int64{8, 7}
	reordered, _, _, err := embeddingFunctionMetadata(schema, schema.Functions[0])
	require.NoError(t, err)
	require.Equal(t, first, reordered)
	schema.FileResourceIds = []int64{8, 9}
	rebound, _, _, err := embeddingFunctionMetadata(schema, schema.Functions[0])
	require.NoError(t, err)
	require.NotEqual(t, first, rebound, "same analyzer options with different resource IDs require a new runner")
}
