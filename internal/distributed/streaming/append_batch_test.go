package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNewAppendBatchConfigFromParams(t *testing.T) {
	cfg := newAppendBatchConfigFromParams()
	assert.Equal(t, 0, cfg.SmallMessageThreshold)
	assert.Equal(t, 1024*1024, cfg.MaxBatchSize)
	assert.Equal(t, 64, cfg.MaxMessageCount)
	assert.Equal(t, 5*time.Millisecond, cfg.MaxDelay)
	assert.Equal(t, 3, cfg.CooldownThreshold)
	assert.Equal(t, 10*time.Second, cfg.CooldownDuration)
	assert.False(t, cfg.enabled())
}

func TestAppendBatchConfigProviderCachesLoadedConfig(t *testing.T) {
	loadCount := 0
	cfg := appendBatchTestConfig()
	provider := newAppendBatchConfigProvider(func() appendBatchConfig {
		loadCount++
		return cfg
	}, nil)

	assert.Equal(t, cfg, provider.get())
	assert.Equal(t, cfg, provider.get())
	assert.Equal(t, 1, loadCount)

	cfg.MaxMessageCount = 2
	assert.NoError(t, provider.update(context.Background(), "", "", ""))
	assert.Equal(t, cfg, provider.get())
	assert.Equal(t, cfg, provider.get())
	assert.Equal(t, 2, loadCount)
}

func TestAppendBatchConfigProviderLogsOnlyChangedConfig(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	log.ReplaceGlobals(logger, &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zapcore.InfoLevel),
	})
	t.Cleanup(func() {
		logger, props, err := log.InitLogger(&log.Config{Level: "debug", Stdout: true})
		assert.NoError(t, err)
		log.ReplaceGlobals(logger, props)
	})

	cfg := appendBatchTestConfig()
	provider := newAppendBatchConfigProvider(func() appendBatchConfig {
		return cfg
	}, nil)

	assert.NoError(t, provider.update(context.Background(), "", "", ""))
	assert.Len(t, logs.FilterMessage("wal append batch config updated").All(), 0)

	oldCfg := cfg
	cfg.MaxMessageCount = 2
	assert.NoError(t, provider.update(context.Background(), "", "", ""))

	entries := logs.FilterMessage("wal append batch config updated").All()
	assert.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	assert.Equal(t, oldCfg, fields["oldConfig"])
	assert.Equal(t, cfg, fields["newConfig"])
}

func TestAppendBatchConfigProviderCloseUnregistersCallback(t *testing.T) {
	unregisterCount := 0
	provider := newAppendBatchConfigProvider(appendBatchTestConfig, func(paramtable.ParamChangeCallback) func() {
		return func() {
			unregisterCount++
		}
	})

	provider.close()
	provider.close()
	assert.Equal(t, 1, unregisterCount)
}

func TestAppendBatcherSubmitClosedAndCanceled(t *testing.T) {
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig, appendBatchTestAppendFn(nil))
	batcher.close()

	resp := <-batcher.submit(context.Background(), newInsertMessage(vChannel1))
	assert.ErrorIs(t, resp.Error, ErrWALAccesserClosed)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	batcher = newAppendBatcher(vChannel1, appendBatchTestConfig, appendBatchTestAppendFn(nil))
	resp = <-batcher.submit(ctx, newInsertMessage(vChannel1))
	assert.ErrorIs(t, resp.Error, context.Canceled)
}

func TestAppendBatcherClosePending(t *testing.T) {
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig, appendBatchTestAppendFn(nil))

	respCh := batcher.submit(context.Background(), newInsertMessage(vChannel1))
	batcher.close()

	resp := <-respCh
	assert.ErrorIs(t, resp.Error, ErrWALAccesserClosed)
}

func TestAppendBatcherSkipsCanceledPendingRequest(t *testing.T) {
	appended := make(chan int, 1)
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig, appendBatchTestAppendFn(appended))

	canceledCtx, cancel := context.WithCancel(context.Background())
	respCh1 := batcher.submit(canceledCtx, newInsertMessage(vChannel1))
	respCh2 := batcher.submit(context.Background(), newInsertMessage(vChannel1))
	cancel()

	batcher.flushByTimer()

	resp1 := <-respCh1
	resp2 := <-respCh2
	assert.ErrorIs(t, resp1.Error, context.Canceled)
	assert.NoError(t, resp2.Error)
	assert.Equal(t, 1, <-appended)
}

func TestAppendBatcherFlushAllCanceledRequests(t *testing.T) {
	called := false
	batcher := newAppendBatcher(
		vChannel1,
		appendBatchTestConfig,
		func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponse {
			called = true
			return types.AppendResponse{}
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	respCh := batcher.submit(ctx, newInsertMessage(vChannel1))
	cancel()
	batcher.flushByTimer()

	resp := <-respCh
	assert.ErrorIs(t, resp.Error, context.Canceled)
	assert.False(t, called)
}

func TestAppendBatcherUsesDynamicConfig(t *testing.T) {
	appended := make(chan int, 1)
	cfg := appendBatchTestConfig()
	cfg.MaxMessageCount = 100
	batcher := newAppendBatcher(vChannel1, func() appendBatchConfig {
		return cfg
	}, appendBatchTestAppendFn(appended))

	respCh1 := batcher.submit(context.Background(), newInsertMessage(vChannel1))
	cfg.MaxMessageCount = 2
	respCh2 := batcher.submit(context.Background(), newInsertMessage(vChannel1))

	assert.Equal(t, 2, <-appended)
	assert.NoError(t, (<-respCh1).Error)
	assert.NoError(t, (<-respCh2).Error)
}

func TestAppendBatcherPropagatesFlushErrorToAllRequests(t *testing.T) {
	batchErr := errors.New("batch failed")
	cfg := appendBatchTestConfig()
	cfg.MaxMessageCount = 2
	batcher := newAppendBatcher(vChannel1, func() appendBatchConfig {
		return cfg
	}, func(context.Context, ...message.MutableMessage) types.AppendResponse {
		return types.AppendResponse{Error: batchErr}
	})

	respCh1 := batcher.submit(context.Background(), newInsertMessage(vChannel1))
	respCh2 := batcher.submit(context.Background(), newInsertMessage(vChannel1))

	assert.ErrorIs(t, (<-respCh1).Error, batchErr)
	assert.ErrorIs(t, (<-respCh2).Error, batchErr)
}

func TestAppendBatchContextCancelsOnlyWhenAllRequestsDone(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	ctx, cancel := batchContext([]*appendBatchRequest{
		{ctx: ctx1},
		{ctx: ctx2},
	})
	defer cancel()

	_, ok := ctx.Deadline()
	assert.False(t, ok)

	cancel1()
	assert.Never(t, func() bool {
		return ctx.Err() != nil
	}, 50*time.Millisecond, 10*time.Millisecond)

	cancel2()
	assert.Eventually(t, func() bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}, time.Second, 10*time.Millisecond)
}

func TestAppendBatchContextIgnoresSingleRequestDeadline(t *testing.T) {
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer deadlineCancel()
	otherCtx, otherCancel := context.WithCancel(context.Background())
	ctx, cancel := batchContext([]*appendBatchRequest{
		{ctx: deadlineCtx},
		{ctx: otherCtx},
	})
	defer cancel()

	assert.Never(t, func() bool {
		return ctx.Err() != nil
	}, 50*time.Millisecond, 10*time.Millisecond)

	otherCancel()
	assert.Eventually(t, func() bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}, time.Second, 10*time.Millisecond)
}

func TestAppendBatchContextManualCancel(t *testing.T) {
	ctx, cancel := batchContext([]*appendBatchRequest{{ctx: context.Background()}})
	cancel()
	assert.Eventually(t, func() bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}, time.Second, 10*time.Millisecond)
}

func appendBatchTestConfig() appendBatchConfig {
	return appendBatchConfig{
		SmallMessageThreshold: 1 << 20,
		MaxBatchSize:          1 << 20,
		MaxMessageCount:       64,
		MaxDelay:              time.Hour,
		CooldownThreshold:     3,
		CooldownDuration:      time.Second,
	}
}

func appendBatchTestAppendFn(appended chan<- int) func(context.Context, ...message.MutableMessage) types.AppendResponse {
	return func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponse {
		if appended != nil {
			appended <- len(msgs)
		}
		return types.AppendResponse{
			AppendResult: &types.AppendResult{},
		}
	}
}
