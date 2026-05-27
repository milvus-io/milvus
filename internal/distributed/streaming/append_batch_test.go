package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestNewAppendBatchConfigFromParams(t *testing.T) {
	cfg := newAppendBatchConfigFromParams()
	assert.Equal(t, 64*1024, cfg.SmallMessageThreshold)
	assert.Equal(t, 1024*1024, cfg.MaxBatchSize)
	assert.Equal(t, 64, cfg.MaxMessageCount)
	assert.Equal(t, 5*time.Millisecond, cfg.MaxDelay)
	assert.Equal(t, 3, cfg.CooldownThreshold)
	assert.Equal(t, 10*time.Second, cfg.CooldownDuration)
	assert.True(t, cfg.enabled())
}

func TestAppendBatcherSubmitClosedAndCanceled(t *testing.T) {
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig(), appendBatchTestAppendFn(nil))
	batcher.close()

	resp := <-batcher.submit(context.Background(), newInsertMessage(vChannel1))
	assert.ErrorIs(t, resp.Error, ErrWALAccesserClosed)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	batcher = newAppendBatcher(vChannel1, appendBatchTestConfig(), appendBatchTestAppendFn(nil))
	resp = <-batcher.submit(ctx, newInsertMessage(vChannel1))
	assert.ErrorIs(t, resp.Error, context.Canceled)
}

func TestAppendBatcherClosePending(t *testing.T) {
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig(), appendBatchTestAppendFn(nil))

	respCh := batcher.submit(context.Background(), newInsertMessage(vChannel1))
	batcher.close()

	resp := <-respCh
	assert.ErrorIs(t, resp.Error, ErrWALAccesserClosed)
}

func TestAppendBatcherSkipsCanceledPendingRequest(t *testing.T) {
	appended := make(chan int, 1)
	batcher := newAppendBatcher(vChannel1, appendBatchTestConfig(), appendBatchTestAppendFn(appended))

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
		appendBatchTestConfig(),
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
