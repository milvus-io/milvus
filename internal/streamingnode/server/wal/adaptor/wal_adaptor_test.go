package adaptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
)

func TestWalAdaptorReadFail(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	expectedErr := errors.New("test")
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, ro walimpls.ReadOption) (walimpls.ScannerImpls, error) {
			return nil, expectedErr
		})

	lAdapted := adaptImplsToWAL(l, nil, func() {})
	scanner, err := lAdapted.Read(context.Background(), wal.ReadOption{})
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	assert.ErrorIs(t, scanner.Error(), expectedErr)
}

func TestWALAdaptor(t *testing.T) {
	// Create a mock WAL implementation
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
			return nil, nil
		})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ro walimpls.ReadOption) (walimpls.ScannerImpls, error) {
		scanner := mock_walimpls.NewMockScannerImpls(t)
		ch := make(chan message.ImmutableMessage, 1)
		scanner.EXPECT().Chan().Return(ch)
		scanner.EXPECT().Close().RunAndReturn(func() error {
			close(ch)
			return nil
		})
		return scanner, nil
	})
	l.EXPECT().Close().Return()

	lAdapted := adaptImplsToWAL(l, nil, func() {})
	assert.NotNil(t, lAdapted.Channel())
	_, err := lAdapted.Append(context.Background(), nil)
	assert.NoError(t, err)
	lAdapted.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assert.Nil(t, err)
	})

	// Test in concurrency env.
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			scanner, err := lAdapted.Read(context.Background(), wal.ReadOption{})
			if err != nil {
				assertShutdownError(t, err)
				return
			}
			assert.NoError(t, err)
			<-scanner.Chan()
		}(i)
	}
	time.Sleep(time.Second * 1)
	lAdapted.Close()

	// All wal should be closed with Opener.
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Errorf("wal close should be fast")
	case <-ch:
	}

	_, err = lAdapted.Append(context.Background(), nil)
	assertShutdownError(t, err)
	lAdapted.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assertShutdownError(t, err)
	})
	_, err = lAdapted.Read(context.Background(), wal.ReadOption{})
	assertShutdownError(t, err)
}

func assertShutdownError(t *testing.T, err error) {
	e := status.AsStreamingError(err)
	assert.Equal(t, e.Code, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN)
}

func TestNoInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	lWithInterceptors := adaptImplsToWAL(l, nil, func() {})

	_, err := lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)
	lWithInterceptors.Close()
}

func TestWALWithInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	b := mock_interceptors.NewMockInterceptorBuilder(t)
	readyCh := make(chan struct{})
	b.EXPECT().Build(mock.Anything).RunAndReturn(func(ibp interceptors.InterceptorBuildParam) interceptors.BasicInterceptor {
		interceptor := mock_interceptors.NewMockInterceptorWithReady(t)
		interceptor.EXPECT().Ready().Return(readyCh)
		interceptor.EXPECT().DoAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (message.MessageID, error)) (message.MessageID, error) {
				return f(ctx, mm)
			})
		interceptor.EXPECT().Close().Run(func() {})
		return interceptor
	})
	lWithInterceptors := adaptImplsToWAL(l, []interceptors.InterceptorBuilder{b}, func() {})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// Interceptor is not ready, so the append/read will be blocked until timeout.
	_, err := lWithInterceptors.Append(ctx, nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Interceptor is ready, so the append/read will return soon.
	close(readyCh)
	_, err = lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)

	lWithInterceptors.Close()
}
