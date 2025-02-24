package interceptors_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestChainInterceptor(t *testing.T) {
	for i := 0; i < 5; i++ {
		testChainInterceptor(t, i)
	}
}

func TestChainReady(t *testing.T) {
	count := 5
	channels := make([]chan struct{}, 0, count)
	ips := make([]interceptors.Interceptor, 0, count)
	for i := 0; i < count; i++ {
		ch := make(chan struct{})
		channels = append(channels, ch)
		interceptor := mock_interceptors.NewMockInterceptorWithReady(t)
		interceptor.EXPECT().Ready().Return(ch)
		interceptor.EXPECT().Close().Return()
		ips = append(ips, interceptor)
	}
	chainInterceptor := interceptors.NewChainedInterceptor(ips...)

	for i := 0; i < count; i++ {
		// part of interceptors is not ready
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		select {
		case <-chainInterceptor.Ready():
			t.Fatal("should not ready")
		case <-ctx.Done():
		}
		close(channels[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	select {
	case <-chainInterceptor.Ready():
	case <-ctx.Done():
		t.Fatal("interceptor should be ready now")
	}
	chainInterceptor.Close()

	interceptor := mock_interceptors.NewMockInterceptorWithReady(t)
	ch := make(chan struct{})
	interceptor.EXPECT().Ready().Return(ch)
	interceptor.EXPECT().Close().Return()
	chainInterceptor = interceptors.NewChainedInterceptor(interceptor)
	chainInterceptor.Close()

	// closed chain interceptor should block the ready (internal interceptor is not ready)
	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	select {
	case <-chainInterceptor.Ready():
		t.Fatal("chan interceptor that closed but internal interceptor is not ready should block the ready")
	case <-ctx.Done():
	}
}

func testChainInterceptor(t *testing.T, count int) {
	type record struct {
		before bool
		after  bool
		closed bool
	}

	appendInterceptorRecords := make([]record, 0, count)
	ips := make([]interceptors.Interceptor, 0, count)
	for i := 0; i < count; i++ {
		j := i
		appendInterceptorRecords = append(appendInterceptorRecords, record{})

		interceptor := mock_interceptors.NewMockInterceptor(t)
		interceptor.EXPECT().DoAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (message.MessageID, error)) (message.MessageID, error) {
				appendInterceptorRecords[j].before = true
				msgID, err := f(ctx, mm)
				appendInterceptorRecords[j].after = true
				return msgID, err
			})
		interceptor.EXPECT().Close().Run(func() {
			appendInterceptorRecords[j].closed = true
		})
		ips = append(ips, interceptor)
	}
	interceptor := interceptors.NewChainedInterceptor(ips...)

	// fast return
	<-interceptor.Ready()

	msg, err := interceptor.DoAppend(context.Background(), nil, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	assert.NoError(t, err)
	assert.Nil(t, msg)
	interceptor.Close()
	for i := 0; i < count; i++ {
		assert.True(t, appendInterceptorRecords[i].before)
		assert.True(t, appendInterceptorRecords[i].after)
		assert.True(t, appendInterceptorRecords[i].closed)
	}
}
