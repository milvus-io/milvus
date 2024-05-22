package extends

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNoInterceptor(t *testing.T) {
	l := mock_wal.NewMockBasicWAL(t)
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (mqwrapper.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	lWithInterceptors := newWALWithInterceptors(l)

	_, err := lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)
	lWithInterceptors.Close()
}

func TestWALWithInterceptor(t *testing.T) {
	l := mock_wal.NewMockBasicWAL(t)
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (mqwrapper.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	b := mock_wal.NewMockInterceptorBuilder(t)

	readyCh := make(chan struct{})
	b.EXPECT().Build(mock.Anything).RunAndReturn(func(bw wal.BasicWAL) wal.AppendInterceptor {
		interceptor := mock_wal.NewMockAppendInterceptorWithReady(t)
		interceptor.EXPECT().Ready().Return(readyCh)
		interceptor.EXPECT().Do(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (mqwrapper.MessageID, error)) (mqwrapper.MessageID, error) {
				return f(ctx, mm)
			})
		interceptor.EXPECT().Close().Run(func() {})
		return interceptor
	})
	lWithInterceptors := newWALWithInterceptors(l, b)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// Interceptor is not ready, so the append will be blocked until timeout.
	_, err := lWithInterceptors.Append(ctx, nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Interceptor is ready, so the append will return soon.
	close(readyCh)
	_, err = lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)

	lWithInterceptors.Close()
}
