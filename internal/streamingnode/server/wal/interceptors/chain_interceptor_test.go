package interceptors_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}
}

func TestChainInterceptor(t *testing.T) {
	for i := 0; i < 5; i++ {
		testChainInterceptor(t, i, false)
		testChainInterceptor(t, 5, true)
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

func testChainInterceptor(t *testing.T, count int, named bool) {
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

		if !named {
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
		} else {
			interceptor := mock_interceptors.NewMockInterceptorWithMetrics(t)
			interceptor.EXPECT().DoAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (message.MessageID, error)) (message.MessageID, error) {
					appendInterceptorRecords[j].before = true
					// time.Sleep(time.Duration(j) * 10 * time.Millisecond)
					msgID, err := f(ctx, mm)
					appendInterceptorRecords[j].after = true
					// time.Sleep(time.Duration(j) * 20 * time.Millisecond)
					return msgID, err
				})
			interceptor.EXPECT().Name().Return(fmt.Sprintf("interceptor-%d", j))
			interceptor.EXPECT().Close().Run(func() {
				appendInterceptorRecords[j].closed = true
			})
			ips = append(ips, interceptor)
		}
	}
	interceptor := interceptors.NewChainedInterceptor(ips...)

	// fast return
	<-interceptor.Ready()

	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeDelete).Maybe()
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	mw := metricsutil.NewWriteMetrics(types.PChannelInfo{}, "rocksmq")
	m := mw.StartAppend(msg)
	ctx := utility.WithAppendMetricsContext(context.Background(), m)
	msgID, err := interceptor.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	assert.NoError(t, err)
	assert.Nil(t, msgID)
	interceptor.Close()
	if named {
		cnt := 0
		m.RangeOverInterceptors(func(name string, ims []*metricsutil.InterceptorMetrics) {
			assert.NotEmpty(t, name)
			for _, im := range ims {
				assert.NotZero(t, im.Before)
				assert.NotZero(t, im.After)
				cnt++
			}
		})
		assert.Equal(t, count, cnt)
	}
	for i := 0; i < count; i++ {
		assert.True(t, appendInterceptorRecords[i].before, i)
		assert.True(t, appendInterceptorRecords[i].after, i)
		assert.True(t, appendInterceptorRecords[i].closed, i)
	}
}
