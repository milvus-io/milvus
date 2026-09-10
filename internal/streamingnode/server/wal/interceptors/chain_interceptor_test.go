package interceptors_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

type chainInterceptorRecord struct {
	before bool
	after  bool
	closed bool
}

type chainInterceptorStub struct {
	record *chainInterceptorRecord
	delay  time.Duration
}

func (s *chainInterceptorStub) DoAppend(
	ctx context.Context,
	msg message.MutableMessage,
	appendOp interceptors.Append,
) (message.MessageID, error) {
	s.record.before = true
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	msgID, err := appendOp(ctx, msg)
	s.record.after = true
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return msgID, err
}

func (s *chainInterceptorStub) Close() {
	s.record.closed = true
}

type namedChainInterceptorStub struct {
	*chainInterceptorStub
	name string
}

func (s *namedChainInterceptorStub) Name() string {
	return s.name
}

func testChainInterceptor(t *testing.T, count int, named bool) {
	appendInterceptorRecords := make([]chainInterceptorRecord, count)
	ips := make([]interceptors.Interceptor, 0, count)
	for i := 0; i < count; i++ {
		interceptor := &chainInterceptorStub{record: &appendInterceptorRecords[i]}
		if named {
			interceptor.delay = time.Microsecond
			ips = append(ips, &namedChainInterceptorStub{
				chainInterceptorStub: interceptor,
				name:                 fmt.Sprintf("interceptor-%d", i),
			})
			continue
		}
		ips = append(ips, interceptor)
	}
	interceptor := interceptors.NewChainedInterceptor(ips...)

	// fast return
	<-interceptor.Ready()

	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable()
	mw := metricsutil.NewWriteMetrics(types.PChannelInfo{}, message.WALNameRocksmq)
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
