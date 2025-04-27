package adaptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/mock_wab"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestWalAdaptorReadFail(t *testing.T) {
	resource.InitForTest(t)

	l := mock_walimpls.NewMockWALImpls(t)
	expectedErr := errors.New("test")
	l.EXPECT().WALName().Return("test")
	cnt := atomic.NewInt64(2)
	l.EXPECT().Append(mock.Anything, mock.Anything).Return(walimplstest.NewTestMessageID(1), nil)
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, ro walimpls.ReadOption) (walimpls.ScannerImpls, error) {
			if cnt.Dec() < 0 {
				s := mock_walimpls.NewMockScannerImpls(t)
				s.EXPECT().Chan().Return(make(chan message.ImmutableMessage, 1))
				s.EXPECT().Close().Return(nil)
				return s, nil
			}
			return nil, expectedErr
		}).Maybe()
	l.EXPECT().Close().Return()

	// Test adapt to a read-only wal.
	l.EXPECT().Channel().Return(types.PChannelInfo{AccessMode: types.AccessModeRO})
	lAdapted, err := adaptImplsToWAL(context.Background(), l, nil, func() {})
	assert.NoError(t, err)
	scanner, err := lAdapted.Read(context.Background(), wal.ReadOption{
		VChannel: "test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	time.Sleep(time.Second)
	assert.True(t, lAdapted.IsAvailable())
	lAdapted.Close()
	assert.False(t, lAdapted.IsAvailable())
	scanner.Close()

	// A rw wal should use the write ahead buffer to sync time tick.
	writeAheadBuffer := mock_wab.NewMockROWriteAheadBuffer(t)
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	operator.EXPECT().Channel().Return(types.PChannelInfo{}).Maybe()
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Return().Maybe()
	operator.EXPECT().WriteAheadBuffer().Return(writeAheadBuffer).Maybe()
	resource.Resource().TimeTickInspector().RegisterSyncOperator(
		operator,
	)

	// Test adapt to a read-write wal.
	l.EXPECT().Channel().Unset()
	l.EXPECT().Channel().Return(types.PChannelInfo{AccessMode: types.AccessModeRW})
	lAdapted, err = adaptImplsToWAL(context.Background(), l, nil, func() {})
	assert.NoError(t, err)
	scanner, err = lAdapted.Read(context.Background(), wal.ReadOption{
		VChannel: "test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	time.Sleep(time.Second)
	scanner.Close()
	assert.True(t, lAdapted.IsAvailable())
	lAdapted.Close()
	assert.False(t, lAdapted.IsAvailable())
}

func TestWALAdaptor(t *testing.T) {
	snMeta := mock_metastore.NewMockStreamingNodeCataLog(t)
	snMeta.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	snMeta.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snMeta))

	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	operator.EXPECT().Channel().Return(types.PChannelInfo{})
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Return()
	buffer := mock_wab.NewMockROWriteAheadBuffer(t)
	operator.EXPECT().WriteAheadBuffer().Return(buffer)
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)

	// Create a mock WAL implementation
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().WALName().Return("test")
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(1), nil
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

	l.EXPECT().Channel().Return(types.PChannelInfo{AccessMode: types.AccessModeRO})

	// Test read only wal
	lAdapted, err := adaptImplsToWAL(context.Background(), l, nil, func() {})
	assert.NoError(t, err)
	assert.Panics(t, func() {
		lAdapted.Append(context.Background(), nil)
	})
	assert.Panics(t, func() {
		lAdapted.AppendAsync(context.Background(), nil, nil)
	})
	assert.Panics(t, func() {
		lAdapted.GetLatestMVCCTimestamp(context.Background(), "test")
	})
	lAdapted.Close()

	// Test read-write wal
	l.EXPECT().Channel().Unset()
	l.EXPECT().Channel().Return(types.PChannelInfo{AccessMode: types.AccessModeRW})
	lAdapted, err = adaptImplsToWAL(context.Background(), l, nil, func() {})
	assert.NoError(t, err)
	assert.NotNil(t, lAdapted.Channel())

	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().WithWALTerm(mock.Anything).Return(msg).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().IsPersisted().Return(true).Maybe()
	msg.EXPECT().MarshalLogObject(mock.Anything).Return(nil).Maybe()
	_, err = lAdapted.Append(context.Background(), msg)
	assert.NoError(t, err)
	lAdapted.AppendAsync(context.Background(), msg, func(mi *wal.AppendResult, err error) {
		assert.Nil(t, err)
	})

	// Test in concurrency env.
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			scanner, err := lAdapted.Read(context.Background(), wal.ReadOption{VChannel: "test"})
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

	_, err = lAdapted.Append(context.Background(), msg)
	assertShutdownError(t, err)
	lAdapted.AppendAsync(context.Background(), msg, func(mi *wal.AppendResult, err error) {
		assertShutdownError(t, err)
	})
	_, err = lAdapted.Read(context.Background(), wal.ReadOption{VChannel: "test"})
	assertShutdownError(t, err)
}

func assertShutdownError(t *testing.T, err error) {
	e := status.AsStreamingError(err)
	assert.Equal(t, e.Code, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN)
}

func TestNoInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().WALName().Return("test")
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(1), nil
	})
	l.EXPECT().Close().Run(func() {})

	lWithInterceptors, err := adaptImplsToWAL(context.Background(), l, nil, func() {})
	assert.NoError(t, err)

	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().IsPersisted().Return(true).Maybe()
	msg.EXPECT().WithWALTerm(mock.Anything).Return(msg).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().MarshalLogObject(mock.Anything).Return(nil).Maybe()
	_, err = lWithInterceptors.Append(context.Background(), msg)
	assert.NoError(t, err)
	lWithInterceptors.Close()
}

func TestWALWithInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(1), nil
	})
	l.EXPECT().WALName().Return("test")
	l.EXPECT().Close().Run(func() {})

	b := mock_interceptors.NewMockInterceptorBuilder(t)
	readyCh := make(chan struct{})
	b.EXPECT().Build(mock.Anything).RunAndReturn(func(ibp *interceptors.InterceptorBuildParam) interceptors.Interceptor {
		interceptor := mock_interceptors.NewMockInterceptorWithReady(t)
		interceptor.EXPECT().Ready().Return(readyCh)
		interceptor.EXPECT().DoAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (message.MessageID, error)) (message.MessageID, error) {
				return f(ctx, mm)
			})
		interceptor.EXPECT().Close().Run(func() {})
		return interceptor
	})
	lWithInterceptors, err := adaptImplsToWAL(context.Background(), l, []interceptors.InterceptorBuilder{b}, func() {})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// Interceptor is not ready, so the append/read will be blocked until timeout.
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().WithWALTerm(mock.Anything).Return(msg).Maybe()
	msg.EXPECT().MessageType().Return(message.MessageTypeInsert).Maybe()
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().IsPersisted().Return(true).Maybe()
	msg.EXPECT().MarshalLogObject(mock.Anything).Return(nil).Maybe()
	_, err = lWithInterceptors.Append(ctx, msg)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Interceptor is ready, so the append/read will return soon.
	close(readyCh)
	_, err = lWithInterceptors.Append(context.Background(), msg)
	assert.NoError(t, err)

	lWithInterceptors.Close()
}
