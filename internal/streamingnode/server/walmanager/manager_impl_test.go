package walmanager

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/partialupdate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenManager(t *testing.T) {
	resource.InitForTest(t)

	m, err := OpenManager()
	assert.NoError(t, err)
	assert.NotNil(t, m)
	m.Close()
}

func TestPartialUpdateInterceptorRunsAfterShard(t *testing.T) {
	builders := newInterceptorBuilders()
	assert.Len(t, builders, 6)
	assert.IsType(t, shard.NewInterceptorBuilder(), builders[4])
	assert.IsType(t, partialupdate.NewInterceptorBuilder(), builders[5])
}

func TestManager(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Metrics().Return(types.RWWALMetrics{}).Maybe()
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Close().Return()
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Metrics().Return(types.RWWALMetrics{})
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)
	channelName := "ch1"

	l, err := m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	h, err := m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 0)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assert.NoError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 1,
	})
	assertErrorOperationIgnored(t, err)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 2,
	})
	assert.NoError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorOperationIgnored(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorTermExpired(t, err)
	assert.Nil(t, l)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assert.NoError(t, err)
	assert.NotNil(t, l)

	h, err = m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 1)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 3,
	})
	assert.NoError(t, err)

	h, err = m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 2)

	m.Close()

	h, err = m.Metrics()
	assertShutdownError(t, err)
	assert.Nil(t, h)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 4,
	})
	assertShutdownError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)
	assert.Nil(t, l)
}

func assertShutdownError(t *testing.T, err error) {
	assert.Error(t, err)
	e := status.AsStreamingError(err)
	assert.Equal(t, e.Code, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN)
}

// TestManagerCloseRejectsRemoveDuringClose verifies that a RemoveWAL arriving
// while the manager is closing fails fast with a shutdown error, instead of
// being accepted and waiting on a stuck wal lifetime background task.
// Regression test for https://github.com/milvus-io/milvus/issues/53237.
func TestManagerCloseRejectsRemoveDuringClose(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	// The first open blocks until the gate is released, so Close() has to
	// wait for an in-flight Open operation.
	enteredOpen := make(chan struct{})
	openGate := make(chan struct{})
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			close(enteredOpen)
			<-openGate
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)

	// Start an Open and wait until it is in-flight (holding the manager
	// lifetime and blocking the wal lifetime background task).
	openDone := make(chan error, 1)
	go func() { openDone <- m.Open(context.Background(), types.PChannelInfo{Name: "ch1", Term: 1}) }()
	<-enteredOpen

	// Start closing; Close() now must wait for the in-flight Open.
	closeDone := make(chan struct{})
	go func() { m.Close(); close(closeDone) }()

	// While the manager is closing, RemoveWAL must be rejected immediately.
	// Calls that race ahead of the close use a canceled context and return
	// context.Canceled without blocking; once the closing state is set they
	// return the shutdown error.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("manager Close did not reject RemoveWAL in time")
		default:
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := m.Remove(ctx, types.PChannelInfo{Name: "ch2", Term: 1})
		if err != nil && status.AsStreamingError(err).Code == streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN {
			break
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error while manager is closing: %v", err)
		}
	}

	// Release the stuck open so Close() can finish.
	close(openGate)
	assert.NoError(t, <-openDone)
	<-closeDone
}
