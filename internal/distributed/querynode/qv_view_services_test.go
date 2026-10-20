package grpcquerynode

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type distributedQueryNodeTaskFunc func(context.Context) error

func (f distributedQueryNodeTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

func TestLazyQNSegmentManagerNilCallbacksUseNodeScheduler(t *testing.T) {
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)

	started := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := scheduler.Submit(distributedQueryNodeTaskFunc(func(context.Context) error {
		close(started)
		<-releaseBlocker
		return nil
	}))
	<-started

	mgr := &lazyQNSegmentManager{scheduler: scheduler}
	unrecoverable := make(chan struct{}, 1)
	dropped := make(chan struct{}, 1)
	mgr.Acquire(qnview.AcquireSegments{OnUnrecoverable: func() { unrecoverable <- struct{}{} }})
	mgr.Release(qnview.ReleaseSegments{OnDropped: func() { dropped <- struct{}{} }})

	select {
	case <-unrecoverable:
		t.Fatal("acquire callback bypassed node scheduler")
	case <-dropped:
		t.Fatal("release callback bypassed node scheduler")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseBlocker)
	require.NoError(t, blocker.Wait(context.Background()))
	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire callback")
	}
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for release callback")
	}
}

func TestRegisterQueryViewSyncServer(t *testing.T) {
	server := grpc.NewServer()
	registerQueryViewSyncServer(server, noopQNSegmentManager{})

	_, ok := server.GetServiceInfo()["milvus.proto.view.ViewSyncService"]
	assert.True(t, ok)
}

func TestRegisterQueryViewServers(t *testing.T) {
	server := grpc.NewServer()
	registerQueryViewServers(server, noopQNSegmentManager{}, 100)

	services := server.GetServiceInfo()
	_, ok := services["milvus.proto.view.ViewSyncService"]
	assert.True(t, ok)
	_, ok = services["milvus.proto.view.ViewQueryService"]
	assert.True(t, ok)
}

type noopQNSegmentManager struct{}

func (noopQNSegmentManager) Acquire(qnview.AcquireSegments) {}

func (noopQNSegmentManager) Release(qnview.ReleaseSegments) {}

func (noopQNSegmentManager) AcquireSealedSegmentHandles(context.Context, qviews.QueryViewKey, *viewpb.QueryViewOfQueryNode, uint64) ([]qnview.SealedSegmentHandle, error) {
	return nil, nil
}

func (noopQNSegmentManager) WaitTransformVisible(context.Context, qviews.QueryViewKey, uint64) error {
	return nil
}

type queryViewWALWithTransport struct{ streaming.WALAccesser }

func (*queryViewWALWithTransport) TransformLogStreamManager() wal.TransformLogStreamManager {
	panic("patch with mockey")
}

func TestQueryViewTransformTransportUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name string
		wal  streaming.WALAccesser
	}{
		{name: "WAL not initialized"},
		{name: "remote transport unavailable", wal: &queryViewWALWithTransport{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(streaming.WAL).Return(tc.wal).Build()
			defer patch.UnPatch()
			patchProvider := mockey.Mock((*queryViewWALWithTransport).TransformLogStreamManager).
				Return(wal.NewTransformLogErrorAccesser(merr.ErrServiceNotReady)).Build()
			defer patchProvider.UnPatch()
			manager := queryViewTransformLogStreamManager()
			require.NotNil(t, manager)
			stream, err := manager.AcquireStream(t.Context(), "pchannel")
			require.Nil(t, stream)
			require.ErrorIs(t, err, merr.ErrServiceNotReady)
		})
	}
}

func TestQueryViewTransformTransportInstalled(t *testing.T) {
	patchWAL := mockey.Mock(streaming.WAL).Return(&queryViewWALWithTransport{}).Build()
	defer patchWAL.UnPatch()
	expected := wal.NewTransformLogErrorAccesser(context.Canceled)
	patchProvider := mockey.Mock((*queryViewWALWithTransport).TransformLogStreamManager).Return(expected).Build()
	defer patchProvider.UnPatch()
	require.Equal(t, expected, queryViewTransformLogStreamManager())
}
