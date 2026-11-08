package manager

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	streamingresolver "github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManagerClosesPendingConnection(t *testing.T) {
	paramtable.Init()
	r := &fakeResolver{state: newQueryNodeVersionedState(1, map[int64]queryNodeSessionInfo{10: {}})}
	builder := mockey.Mock(streamingresolver.NewSessionBuilder).Return(&fakeResolverBuilder{resolver: r}).Build()
	defer builder.UnPatch()
	started := make(chan struct{}, 1)
	dial := mockey.Mock(grpc.DialContext).To(func(ctx context.Context, _ string, _ ...grpc.DialOption) (*grpc.ClientConn, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer dial.UnPatch()
	client := NewManagerClient(nil)
	closed := false
	defer func() {
		if !closed {
			client.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("lazy connection did not start")
	}
	nodes, err := client.GetAllQueryNodes(t.Context())
	require.NoError(t, err)
	require.Contains(t, nodes, int64(10))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = client.CreateViewSyncClient(ctx, 10)
	require.ErrorIs(t, err, context.Canceled)
	client.Close()
	closed = true
	_, err = client.GetAllQueryNodes(t.Context())
	require.True(t, status.AsStreamingError(err).IsOnShutdown())
	require.False(t, isQueryableQueryNodeSession(nil))
}
