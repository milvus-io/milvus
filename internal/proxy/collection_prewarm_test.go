package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPrewarmAdmissionDoesNotWaitOrFollowClientCancellation(t *testing.T) {
	paramtable.Init()
	lifetime, stop := context.WithCancel(t.Context())
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // The completed load request no longer owns background admission.
	mix := mocks.NewMockMixCoordClient(t)
	started := make(chan context.Context, 1)
	finished := make(chan struct{})
	mix.EXPECT().PrewarmCollection(mock.Anything, mock.MatchedBy(func(req *querypb.PrewarmCollectionRequest) bool {
		return req.GetCollectionID() == 100
	})).RunAndReturn(func(ctx context.Context, _ *querypb.PrewarmCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
		defer close(finished)
		started <- ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}).Once()
	node := &Proxy{ctx: lifetime, mixCoord: mix}
	node.prewarmCollection(ctx, 100)
	select {
	case warmCtx := <-started:
		require.NoError(t, warmCtx.Err())
	case <-time.After(3 * time.Second):
		t.Fatal("prewarm was not admitted")
	}
	stop()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy shutdown did not cancel admission")
	}
}
