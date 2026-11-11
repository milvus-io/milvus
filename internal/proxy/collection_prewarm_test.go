package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPrewarmAdmissionDoesNotWaitOrFollowClientCancellation(t *testing.T) {
	paramtable.Init()
	lifetime, stop := context.WithCancel(t.Context())
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // The completed load request no longer owns background admission.

	started := make(chan context.Context, 1)
	finished := make(chan struct{})
	client := &prewarmTestClient{warm: func(ctx context.Context, collectionID int64) error {
		require.EqualValues(t, 100, collectionID)
		defer close(finished)
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}}
	node := &Proxy{ctx: lifetime, viewQueryClient: client}
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

type prewarmTestClient struct {
	queryclient.Client
	warm func(context.Context, int64) error
}

func (c *prewarmTestClient) Prewarm(ctx context.Context, collectionID int64) error {
	return c.warm(ctx, collectionID)
}
