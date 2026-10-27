package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	resumablereplicas "github.com/milvus-io/milvus/internal/distributed/streaming/internal/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type replicaQueryHandler struct{ handler.HandlerClient }

func (*replicaQueryHandler) QueryViewClient() handler.QueryViewClient { panic("mockey") }

func TestQueryViewClientDiscoveryLifetime(t *testing.T) {
	mockey.PatchConvey("streaming owns logical subscriptions", t, func() {
		mockey.Mock((*replicaQueryHandler).QueryViewClient).Return(nil).Build()
		d := replicadiscovery.NewDirectory()
		defer d.Close()
		owner := &walAccesserImpl{
			lifetime: typeutil.NewLifetime(), handlerClient: &replicaQueryHandler{},
			replicaClient: resumablereplicas.NewResumingClient(func(ctx context.Context, pc string) (replicadiscovery.Stream, error) {
				return d.AcquireStream(ctx, pc, nil), nil
			}),
		}
		defer owner.replicaClient.Close()
		client := owner.QueryViewClient()
		sub, err := client.SubscribeReplicas("p_1v0", func(replicadiscovery.Snapshot) {})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return sub.Snapshot().Valid }, time.Second, time.Millisecond)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		owner.replicaClient.Close()
		owner.lifetime.Wait()
		require.False(t, sub.Snapshot().Valid)
		_, err = client.SubscribeReplicas("p_1v0", func(replicadiscovery.Snapshot) {})
		require.ErrorIs(t, err, ErrWALAccesserClosed)
	})
}
