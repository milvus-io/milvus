package streaming

import (
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// QueryViewClient combines owner-routed query RPCs and resumable discovery.
// HandlerClient itself provides only physical, non-resuming operations.
type QueryViewClient interface {
	handler.QueryViewClient
	SubscribeReplicas(string, replicadiscovery.Handler) (replicadiscovery.Subscription, error)
}

type queryViewClient struct {
	handler.QueryViewClient
	owner *walAccesserImpl
}

func (w *walAccesserImpl) QueryViewClient() QueryViewClient {
	return queryViewClient{QueryViewClient: w.handlerClient.QueryViewClient(), owner: w}
}

func (c queryViewClient) SubscribeReplicas(vc string, h replicadiscovery.Handler) (replicadiscovery.Subscription, error) {
	if !c.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer c.owner.lifetime.Done()
	return c.owner.replicaClient.Subscribe(vc, h)
}
