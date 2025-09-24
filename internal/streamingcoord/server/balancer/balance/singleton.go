package balance

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var singleton = syncutil.NewFuture[balancer.Balancer]()

func Register(balancer balancer.Balancer) {
	singleton.Set(balancer)
}

func GetWithContext(ctx context.Context) (balancer.Balancer, error) {
	return singleton.GetWithContext(ctx)
}

func Release() {
	if !singleton.Ready() {
		return
	}
	singleton.Get().Close()
}
