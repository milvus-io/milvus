package balance

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var (
	singletonMu sync.RWMutex
	singleton   = syncutil.NewFuture[balancer.Balancer]()
)

func Register(balancer balancer.Balancer) {
	singletonMu.RLock()
	s := singleton
	singletonMu.RUnlock()
	s.Set(balancer)
}

func SetFileResourceChecker(checker balancer.FileResourceChecker) {
	singletonMu.RLock()
	s := singleton
	singletonMu.RUnlock()
	s.Get().SetFileResourceChecker(checker)
}

func GetWithContext(ctx context.Context) (balancer.Balancer, error) {
	singletonMu.RLock()
	s := singleton
	singletonMu.RUnlock()
	return s.GetWithContext(ctx)
}

func Release() {
	singletonMu.RLock()
	s := singleton
	singletonMu.RUnlock()
	if !s.Ready() {
		return
	}
	s.Get().Close()
}
