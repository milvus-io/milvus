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

// TryGet returns the registered balancer without waiting for startup. Planning
// snapshots can defer work when it is unavailable instead of holding their
// admission lock until streaming initialization finishes.
func TryGet() (balancer.Balancer, bool) {
	singletonMu.RLock()
	s := singleton
	singletonMu.RUnlock()
	if !s.Ready() {
		return nil, false
	}
	return s.Get(), true
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
