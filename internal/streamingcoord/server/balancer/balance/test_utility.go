//go:build test
// +build test

package balance

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func ResetBalancer() {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	singleton = syncutil.NewFuture[balancer.Balancer]()
}
