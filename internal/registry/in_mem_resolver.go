package registry

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	qnClient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/wrappers"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	once sync.Once

	resolver atomic.Pointer[InMemResolver]
)

func GetInMemoryResolver() *InMemResolver {
	r := resolver.Load()
	if r == nil {
		once.Do(func() {
			newResolver := &InMemResolver{
				queryNodes: typeutil.NewConcurrentMap[int64, types.QueryNode](),
			}
			resolver.Store(newResolver)
		})
		r = resolver.Load()
	}
	return r
}

type InMemResolver struct {
	queryNodes *typeutil.ConcurrentMap[int64, types.QueryNode]
}

func (r *InMemResolver) RegisterQueryNode(id int64, qn types.QueryNode) {
	r.queryNodes.Insert(id, qn)
}

func (r *InMemResolver) ResolveQueryNode(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	qn, ok := r.queryNodes.Get(nodeID)
	if !ok {
		return qnClient.NewClient(ctx, addr, nodeID)
	}
	return wrappers.WrapQueryNodeServerAsClient(qn), nil
}
