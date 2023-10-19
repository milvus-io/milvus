package registry

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	qnClient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/wrappers"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
				rootcoords: typeutil.NewConcurrentMap[int64, types.RootCoord](),
				queryNodes: typeutil.NewConcurrentMap[int64, types.QueryNode](),
			}
			resolver.Store(newResolver)
		})
		r = resolver.Load()
	}
	return r
}

type InMemResolver struct {
	rootcoords *typeutil.ConcurrentMap[int64, types.RootCoord]
	queryNodes *typeutil.ConcurrentMap[int64, types.QueryNode]
}

func (r *InMemResolver) RegisterQueryNode(id int64, qn types.QueryNode) {
	r.queryNodes.Insert(id, qn)
}

func (r *InMemResolver) RegisterRootCoord(id int64, rc types.RootCoord) {
	r.rootcoords.Insert(id, rc)
}

func (r *InMemResolver) ResolveQueryNode(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	qn, ok := r.queryNodes.Get(nodeID)
	if !ok {
		return qnClient.NewClient(ctx, addr, nodeID)
	}
	return wrappers.WrapQueryNodeServerAsClient(qn), nil
}

func (r *InMemResolver) ResolveRootCoord(ctx context.Context, addr string, nodeID int64) (types.RootCoordClient, error) {
	rc, ok := r.rootcoords.Get(nodeID)
	if !ok {
		return nil, merr.WrapErrServiceInternal("rootcoord not in-memory")
	}
	return wrappers.WrapRootCoordServerAsClient(rc), nil
}
