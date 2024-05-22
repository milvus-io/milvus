package client

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"google.golang.org/grpc"
)

type stateService struct {
	*clientImpl
}

func (c *stateService) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("state client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getStateService(ctx)
	if err != nil {
		return nil, err
	}
	return service.GetComponentStates(ctx, in, opts...)
}
