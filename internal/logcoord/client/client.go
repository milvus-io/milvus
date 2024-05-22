package client

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

var _ Client = (*clientImpl)(nil)

// Client is the interface of log service client.
type Client interface {
	// Channel access channel service.
	Channel() ChannelService

	// Assignment access assignment service.
	Assignment() AssignmentService

	// State access state service.
	State() logpb.LogCoordStateServiceClient

	// Close close the client.
	Close()
}

type ChannelService interface {
	CreatePChannel(ctx context.Context, name string) error
	RemovePChannel(ctx context.Context, name string) error
	CreateVChannel(ctx context.Context, pName string, vNames []string) error
	RemoveVChannel(ctx context.Context, pName string, vNames []string) error
}

type AssignmentService interface {
	AssignmentDiscover(ctx context.Context, cb func(*logpb.AssignmentDiscoverResponse) error) error

	ReportLogError(ctx context.Context, req *logpb.ReportLogErrorRequest) error
}

type clientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	conn     *lazyconn.LazyGRPCConn
	rb       resolver.Builder
}

func (c *clientImpl) Channel() ChannelService {
	return &channelService{
		clientImpl: c,
	}
}

func (c *clientImpl) Assignment() AssignmentService {
	return &assignmentService{
		clientImpl: c,
	}
}

func (c *clientImpl) State() logpb.LogCoordStateServiceClient {
	return &stateService{
		clientImpl: c,
	}
}

func (c *clientImpl) Close() {
	c.lifetime.SetState(lifetime.Stopped)
	c.lifetime.Wait()
	c.lifetime.Close()
	c.conn.Close()
	c.rb.Close()
}

// getChannelService returns a channel service client.
func (c *clientImpl) getChannelService(ctx context.Context) (logpb.LogCoordChannelServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return logpb.NewLogCoordChannelServiceClient(conn), nil
}

// getAssignmentService returns a channel service client.
func (c *clientImpl) getAssignmentService(ctx context.Context) (logpb.LogCoordAssignmentServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return logpb.NewLogCoordAssignmentServiceClient(conn), nil
}

// getStateService returns a channel service client.
func (c *clientImpl) getStateService(ctx context.Context) (logpb.LogCoordStateServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return logpb.NewLogCoordStateServiceClient(conn), nil
}
