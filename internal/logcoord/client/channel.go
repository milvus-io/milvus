package client

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

type channelService struct {
	*clientImpl
}

func (c *channelService) CreatePChannel(ctx context.Context, name string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.CreatePChannel(ctx, &logpb.CreatePChannelRequest{
		PChannelName: name,
	})
	return err
}

func (c *channelService) RemovePChannel(ctx context.Context, name string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.RemovePChannel(ctx, &logpb.RemovePChannelRequest{
		PChannelName: name,
	})
	return err
}

func (c *channelService) CreateVChannel(ctx context.Context, pName string, vNames []string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.CreateVChannel(ctx, &logpb.CreateVChannelRequest{
		PChannelName: pName,
		VChannelName: vNames,
	})
	return err
}

func (c *channelService) RemoveVChannel(ctx context.Context, pName string, vNames []string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.RemoveVChannel(ctx, &logpb.RemoveVChannelRequest{
		PChannelName: pName,
		VChannelName: vNames,
	})
	return err
}
