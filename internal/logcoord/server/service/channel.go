package service

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/logcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// NewChannelService opens a meta instance and create channel service.
func NewChannelService(
	channelMeta channel.Meta,
	balancer balancer.Balancer,
) ChannelService {
	return &channelServiceImpl{
		channelMeta: channelMeta,
		balancer:    balancer,
	}
}

var _ ChannelService = (*channelServiceImpl)(nil)

type ChannelService interface {
	logpb.LogCoordChannelServiceServer
}

type channelServiceImpl struct {
	mu          sync.Mutex
	channelMeta channel.Meta
	balancer    balancer.Balancer
}

func (cs *channelServiceImpl) CreatePChannel(ctx context.Context, request *logpb.CreatePChannelRequest) (*logpb.CreatePChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Create a pchannel in meta.
	err := cs.channelMeta.CreatePChannel(ctx, request.PChannelName)
	if errors.Is(err, channel.ErrExists) {
		return nil, status.NewChannelExist("create pchannel %s", request.PChannelName)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// Add new pChannel to balancer.
	pChannel := cs.channelMeta.GetPChannel(ctx, request.PChannelName)
	if pChannel == nil {
		log.Warn("unexpected pChannel not found", zap.String("pChannelName", request.PChannelName))
		return nil, status.NewInner("channel meta error: pChannel not found")
	}
	cs.balancer.UpdateChannel(map[string]channel.PhysicalChannel{
		pChannel.Name(): pChannel,
	})

	return &logpb.CreatePChannelResponse{}, nil
}

func (cs *channelServiceImpl) RemovePChannel(ctx context.Context, request *logpb.RemovePChannelRequest) (*logpb.RemovePChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()
	err := cs.channelMeta.RemovePChannel(ctx, request.PChannelName)
	if errors.Is(err, channel.ErrNotExists) {
		return nil, status.NewChannelNotExist("remove pchannel %s", request.PChannelName)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// remove pChannel from balancer.
	cs.balancer.UpdateChannel(map[string]channel.PhysicalChannel{
		request.PChannelName: nil,
	})
	return &logpb.RemovePChannelResponse{}, nil
}

func (cs *channelServiceImpl) CreateVChannel(ctx context.Context, request *logpb.CreateVChannelRequest) (*logpb.CreateVChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// TODO: We should move the p v allocation logic on logcoord.
	pChannel := cs.channelMeta.GetPChannel(ctx, request.PChannelName)
	if pChannel == nil {
		return nil, status.NewChannelNotExist("create vchannel %v on pchannel %s", request.VChannelName, request.PChannelName)
	}

	err := pChannel.AddVChannel(ctx, request.VChannelName...)
	if errors.Is(err, channel.ErrDropped) {
		return nil, status.NewChannelNotExist("create vchannel %v on pchannel %s", request.VChannelName, request.PChannelName)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// Trigger a re-balance.
	cs.balancer.ReBalance()
	return &logpb.CreateVChannelResponse{}, nil
}

func (cs *channelServiceImpl) RemoveVChannel(ctx context.Context, request *logpb.RemoveVChannelRequest) (*logpb.RemoveVChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()

	pChannel := cs.channelMeta.GetPChannel(ctx, request.PChannelName)
	if pChannel == nil {
		return nil, status.NewChannelNotExist("remove vchannel %v on pchannel %s", request.VChannelName, request.PChannelName)
	}

	err := pChannel.RemoveVChannel(ctx, request.VChannelName...)
	if errors.Is(err, channel.ErrDropped) {
		return nil, status.NewChannelNotExist("remove vchannel %v on pchannel %s", request.VChannelName, request.PChannelName)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// Trigger a re-balance.
	cs.balancer.ReBalance()
	return &logpb.RemoveVChannelResponse{}, nil
}
