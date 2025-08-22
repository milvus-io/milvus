package streaming

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ ReplicateService = replicateService{}

type replicateService struct {
	*walAccesserImpl
}

func (s replicateService) UpdateReplicateConfiguration(ctx context.Context, config *milvuspb.ReplicateConfiguration) error {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	return s.streamingCoordClient.Assignment().UpdateReplicateConfiguration(ctx, config)
}

func (s replicateService) GetReplicateConfiguration(ctx context.Context) (*milvuspb.ReplicateConfiguration, error) {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	config, err := s.streamingCoordClient.Assignment().GetReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (s replicateService) GetWALCheckpoint(ctx context.Context, channelName string) (*streamingpb.ReplicateWALCheckpoint, error) {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	checkpoint, err := s.handlerClient.GetWALCheckpoint(ctx, channelName)
	if err != nil {
		return nil, err
	}

	return &streamingpb.ReplicateWALCheckpoint{
		// TODO: sheep, assign replicate checkpoint.
		ReplicateMessageID: checkpoint.MessageId,
	}, nil
}
