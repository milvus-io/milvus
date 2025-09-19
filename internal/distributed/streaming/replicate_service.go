package streaming

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ ReplicateService = replicateService{}

type replicateService struct {
	*walAccesserImpl
}

// Append appends the message into current cluster.
func (s replicateService) Append(ctx context.Context, rmsg message.ReplicateMutableMessage) (*types.AppendResult, error) {
	rh := rmsg.ReplicateHeader()
	if rh == nil {
		panic("message is not a replicate message")
	}

	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	msg, err := s.overwriteReplicateMessage(ctx, rmsg, rh)
	if err != nil {
		return nil, err
	}
	return s.appendToWAL(ctx, msg)
}

func (s replicateService) UpdateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	return s.streamingCoordClient.Assignment().UpdateReplicateConfiguration(ctx, config)
}

func (s replicateService) GetReplicateConfiguration(ctx context.Context) (*replicateutil.ConfigHelper, error) {
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

func (s replicateService) GetReplicateCheckpoint(ctx context.Context, channelName string) (*wal.ReplicateCheckpoint, error) {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	checkpoint, err := s.handlerClient.GetReplicateCheckpoint(ctx, channelName)
	if err != nil {
		return nil, err
	}

	return checkpoint, nil
}

// overwriteReplicateMessage overwrites the replicate message.
// because some message such as create collection message write vchannel in its body, so we need to overwrite the message.
func (s replicateService) overwriteReplicateMessage(ctx context.Context, msg message.ReplicateMutableMessage, rh *message.ReplicateHeader) (message.MutableMessage, error) {
	cfg, err := s.streamingCoordClient.Assignment().GetReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	// Get target vchannel on current cluster that should be written to
	currentCluster := cfg.GetCluster(s.clusterID)
	if currentCluster.Role() == replicateutil.RolePrimary {
		return nil, status.NewReplicateViolation("primary cluster cannot receive replicate message")
	}
	sourceCluster := cfg.GetCluster(rh.ClusterID)
	if sourceCluster == nil {
		return nil, status.NewReplicateViolation("source cluster %s not found in replicate configuration", rh.ClusterID)
	}
	targetVChannel, err := s.getTargetVChannel(sourceCluster, msg.VChannel())
	if err != nil {
		return nil, err
	}

	// Get target broadcast vchannels on current cluster that should be written to
	if bh := msg.BroadcastHeader(); bh != nil {
		// broadcast header have vchannels, so we need to overwrite it.
		targetBroadcastVChannels := make([]string, 0, len(bh.VChannels))
		for _, vchannel := range bh.VChannels {
			targetBroadcastVChannel, err := s.getTargetVChannel(sourceCluster, vchannel)
			if err != nil {
				return nil, status.NewReplicateViolation("failed to get target channel, %s", err.Error())
			}
			targetBroadcastVChannels = append(targetBroadcastVChannels, targetBroadcastVChannel)
		}
		msg.OverwriteReplicateVChannel(targetVChannel, targetBroadcastVChannels)
	} else {
		msg.OverwriteReplicateVChannel(targetVChannel)
	}

	// create collection message will set the vchannel in its body, so we need to overwrite it.
	if msg.MessageType() == message.MessageTypeCreateCollection {
		if err := s.overwriteCreateCollectionMessage(sourceCluster, msg); err != nil {
			return nil, err
		}
	}

	if funcutil.IsControlChannel(msg.VChannel()) {
		assignments, err := s.streamingCoordClient.Assignment().GetLatestAssignments(ctx)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(msg.VChannel(), assignments.PChannelOfCChannel()) {
			return nil, status.NewReplicateViolation("invalid control channel %s, expected pchannel %s", msg.VChannel(), assignments.PChannelOfCChannel())
		}
	}
	return msg, nil
}

// getTargetVChannel gets the target vchannel of the source vchannel.
func (s replicateService) getTargetVChannel(sourceCluster *replicateutil.MilvusCluster, sourceVChannel string) (string, error) {
	sourcePChannel := funcutil.ToPhysicalChannel(sourceVChannel)
	targetPChannel, err := sourceCluster.GetTargetChannel(sourcePChannel, s.clusterID)
	if err != nil {
		return "", status.NewReplicateViolation("failed to get target channel, %s", err.Error())
	}
	return strings.Replace(sourceVChannel, sourcePChannel, targetPChannel, 1), nil
}

// overwriteCreateCollectionMessage overwrites the create collection message.
func (s replicateService) overwriteCreateCollectionMessage(sourceCluster *replicateutil.MilvusCluster, msg message.ReplicateMutableMessage) error {
	createCollectionMsg := message.MustAsMutableCreateCollectionMessageV1(msg)
	body := createCollectionMsg.MustBody()
	for idx, sourcePChannel := range body.PhysicalChannelNames {
		targetPChannel, err := sourceCluster.GetTargetChannel(sourcePChannel, s.clusterID)
		if err != nil {
			return status.NewReplicateViolation("failed to get target channel, %s", err.Error())
		}
		body.PhysicalChannelNames[idx] = targetPChannel
		body.VirtualChannelNames[idx] = strings.Replace(body.VirtualChannelNames[idx], sourcePChannel, targetPChannel, 1)
	}
	createCollectionMsg.OverwriteBody(body)
	return nil
}
