package streaming

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// buildSkipMessageTypes builds a set of message type names to skip during replication.
func buildSkipMessageTypes(types []string) map[string]struct{} {
	m := make(map[string]struct{}, len(types))
	for _, t := range types {
		if t != "" {
			m[t] = struct{}{}
		}
	}
	return m
}

var _ ReplicateService = replicateService{}

type replicateService struct {
	*walAccesserImpl
	skipMessageTypes map[string]struct{}
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

func (s replicateService) GetReplicateConfiguration(ctx context.Context) (*commonpb.ReplicateConfiguration, error) {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	// Use WithFreshRead to ensure strong consistency after UpdateReplicateConfiguration.
	configHelper, err := s.streamingCoordClient.Assignment().GetReplicateConfiguration(ctx, assignment.WithFreshRead())
	if err != nil {
		return nil, err
	}

	return replicateutil.SanitizeReplicateConfiguration(configHelper.GetReplicateConfiguration()), nil
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

// shouldSkipReplicateMessageType checks if the given message type should be skipped during replication.
func (s replicateService) shouldSkipReplicateMessageType(msgType message.MessageType) bool {
	_, ok := s.skipMessageTypes[msgType.String()]
	return ok
}

// overwriteReplicateMessage overwrites the replicate message.
// because some message such as create collection message write vchannel in its body, so we need to overwrite the message.
func (s replicateService) overwriteReplicateMessage(ctx context.Context, msg message.ReplicateMutableMessage, rh *message.ReplicateHeader) (message.MutableMessage, error) {
	if s.shouldSkipReplicateMessageType(msg.MessageType()) {
		return nil, status.NewIgnoreOperation("message type %s is configured to be skipped during replication", msg.MessageType())
	}

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

	// For pchannel-increasing AlterReplicateConfig messages, use the NEW config from the
	// message header to map ALL channels (including newly added ones).
	// The current config only knows about old pchannels, so both the main vchannel and
	// broadcast vchannels need the new config for mapping.
	channelMappingSourceCluster := sourceCluster
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		alterMsg := message.MustAsMutableAlterReplicateConfigMessageV2(msg)
		if alterMsg.Header().GetIsPchannelIncreasing() {
			newCfg, newCfgErr := replicateutil.NewConfigHelper(s.clusterID, alterMsg.Header().GetReplicateConfiguration())
			if newCfgErr != nil {
				return nil, status.NewReplicateViolation("failed to parse new replicate config from message header: %s", newCfgErr.Error())
			}
			channelMappingSourceCluster = newCfg.GetCluster(rh.ClusterID)
			if channelMappingSourceCluster == nil {
				return nil, status.NewReplicateViolation("source cluster %s not found in new replicate configuration", rh.ClusterID)
			}
		}
	}

	targetVChannel, err := s.getTargetVChannel(channelMappingSourceCluster, msg.VChannel())
	if err != nil {
		return nil, err
	}

	// Get target broadcast vchannels on current cluster that should be written to
	if bh := msg.BroadcastHeader(); bh != nil {
		targetBroadcastVChannels := make([]string, 0, len(bh.VChannels))
		for _, vchannel := range bh.VChannels {
			targetBroadcastVChannel, err := s.getTargetVChannel(channelMappingSourceCluster, vchannel)
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
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		if err := s.overwriteCreateCollectionMessage(sourceCluster, msg); err != nil {
			return nil, err
		}
	case message.MessageTypeAlterReplicateConfig:
		if err := s.overwriteAlterReplicateConfigMessage(cfg, msg); err != nil {
			return nil, err
		}
	case message.MessageTypeAlterLoadConfig:
		s.overwriteAlterLoadConfigMessage(msg)
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

// overwriteAlterReplicateConfigMessage overwrites the alter replicate configuration message.
func (s replicateService) overwriteAlterReplicateConfigMessage(currentReplicateConfig *replicateutil.ConfigHelper, msg message.ReplicateMutableMessage) error {
	alterReplicateConfigMsg := message.MustAsMutableAlterReplicateConfigMessageV2(msg)
	cfg := alterReplicateConfigMsg.Header().ReplicateConfiguration
	_, err := replicateutil.NewConfigHelper(s.clusterID, cfg)
	if err == nil {
		return nil
	}
	if !errors.Is(err, replicateutil.ErrCurrentClusterNotFound) {
		return err
	}

	// Current cluster not found in the replicate configuration,
	// it means that the current cluster is removed from the replicate topology and become a independent cluster.
	// So we need to overwrite the replicate configuration to make current cluster to be a primary cluster without replicate topology.
	cluster := currentReplicateConfig.GetCurrentCluster()
	alterReplicateConfigMsg.OverwriteHeader(&message.AlterReplicateConfigMessageHeader{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{cluster.MilvusCluster},
		},
	})
	return nil
}

// overwriteAlterLoadConfigMessage sets use_local_replica_config flag on replicated AlterLoadConfig messages
// when streaming.replication.useLocalReplicaConfig is enabled.
// This allows the secondary cluster to use its own cluster-level replica/resource-group config
// instead of blindly applying the primary's config.
func (s replicateService) overwriteAlterLoadConfigMessage(msg message.ReplicateMutableMessage) {
	if !paramtable.Get().StreamingCfg.ReplicationUseLocalReplicaConfig.GetAsBool() {
		return
	}
	alterLoadConfigMsg := message.MustAsMutableAlterLoadConfigMessageV2(msg)
	header := alterLoadConfigMsg.Header()
	header.UseLocalReplicaConfig = true
	alterLoadConfigMsg.OverwriteHeader(header)
}
