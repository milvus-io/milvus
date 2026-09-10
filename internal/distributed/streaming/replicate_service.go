package streaming

import (
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	if err := s.waitAppendFirstReplicas(ctx, msg); err != nil {
		return nil, err
	}
	return s.appendReplicateMessageToWAL(ctx, msg)
}

// waitAppendFirstReplicas is the secondary cluster's append gate.
//
// A broadcast whose header names append-first vchannels is ordered on the
// primary by the broadcaster: it appends and persists that group before any
// other replica. Replication carries the replicas to a secondary as independent
// per-pchannel streams, which restores no order between them at all, so a
// SplitShard target's genesis could otherwise be appended here before the
// source's fence -- inverting the one invariant the split rests on (nothing on a
// target precedes T_switch, nothing on a source follows it) and, with it, the
// order of a delete against an insert of the same primary key.
//
// The gate closes that gap on the receiving side, because only the receiving
// side knows: the sender sees one replica at a time and has no way to observe
// another cluster's ticks. The streamingcoord ack state is the fact it waits on.
//
// It cannot deadlock: an append-first replica never waits for anything, and
// every other replica waits only on append-first ones, so the wait graph has no
// cycle even when a rehash fences several sources at once.
//
// Called AFTER the remap, so the names it compares and the names it waits on are
// both this cluster's.
func (s replicateService) waitAppendFirstReplicas(ctx context.Context, msg message.MutableMessage) error {
	bh := msg.BroadcastHeader()
	if bh == nil || len(bh.AppendFirstVChannels) == 0 {
		return nil
	}
	if slices.Contains(bh.AppendFirstVChannels, msg.VChannel()) {
		// This replica IS one of the append-first ones; it is what the others
		// are waiting for.
		return nil
	}
	// The error is returned as it stands. Everything that can fail here is
	// transient -- the replicate stream's context ending, the coord being
	// unreachable, the broadcaster shutting down -- and the replicate stream
	// retries from its checkpoint, re-entering the wait.
	return s.streamingCoordClient.Broadcast().WaitVChannelsAcked(ctx, bh.BroadcastID, bh.AppendFirstVChannels)
}

func (s replicateService) UpdateReplicateConfiguration(ctx context.Context, req *milvuspb.UpdateReplicateConfigurationRequest) error {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	return s.streamingCoordClient.Assignment().UpdateReplicateConfiguration(ctx, req)
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

func (s replicateService) GetSalvageCheckpoint(ctx context.Context, channelName string) ([]*wal.ReplicateCheckpoint, error) {
	if !s.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer s.lifetime.Done()

	return s.handlerClient.GetSalvageCheckpoint(ctx, channelName)
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
	case message.MessageTypeSplitShard:
		if err := s.overwriteSplitShardMessage(sourceCluster, msg); err != nil {
			return nil, err
		}
	case message.MessageTypeAlterCollection:
		if err := s.overwriteShardSplitRoutingMessage(sourceCluster, msg); err != nil {
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

// overwriteAlterReplicateConfigMessage overwrites the alter replicate configuration message.
func (s replicateService) overwriteAlterReplicateConfigMessage(currentReplicateConfig *replicateutil.ConfigHelper, msg message.ReplicateMutableMessage) error {
	alterReplicateConfigMsg := message.MustAsMutableAlterReplicateConfigMessageV2(msg)
	header := alterReplicateConfigMsg.Header()

	// Check ignore field - if true, skip processing
	// This is used for incomplete switchover messages that should be ignored after force promote
	if header.Ignore {
		return nil
	}

	cfg := header.ReplicateConfiguration
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

// overwriteSplitShardMessage rewrites every channel name a SplitShard carries
// into this cluster's namespace: the sources it fences and the targets it
// creates, in the header, and the routing post-image (plus the target genesis,
// should it ever carry a channel list) in the body.
//
// Everything else in the message is deliberately left alone. Collection id,
// partition ids, the split task id, the residues and the modulus are the same
// facts in both clusters -- ids are replicated, and routing is a property of the
// data, not of where it is stored -- so remapping them would break the very
// correspondence replication exists to keep.
func (s replicateService) overwriteSplitShardMessage(sourceCluster *replicateutil.MilvusCluster, msg message.ReplicateMutableMessage) error {
	splitShardMsg := message.MustAsMutableSplitShardMessageV2(msg)
	header := splitShardMsg.Header()
	if err := s.overwriteVChannelNames(sourceCluster, header.SourceVchannels); err != nil {
		return err
	}
	for _, target := range header.GetTargets() {
		targetVChannel, err := s.getTargetVChannel(sourceCluster, target.GetVchannel())
		if err != nil {
			return err
		}
		target.Vchannel = targetVChannel
	}
	splitShardMsg.OverwriteHeader(header)

	body := splitShardMsg.MustBody()
	if err := s.overwriteRoutingChannelNames(sourceCluster, body.GetRouting()); err != nil {
		return err
	}
	if genesis := body.GetGenesis(); genesis != nil {
		if err := s.overwriteVChannelNames(sourceCluster, genesis.VirtualChannelNames); err != nil {
			return err
		}
		if err := s.overwritePChannelNames(sourceCluster, genesis.PhysicalChannelNames); err != nil {
			return err
		}
	}
	splitShardMsg.OverwriteBody(body)
	return nil
}

// overwriteShardSplitRoutingMessage rewrites the channel names of an
// AlterCollection that commits a shard-split routing post-image -- the split's
// adoption message, replicated like any other AlterCollection.
//
// Every other AlterCollection is left untouched: the routing mask is the only
// one whose updates carry channel names at all.
func (s replicateService) overwriteShardSplitRoutingMessage(sourceCluster *replicateutil.MilvusCluster, msg message.ReplicateMutableMessage) error {
	alterCollectionMsg := message.MustAsMutableAlterCollectionMessageV2(msg)
	if !slices.Contains(alterCollectionMsg.Header().GetUpdateMask().GetPaths(), message.FieldMaskCollectionShardSplitRouting) {
		return nil
	}
	body := alterCollectionMsg.MustBody()
	if err := s.overwriteRoutingChannelNames(sourceCluster, body.GetUpdates()); err != nil {
		return err
	}
	alterCollectionMsg.OverwriteBody(body)
	return nil
}

// overwriteRoutingChannelNames rewrites the channel names of a routing
// post-image in place.
//
// The shard infos are rewritten too, not only the two name lists: a shard info
// names its own vchannel so that a consumer can key by it instead of by position
// (internal/util/routing/table.go REFUSES a shard info whose name disagrees with
// the vchannel at its position), so a name left in the source cluster's
// namespace would make the whole routing table unreadable here rather than
// merely stale.
func (s replicateService) overwriteRoutingChannelNames(sourceCluster *replicateutil.MilvusCluster, updates *message.AlterCollectionMessageUpdates) error {
	if updates == nil {
		return nil
	}
	if err := s.overwriteVChannelNames(sourceCluster, updates.VirtualChannelNames); err != nil {
		return err
	}
	if err := s.overwritePChannelNames(sourceCluster, updates.PhysicalChannelNames); err != nil {
		return err
	}
	for _, shardInfo := range updates.GetShardInfos() {
		// An empty name is the persisted shape of a collection older than the
		// field, and means "key me by position"; there is nothing to map.
		if shardInfo.GetVchannelName() == "" {
			continue
		}
		vchannel, err := s.getTargetVChannel(sourceCluster, shardInfo.GetVchannelName())
		if err != nil {
			return err
		}
		shardInfo.VchannelName = vchannel
	}
	return nil
}

// overwriteVChannelNames rewrites a list of vchannel names in place.
func (s replicateService) overwriteVChannelNames(sourceCluster *replicateutil.MilvusCluster, vchannels []string) error {
	for idx, vchannel := range vchannels {
		targetVChannel, err := s.getTargetVChannel(sourceCluster, vchannel)
		if err != nil {
			return err
		}
		vchannels[idx] = targetVChannel
	}
	return nil
}

// overwritePChannelNames rewrites a list of pchannel names in place.
func (s replicateService) overwritePChannelNames(sourceCluster *replicateutil.MilvusCluster, pchannels []string) error {
	for idx, pchannel := range pchannels {
		targetPChannel, err := sourceCluster.GetTargetChannel(pchannel, s.clusterID)
		if err != nil {
			return status.NewReplicateViolation("failed to get target channel, %s", err.Error())
		}
		pchannels[idx] = targetPChannel
	}
	return nil
}
