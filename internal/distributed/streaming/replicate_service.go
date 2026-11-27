package streaming

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	msg, err = s.waitAppendFirstCopies(ctx, msg)
	if err != nil {
		return nil, err
	}
	result, err := s.appendReplicateMessageToWAL(ctx, msg)
	if err != nil {
		return nil, err
	}
	s.reportAppendFirstCopy(ctx, msg, result)
	return result, nil
}

// reportAppendFirstCopy tells the streaming coord of this cluster the local
// time tick of an append-first copy as soon as it is appended, so that the
// copies waiting for it (waitAppendFirstCopies) need not wait for its
// acknowledgement. For the fence of a shard split the acknowledgement comes
// only after the residual of the source has flushed; what the genesis of a
// target needs is a barrier above the local time tick of the fence, which
// the append result gives now.
//
// The report is best effort: if it fails, or the coordinator loses it, the
// waiting copies are released by the acknowledgement as before. A copy the
// replicate interceptor dropped as a duplicate returns an error and is not
// reported; its first append was.
func (s replicateService) reportAppendFirstCopy(ctx context.Context, msg message.MutableMessage, result *types.AppendResult) {
	bh := msg.BroadcastHeader()
	if bh == nil || result == nil || result.TimeTick == 0 || !slices.Contains(bh.AppendFirstVChannels, msg.VChannel()) {
		return
	}
	if err := s.streamingCoordClient.Broadcast().ReportVChannelAppended(ctx, bh.BroadcastID, msg.VChannel(), result.TimeTick); err != nil {
		mlog.Warn(ctx, "report of an appended append-first copy failed, the copies waiting for it wait for its acknowledgement",
			mlog.Uint64("broadcastID", bh.BroadcastID), mlog.String("vchannel", msg.VChannel()),
			mlog.Uint64("timeTick", result.TimeTick), mlog.Err(err))
	}
}

// waitAppendFirstCopies is the append gate of a secondary cluster.
//
// A broadcast that names an append-first vchannel is ordered on the primary
// by its broadcaster: that copy is appended and acknowledged before any other
// copy. Replication carries the copies as independent per-pchannel streams and
// restores no order between them. For a shard split that order is what keeps
// every event of a target above the fence of its source, so it must hold in
// the WAL of this cluster too.
//
// A copy that is not append-first therefore waits here until the append-first
// copies are appended on this cluster, and is then appended with their local
// time tick as its barrier, so its own time tick is above it. The replicate
// stream of an append-first copy reports its local time tick to the streaming
// coord once it is appended (reportAppendFirstCopy); without that report the
// wait ends at the acknowledgement of the copy, with the acknowledged time
// tick. Either is the time tick of a persisted copy, at or above the first.
// The replicate stream of a pchannel appends in order and waits for each
// append, so everything behind the gated copy on that stream waits with it.
// Only the streams of those copies wait; an append-first copy and the copy on
// the control channel never do.
//
// The wait cannot form a cycle. On the primary every append-first copy has a
// time tick below every other copy of its broadcast, and each stream delivers
// its pchannel in time tick order. Take the stream head with the smallest
// primary time tick: if it is gated, its append-first copy has a smaller time
// tick still, so that copy is already appended here or is itself a stream
// head, which contradicts the choice.
//
// It runs after the vchannels were renamed, so every name is a name of this
// cluster.
func (s replicateService) waitAppendFirstCopies(ctx context.Context, msg message.MutableMessage) (message.MutableMessage, error) {
	bh := msg.BroadcastHeader()
	if bh == nil || len(bh.AppendFirstVChannels) == 0 {
		return msg, nil
	}
	if slices.Contains(bh.AppendFirstVChannels, msg.VChannel()) {
		// This is the copy the others wait for.
		return msg, nil
	}
	if funcutil.IsControlChannel(msg.VChannel()) {
		// The copy on the control channel has no effect on a WAL; it orders the
		// ack callback, which runs only after every copy is acknowledged. Its
		// stream carries the control copy of every DDL of the cluster, so a
		// wait here would hold all of them.
		return msg, nil
	}
	// A copy this cluster already appended is dropped by the replicate
	// interceptor as a duplicate. It must not wait: its broadcast task may be
	// collected by now.
	covered, err := s.coveredByReplicateCheckpoint(ctx, msg)
	if err != nil {
		return nil, err
	}
	if covered {
		return msg, nil
	}

	logger := mlog.With(
		mlog.Uint64("broadcastID", bh.BroadcastID),
		mlog.String("vchannel", msg.VChannel()),
		mlog.Strings("appendFirstVChannels", bh.AppendFirstVChannels),
		mlog.String("messageType", msg.MessageType().String()))
	logger.Info(ctx, "replicated append waits for the append-first copies of its broadcast")
	start := time.Now()
	timeTick, err := s.streamingCoordClient.Broadcast().WaitVChannelsAcked(ctx, msg.MessageType(), bh.BroadcastID, bh.AppendFirstVChannels)
	if err != nil {
		// The stream appends the message again from its checkpoint and waits again.
		logger.Warn(ctx, "wait for the append-first copies failed, the replicate stream retries",
			mlog.Duration("waited", time.Since(start)), mlog.Err(err))
		return nil, err
	}
	logger.Info(ctx, "append-first copies are acknowledged, the replicated append proceeds",
		mlog.Duration("waited", time.Since(start)), mlog.Uint64("barrierTimeTick", timeTick))
	if msg.BarrierTimeTick() == 0 {
		msg = msg.WithBarrierTimeTick(timeTick)
	}
	return msg, nil
}

// coveredByReplicateCheckpoint reports whether the WAL of this cluster already
// appended the message, judged the way the replicate interceptor judges a
// duplicate: by the replicate checkpoint of its pchannel. The checkpoint only
// moves forward, so a message that is covered now is still covered when it
// reaches the interceptor.
func (s replicateService) coveredByReplicateCheckpoint(ctx context.Context, msg message.MutableMessage) (bool, error) {
	rh := msg.ReplicateHeader()
	if rh == nil {
		return false, nil
	}
	checkpoint, err := s.handlerClient.GetReplicateCheckpoint(ctx, funcutil.ToPhysicalChannel(msg.VChannel()))
	if err != nil {
		return false, err
	}
	if checkpoint == nil || checkpoint.ClusterID != rh.ClusterID {
		return false, nil
	}
	return rh.TimeTick <= checkpoint.TimeTick, nil
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
	if cfg == nil {
		return nil, status.NewReplicateViolation("cluster has no replicate configuration, cannot receive replicate message")
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
		// The header was written by another cluster. An append-first vchannel
		// that is not a vchannel of the broadcast cannot be renamed; refuse the
		// message instead of failing the process on it.
		for _, vchannel := range bh.AppendFirstVChannels {
			if !slices.Contains(bh.VChannels, vchannel) {
				return nil, status.NewReplicateViolation(
					"malformed broadcast header of replicated %s message %d: append-first vchannel %s is not one of its vchannels",
					msg.MessageType(), bh.BroadcastID, vchannel)
			}
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

// overwriteSplitShardMessage renames every vchannel a SplitShard message names
// into the vchannels of this cluster: the source and the targets in the
// header, and the shards of the routing in the body.
//
// Nothing else is changed. The collection id, the partition ids, the split
// task id, the residues and the modulus are the same facts in both clusters.
// The message carries no value local to the source cluster.
func (s replicateService) overwriteSplitShardMessage(sourceCluster *replicateutil.MilvusCluster, msg message.ReplicateMutableMessage) error {
	splitMsg := message.MustAsMutableSplitShardMessageV2(msg)
	header := splitMsg.Header()
	source, err := s.getTargetVChannel(sourceCluster, header.GetSourceVchannel())
	if err != nil {
		return err
	}
	header.SourceVchannel = source
	for idx, vchannel := range header.GetTargetVchannels() {
		target, err := s.getTargetVChannel(sourceCluster, vchannel)
		if err != nil {
			return err
		}
		header.TargetVchannels[idx] = target
	}
	splitMsg.OverwriteHeader(header)

	body := splitMsg.MustBody()
	for _, shard := range body.GetRouting().GetShards() {
		vchannel, err := s.getTargetVChannel(sourceCluster, shard.GetVchannelName())
		if err != nil {
			return err
		}
		shard.VchannelName = vchannel
	}
	splitMsg.OverwriteBody(body)
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
