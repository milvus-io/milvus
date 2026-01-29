package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

var errReplicateConfigurationSame = errors.New("same replicate configuration")

// NewAssignmentService returns a new assignment service.
func NewAssignmentService() streamingpb.StreamingCoordAssignmentServiceServer {
	assignmentService := &assignmentServiceImpl{
		listenerTotal: metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()),
	}
	registry.RegisterAlterReplicateConfigV2AckCallback(assignmentService.alterReplicateConfiguration)
	return assignmentService
}

type AssignmentService interface {
	streamingpb.StreamingCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	streamingpb.UnimplementedStreamingCoordAssignmentServiceServer

	listenerTotal prometheus.Gauge
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(server streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer) error {
	s.listenerTotal.Inc()
	defer s.listenerTotal.Dec()

	balancer, err := balance.GetWithContext(server.Context())
	if err != nil {
		return err
	}
	return discover.NewAssignmentDiscoverServer(balancer, server).Execute()
}

// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
func (s *assignmentServiceImpl) UpdateReplicateConfiguration(ctx context.Context, req *streamingpb.UpdateReplicateConfigurationRequest) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	config := req.GetConfiguration()

	log.Ctx(ctx).Info("UpdateReplicateConfiguration received",
		zap.Bool("forcePromote", req.GetForcePromote()),
		replicateutil.ConfigLogField(config),
	)

	// check if the configuration is same.
	// so even if current cluster is not primary, we can still make a idempotent success result.
	if _, err := s.validateReplicateConfiguration(ctx, config); err != nil {
		if errors.Is(err, errReplicateConfigurationSame) {
			log.Ctx(ctx).Info("configuration is same, ignored")
			return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
		}
		return nil, err
	}

	// Force promote path - promotes secondary cluster to standalone primary
	if req.GetForcePromote() {
		return s.handleForcePromote(ctx, config)
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		if errors.Is(err, broadcast.ErrNotPrimary) {
			// current cluster is not primary, but we support an idempotent broadcast cross replication cluster.
			// For example, we have A/B/C three clusters, and A is primary in the replicating topology.
			// The milvus client can broadcast the UpdateReplicateConfiguration to all clusters,
			// if all clusters returne success, we can consider the UpdateReplicateConfiguration is successful and sync up between A/B/C.
			// so if current cluster is not primary, its UpdateReplicateConfiguration will be replicated by CDC,
			// so we should wait until the replication configuration is changed into the same one.
			return &streamingpb.UpdateReplicateConfigurationResponse{}, s.waitUntilPrimaryChangeOrConfigurationSame(ctx, config)
		}
		return nil, err
	}
	defer broadcaster.Close()

	msg, err := s.validateReplicateConfiguration(ctx, config)
	if err != nil {
		if errors.Is(err, errReplicateConfigurationSame) {
			log.Ctx(ctx).Info("configuration is same after cluster resource key is acquired, ignored")
			return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
		}
		return nil, err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	if err != nil {
		return nil, err
	}
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

// waitUntilPrimaryChangeOrConfigurationSame waits until the primary changes or the configuration is same.
func (s *assignmentServiceImpl) waitUntilPrimaryChangeOrConfigurationSame(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	errDone := errors.New("done")
	err = b.WatchChannelAssignments(ctx, func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		if proto.Equal(config, param.ReplicateConfiguration) {
			return errDone
		}
		return nil
	})
	if errors.Is(err, errDone) {
		return nil
	}
	return err
}

// validateReplicateConfiguration validates the replicate configuration.
func (s *assignmentServiceImpl) validateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) (message.BroadcastMutableMessage, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	// get all pchannels
	latestAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return nil, err
	}

	// double check if the configuration is same after resource key is acquired.
	if proto.Equal(config, latestAssignment.ReplicateConfiguration) {
		return nil, errReplicateConfigurationSame
	}

	controlChannel := streaming.WAL().ControlChannel()
	pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, channel *channel.PChannelMeta) string {
		return channel.Name()
	})
	broadcastPChannels := lo.Map(pchannels, func(pchannel string, _ int) string {
		if funcutil.IsOnPhysicalChannel(controlChannel, pchannel) {
			// return control channel if the control channel is on the pchannel.
			return controlChannel
		}
		return pchannel
	})

	// validate the configuration itself
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentConfig := latestAssignment.ReplicateConfiguration
	incomingConfig := config
	validator := replicateutil.NewReplicateConfigValidator(incomingConfig, currentConfig, currentClusterID, pchannels)
	if err := validator.Validate(); err != nil {
		log.Ctx(ctx).Warn("UpdateReplicateConfiguration fail", zap.Error(err))
		return nil, err
	}

	// TODO: validate the incoming configuration is compatible with the current config.
	if _, err := replicateutil.NewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), config); err != nil {
		return nil, err
	}
	b := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: config,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(broadcastPChannels).
		MustBuildBroadcast()
	return b, nil
}

// validateForcePromoteConfiguration validates that the force promote configuration is safe.
// Requirements:
// 1. Must contain ONLY the current cluster
// 2. Must have NO topology (no replication relationships)
func validateForcePromoteConfiguration(config *commonpb.ReplicateConfiguration, currentClusterID string) error {
	// Use config helper to validate the configuration structure
	helper, err := replicateutil.NewConfigHelper(currentClusterID, config)
	if err != nil {
		return status.NewInvaildArgument("invalid replicate configuration for force promote: %v", err)
	}

	// Check that configuration contains exactly one cluster (the current cluster)
	if len(config.Clusters) != 1 {
		return status.NewInvaildArgument(
			"force promote requires configuration with exactly one cluster (current cluster only), got %d clusters",
			len(config.Clusters))
	}

	// Check that the single cluster is the current cluster
	if config.Clusters[0].ClusterId != currentClusterID {
		return status.NewInvaildArgument(
			"force promote requires configuration with only current cluster %s, got cluster %s",
			currentClusterID,
			config.Clusters[0].ClusterId)
	}

	// Check that there is NO topology (no replication)
	if len(config.CrossClusterTopology) > 0 {
		return status.NewInvaildArgument(
			"force promote requires configuration with no topology (single primary cluster), got %d topology edges",
			len(config.CrossClusterTopology))
	}

	// Verify the cluster role is primary (should be true for single cluster with no topology)
	if helper.GetCurrentCluster().Role() != replicateutil.RolePrimary {
		return status.NewInvaildArgument("force promote configuration must result in current cluster being primary")
	}

	return nil
}

// handleForcePromote handles force promote logic for replicate configuration.
// It promotes a secondary cluster to standalone primary immediately without waiting for CDC replication.
func (s *assignmentServiceImpl) handleForcePromote(ctx context.Context, config *commonpb.ReplicateConfiguration) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	log.Ctx(ctx).Warn("Force promote replicate configuration requested")

	// VALIDATION 1: Must be a secondary cluster (not primary)
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	latestAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return nil, err
	}

	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentConfig := latestAssignment.ReplicateConfiguration

	// Check if current cluster is primary by attempting to acquire cluster lock
	// Only non-primary clusters should use force promote
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err == nil {
		// Successfully acquired lock = we ARE primary cluster
		broadcaster.Close()
		return nil, status.NewInvaildArgument("force promote can only be used on secondary clusters, current cluster is primary")
	}
	if !errors.Is(err, broadcast.ErrNotPrimary) {
		// Some other error, not the expected ErrNotPrimary
		return nil, err
	}
	// We got ErrNotPrimary, which is what we expect for secondary cluster - continue

	// VALIDATION 2: New config must contain ONLY current cluster and no topology
	if err := validateForcePromoteConfiguration(config, currentClusterID); err != nil {
		return nil, err
	}

	// Double check if configuration is same
	if proto.Equal(config, currentConfig) {
		log.Ctx(ctx).Info("configuration is same in force promote, ignored")
		return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
	}

	// Validate the configuration using existing validator
	controlChannel := streaming.WAL().ControlChannel()
	pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, channel *channel.PChannelMeta) string {
		return channel.Name()
	})

	validator := replicateutil.NewReplicateConfigValidator(config, currentConfig, currentClusterID, pchannels)
	if err := validator.Validate(); err != nil {
		log.Ctx(ctx).Warn("Force promote replicate configuration validation failed", zap.Error(err))
		return nil, err
	}

	// Create the AlterReplicateConfigMessage with force promote flag
	broadcastPChannels := lo.Map(pchannels, func(pchannel string, _ int) string {
		if funcutil.IsOnPhysicalChannel(controlChannel, pchannel) {
			return controlChannel
		}
		return pchannel
	})

	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: config,
			ForcePromote:           true, // marks as force promote
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(broadcastPChannels).
		MustBuildBroadcast()

	// Use Broadcast().Append() to broadcast the message
	// This properly broadcasts to all vchannels and handles resource keys
	broadcastService := streaming.WAL().Broadcast()
	broadcastResult, err := broadcastService.Append(ctx, msg)
	if err != nil {
		log.Ctx(ctx).Error("Failed to broadcast force promote AlterReplicateConfigMessage", zap.Error(err))
		return nil, err
	}

	// Convert types.BroadcastAppendResult to message.BroadcastResultAlterReplicateConfigMessageV2
	msgAppendResults := make(map[string]*message.AppendResult)
	for vchannel, appendResult := range broadcastResult.AppendResults {
		msgAppendResults[vchannel] = &message.AppendResult{
			MessageID:              appendResult.MessageID,
			LastConfirmedMessageID: appendResult.LastConfirmedMessageID,
			TimeTick:               appendResult.TimeTick,
		}
	}

	result := message.BroadcastResultAlterReplicateConfigMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.AlterReplicateConfigMessageHeader, *message.AlterReplicateConfigMessageBody](msg),
		Results: msgAppendResults,
	}

	// Apply the configuration using the normal update method
	// The channel manager will detect force promote flag from message header
	if err := balancer.UpdateReplicateConfiguration(ctx, result); err != nil {
		return nil, err
	}

	log.Ctx(ctx).Info("Force promote replicate configuration completed successfully")
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

// alterReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) alterReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}

	// Update the configuration first
	if err := balancer.UpdateReplicateConfiguration(ctx, result); err != nil {
		return err
	}

	// Check if this is a force promote by looking at the message header
	isForcePromote := result.Message.Header().ForcePromote

	// If this is a force promote, supplement incomplete broadcasts
	if isForcePromote {
		log.Ctx(ctx).Info("Force promote completed, supplementing incomplete broadcasts")

		if err := s.supplementIncompleteBroadcasts(ctx); err != nil {
			log.Ctx(ctx).Warn("Failed to supplement incomplete broadcasts", zap.Error(err))
			return err
		}

		log.Ctx(ctx).Info("Completed supplementing incomplete broadcasts")
	}

	return nil
}

// supplementIncompleteBroadcasts supplements any incomplete broadcast messages
// by re-sending to vchannels that haven't received them.
func (s *assignmentServiceImpl) supplementIncompleteBroadcasts(ctx context.Context) error {
	log.Ctx(ctx).Info("Supplementing incomplete broadcasts after force promote")

	// Get broadcaster to access pending tasks
	broadcaster, err := broadcast.GetWithContext(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get broadcaster")
	}

	// Get all pending broadcast messages
	pendingMessages := broadcaster.GetPendingBroadcastMessages()
	if len(pendingMessages) == 0 {
		log.Ctx(ctx).Info("No pending broadcast messages to supplement")
		return nil
	}

	log.Ctx(ctx).Info("Found pending broadcast messages to supplement",
		zap.Int("pendingCount", len(pendingMessages)))

	// Append pending messages to their respective vchannels
	appendResults := streaming.WAL().AppendMessages(ctx, pendingMessages...)

	// Check for errors and count successes
	var lastResultErr error
	supplementCount := 0
	failureCount := 0
	for i, result := range appendResults.Responses {
		if result.Error != nil {
			log.Ctx(ctx).Warn("Failed to supplement message",
				zap.String("vchannel", pendingMessages[i].VChannel()),
				zap.Error(result.Error))
			failureCount++
			lastResultErr = result.Error
			continue
		}
		supplementCount++
	}

	log.Ctx(ctx).Info("Completed supplementing incomplete broadcasts",
		zap.Int("supplementCount", supplementCount),
		zap.Int("failureCount", failureCount),
		zap.Int("totalPending", len(pendingMessages)),
		zap.Error(lastResultErr),
	)

	return lastResultErr
}

// UpdateWALBalancePolicy is used to update the WAL balance policy.
func (s *assignmentServiceImpl) UpdateWALBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return balancer.UpdateBalancePolicy(ctx, req)
}
