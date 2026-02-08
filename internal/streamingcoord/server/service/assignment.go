package service

import (
	"context"
	"sort"

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

	// Force promote path - promotes secondary cluster to standalone primary
	// Check this BEFORE normal validation since force promote requires empty config
	if req.GetForcePromote() {
		return s.handleForcePromote(ctx, config)
	}

	// check if the configuration is same.
	// so even if current cluster is not primary, we can still make a idempotent success result.
	if _, err := s.validateReplicateConfiguration(ctx, config); err != nil {
		if errors.Is(err, errReplicateConfigurationSame) {
			log.Ctx(ctx).Info("configuration is same, ignored")
			return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
		}
		return nil, err
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

// handleForcePromote handles force promote logic for replicate configuration.
// It promotes a secondary cluster to standalone primary immediately without waiting for CDC replication.
func (s *assignmentServiceImpl) handleForcePromote(ctx context.Context, config *commonpb.ReplicateConfiguration) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	log.Ctx(ctx).Warn("Force promote replicate configuration requested")

	// VALIDATION 1: Force promote requires empty cluster and topology fields
	// The configuration will be constructed from the current cluster's existing meta
	if config.Clusters != nil || config.CrossClusterTopology != nil {
		return nil, status.NewInvaildArgument(
			"force promote requires empty cluster and topology fields; it promotes the cluster to primary automatically")
	}

	// Use WithSecondaryClusterResourceKey to:
	// 1. Acquire exclusive cluster-level resource key
	// 2. Verify the cluster is secondary (not primary)
	broadcaster, err := broadcast.StartBroadcastWithSecondaryClusterResourceKey(ctx)
	if err != nil {
		if errors.Is(err, broadcast.ErrNotSecondary) {
			return nil, status.NewInvaildArgument("force promote can only be used on secondary clusters, current cluster is primary")
		}
		return nil, err
	}
	defer broadcaster.Close()

	// Validate and construct force promote configuration
	forcePromoteConfig, pchannels, err := s.validateForcePromoteConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	// Config is same (already standalone primary), no-op
	if forcePromoteConfig == nil {
		return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
	}

	// Create the AlterReplicateConfigMessage with force promote flag
	controlChannel := streaming.WAL().ControlChannel()
	broadcastPChannels := lo.Map(pchannels, func(pchannel string, _ int) string {
		if funcutil.IsOnPhysicalChannel(controlChannel, pchannel) {
			return controlChannel
		}
		return pchannel
	})

	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: forcePromoteConfig,
			ForcePromote:           true, // marks as force promote
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(broadcastPChannels, message.OptBuildBroadcastAckSyncUp()). // Disable fast DDL ack
		MustBuildBroadcast()

	// Use Broadcast() to broadcast the message
	// The ACK callback will handle DDL fixing and meta saving
	_, err = broadcaster.Broadcast(ctx, msg)
	if err != nil {
		log.Ctx(ctx).Error("Failed to broadcast force promote AlterReplicateConfigMessage", zap.Error(err))
		return nil, err
	}

	log.Ctx(ctx).Info("Force promote replicate configuration broadcast completed successfully")
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

// validateForcePromoteConfiguration validates the current cluster state and constructs
// the force promote configuration. It returns the new config and the list of pchannels.
func (s *assignmentServiceImpl) validateForcePromoteConfiguration(ctx context.Context) (*commonpb.ReplicateConfiguration, []string, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	latestAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return nil, nil, err
	}

	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentConfig := latestAssignment.ReplicateConfiguration

	// Force promote requires current config to exist (secondary must have been configured)
	if currentConfig == nil {
		return nil, nil, status.NewInvaildArgument("force promote requires existing replicate configuration; current cluster has no configuration")
	}

	// Get the current cluster from existing config directly (don't construct a new one)
	var currentCluster *commonpb.MilvusCluster
	for _, cluster := range currentConfig.GetClusters() {
		if cluster.GetClusterId() == currentClusterID {
			currentCluster = cluster
			break
		}
	}
	if currentCluster == nil {
		return nil, nil, status.NewInvaildArgument("force promote requires current cluster in existing configuration; cluster %s not found in config", currentClusterID)
	}

	// Get pchannels from PChannelView for validation
	pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, ch *channel.PChannelMeta) string {
		return ch.Name()
	})
	// Sort pchannels for consistent ordering (map iteration order is randomized)
	sort.Strings(pchannels)

	// Construct the standalone primary configuration using the current cluster from existing config
	// This configuration makes the current cluster a standalone primary with no replication
	forcePromoteConfig := &commonpb.ReplicateConfiguration{
		Clusters:             []*commonpb.MilvusCluster{currentCluster},
		CrossClusterTopology: nil, // No topology means standalone primary
	}

	// Double check if configuration is same (already standalone primary)
	if proto.Equal(forcePromoteConfig, currentConfig) {
		log.Ctx(ctx).Info("configuration is same in force promote, ignored")
		return nil, nil, nil
	}

	// Validate the configuration using existing validator
	validator := replicateutil.NewReplicateConfigValidator(forcePromoteConfig, currentConfig, currentClusterID, pchannels)
	if err := validator.Validate(); err != nil {
		log.Ctx(ctx).Warn("Force promote replicate configuration validation failed", zap.Error(err))
		return nil, nil, err
	}

	return forcePromoteConfig, pchannels, nil
}

// alterReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) alterReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	header := result.Message.Header()

	// Check ignore field first - skip all processing if true
	// This is used for incomplete switchover messages that should be ignored after force promote
	if header.Ignore {
		log.Ctx(ctx).Info("AlterReplicateConfig message has ignore flag set, skipping processing",
			zap.Bool("forcePromote", header.ForcePromote))
		return nil
	}

	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}

	// Update the configuration
	// For force promote, incomplete broadcasts are already fixed by ackCallbackScheduler
	// before this callback is invoked.
	return balancer.UpdateReplicateConfiguration(ctx, result)
}

// UpdateWALBalancePolicy is used to update the WAL balance policy.
func (s *assignmentServiceImpl) UpdateWALBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return balancer.UpdateBalancePolicy(ctx, req)
}
