package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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

	log.Ctx(ctx).Info("UpdateReplicateConfiguration received", replicateutil.ConfigLogFields(config)...)

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

	cc := channel.GetClusterChannels(channel.OptIncludeUnavailableInReplication())

	// validate the configuration itself
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentConfig := latestAssignment.ReplicateConfiguration
	incomingConfig := config
	validator := replicateutil.NewReplicateConfigValidator(incomingConfig, currentConfig, currentClusterID, cc.Channels)
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
			IsPchannelIncreasing:   validator.IsPChannelIncreasing(),
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithClusterLevelBroadcast(cc).
		MustBuildBroadcast()
	return b, nil
}

// alterReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) alterReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
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
