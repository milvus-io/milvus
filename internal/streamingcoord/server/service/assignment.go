package service

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

// NewAssignmentService returns a new assignment service.
func NewAssignmentService() streamingpb.StreamingCoordAssignmentServiceServer {
	assignmentService := &assignmentServiceImpl{
		listenerTotal: metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()),
	}
	// TODO: after recovering from wal, add it to here.
	// registry.RegisterAlterReplicateConfigV2AckCallback(assignmentService.AlterReplicateConfiguration)
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

	// TODO: after recovering from wal, do a broadcast operation here.
	msg, err := s.validateReplicateConfiguration(ctx, config)
	if err != nil {
		return nil, err
	}

	// TODO: After recovering from wal, we can get the immutable message from wal system.
	// Now, we just mock the immutable message here.
	mutableMsg := msg.SplitIntoMutableMessage()
	mockMessages := make([]message.ImmutableAlterReplicateConfigMessageV2, 0)
	for _, msg := range mutableMsg {
		mockMessages = append(mockMessages,
			message.MustAsImmutableAlterReplicateConfigMessageV2(msg.WithTimeTick(0).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(1))),
		)
	}

	// TODO: After recovering from wal, remove the operation here.
	if err := s.AlterReplicateConfiguration(ctx, mockMessages...); err != nil {
		return nil, err
	}
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
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
	pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, channel *channel.PChannelMeta) string {
		return channel.Name()
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
		WithBroadcast(pchannels).
		MustBuildBroadcast()

	// TODO: After recovering from wal, remove the operation here.
	b.WithBroadcastID(1)
	return b, nil
}

// AlterReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) AlterReplicateConfiguration(ctx context.Context, msgs ...message.ImmutableAlterReplicateConfigMessageV2) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	return balancer.UpdateReplicateConfiguration(ctx, msgs...)
}

// UpdateWALBalancePolicy is used to update the WAL balance policy.
func (s *assignmentServiceImpl) UpdateWALBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return balancer.UpdateBalancePolicy(ctx, req)
}
