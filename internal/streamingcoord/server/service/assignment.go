package service

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
	"github.com/milvus-io/milvus/internal/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

// NewAssignmentService returns a new assignment service.
func NewAssignmentService(
	balancer *syncutil.Future[balancer.Balancer],
) streamingpb.StreamingCoordAssignmentServiceServer {
	return &assignmentServiceImpl{
		balancer:      balancer,
		listenerTotal: metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()),
	}
}

type AssignmentService interface {
	streamingpb.StreamingCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	balancer      *syncutil.Future[balancer.Balancer]
	listenerTotal prometheus.Gauge
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(server streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer) error {
	s.listenerTotal.Inc()
	defer s.listenerTotal.Dec()

	balancer, err := s.balancer.GetWithContext(server.Context())
	if err != nil {
		return err
	}
	return discover.NewAssignmentDiscoverServer(balancer, server).Execute()
}

// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
func (s *assignmentServiceImpl) UpdateReplicateConfiguration(ctx context.Context, req *milvuspb.UpdateReplicateConfigurationRequest) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	config := req.GetReplicateConfiguration()
	log.Ctx(ctx).Info("UpdateReplicateConfiguration received", replicateutil.ConfigLogFields(config)...)
	validator := NewReplicateConfigValidator(config)
	if err := validator.Validate(); err != nil {
		log.Ctx(ctx).Warn("UpdateReplicateConfiguration fail", zap.Error(err))
		return nil, err
	}
	err := resource.Resource().StreamingCatalog().SaveReplicateConfiguration(ctx, config)
	if err != nil {
		return nil, err
	}
	log.Ctx(ctx).Info("UpdateReplicateConfiguration success", replicateutil.ConfigLogFields(config)...)
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}
