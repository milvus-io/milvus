package service

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
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

// UpdateWALBalancePolicy is used to update the WAL balance policy.
func (s *assignmentServiceImpl) UpdateWALBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error) {
	balancer, err := s.balancer.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	if err = balancer.UpdateBalancePolicy(ctx, req); err != nil {
		return nil, err
	}
	return &streamingpb.UpdateWALBalancePolicyResponse{}, nil
}
