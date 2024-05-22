package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/logcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/logcoord/server/collector"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ logpb.LogCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

// NewAssignmentService returns a new assignment service.
func NewAssignmentService(
	balancer balancer.Balancer,
	collector *collector.Collector,
) logpb.LogCoordAssignmentServiceServer {
	return &assignmentServiceImpl{
		balancer:  balancer,
		collector: collector,
	}
}

type AssignmentService interface {
	logpb.LogCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	balancer  balancer.Balancer
	collector *collector.Collector
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(request *logpb.AssignmentDiscoverRequest, streamServer logpb.LogCoordAssignmentService_AssignmentDiscoverServer) error {
	return s.watch(streamServer.Context(), func(state *logpb.AssignmentDiscoverResponse) error {
		return streamServer.Send(state)
	})
}

// ReportLogError reports an error to the assignment manager.
func (s *assignmentServiceImpl) ReportLogError(ctx context.Context, request *logpb.ReportLogErrorRequest) (*logpb.ReportLogErrorResponse, error) {
	// Trigger a collector immediately.
	s.collector.Trigger()
	return &logpb.ReportLogErrorResponse{}, nil
}

// watch watches the state of all log nodes.
// The callback will be called when the state changes.
// Watch exits when the context is done or callback returns error.
func (s *assignmentServiceImpl) watch(ctx context.Context, cb func(state *logpb.AssignmentDiscoverResponse) error) error {
	metrics.LogCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	defer metrics.LogCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()).Dec()

	return s.balancer.WatchBalanceResult(ctx, func(v *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error {
		state := &logpb.AssignmentDiscoverResponse{
			Version: &logpb.VersionPair{
				Global: v.Global,
				Local:  v.Local,
			},
			Addresses: make([]*logpb.LogNodeAssignment, 0, len(nodeStatus)),
		}
		for _, node := range nodeStatus {
			channels := make([]*logpb.PChannelInfo, 0, len(node.Channels))
			for _, ch := range node.Channels {
				channels = append(channels, ch)
			}
			state.Addresses = append(state.Addresses, &logpb.LogNodeAssignment{
				ServerID: node.ServerID,
				Address:  node.Address,
				Channels: channels,
			})
		}
		return cb(state)
	})
}
