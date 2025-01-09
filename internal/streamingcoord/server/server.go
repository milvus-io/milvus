package server

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy" // register the balancer policy
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// Server is the streamingcoord server.
type Server struct {
	logger *log.MLogger

	// session of current server.
	session sessionutil.SessionInterface

	// service level variables.
	assignmentService service.AssignmentService
	broadcastService  service.BroadcastService

	// basic component variables can be used at service level.
	balancer    *syncutil.Future[balancer.Balancer]
	broadcaster *syncutil.Future[broadcaster.Broadcaster]
}

// Init initializes the streamingcoord server.
func (s *Server) Start(ctx context.Context) (err error) {
	s.logger.Info("init streamingcoord...")
	if err := s.initBasicComponent(ctx); err != nil {
		s.logger.Warn("init basic component of streamingcoord failed", zap.Error(err))
		return err
	}
	// Init all grpc service of streamingcoord server.
	s.logger.Info("streamingcoord initialized")
	return nil
}

// initBasicComponent initialize all underlying dependency for streamingcoord.
func (s *Server) initBasicComponent(ctx context.Context) (err error) {
	futures := make([]*conc.Future[struct{}], 0)
	if streamingutil.IsStreamingServiceEnabled() {
		futures = append(futures, conc.Go(func() (struct{}, error) {
			s.logger.Info("start recovery balancer...")
			// Read new incoming topics from configuration, and register it into balancer.
			newIncomingTopics := util.GetAllTopicsFromConfiguration()
			balancer, err := balancer.RecoverBalancer(ctx, "pchannel_count_fair", newIncomingTopics.Collect()...)
			if err != nil {
				s.logger.Warn("recover balancer failed", zap.Error(err))
				return struct{}{}, err
			}
			s.balancer.Set(balancer)
			s.logger.Info("recover balancer done")
			return struct{}{}, nil
		}))
	}
	// The broadcaster of msgstream is implemented on current streamingcoord to reduce the development complexity.
	// So we need to recover it.
	futures = append(futures, conc.Go(func() (struct{}, error) {
		s.logger.Info("start recovery broadcaster...")
		broadcaster, err := broadcaster.RecoverBroadcaster(ctx, broadcaster.NewAppendOperator())
		if err != nil {
			s.logger.Warn("recover broadcaster failed", zap.Error(err))
			return struct{}{}, err
		}
		s.broadcaster.Set(broadcaster)
		s.logger.Info("recover broadcaster done")
		return struct{}{}, nil
	}))
	return conc.AwaitAll(futures...)
}

// RegisterGRPCService register all grpc service to grpc server.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	if streamingutil.IsStreamingServiceEnabled() {
		streamingpb.RegisterStreamingCoordAssignmentServiceServer(grpcServer, s.assignmentService)
	}
	streamingpb.RegisterStreamingCoordBroadcastServiceServer(grpcServer, s.broadcastService)
}

// Close closes the streamingcoord server.
func (s *Server) Stop() {
	if s.balancer.Ready() {
		s.logger.Info("start close balancer...")
		s.balancer.Get().Close()
	} else {
		s.logger.Info("balancer not ready, skip close")
	}
	if s.broadcaster.Ready() {
		s.logger.Info("start close broadcaster...")
		s.broadcaster.Get().Close()
	} else {
		s.logger.Info("broadcaster not ready, skip close")
	}
	s.logger.Info("streamingcoord server stopped")
}
