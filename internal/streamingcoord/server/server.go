package server

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy" // register the balancer policy
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
)

// Server is the streamingcoord server.
type Server struct {
	logger *log.MLogger

	// session of current server.
	session sessionutil.SessionInterface

	// service level variables.
	assignmentService service.AssignmentService
	broadcastService  service.BroadcastService
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
	futures = append(futures, conc.Go(func() (struct{}, error) {
		s.logger.Info("start recovery balancer...")
		// Read new incoming topics from configuration, and register it into balancer.
		newIncomingTopics := util.GetAllTopicsFromConfiguration()
		balancer, err := balancer.RecoverBalancer(ctx, newIncomingTopics.Collect()...)
		if err != nil {
			s.logger.Warn("recover balancer failed", zap.Error(err))
			return struct{}{}, err
		}
		balance.Register(balancer)
		s.logger.Info("recover balancer done")
		return struct{}{}, nil
	}))
	// The broadcaster of msgstream is implemented on current streamingcoord to reduce the development complexity.
	// So we need to recover it.
	futures = append(futures, conc.Go(func() (struct{}, error) {
		s.logger.Info("start recovery broadcaster...")
		broadcaster, err := broadcaster.RecoverBroadcaster(ctx)
		if err != nil {
			s.logger.Warn("recover broadcaster failed", zap.Error(err))
			return struct{}{}, err
		}
		broadcast.Register(broadcaster)
		s.logger.Info("recover broadcaster done")
		return struct{}{}, nil
	}))
	return conc.AwaitAll(futures...)
}

// RegisterGRPCService register all grpc service to grpc server.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	streamingpb.RegisterStreamingCoordAssignmentServiceServer(grpcServer, s.assignmentService)
	streamingpb.RegisterStreamingCoordBroadcastServiceServer(grpcServer, s.broadcastService)
}

// Close closes the streamingcoord server.
func (s *Server) Stop() {
	s.logger.Info("start close balancer...")
	balance.Release()
	s.logger.Info("start close broadcaster...")
	broadcast.Release()
	s.logger.Info("release streamingcoord resource...")
	resource.Release()
	s.logger.Info("streamingcoord server stopped")
}
