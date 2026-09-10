package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy" // register the balancer policy
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

// Server is the streamingcoord server.
type Server struct {
	logger *mlog.Logger

	// session of current server.
	session sessionutil.SessionInterface

	// service level variables.
	assignmentService service.AssignmentService
	broadcastService  service.BroadcastService
}

// Init initializes the streamingcoord server.
func (s *Server) Start(ctx context.Context, checker balancer.FileResourceChecker) (err error) {
	s.logger.Info(ctx, "init streamingcoord...")
	if err := s.initBasicComponent(ctx); err != nil {
		s.logger.Warn(ctx, "init basic component of streamingcoord failed", mlog.Err(err))
		return err
	}
	balance.SetFileResourceChecker(checker)
	// Init all grpc service of streamingcoord server.
	s.logger.Info(ctx, "streamingcoord initialized")
	return nil
}

// initBasicComponent initialize all underlying dependency for streamingcoord.
func (s *Server) initBasicComponent(ctx context.Context) (err error) {
	futures := make([]*conc.Future[struct{}], 0)
	futures = append(futures, conc.Go(func() (struct{}, error) {
		s.logger.Info(ctx, "start recovery balancer...")
		// Create a provider that reads channel names from configuration
		// and collection metadata recovered by RootCoord.
		provider := util.NewConfigChannelProviderWithPChannelStatsManager(channel.StaticPChannelStatsManager)
		balancer, err := balancer.RecoverBalancer(ctx, provider)
		if err != nil {
			provider.Close()
			s.logger.Warn(ctx, "recover balancer failed", mlog.Err(err))
			return struct{}{}, err
		}
		balance.Register(balancer)
		s.logger.Info(ctx, "recover balancer done")
		return struct{}{}, nil
	}))
	// The broadcaster of msgstream is implemented on current streamingcoord to reduce the development complexity.
	// So we need to recover it.
	futures = append(futures, conc.Go(func() (struct{}, error) {
		s.logger.Info(ctx, "start recovery broadcaster...")
		broadcaster, err := broadcaster.RecoverBroadcaster(ctx)
		if err != nil {
			s.logger.Warn(ctx, "recover broadcaster failed", mlog.Err(err))
			return struct{}{}, err
		}
		broadcast.Register(broadcaster)
		s.logger.Info(ctx, "recover broadcaster done")
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
	s.logger.Info(context.TODO(), "start close balancer...")
	balance.Release()
	s.logger.Info(context.TODO(), "start close broadcaster...")
	broadcast.Release()
	s.logger.Info(context.TODO(), "release streamingcoord resource...")
	resource.Release()
	s.logger.Info(context.TODO(), "streamingcoord server stopped")
}
