package server

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy" // register the balancer policy
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// Server is the streamingcoord server.
type Server struct {
	// session of current server.
	session sessionutil.SessionInterface

	// service level variables.
	assignmentService     service.AssignmentService
	componentStateService *componentutil.ComponentStateService // state.

	// basic component variables can be used at service level.
	balancer *syncutil.Future[balancer.Balancer]
}

// Init initializes the streamingcoord server.
func (s *Server) Init(ctx context.Context) (err error) {
	log.Info("init streamingcoord server...")

	// Init all underlying component of streamingcoord server.
	if err := s.initBasicComponent(ctx); err != nil {
		log.Error("init basic component of streamingcoord server failed", zap.Error(err))
		return err
	}
	// Init all grpc service of streamingcoord server.
	s.componentStateService.OnInitialized(s.session.GetServerID())
	log.Info("streamingcoord server initialized")
	return nil
}

// initBasicComponent initialize all underlying dependency for streamingcoord.
func (s *Server) initBasicComponent(ctx context.Context) error {
	// Init balancer
	var err error
	// Read new incoming topics from configuration, and register it into balancer.
	newIncomingTopics := util.GetAllTopicsFromConfiguration()
	balancer, err := balancer.RecoverBalancer(ctx, "pchannel_count_fair", newIncomingTopics.Collect()...)
	if err != nil {
		return err
	}
	s.balancer.Set(balancer)
	return err
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	streamingpb.RegisterStreamingCoordAssignmentServiceServer(grpcServer, s.assignmentService)
	streamingpb.RegisterStreamingCoordStateServiceServer(grpcServer, s.componentStateService)
}

// Start starts the streamingcoord server.
func (s *Server) Start() {
	// Just do nothing now.
	log.Info("start streamingcoord server")
}

// Stop stops the streamingcoord server.
func (s *Server) Stop() {
	s.componentStateService.OnStopping()
	log.Info("close balancer...")
	s.balancer.Get().Close()
	log.Info("streamingcoord server stopped")
}
