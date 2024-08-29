package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	_ "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	_ "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

// Server is the streamingnode server.
type Server struct {
	// session of current server.
	session    *sessionutil.Session
	grpcServer *grpc.Server

	// service level instances.
	handlerService        service.HandlerService
	managerService        service.ManagerService
	componentStateService *componentutil.ComponentStateService // state.

	// basic component instances.
	walManager walmanager.Manager
}

// Init initializes the streamingnode server.
func (s *Server) Init(ctx context.Context) (err error) {
	log.Info("init streamingnode server...")
	s.componentStateService.OnInitializing()
	// init all basic components.
	s.initBasicComponent(ctx)

	// init all service.
	s.initService(ctx)
	log.Info("streamingnode server initialized")
	s.componentStateService.OnInitialized(s.session.ServerID)
	return nil
}

// Start starts the streamingnode server.
func (s *Server) Start() {
	resource.Resource().Flusher().Start()
	log.Info("flusher started")
}

// Stop stops the streamingnode server.
func (s *Server) Stop() {
	log.Info("stopping streamingnode server...")
	s.componentStateService.OnStopping()
	log.Info("close wal manager...")
	s.walManager.Close()
	log.Info("streamingnode server stopped")
	log.Info("stopping flusher...")
	resource.Resource().Flusher().Stop()
	log.Info("flusher stopped")
}

// Health returns the health status of the streamingnode server.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentStateService.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.State.StateCode
}

// initBasicComponent initialize all underlying dependency for streamingnode.
func (s *Server) initBasicComponent(_ context.Context) {
	var err error
	s.walManager, err = walmanager.OpenManager()
	if err != nil {
		panic("open wal manager failed")
	}
}

// initService initializes the grpc service.
func (s *Server) initService(_ context.Context) {
	s.handlerService = service.NewHandlerService(s.walManager)
	s.managerService = service.NewManagerService(s.walManager)
	s.registerGRPCService(s.grpcServer)
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) registerGRPCService(grpcServer *grpc.Server) {
	streamingpb.RegisterStreamingNodeHandlerServiceServer(grpcServer, s.handlerService)
	streamingpb.RegisterStreamingNodeManagerServiceServer(grpcServer, s.managerService)
	streamingpb.RegisterStreamingNodeStateServiceServer(grpcServer, s.componentStateService)
}
