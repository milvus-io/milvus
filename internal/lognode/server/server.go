package server

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/pipeline"
	"github.com/milvus-io/milvus/internal/lognode/server/service"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/conc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// LogNode is the lognode server.
type LogNode struct {
	ctx    context.Context
	cancel context.CancelFunc

	// session of current server.
	session *sessionutil.Session

	// basic component variables managed by external.
	rc         types.RootCoordClient
	etcdClient *clientv3.Client
	grpcServer *grpc.Server

	// service level instances.
	handlerService service.HandlerService
	managerService service.ManagerService

	// basic component instances.
	walManager            walmanager.Manager
	componentStateService *componentutil.ComponentStateService // state.

	factory         dependency.Factory
	allocator       timestamp.Allocator
	pipelineManager pipeline.Manager
	dispClient      msgdispatcher.Client

	pool *conc.Pool[any]
}

// Init initializes the lognode server.
func (s *LogNode) Init(ctx context.Context) (err error) {
	log.Info("init lognode server...")
	s.componentStateService.OnInitializing()
	s.allocator = timestamp.NewAllocator(s.rc)
	// init all basic components.
	s.initBasicComponent(ctx)

	// init all service.
	s.initService(ctx)
	log.Info("lognode server initialized")
	s.componentStateService.OnInitialized(s.session.ServerID)
	return nil
}

// Start starts the lognode server.
func (s *LogNode) Start() {
	log.Info("start lognode server")

	log.Info("lognode service started")
}

// Stop stops the lognode server.
func (s *LogNode) Stop() {
	log.Info("stopping lognode server...")
	s.componentStateService.OnStopping()
	log.Info("close wal manager...")
	s.walManager.Close()
	log.Info("lognode server stopped")
}

// Health returns the health status of the lognode server.
func (s *LogNode) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentStateService.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.State.StateCode
}

// initBasicComponent initialize all underlying dependency for lognode.
func (s *LogNode) initBasicComponent(ctx context.Context) {
	var err error
	s.walManager, err = walmanager.OpenManager(
		&walmanager.OpenOption{
			InterceptorBuilders: []wal.InterceptorBuilder{
				timetick.NewInterceptorBuilder(s.allocator),
			},
		},
	)
	if err != nil {
		panic("open wal manager failed")
	}
}

// initService initializes the grpc service.
func (s *LogNode) initService(ctx context.Context) {
	s.handlerService = service.NewHandlerService(s.walManager)
	s.managerService = service.NewManagerService(s.walManager)
	s.registerGRPCService(s.grpcServer)
}

// registerGRPCService register all grpc service to grpc server.
func (s *LogNode) registerGRPCService(grpcServer *grpc.Server) {
	logpb.RegisterLogNodeHandlerServiceServer(grpcServer, s.handlerService)
	logpb.RegisterLogNodeManagerServiceServer(grpcServer, s.managerService)
	logpb.RegisterLogNodeStateServiceServer(grpcServer, s.componentStateService)
}
