package server

import (
	"context"

	"github.com/milvus-io/milvus/internal/logcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/logcoord/server/collector"
	"github.com/milvus-io/milvus/internal/logcoord/server/service"
	"github.com/milvus-io/milvus/internal/lognode/client/manager"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the logcoord server.
type Server struct {
	// session of current server.
	session sessionutil.SessionInterface

	// basic component variables managed by external.
	etcdClient *clientv3.Client
	catalog    metastore.LogCoordCataLog

	// service level variables.
	channelService    service.ChannelService
	assignmentService service.AssignmentService

	// basic component variables can be used at service level.
	channelMeta            channel.Meta
	balancer               balancer.Balancer
	logNodeManager         manager.ManagerClient                // manage the lognode client.
	logNodeStatusCollector *collector.Collector                 // collect lognode status.
	componentStateService  *componentutil.ComponentStateService // state.
}

// NewLogCoord creates a new logcoord server.
func NewLogCoord() *Server {
	return &Server{
		componentStateService: componentutil.NewComponentStateService(typeutil.LogCoordRole),
	}
}

// Init initializes the logcoord server.
func (s *Server) Init(ctx context.Context) (err error) {
	log.Info("init logcoord server...")
	s.componentStateService.OnInitializing()

	// Init all underlying component of logcoord server.
	if err := s.initBasicComponent(ctx); err != nil {
		log.Error("init basic component of logcoord server failed", zap.Error(err))
		return err
	}
	// Init all grpc service of logcoord server.
	s.initService(ctx)
	s.componentStateService.OnInitialized(s.session.GetServerID())
	log.Info("logcoord server initialized")
	return nil
}

// initBasicComponent initialize all underlying dependency for logcoord.
func (s *Server) initBasicComponent(ctx context.Context) error {
	// Create layout.
	// 1. Get PChannel Meta from catalog.
	log.Info("init channel meta...")
	channelDatas, err := s.catalog.ListPChannel(ctx)
	if err != nil {
		return status.NewInner("fail to list all PChannel info, %s", err.Error())
	}
	channels := make(map[string]channel.PhysicalChannel, len(channelDatas))
	for name, data := range channelDatas {
		channels[name] = channel.NewPhysicalChannel(s.catalog, data)
	}
	s.channelMeta = channel.NewMeta(s.catalog, channels)
	log.Info("init channel meta done")

	// 2. Connect to log node manager to manage and query status of underlying log nodes.
	log.Info("dial to log node manager...")
	s.logNodeManager = manager.DialContext(ctx, s.etcdClient)
	log.Info("dial to log node manager done")

	// 3. Create layout of cluster.
	// Fetch copy of pChannels from meta storage.
	pChannels := s.channelMeta.GetPChannels(ctx)
	// Fetch node status from log nodes.
	log.Info("fetch log node status...")
	nodeStatus, err := s.logNodeManager.CollectAllStatus(ctx)
	if err != nil {
		return status.NewInner("collect all lognode status failed, %s", err.Error())
	}
	layout := layout.NewLayout(pChannels, nodeStatus)
	log.Info("fetch log node status done")

	// 4. Create Balancer.
	s.balancer = balancer.NewBalancer(balancer.NewChannelCountFairPolicy(), layout, s.logNodeManager)

	// 5. Create a node status collector.
	s.logNodeStatusCollector = collector.NewCollector(s.logNodeManager, s.balancer)
	return nil
}

// initService initializes the grpc service.
func (s *Server) initService(ctx context.Context) {
	s.channelService = service.NewChannelService(s.channelMeta, s.balancer)
	s.assignmentService = service.NewAssignmentService(s.balancer, s.logNodeStatusCollector)
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	logpb.RegisterLogCoordAssignmentServiceServer(grpcServer, s.assignmentService)
	logpb.RegisterLogCoordChannelServiceServer(grpcServer, s.channelService)
	logpb.RegisterLogCoordStateServiceServer(grpcServer, s.componentStateService)
}

// Start starts the logcoord server.
func (s *Server) Start() {
	log.Info("start logcoord server")
	s.logNodeStatusCollector.Start()
	log.Info("logcoord service started")
}

// Stop stops the logcoord server.
func (s *Server) Stop() {
	s.logNodeStatusCollector.Stop()
	log.Info("stopping logcoord server...")
	s.componentStateService.OnStopping()
	log.Info("close balancer...")
	s.balancer.Close()
	log.Info("close log node manager client...")
	s.logNodeManager.Close()
	log.Info("close log node grpc conn...")
	log.Info("logcoord server stopped")
}
