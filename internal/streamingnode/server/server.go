package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	_ "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/kafka"
	_ "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	_ "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Server is the streamingnode server.
type Server struct {
	// session of current server.
	session    *sessionutil.Session
	grpcServer *grpc.Server

	// service level instances.
	handlerService service.HandlerService
	managerService service.ManagerService

	// basic component instances.
	walManager walmanager.Manager

	// Event watching
	watchCtx    context.Context
	watchCancel context.CancelFunc
}

// Init initializes the streamingnode server.
func (s *Server) init() {
	log.Info("init streamingnode server...")
	// init all basic components.
	s.initBasicComponent()

	// init all service.
	s.initService()
	log.Info("streamingnode server initialized")

	// init storage v2 file system.
	if err := initcore.InitStorageV2FileSystem(paramtable.Get()); err != nil {
		panic(fmt.Sprintf("unrecoverable error happens at init storage v2 file system, %+v", err))
	}

	// init paramtable change callback for core related config
	initcore.SetupCoreConfigChangelCallback()
	if hookutil.IsClusterEncyptionEnabled() {
		message.RegisterCipher(hookutil.GetCipher())
	}
}

// StartWatching starts watching events from MixCoord
func (s *Server) StartWatching() {
	s.watchCtx, s.watchCancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-s.watchCtx.Done():
				return
			default:
				if err := s.watchEvents(); err != nil {
					log.Ctx(s.watchCtx).Warn("Event watching failed, retrying...", zap.Error(err))
					continue
				}
			}
		}
	}()
}

func (s *Server) watchEvents() error {
	mixCoord := resource.Resource().MixCoordClient().Get()
	if mixCoord == nil {
		return fmt.Errorf("mixCoord client not ready")
	}

	ctx := s.watchCtx
	stream, err := mixCoord.Watch(ctx, &datapb.WatchRequest{
		EventType: datapb.EventType_EventType_PrimaryKeyIndexBuilt,
	})
	if err != nil {
		return err
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		log.Ctx(ctx).Info("Received event from MixCoord",
			zap.String("type", event.EventType.String()))

		switch event.EventType {
		case datapb.EventType_EventType_PrimaryKeyIndexBuilt:
			s.handlePrimaryKeyIndexBuilt(event.EventData)
		}
	}
}

func (s *Server) handlePrimaryKeyIndexBuilt(data []byte) {
	var eventData datapb.PrimaryKeyIndexBuiltData
	if err := proto.Unmarshal(data, &eventData); err != nil {
		log.Ctx(s.watchCtx).Error("failed to unmarshal primary key index built event data", zap.Error(err))
		return
	}

	resource.Resource().PrimaryIndexManager().LoadSealedIndex(&eventData)
}

// Stop stops the streamingnode server.
func (s *Server) Stop() {
	log.Info("stopping streamingnode server...")
	log.Info("close wal manager...")
	s.walManager.Close()
	log.Info("release streamingnode resources...")
	resource.Release()
	log.Info("streamingnode server stopped")
	if s.watchCancel != nil {
		s.watchCancel()
	}
}

// initBasicComponent initialize all underlying dependency for streamingnode.
func (s *Server) initBasicComponent() {
	var err error
	s.walManager, err = walmanager.OpenManager()
	if err != nil {
		panic(fmt.Sprintf("open wal manager failed, %+v", err))
	}
	// Register the wal manager to the local registry.
	registry.RegisterLocalWALManager(s.walManager)
}

// initService initializes the grpc service.
func (s *Server) initService() {
	s.handlerService = service.NewHandlerService(s.walManager)
	s.managerService = service.NewManagerService(s.walManager)
	s.registerGRPCService(s.grpcServer)
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) registerGRPCService(grpcServer *grpc.Server) {
	streamingpb.RegisterStreamingNodeHandlerServiceServer(grpcServer, s.handlerService)
	streamingpb.RegisterStreamingNodeManagerServiceServer(grpcServer, s.managerService)
}
