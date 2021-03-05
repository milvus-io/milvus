package grpcindexnode

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	grpcindexserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"google.golang.org/grpc"
)

type Server struct {
	impl *indexnode.IndexNode

	grpcServer  *grpc.Server
	grpcErrChan chan error

	indexServiceClient *grpcindexserviceclient.Client
	loopCtx            context.Context
	loopCancel         func()
	loopWg             sync.WaitGroup
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return nil
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) startGrpcLoop(grpcPort int) {

	defer s.loopWg.Done()

	log.Println("network port: ", grpcPort)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Printf("GrpcServer:failed to listen: %v", err)
		s.grpcErrChan <- err
		return
	}

	ctx, cancel := context.WithCancel(s.loopCtx)
	defer cancel()

	tracer := opentracing.GlobalTracer()
	s.grpcServer = grpc.NewServer(grpc.UnaryInterceptor(
		otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(
			otgrpc.OpenTracingStreamServerInterceptor(tracer)))
	indexpb.RegisterIndexNodeServer(s.grpcServer, s)
	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) init() error {
	var err error
	Params.Init()
	if !funcutil.CheckPortAvailable(Params.Port) {
		Params.Port = funcutil.GetAvailablePort()
	}
	Params.LoadFromEnv()
	Params.LoadFromArgs()

	Params.Address = Params.IP + ":" + strconv.FormatInt(int64(Params.Port), 10)

	defer func() {
		if err != nil {
			err = s.Stop()
			if err != nil {
				log.Println("Init failed, and Stop failed")
			}
		}
	}()

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.Port)
	// wait for grpc server loop start
	err = <-s.grpcErrChan
	if err != nil {
		return err
	}

	indexServiceAddr := Params.IndexServerAddress
	s.indexServiceClient = grpcindexserviceclient.NewClient(indexServiceAddr)
	err = s.indexServiceClient.Init()
	if err != nil {
		return err
	}
	s.impl.SetIndexServiceClient(s.indexServiceClient)

	indexnode.Params.Init()
	indexnode.Params.Port = Params.Port
	indexnode.Params.IP = Params.IP
	indexnode.Params.Address = Params.Address

	s.impl.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	err = s.impl.Init()
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) start() error {
	err := s.impl.Start()
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Stop() error {
	s.loopCancel()
	if s.impl != nil {
		s.impl.Stop()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.loopWg.Wait()

	return nil
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	return s.impl.BuildIndex(ctx, req)
}

func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.impl.DropIndex(ctx, request)
}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.impl.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.impl.GetStatisticsChannel(ctx)
}

func NewServer(ctx context.Context) (*Server, error) {
	ctx1, cancel := context.WithCancel(ctx)
	node, err := indexnode.NewIndexNode(ctx1)
	if err != nil {
		defer cancel()
		return nil, err
	}

	return &Server{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		impl:        node,
		grpcErrChan: make(chan error),
	}, nil
}
