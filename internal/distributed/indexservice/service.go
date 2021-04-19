package grpcindexservice

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Server struct {
	indexservice *indexservice.IndexService

	grpcServer  *grpc.Server
	grpcErrChan chan error

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	closer io.Closer
}

func (s *Server) Run() error {

	if err := s.init(); err != nil {
		return err
	}

	if err := s.start(); err != nil {
		return err
	}
	return nil
}

func (s *Server) init() error {
	Params.Init()
	indexservice.Params.Init()

	s.loopWg.Add(1)
	go s.startGrpcLoop(Params.ServicePort)
	// wait for grpc indexservice loop start
	if err := <-s.grpcErrChan; err != nil {
		return err
	}
	s.indexservice.UpdateStateCode(internalpb2.StateCode_INITIALIZING)

	if err := s.indexservice.Init(); err != nil {
		return err
	}
	return nil
}

func (s *Server) start() error {
	if err := s.indexservice.Start(); err != nil {
		return err
	}
	log.Println("indexService started")
	return nil
}

func (s *Server) Stop() error {
	if err := s.closer.Close(); err != nil {
		return err
	}
	if s.indexservice != nil {
		s.indexservice.Stop()
	}

	s.loopCancel()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	s.loopWg.Wait()
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	return s.indexservice.RegisterNode(ctx, req)
}

func (s *Server) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	return s.indexservice.BuildIndex(ctx, req)
}

func (s *Server) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	return s.indexservice.GetIndexStates(ctx, req)
}

func (s *Server) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.indexservice.DropIndex(ctx, request)
}

func (s *Server) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	return s.indexservice.GetIndexFilePaths(ctx, req)
}

func (s *Server) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	return s.indexservice.NotifyBuildIndex(ctx, nty)
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
	indexpb.RegisterIndexServiceServer(s.grpcServer, s)

	go funcutil.CheckGrpcReady(ctx, s.grpcErrChan)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErrChan <- err
	}

}

func (s *Server) GetComponentStates(ctx context.Context, empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return s.indexservice.GetComponentStates(ctx)
}

func (s *Server) GetTimeTickChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.indexservice.GetTimeTickChannel(ctx)
}

func (s *Server) GetStatisticsChannel(ctx context.Context, empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return s.indexservice.GetStatisticsChannel(ctx)
}

func NewServer(ctx context.Context) (*Server, error) {

	ctx1, cancel := context.WithCancel(ctx)
	serverImp, err := indexservice.NewIndexService(ctx)
	if err != nil {
		defer cancel()
		return nil, err
	}
	s := &Server{
		loopCtx:      ctx1,
		loopCancel:   cancel,
		indexservice: serverImp,
		grpcErrChan:  make(chan error),
	}

	cfg := &config.Configuration{
		ServiceName: "index_service",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(tracer)
	s.closer = closer

	return s, nil
}
