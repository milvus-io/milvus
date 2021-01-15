package grpcindexservice

import (
	"github.com/zilliztech/milvus-distributed/internal/indexservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"google.golang.org/grpc"
)

type Server struct {
	server indexservice.Interface

	grpcServer *grpc.Server
}

func (g Server) Init() {
	panic("implement me")
}

func (g Server) Start() {
	panic("implement me")
}

func (g Server) Stop() {
	panic("implement me")
}

func (g Server) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (g Server) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (g Server) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (g Server) RegisterNode(req indexpb.RegisterNodeRequest) (indexpb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (g Server) BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error) {
	panic("implement me")
}

func (g Server) GetIndexStates(req indexpb.IndexStatesRequest) (indexpb.IndexStatesResponse, error) {
	panic("implement me")
}

func (g Server) GetIndexFilePaths(req indexpb.IndexFilePathRequest) (indexpb.IndexFilePathsResponse, error) {
	panic("implement me")
}

//varindex

func NewServer() *Server {
	return &Server{
		server: &indexservice.IndexService{},
	}
}
