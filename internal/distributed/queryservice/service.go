package queryservice

import (
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	queryServiceImpl "github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type Server struct {
	grpcServer   *grpc.Server
	queryService queryServiceImpl.Interface
}

func (s *Server) Init() {
	panic("implement me")
}

func (s *Server) Start() {
	panic("implement me")
}

func (s *Server) Stop() {
	panic("implement me")
}

func (s *Server) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (s *Server) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (s *Server) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (s *Server) RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (s *Server) ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error) {
	panic("implement me")
}

func (s *Server) LoadCollection(req querypb.LoadCollectionRequest) error {
	panic("implement me")
}

func (s *Server) ReleaseCollection(req querypb.ReleaseCollectionRequest) error {
	panic("implement me")
}

func (s *Server) ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error) {
	panic("implement me")
}

func (s *Server) GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error) {
	panic("implement me")
}

func (s *Server) LoadPartitions(req querypb.LoadPartitionRequest) error {
	panic("implement me")
}

func (s *Server) ReleasePartitions(req querypb.ReleasePartitionRequest) error {
	panic("implement me")
}

func (s *Server) CreateQueryChannel() (querypb.CreateQueryChannelResponse, error) {
	panic("implement me")
}
