package queryservice

import (
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	queryServiceImpl "github.com/zilliztech/milvus-distributed/internal/queryservice"
)

type Server struct {
	grpcServer   *grpc.Server
	queryService queryServiceImpl.QueryService
}

func (s *Server) RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error) {
	return s.queryService.RegisterNode(req)
}

func (s *Server) ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error) {
	return s.ShowCollections(req)
}

func (s *Server) LoadCollection(req querypb.LoadCollectionRequest) error {
	return s.LoadCollection(req)
}

func (s *Server) ReleaseCollection(req querypb.ReleaseCollectionRequest) error {
	return s.ReleaseCollection(req)
}

func (s *Server) ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error) {
	return s.ShowPartitions(req)
}

func (s *Server) GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error) {
	return s.GetPartitionStates(req)
}

func (s *Server) LoadPartitions(req querypb.LoadPartitionRequest) error {
	return s.LoadPartitions(req)
}

func (s *Server) ReleasePartitions(req querypb.ReleasePartitionRequest) error {
	return s.ReleasePartitions(req)
}

func (s *Server) CreateQueryChannel() (querypb.CreateQueryChannelResponse, error) {
	return s.CreateQueryChannel()
}
