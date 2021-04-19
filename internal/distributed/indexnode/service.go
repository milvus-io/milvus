package grpcindexnode

import (
	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"google.golang.org/grpc"
)

type Server struct {
	node indexnode.Interface

	grpcServer *grpc.Server
}

func NewGrpcServer() *Server {
	ret := &Server{
		node: &indexnode.IndexNode{},
	}
	return ret
}
