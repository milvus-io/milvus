package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/datanode"
	"google.golang.org/grpc"
)

type Server struct {
	node       datanode.Node
	brpcServer *grpc.Server
}
