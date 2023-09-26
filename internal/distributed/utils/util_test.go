package utils

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"google.golang.org/grpc"
)

func TestGracefulStopGrpcServer(t *testing.T) {
	paramtable.Init()

	// expected close by gracefulStop
	s1 := grpc.NewServer()
	GracefulStopGRPCServer(s1)

	// expected not panic
	GracefulStopGRPCServer(nil)
}
