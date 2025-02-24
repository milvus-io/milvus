package utils

import (
	"testing"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGracefulStopGrpcServer(t *testing.T) {
	paramtable.Init()

	// expected close by gracefulStop
	s1 := grpc.NewServer()
	GracefulStopGRPCServer(s1)

	// expected not panic
	GracefulStopGRPCServer(nil)
}
