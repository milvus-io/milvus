package utils

import (
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestGracefulStopGrpcServer(t *testing.T) {
	Params.Init()

	// expected close by gracefulStop
	s1 := grpc.NewServer()
	GracefulStopGRPCServer(s1, 1*time.Second)

	// expected not panic
	GracefulStopGRPCServer(nil, 1*time.Second)
}
