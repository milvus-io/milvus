package utils

import (
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestGracefulStopGrpcServer(t *testing.T) {
	// expected close by gracefulStop
	s1 := grpc.NewServer()
	GracefulStopGRPCServer(s1, time.Second*5)

	// expected not panic
	GracefulStopGRPCServer(nil, time.Second*5)
}
