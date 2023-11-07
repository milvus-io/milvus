package utils

import (
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
)

func GracefulStopGRPCServer(s *grpc.Server, timeout time.Duration) {
	if s == nil {
		return
	}
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		log.Debug("try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(timeout):
		// took too long, manually close grpc server
		log.Debug("stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}
