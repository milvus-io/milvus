package utils

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"google.golang.org/grpc"
)

var Params paramtable.ComponentParam

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
