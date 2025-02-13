package utils

import (
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func GracefulStopGRPCServer(s *grpc.Server) {
	if s == nil {
		return
	}
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		log.Info("try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(paramtable.Get().ProxyGrpcServerCfg.GracefulStopTimeout.GetAsDuration(time.Second)):
		// took too long, manually close grpc server
		log.Info("force to stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}
