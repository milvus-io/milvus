package mlog

import (
	"sync"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc/grpclog"
)

// Regression for #54965: gRPC logging must remain safe while the backing
// logger is reconfigured. Run with go test -race to detect unsafe global swaps.
func TestReplaceGRPCLoggerConcurrentWithLogging(t *testing.T) {
	previous := globalGRPCLogger.backend.Load()
	if previous == nil {
		t.Fatal("gRPC logger must be initialized before use")
	}
	defer globalGRPCLogger.backend.Store(previous)

	const workers = 8
	const iterations = 500
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers + 1)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				grpclog.Info("concurrent gRPC log")
				grpclog.Infof("concurrent gRPC log %d", i)
				_ = grpclog.V(0)
			}
		}()
	}
	go func() {
		defer wg.Done()
		<-start
		logger := zap.NewNop()
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				ReplaceGRPCLogger(logger, "INFO")
			} else {
				ReplaceGRPCLogger(logger, "WARNING")
			}
		}
	}()

	close(start)
	wg.Wait()
}
