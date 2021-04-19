package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/logutil"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proxyservice"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	msFactory := pulsarms.NewFactory()
	proxyservice.Params.Init()
	logutil.SetupLogger(&proxyservice.Params.Log)
	s, err := components.NewProxyService(ctx, msFactory)
	if err != nil {
		log.Fatal("create proxy service error: " + err.Error())
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		log.Debug("receive stop signal")
		cancel()
	}()

	if err := s.Run(); err != nil {
		log.Fatal("init server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Debug("Got signal to exit", zap.String("signal", sig.String()))

	if err := s.Stop(); err != nil {
		log.Fatal("stop server failed", zap.Error(err))
	}
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	os.Exit(code)
}
