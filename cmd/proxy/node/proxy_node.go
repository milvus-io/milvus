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
	"github.com/zilliztech/milvus-distributed/internal/proxynode"
)

func main() {
	os.Setenv("DEPLOY_MODE", "DISTRIBUTED")
	ctx, cancel := context.WithCancel(context.Background())
	msFactory := pulsarms.NewFactory()
	proxynode.Params.Init()
	logutil.SetupLogger(&proxynode.Params.Log)
	n, err := components.NewProxyNode(ctx, msFactory)
	if err != nil {
		log.Error("create server failed", zap.Error(err))
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
		log.Debug("receive stop signal ...")
		cancel()
	}()

	if err := n.Run(); err != nil {
		log.Fatal("Init server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Debug("Got signal to exit", zap.String("signal", sig.String()))

	if err := n.Stop(); err != nil {
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
