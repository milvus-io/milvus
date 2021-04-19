package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	distributed "github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterservice.Params.Init()
	log.SetupLogger(&masterservice.Params.Log)
	defer func() {
		if err := log.Sync(); err != nil {
			panic(err)
		}
	}()

	msFactory := pulsarms.NewFactory()
	ms, err := distributed.NewMasterService(ctx, msFactory)
	if err != nil {
		panic(err)
	}
	if err = ms.Run(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Debug("Get signal to exit", zap.String("signal", sig.String()))
	err = ms.Stop()
	if err != nil {
		panic(err)
	}
}
