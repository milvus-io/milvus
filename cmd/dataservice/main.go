package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/dataservice"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	dataservice.Params.Init()
	log.SetupLogger(&dataservice.Params.Log)
	defer log.Sync()
	msFactory := pulsarms.NewFactory()

	svr, err := components.NewDataService(ctx, msFactory)
	if err != nil {
		panic(err)
	}
	if err = svr.Run(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sc
	cancel()
	if err := svr.Stop(); err != nil {
		panic(err)
	}
	log.Debug("shut down data service")
}
