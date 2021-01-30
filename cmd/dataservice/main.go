package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/components"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	svr, err := components.NewDataService(ctx)
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
	log.Println("shut down data service")
}
