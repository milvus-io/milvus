package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	distributed "github.com/zilliztech/milvus-distributed/cmd/distributed/components"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svr, err := distributed.NewQueryService(ctx)
	if err != nil {
		panic(err)
	}

	if err := svr.Run(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Printf("Got %s signal to exit", sig.String())

	if err := svr.Stop(); err != nil {
		panic(err)
	}
}
