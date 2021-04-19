package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	distributed "github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msFactory := pulsarms.NewFactory()
	svr, err := distributed.NewQueryNode(ctx, msFactory)

	if err != nil {
		panic(err)
	}

	if err = svr.Run(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	log.Print("Got signal to exit", sig.String())

	if err := svr.Stop(); err != nil {
		panic(err)
	}
}
