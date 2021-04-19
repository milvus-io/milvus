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

	dn, err := distributed.NewDataNode(ctx, msFactory)
	if err != nil {
		panic(err)
	}
	if err = dn.Run(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	log.Println("Got signal to exit signal:", sig.String())

	err = dn.Stop()
	if err != nil {
		panic(err)
	}
}
