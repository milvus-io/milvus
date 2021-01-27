package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcqueryservice "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svr := grpcqueryservice.NewServer(ctx)

	if err := svr.Init(); err != nil {
		panic(err)
	}

	log.Printf("query service address : %s", queryservice.Params.Address)

	if err := svr.Start(); err != nil {
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
	_ = svr.Stop()
}
