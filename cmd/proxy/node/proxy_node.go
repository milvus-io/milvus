package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcproxynode "github.com/zilliztech/milvus-distributed/internal/distributed/proxynode"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := grpcproxynode.NewServer(ctx)
	if err != nil {
		log.Print("create server failed", zap.Error(err))
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
		log.Println("receive stop signal ...")
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("Init server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	if err := svr.Stop(); err != nil {
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
