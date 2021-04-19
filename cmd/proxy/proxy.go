package main

import (
	"flag"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"


	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/proxy"
)

func main() {
	var yamlFile string
	flag.StringVar(&yamlFile, "yaml", "", "yaml file")
	flag.Parse()
	flag.Usage()
	log.Println("yaml file: ", yamlFile)
	conf.LoadConfig(yamlFile)

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := proxy.CreateProxy(ctx)
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
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
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
