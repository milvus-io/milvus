package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/reader"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
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
	pulsarAddr, _ := gparams.GParams.Load("pulsar.address")
	pulsarPort, _ := gparams.GParams.Load("pulsar.port")
	pulsarAddr += ":" + pulsarPort
	reader.StartQueryNode(ctx, pulsarAddr)

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
