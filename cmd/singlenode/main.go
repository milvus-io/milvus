package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	"github.com/zilliztech/milvus-distributed/internal/proxynode"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
)

func InitProxy(wg *sync.WaitGroup) {
	defer wg.Done()
	//proxynode.Init()
	//fmt.Println("ProxyID is", proxynode.Params.ProxyID())
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := proxynode.NewProxyNodeImpl(ctx)
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

	if err := svr.Init(); err != nil {
		log.Fatal("init server failed", zap.Error(err))
	}

	if err := svr.Start(); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	svr.Stop()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func InitQueryNode(wg *sync.WaitGroup) {
	defer wg.Done()
	querynode.Init()
	fmt.Println("QueryNodeID is", querynode.Params.QueryNodeID)
	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	svr := querynode.NewQueryNode(ctx, 0)

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

	svr.Start()

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	svr.Stop()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func InitIndexBuilder(wg *sync.WaitGroup) {
	defer wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := indexnode.NewNodeImpl(ctx)
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

	if err := svr.Init(); err != nil {
		log.Fatal("init builder server failed", zap.Error(err))
	}

	if err := svr.Start(); err != nil {
		log.Fatal("run builder server failed", zap.Error(err))
	}

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	svr.Stop()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func main() {
	var wg sync.WaitGroup
	flag.Parse()
	time.Sleep(time.Second * 1)
	wg.Add(1)
	go InitProxy(&wg)
	wg.Add(1)
	go InitQueryNode(&wg)
	wg.Add(1)
	go InitIndexBuilder(&wg)
	wg.Wait()
}

func exit(code int) {
	os.Exit(code)
}
