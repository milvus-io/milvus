package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/indexbuilder"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/proxy"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
	"github.com/zilliztech/milvus-distributed/internal/writenode"
)

func InitMaster(cpuprofile *string, wg *sync.WaitGroup) {
	defer wg.Done()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	master.Init()

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())

	svr, err := master.CreateServer(ctx)
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}

	if err := svr.Run(int64(master.Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Print("Got signal to exit", zap.String("signal", sig.String()))
	cancel()
	svr.Close()
}

func InitProxy(wg *sync.WaitGroup) {
	defer wg.Done()
	proxy.Init()
	fmt.Println("ProxyID is", proxy.Params.ProxyID())
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

	if err := svr.Start(); err != nil {
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

	if err := svr.Start(); err != nil {
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

func InitIndexBuilder(wg *sync.WaitGroup) {
	defer wg.Done()
	indexbuilder.Init()
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := indexbuilder.CreateBuilder(ctx)
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

	if err := svr.Start(); err != nil {
		log.Fatal("run builder server failed", zap.Error(err))
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

func InitWriteNode(wg *sync.WaitGroup) {
	defer wg.Done()
	writenode.Init()
	fmt.Println("WriteNodeID is", writenode.Params.WriteNodeID)
	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	svr := writenode.NewWriteNode(ctx, 111111)

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

	if err := svr.Start(); err != nil {
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

func main() {
	var wg sync.WaitGroup
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()
	wg.Add(1)
	go InitMaster(cpuprofile, &wg)
	time.Sleep(time.Second * 1)
	go InitProxy(&wg)
	go InitQueryNode(&wg)
	go InitIndexBuilder(&wg)
	go InitWriteNode(&wg)
	wg.Wait()
}

func exit(code int) {
	os.Exit(code)
}
