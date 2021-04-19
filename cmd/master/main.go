package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/master"
	"go.uber.org/zap"
)

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

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
