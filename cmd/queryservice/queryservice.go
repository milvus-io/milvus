package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/milvus-distributed/internal/logutil"

	"go.uber.org/zap"

	distributed "github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/queryservice"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryservice.Params.Init()
	logutil.SetupLogger(&queryservice.Params.Log)
	defer func() {
		if err := log.Sync(); err != nil {
			panic(err)
		}
	}()

	msFactory := msgstream.NewPmsFactory()

	svr, err := distributed.NewQueryService(ctx, msFactory)
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
	log.Debug("Get signal to exit", zap.String("signal", sig.String()))

	if err := svr.Stop(); err != nil {
		panic(err)
	}
}
