package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	distributed "github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/logutil"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	querynode.Params.Init()
	logutil.SetupLogger(&querynode.Params.Log)
	defer func() {
		if err := log.Sync(); err != nil {
			panic(err)
		}
	}()

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
	log.Debug("Get signal to exit", zap.String("signal", sig.String()))

	if err := svr.Stop(); err != nil {
		panic(err)
	}
}
