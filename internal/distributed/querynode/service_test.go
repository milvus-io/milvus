package grpcquerynode

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
)

const (
	debug                = true
	ctxTimeInMillisecond = 2000
)

func TestQueryNodeDistributed_Service(t *testing.T) {
	// Creates server.
	var ctx context.Context
	var cancel context.CancelFunc
	if debug {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
	}
	go mockMain(ctx)
	<-ctx.Done()
	cancel()
}

func mockMain(ctx context.Context) {
	svr := newServerMock(ctx)
	if err := svr.Init(); err != nil {
		panic(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	if err := svr.Start(); err != nil {
		panic(err)
	}
	defer svr.Stop()

	<-ctx.Done()
	log.Print("Got signal to exit", zap.String("signal", sig.String()))

	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}

func newServerMock(ctx context.Context) *Server {
	factory := pulsarms.NewFactory()
	server := &Server{
		node: querynode.NewQueryNodeWithoutID(ctx, factory),
	}

	if err := server.node.SetQueryService(&queryServiceMock{}); err != nil {
		panic(err)
	}
	if err := server.node.SetMasterService(&MasterServiceMock{}); err != nil {
		panic(err)
	}
	if err := server.node.SetIndexService(&IndexServiceMock{}); err != nil {
		panic(err)
	}
	if err := server.node.SetDataService(&DataServiceMock{}); err != nil {
		panic(err)
	}

	return server
}
