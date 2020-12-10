package indexbuilder

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
	"log"
	"os"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var ctx context.Context
var cancel func()
var clientConn *grpc.ClientConn

var buildClient indexbuilderpb.IndexBuildServiceClient

var builderServer *Builder

var testNum = 10

func startBuilder(ctx context.Context) {

	builderServer, err := CreateBuilder(ctx)
	if err != nil {
		log.Print("create builder failed", zap.Error(err))
	}

	// TODO: change to wait until master is ready
	if err := builderServer.Start(); err != nil {
		log.Fatal("run builder failed", zap.Error(err))
	}
}

func setup() {
	Params.Init()
	ctx, cancel = context.WithCancel(context.Background())

	startBuilder(ctx)
	addr := Params.Address
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Connect to builder server failed, error= %v", err)
	}
	clientConn = conn
	buildClient = indexbuilderpb.NewIndexBuildServiceClient(clientConn)

}

func shutdown() {
	cancel()
	builderServer.Close()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}
