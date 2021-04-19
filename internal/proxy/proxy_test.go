package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	mockMaster "github.com/zilliztech/milvus-distributed/internal/master/mock"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var ctx context.Context
var cancel func()
var proxyAddress = "127.0.0.1:5053"
var proxyConn *grpc.ClientConn
var proxyClient servicepb.MilvusServiceClient

var proxyServer *Proxy

var masterServer *mockMaster.Master

func startMaster(ctx context.Context) {
	//conf.LoadConfig("config.yaml")
	fmt.Println("THIS is test before.")
	svr, err := mockMaster.CreateServer(ctx)
	masterServer = svr
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())

}

func startProxy(ctx context.Context) {

	svr, err := CreateProxy(ctx)
	proxyServer = svr
	if err != nil {
		log.Print("create proxy failed", zap.Error(err))
	}

	// TODO: change to wait until master is ready
	if err := svr.Run(); err != nil {
		log.Fatal("run proxy failed", zap.Error(err))
	}
}

func setup() {
	conf.LoadConfig("config.yaml")
	ctx, cancel = context.WithCancel(context.Background())

	startMaster(ctx)
	startProxy(ctx)

	conn, err := grpc.DialContext(ctx, proxyAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Connect to proxy failed, error= %v", err)
	}
	proxyConn = conn
	proxyClient = servicepb.NewMilvusServiceClient(proxyConn)

}

func shutdown() {
	cancel()
	masterServer.Close()
	proxyServer.Close()
}

func TestProxy_CreateCollection(t *testing.T) {
	fmt.Println(proxyClient)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}
