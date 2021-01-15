package indexbuilder

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	indexbuilderclient "github.com/zilliztech/milvus-distributed/internal/indexbuilder/client"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
)

var ctx context.Context
var cancel func()

var buildClient *indexbuilderclient.Client

var builderServer *Builder

var masterPort = 53101
var masterServer *master.Master

func makeMasterAddress(port int64) string {
	masterAddr := "127.0.0.1:" + strconv.FormatInt(port, 10)
	return masterAddr
}

func refreshMasterAddress() {
	masterAddr := makeMasterAddress(int64(masterPort))
	Params.MasterAddress = masterAddr
	master.Params.Port = masterPort
}

func startMaster(ctx context.Context) {
	master.Init()
	refreshMasterAddress()
	etcdAddr := master.Params.EtcdAddress
	metaRootPath := master.Params.MetaRootPath

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		panic(err)
	}
	_, err = etcdCli.Delete(context.TODO(), metaRootPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	svr, err := master.CreateServer(ctx)
	masterServer = svr
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}
	if err := svr.Run(int64(master.Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())

}

func startBuilder(ctx context.Context) {
	var err error
	builderServer, err = CreateBuilder(ctx)
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
	startMaster(ctx)
	startBuilder(ctx)
	addr := Params.Address
	var err error
	buildClient, err = indexbuilderclient.NewBuildIndexClient(ctx, addr)
	if err != nil {
		panic("Create buildClient Failed!")
	}

}

func shutdown() {
	cancel()
	builderServer.Close()
	masterServer.Close()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func TestBuilder_GRPC(t *testing.T) {
	typeParams := make(map[string]string)
	typeParams["a"] = "1"
	indexParams := make(map[string]string)
	indexParams["b"] = "2"
	columnDataPaths := []string{"dataA", "dataB"}
	indexID, err := buildClient.BuildIndexWithoutID(columnDataPaths, typeParams, indexParams)
	assert.Nil(t, err)

	select {
	case <-time.After(time.Second * 3):
	}

	description, err := buildClient.DescribeIndex(indexID)
	assert.Nil(t, err)
	assert.Equal(t, indexbuilderpb.IndexStatus_FINISHED, description.Status)
	assert.Equal(t, indexID, description.ID)

	indexDataPaths, err := buildClient.GetIndexFilePaths(indexID)
	assert.Nil(t, err)
	assert.NotNil(t, indexDataPaths)

}
