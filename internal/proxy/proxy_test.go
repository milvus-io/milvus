package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master"
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

var masterServer *master.Master

func startMaster(ctx context.Context) {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	rootPath := conf.Config.Etcd.Rootpath
	kvRootPath := path.Join(rootPath, "kv")
	metaRootPath := path.Join(rootPath, "meta")
	tsoRootPath := path.Join(rootPath, "timestamp")

	svr, err := master.CreateServer(ctx, kvRootPath, metaRootPath, tsoRootPath, []string{etcdAddr})
	masterServer = svr
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}

	if err := svr.Run(int64(conf.Config.Master.Port)); err != nil {
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
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
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
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		i := i
		cs := &schemapb.CollectionSchema{
			Name:        "CreateCollection" + strconv.FormatInt(int64(i), 10),
			Description: "no description",
			AutoID:      true,
			Fields:      make([]*schemapb.FieldSchema, 1),
		}
		cs.Fields[0] = &schemapb.FieldSchema{
			Name:        "Field" + strconv.FormatInt(int64(i), 10),
			Description: "no description",
			DataType:    schemapb.DataType_INT32,
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			resp, err := proxyClient.CreateCollection(ctx, cs)
			if err != nil {
				t.Error(err)
			}
			msg := "Create Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
		}(&wg)
	}
	wg.Wait()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}
