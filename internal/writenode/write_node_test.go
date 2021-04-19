package writenode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/master"
)

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "-test-write-node" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
}

func startMaster(ctx context.Context) {
	master.Init()
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

	masterPort := 53101
	master.Params.Port = masterPort
	svr, err := master.CreateServer(ctx)
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}
	if err := svr.Run(int64(master.Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())
	Params.MasterAddress = master.Params.Address + ":" + strconv.Itoa(masterPort)
}

func TestMain(m *testing.M) {
	Params.Init()
	refreshChannelNames()
	p := Params
	fmt.Println(p)
	exitCode := m.Run()
	os.Exit(exitCode)
}
