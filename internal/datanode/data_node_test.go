package datanode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
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
	Params.DDChannelNames = []string{"datanode-test"}
	Params.SegmentStatisticsChannelName = "segtment-statistics"
	Params.CompleteFlushChannelName = "flush-completed"
	Params.TimeTickChannelName = "hard-timetick"
	suffix := "-test-data-node" + strconv.FormatInt(rand.Int63n(100), 10)
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
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = true
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	startMaster(ctx)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func newMetaTable() *metaTable {
	etcdClient, _ := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})

	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	mt, _ := NewMetaTable(etcdKV)
	return mt
}

func clearEtcd(rootPath string) error {
	etcdAddr := Params.EtcdAddress
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, rootPath)

	err = etcdKV.RemoveWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/segment ")

	err = etcdKV.RemoveWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/ddl")
	return nil

}
