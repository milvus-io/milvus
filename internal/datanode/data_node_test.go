package datanode

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
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
	Params.InsertChannelNames = []string{"intsert-a-1", "insert-b-1"}
	Params.TimeTickChannelName = "hard-timetick"
	suffix := "-test-data-node" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
}

func TestMain(m *testing.M) {
	Params.Init()

	refreshChannelNames()
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
