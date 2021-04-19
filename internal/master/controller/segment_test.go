package controller

import (
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	masterParams "github.com/zilliztech/milvus-distributed/internal/master/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"go.etcd.io/etcd/clientv3"
)

func newKvBase() *kv.EtcdKV {
	masterParams.Params.Init()

	etcdAddr, err := masterParams.Params.EtcdAddress()
	if err != nil {
		panic(err)
	}

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	etcdRootPath, err := masterParams.Params.EtcdRootPath()
	if err != nil {
		panic(err)
	}
	kvbase := kv.NewEtcdKV(cli, etcdRootPath)
	return kvbase
}

func TestComputeClosetTime(t *testing.T) {
	kvbase := newKvBase()
	var news internalpb.SegmentStats
	for i := 0; i < 10; i++ {
		news = internalpb.SegmentStats{
			SegmentID:  UniqueID(6875940398055133887),
			MemorySize: int64(i * 1000),
		}
		ComputeCloseTime(news, kvbase)
	}
}
