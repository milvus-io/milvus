package controller

import (
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"go.etcd.io/etcd/clientv3"
)

func newKvBase() *kv.EtcdKV {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}
	etcdAddr := "127.0.0.1:" + etcdPort
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	etcdRootPath, err := gparams.GParams.Load("etcd.rootpath")
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
