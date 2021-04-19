package controller

import (
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"go.etcd.io/etcd/clientv3"
)

func newKvBase() *kv.EtcdKV {
	//etcdAddr := conf.Config.Etcd.Address
	//etcdAddr += ":"
	//etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	etcdAddr := "127.0.0.1:" + strconv.Itoa(int(conf.Config.Etcd.Port))
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	kvbase := kv.NewEtcdKV(cli, conf.Config.Etcd.Rootpath)
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
