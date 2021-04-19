package controller

import (
	"strconv"
	"testing"
	"time"

	"github.com/czs007/suvlim/conf"
	"github.com/czs007/suvlim/pkg/master/kv"
	"github.com/czs007/suvlim/pkg/master/segment"
	"go.etcd.io/etcd/clientv3"
)

func newKvBase() kv.Base {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	kvbase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)
	return kvbase
}

func TestComputeClosetTime(t *testing.T) {
	kvbase := newKvBase()
	var news segment.SegmentStats
	for i := 0; i < 10; i++ {
		news = segment.SegmentStats{
			SegementID: uint64(6875940398055133887),
			MemorySize: uint64(i * 1000),
			MemoryRate: 0.9,
		}
		ComputeCloseTime(news, kvbase)
	}
}
