package master

import (
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/controller"
	"github.com/zilliztech/milvus-distributed/internal/master/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"go.etcd.io/etcd/clientv3"
)

func Run() {
	kvBase := newKvBase()
	collectionChan := make(chan *schemapb.CollectionSchema)
	defer close(collectionChan)

	errorCh := make(chan error)
	defer close(errorCh)

	go Server(collectionChan, errorCh, kvBase)
	go controller.SegmentStatsController(kvBase, errorCh)
	go controller.CollectionController(collectionChan, kvBase, errorCh)
	//go timetick.TimeTickService()
	for {
		for v := range errorCh {
			log.Fatal(v)
		}
	}
}

func newKvBase() kv.Base {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	//	defer cli.Close()
	kvBase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)
	return kvBase
}
