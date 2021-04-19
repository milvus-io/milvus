package master

import (
	"fmt"
	"time"

	"github.com/czs007/suvlim/pkg/master/common"
	"github.com/czs007/suvlim/pkg/master/informer"
	"github.com/czs007/suvlim/pkg/master/kv"
	"github.com/czs007/suvlim/pkg/master/mock"
	"go.etcd.io/etcd/clientv3"
)

func SegmentStatsController() {
	ssChan := make(chan mock.SegmentStats, 10)
	defer close(ssChan)
	ssClient := informer.NewPulsarClient()
	go ssClient.Listener(ssChan)
	for {
		select {
		case ss := <-ssChan:
			fmt.Println(ss)
		case <-time.After(5 * time.Second):
			fmt.Println("timeout")
			return
		}
	}
}

func GRPCServer() {

}

func CollectionController() {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:12379"},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, common.ETCD_ROOT_PATH)
	c := mock.FakeCreateCollection(uint64(3333))
	s := mock.FakeCreateSegment(uint64(11111), c, time.Now(), time.Unix(1<<63-1, 0))
	collectionData, _ := mock.Collection2JSON(c)
	segmentData, _ := mock.Segment2JSON(s)
	kvbase.Save("test-collection", collectionData)
	kvbase.Save("test-segment", segmentData)
	fmt.Println(kvbase.Load("test-collection"))
	fmt.Println(kvbase.Load("test-segment"))
}

func Sync() {

}
