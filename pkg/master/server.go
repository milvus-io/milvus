package master

import (
	"fmt"
	"log"
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
	s := mock.FakeCreateSegment(uint64(11111), c, time.Now(), time.Unix(1<<36-1, 0))
	collectionData, _ := mock.Collection2JSON(c)
	segmentData, err := mock.Segment2JSON(s)
	if err != nil {
		log.Fatal(err)
	}
	err = kvbase.Save("test-collection", collectionData)
	if err != nil {
		log.Fatal(err)
	}
	err = kvbase.Save("test-segment", segmentData)
	if err != nil {
		log.Fatal(err)
	}
}

func Sync() {

}
