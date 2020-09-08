package master

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/czs007/suvlim/pkg/master/common"
	pb "github.com/czs007/suvlim/pkg/master/grpc"
	"github.com/czs007/suvlim/pkg/master/informer"
	"github.com/czs007/suvlim/pkg/master/kv"
	"github.com/czs007/suvlim/pkg/master/mock"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func Run() {
	go mock.FakePulsarProducer()
	go GRPCServer()
	go SegmentStatsController()
	go CollectionController()
	for {
	}
}

func SegmentStatsController() {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:12379"},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, common.ETCD_ROOT_PATH)

	ssChan := make(chan mock.SegmentStats, 10)
	defer close(ssChan)
	ssClient := informer.NewPulsarClient()
	go ssClient.Listener(ssChan)
	for {
		select {
		case ss := <-ssChan:
			ComputeCloseTime(ss, kvbase)
		case <-time.After(5 * time.Second):
			fmt.Println("timeout")
			return
		}
	}
}

func ComputeCloseTime(ss mock.SegmentStats, kvbase kv.Base) error {
	if int(ss.MemorySize) > common.SEGMENT_THRESHOLE*0.8 {
		memRate := int(ss.MemoryRate)
		if memRate == 0 {
			memRate = 1
		}
		sec := common.SEGMENT_THRESHOLE * 0.2 / memRate
		data, err := kvbase.Load(strconv.Itoa(int(ss.SegementID)))
		if err != nil {
			return err
		}
		seg, err := mock.JSON2Segment(data)
		if err != nil {
			return err
		}
		seg.CloseTimeStamp = time.Now().Add(time.Duration(sec) * time.Second)
		updateData, err := mock.Segment2JSON(*seg)
		if err != nil {
			return err
		}
		kvbase.Save(strconv.Itoa(int(ss.SegementID)), updateData)
	}
	return nil
}

func GRPCServer() error {
	lis, err := net.Listen("tcp", common.DEFAULT_GRPC_PORT)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, GRPCMasterServer{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return err
	}
	return nil
}

type GRPCMasterServer struct{}

func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *pb.CreateCollectionRequest) (*pb.CreateCollectionResponse, error) {
	return &pb.CreateCollectionResponse{
		CollectionName: in.CollectionName,
	}, nil
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
