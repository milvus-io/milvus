package master

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/czs007/suvlim/conf"
	pb "github.com/czs007/suvlim/pkg/master/grpc/master"
	"github.com/czs007/suvlim/pkg/master/grpc/message"
	messagepb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/czs007/suvlim/pkg/master/id"
	"github.com/czs007/suvlim/pkg/master/informer"
	"github.com/czs007/suvlim/pkg/master/kv"
	"github.com/czs007/suvlim/pkg/master/mock"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func Run() {
	go mock.FakePulsarProducer()
	go SegmentStatsController()
	collectionChan := make(chan *messagepb.Mapping)
	defer close(collectionChan)
	go GRPCServer(collectionChan)
	go CollectionController(collectionChan)
	for {
	}
}

func SegmentStatsController() {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)

	ssChan := make(chan mock.SegmentStats, 10)
	defer close(ssChan)
	ssClient := informer.NewPulsarClient()
	go ssClient.Listener(ssChan)
	for {
		select {
		case ss := <-ssChan:
			ComputeCloseTime(ss, kvbase)
			UpdateSegmentStatus(ss, kvbase)
		case <-time.After(5 * time.Second):
			fmt.Println("timeout")
			return
		}
	}
}

func ComputeCloseTime(ss mock.SegmentStats, kvbase kv.Base) error {
	if int(ss.MemorySize) > int(conf.Config.Master.SegmentThreshole*0.8) {
		currentTime := time.Now()
		memRate := int(ss.MemoryRate)
		if memRate == 0 {
			memRate = 1
		}
		sec := int(conf.Config.Master.SegmentThreshole*0.2) / memRate
		data, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegementID)))
		if err != nil {
			return err
		}
		seg, err := mock.JSON2Segment(data)
		if err != nil {
			return err
		}
		seg.CloseTimeStamp = uint64(currentTime.Add(time.Duration(sec) * time.Second).Unix())
		updateData, err := mock.Segment2JSON(*seg)
		if err != nil {
			return err
		}
		kvbase.Save("segment/"+strconv.Itoa(int(ss.SegementID)), updateData)
		//create new segment
		newSegID := id.New().Uint64()
		newSeg := mock.NewSegment(newSegID, seg.CollectionID, seg.CollectionName, "default", seg.ChannelStart, seg.ChannelEnd, currentTime, time.Unix(1<<36-1, 0))
		newSegData, err := mock.Segment2JSON(*&newSeg)
		if err != nil {
			return err
		}
		//save to kv store
		kvbase.Save("segment/"+strconv.Itoa(int(newSegID)), newSegData)
		// update collection data
		c, _ := kvbase.Load("collection/" + strconv.Itoa(int(seg.CollectionID)))
		collection, err := mock.JSON2Collection(c)
		if err != nil {
			return err
		}
		segIDs := collection.SegmentIDs
		segIDs = append(segIDs, newSegID)
		collection.SegmentIDs = segIDs
		cData, err := mock.Collection2JSON(*collection)
		if err != nil {
			return err
		}
		kvbase.Save("segment/"+strconv.Itoa(int(seg.CollectionID)), cData)
	}
	return nil
}

func UpdateSegmentStatus(ss mock.SegmentStats, kvbase kv.Base) error {
	segmentData, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegementID)))
	if err != nil {
		return err
	}
	seg, err := mock.JSON2Segment(segmentData)
	if err != nil {
		return err
	}
	var changed bool
	changed = false
	if seg.Status != ss.Status {
		changed = true
		seg.Status = ss.Status
	}
	if seg.Rows != ss.Rows {
		changed = true
		seg.Rows = ss.Rows
	}

	if changed {
		segData, err := mock.Segment2JSON(*seg)
		if err != nil {
			return err
		}
		err = kvbase.Save("segment/"+strconv.Itoa(int(seg.CollectionID)), segData)
		if err != nil {
			return err
		}
	}
	return nil
}

func GRPCServer(ch chan *messagepb.Mapping) error {
	defaultGRPCPort := ":"
	defaultGRPCPort += strconv.FormatInt(int64(conf.Config.Master.Port), 10)
	lis, err := net.Listen("tcp", defaultGRPCPort)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, GRPCMasterServer{CreateRequest: ch})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return err
	}
	return nil
}

type GRPCMasterServer struct {
	CreateRequest chan *messagepb.Mapping
}

func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *messagepb.Mapping) (*messagepb.Status, error) {
	//	ms.CreateRequest <- in2
	fmt.Println("Handle a new create collection request")
	err := WriteCollection2Datastore(in)
	if err != nil {
		return &messagepb.Status{
			ErrorCode: 100,
			Reason:    "",
		}, err
	}
	return &messagepb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms GRPCMasterServer) CreateIndex(ctx context.Context, in *messagepb.IndexParam) (*message.Status, error) {
	fmt.Println("Handle a new create index request")
	err := UpdateCollectionIndex(in)
	if err != nil {
		return &messagepb.Status{
			ErrorCode: 100,
			Reason:    "",
		}, err
	}
	return &messagepb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

// func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *pb.CreateCollectionRequest) (*pb.CreateCollectionResponse, error) {
//	return &pb.CreateCollectionResponse{
//		CollectionName: in.CollectionName,
//	}, nil
// }

func CollectionController(ch chan *messagepb.Mapping) {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)
	for collection := range ch {
		sID := id.New().Uint64()
		cID := id.New().Uint64()
		s2ID := id.New().Uint64()
		fieldMetas := []*messagepb.FieldMeta{}
		if collection.Schema != nil {
			fieldMetas = collection.Schema.FieldMetas
		}
		c := mock.NewCollection(cID, collection.CollectionName,
			time.Now(), fieldMetas, []uint64{sID, s2ID},
			[]string{"default"})
		cm := mock.GrpcMarshal(&c)
		s := mock.NewSegment(sID, cID, collection.CollectionName, "default", 0, 511, time.Now(), time.Unix(1<<36-1, 0))
		s2 := mock.NewSegment(s2ID, cID, collection.CollectionName, "default", 512, 1023, time.Now(), time.Unix(1<<36-1, 0))
		collectionData, _ := mock.Collection2JSON(*cm)
		segmentData, err := mock.Segment2JSON(s)
		if err != nil {
			log.Fatal(err)
		}
		s2Data, err := mock.Segment2JSON(s2)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("collection/"+strconv.FormatUint(cID, 10), collectionData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatUint(sID, 10), segmentData)
		if err != nil {
			log.Fatal(err)
		}
		err = kvbase.Save("segment/"+strconv.FormatUint(s2ID, 10), s2Data)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func WriteCollection2Datastore(collection *messagepb.Mapping) error {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)
	sID := id.New().Uint64()
	cID := id.New().Uint64()
	fieldMetas := []*messagepb.FieldMeta{}
	if collection.Schema != nil {
		fieldMetas = collection.Schema.FieldMetas
	}
	c := mock.NewCollection(cID, collection.CollectionName,
		time.Now(), fieldMetas, []uint64{sID},
		[]string{"default"})
	cm := mock.GrpcMarshal(&c)
	s := mock.NewSegment(sID, cID, collection.CollectionName, "default", 0, conf.Config.Pulsar.TopicNum, time.Now(), time.Unix(1<<46-1, 0))
	collectionData, err := mock.Collection2JSON(*cm)
	if err != nil {
		log.Fatal(err)
		return err
	}
	segmentData, err := mock.Segment2JSON(s)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = kvbase.Save("collection/"+strconv.FormatUint(cID, 10), collectionData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = kvbase.Save("segment/"+strconv.FormatUint(sID, 10), segmentData)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil

}

func UpdateCollectionIndex(index *messagepb.IndexParam) error {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	kvbase := kv.NewEtcdKVBase(cli, conf.Config.Etcd.Rootpath)
	collectionName := index.CollectionName
	c, err := kvbase.Load("collection/" + collectionName)
	if err != nil {
		return err
	}
	collection, err := mock.JSON2Collection(c)
	if err != nil {
		return err
	}
	for k, v := range collection.IndexParam {
		if v.IndexName == index.IndexName {
			collection.IndexParam[k] = v
		}
	}
	collection.IndexParam = append(collection.IndexParam, index)
	cm := mock.GrpcMarshal(collection)
	collectionData, err := mock.Collection2JSON(*cm)
	if err != nil {
		return err
	}
	err = kvbase.Save("collection/"+collectionName, collectionData)
	if err != nil {
		return err
	}
	return nil
}
