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
	// go mock.FakePulsarProducer()
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

	segmentCloseLog := make(map[uint64]uint64, 0)

	go ssClient.Listener(ssChan)
	for {
		select {
		case ss := <-ssChan:
			ComputeCloseTime(&segmentCloseLog, ss, kvbase)
			UpdateSegmentStatus(ss, kvbase)
		//case <-time.After(5 * time.Second):
		//	fmt.Println("timeout")
		//	return
		}
	}
}

func GetPhysicalTimeNow() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func ComputeCloseTime(segmentCloseLog *map[uint64]uint64, ss mock.SegmentStats, kvbase kv.Base) error {
	segmentID := ss.SegementID
	if _, ok := (*segmentCloseLog)[segmentID]; ok {
		// This segment has been closed
		return nil
	}

	if int(ss.MemorySize) > int(conf.Config.Master.SegmentThreshole*0.8) {
		currentTime := GetPhysicalTimeNow()
		memRate := int(ss.MemoryRate)
		if memRate == 0 {
			//memRate = 1
			log.Println("memRate = 0")
			return nil
		}
		sec := float64(conf.Config.Master.SegmentThreshole*0.2) / float64(memRate)
		data, err := kvbase.Load("segment/" + strconv.Itoa(int(ss.SegementID)))
		if err != nil {
			log.Println("Load segment failed")
			return err
		}
		seg, err := mock.JSON2Segment(data)
		if err != nil {
			log.Println("JSON2Segment failed")
			return err
		}

		seg.CloseTimeStamp = currentTime + uint64(sec * 1000)
		// Reduce time gap between Proxy and Master
		seg.CloseTimeStamp = seg.CloseTimeStamp + uint64(5 * 1000)
		fmt.Println("Close segment = ", seg.SegmentID, ",Close time = ", seg.CloseTimeStamp)

		updateData, err := mock.Segment2JSON(*seg)
		if err != nil {
			log.Println("Update segment, Segment2JSON failed")
			return err
		}
		err = kvbase.Save("segment/"+strconv.Itoa(int(ss.SegementID)), updateData)
		if err != nil {
			log.Println("Save segment failed")
			return err
		}

		(*segmentCloseLog)[segmentID] = seg.CloseTimeStamp

		//create new segment
		newSegID := id.New().Uint64()
		newSeg := mock.NewSegment(newSegID, seg.CollectionID, seg.CollectionName, "default", seg.ChannelStart, seg.ChannelEnd, currentTime, 1 << 46 - 1)
		newSegData, err := mock.Segment2JSON(*&newSeg)
		if err != nil {
			log.Println("Create new segment, Segment2JSON failed")
			return err
		}

		//save to kv store
		err = kvbase.Save("segment/"+strconv.Itoa(int(newSegID)), newSegData)
		if err != nil {
			log.Println("Save segment failed")
			return err
		}

		// update collection data
		c, _ := kvbase.Load("collection/" + strconv.Itoa(int(seg.CollectionID)))
		collection, err := mock.JSON2Collection(c)
		if err != nil {
			log.Println("JSON2Segment failed")
			return err
		}
		segIDs := collection.SegmentIDs
		segIDs = append(segIDs, newSegID)
		collection.SegmentIDs = segIDs
		cData, err := mock.Collection2JSON(*collection)
		if err != nil {
			log.Println("Collection2JSON failed")
			return err
		}
		err = kvbase.Save("collection/"+strconv.Itoa(int(seg.CollectionID)), cData)
		if err != nil {
			log.Println("Save collection failed")
			return err
		}
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
		s := mock.NewSegment(sID, cID, collection.CollectionName, "default", 0, 511, GetPhysicalTimeNow(), 1 << 46 - 1)
		s2 := mock.NewSegment(s2ID, cID, collection.CollectionName, "default", 512, 1023, GetPhysicalTimeNow(), 1 << 46 - 1)
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

	cID := id.New().Uint64()

	fieldMetas := []*messagepb.FieldMeta{}
	if collection.Schema != nil {
		fieldMetas = collection.Schema.FieldMetas
	}

	queryNodeNum := conf.Config.Master.QueryNodeNum
	topicNum := conf.Config.Pulsar.TopicNum
	var topicNumPerQueryNode int

	if topicNum % queryNodeNum != 0 {
		topicNumPerQueryNode = topicNum / queryNodeNum + 1
	} else {
		topicNumPerQueryNode = topicNum / queryNodeNum
	}

	fmt.Println("QueryNodeNum = ", queryNodeNum)
	fmt.Println("TopicNum = ", topicNum)
	fmt.Println("TopicNumPerQueryNode = ", topicNumPerQueryNode)

	sIDs := make([]uint64, queryNodeNum)

	for i := 0; i < queryNodeNum; i++ {
		// For generating different id
		time.Sleep(1000 * time.Millisecond)

		sIDs[i] = id.New().Uint64()
	}

	c := mock.NewCollection(cID, collection.CollectionName,
		time.Now(), fieldMetas, sIDs,
		[]string{"default"})
	cm := mock.GrpcMarshal(&c)

	collectionData, err := mock.Collection2JSON(*cm)
	if err != nil {
		log.Fatal(err)
		return err
	}

	err = kvbase.Save("collection/"+strconv.FormatUint(cID, 10), collectionData)
	if err != nil {
		log.Fatal(err)
		return err
	}

	for i := 0; i < queryNodeNum; i++ {
		chStart := i * topicNumPerQueryNode
		chEnd := (i + 1) * topicNumPerQueryNode
		if chEnd > topicNum {
			chEnd = topicNum - 1
		}
		s := mock.NewSegment(sIDs[i], cID, collection.CollectionName, "default", chStart, chEnd, GetPhysicalTimeNow(), 1 << 46 - 1)

		segmentData, err := mock.Segment2JSON(s)
		if err != nil {
			log.Fatal(err)
			return err
		}

		err = kvbase.Save("segment/"+strconv.FormatUint(sIDs[i], 10), segmentData)
		if err != nil {
			log.Fatal(err)
			return err
		}
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
