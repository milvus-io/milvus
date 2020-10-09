package proxy_node

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	mpb "github.com/czs007/suvlim/pkg/master/grpc/master"
	pb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"net"
	"sort"
	"testing"
	"time"
	"unsafe"
)

type testMasterServer struct {
	mpb.UnimplementedMasterServer
}

func (*testMasterServer) CreateCollection(ctx context.Context, req *pb.Mapping) (*pb.Status, error) {
	return &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS, Reason: req.CollectionName}, nil
}
func (*testMasterServer) CreateIndex(ctx context.Context, req *pb.IndexParam) (*pb.Status, error) {
	return &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS, Reason: req.IndexName}, nil
}

func startTestMaster(master_addr string, t *testing.T) *grpc.Server {
	lis, err := net.Listen("tcp", master_addr)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	mpb.RegisterMasterServer(s, &testMasterServer{})
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatal(err)
		}
	}()
	return s
}

func startTestProxyServer(proxy_addr string, master_addr string, t *testing.T) *proxyServer {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	var timestamp uint64 = 1000

	p := &proxyServer{
		address:        proxy_addr,
		master_address: master_addr,
		rootPath:       "/proxy/root",
		pulsarAddr:     "pulsar://localhost:6650",
		readerTopics:   []string{"reader1", "reader2"},
		deleteTopic:    "deleteT",
		queryTopic:     "queryer",
		resultTopic:    "resulter",
		resultGroup:    "reusltG",
		numReaderNode:  2,
		proxyId:        1,
		getTimestamp: func(count uint32) ([]Timestamp, pb.Status) {
			timestamp += 100
			t := make([]Timestamp, count)
			for i := 0; i < int(count); i++ {
				t[i] = Timestamp(timestamp)
			}
			return t, pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
		},
		client: client,
		ctx:    ctx,
	}
	go func() {
		if err := startProxyServer(p); err != nil {
			t.Fatal(err)
		}
	}()
	return p
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, unsafe.Sizeof(v))
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func TestProxyServer_CreateCollectionAndIndex(t *testing.T) {
	_ = startTestMaster("localhost:10000", t)
	//defer ms.Stop()
	time.Sleep(100 * time.Millisecond)

	ps := startTestProxyServer("localhost:10001", "localhost:10000", t)
	//defer ps.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := ps.ctx
	conn, err := grpc.DialContext(ctx, "localhost:10001", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cli := pb.NewMilvusServiceClient(conn)
	st, err := cli.CreateCollection(ctx, &pb.Mapping{CollectionName: "testCollectionName"})
	if err != nil {
		t.Fatal(err)
	}

	if st.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("CreateCollection failed")
	}
	if st.Reason != "testCollectionName" {
		t.Fatalf("CreateCollection failed")
	}
	st, err = cli.CreateIndex(ctx, &pb.IndexParam{IndexName: "testIndexName"})
	if err != nil {
		t.Fatal(err)
	}
	if st.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("CreateIndex failed")
	}
	if st.Reason != "testIndexName" {
		t.Fatalf("CreateIndex failed")
	}
}

func TestProxyServer_WatchEtcd(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	col1 := mpb.Collection{
		Id:         1,
		Name:       "c1",
		SegmentIds: []uint64{2, 3},
	}
	seg2 := mpb.Segment{
		SegmentId: 2,
		Rows:      10,
	}
	seg3 := mpb.Segment{
		SegmentId: 3,
		Rows:      10,
	}
	if cb1, err := json.Marshal(&col1); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keyCollectionPath+"/1", string(cb1)); err != nil {
		t.Fatal(err)
	}

	if sb2, err := json.Marshal(&seg2); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keySegmentPath+"/2", string(sb2)); err != nil {
		t.Fatal(err)
	}

	if sb3, err := json.Marshal(&seg3); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keySegmentPath+"/3", string(sb3)); err != nil {
		t.Fatal(err)
	}

	_ = startTestMaster("localhost:10002", t)
	//defer ms.Stop()
	time.Sleep(100 * time.Millisecond)

	ps := startTestProxyServer("localhost:10003", "localhost:10002", t)
	//defer ps.Close()
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.DialContext(ps.ctx, "localhost:10003", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cli := pb.NewMilvusServiceClient(conn)
	cr, err := cli.CountCollection(ps.ctx, &pb.CollectionName{CollectionName: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Status.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("CountCollection failed : %s", cr.Status.Reason)
	}
	if cr.CollectionRowCount != 20 {
		t.Fatalf("collection count expect to be 20, count = %d", cr.CollectionRowCount)
	}

	col4 := mpb.Collection{
		Id:         4,
		Name:       "c4",
		SegmentIds: []uint64{5},
	}
	seg5 := mpb.Segment{
		SegmentId: 5,
		Rows:      10,
	}
	if cb4, err := json.Marshal(&col4); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ps.ctx, "/proxy/root/"+keyCollectionPath+"/4", string(cb4)); err != nil {
		t.Fatal(err)
	}
	if sb5, err := json.Marshal(&seg5); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ps.ctx, "/proxy/root/"+keySegmentPath+"/5", string(sb5)); err != nil {
		t.Fatal(err)
	}
	cr, err = cli.CountCollection(ps.ctx, &pb.CollectionName{CollectionName: "c4"})
	if err != nil {
		t.Fatal(err)
	}
	if cr.Status.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("CountCollection failed : %s", cr.Status.Reason)
	}
	if cr.CollectionRowCount != 10 {
		t.Fatalf("collection count expect to be 10, count = %d", cr.CollectionRowCount)
	}
}

func TestProxyServer_InsertAndDelete(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	col10 := mpb.Collection{
		Id:            10,
		Name:          "col10",
		Schema:        nil,
		CreateTime:    0,
		SegmentIds:    []uint64{11, 12},
		PartitionTags: nil,
		Indexes:       nil,
	}
	seg11 := mpb.Segment{
		SegmentId:    11,
		CollectionId: 10,
		ChannelStart: 0,
		ChannelEnd:   1,
		Status:       mpb.SegmentStatus_OPENED,
	}
	seg12 := mpb.Segment{
		SegmentId:    12,
		CollectionId: 10,
		ChannelStart: 1,
		ChannelEnd:   2,
		Status:       mpb.SegmentStatus_OPENED,
	}
	if cb10, err := json.Marshal(&col10); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keyCollectionPath+"/10", string(cb10)); err != nil {
		t.Fatal(err)
	}
	if sb11, err := json.Marshal(&seg11); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keySegmentPath+"/11", string(sb11)); err != nil {
		t.Fatal(err)
	}
	if sb12, err := json.Marshal(&seg12); err != nil {
		t.Fatal(err)
	} else if _, err := client.Put(ctx, "/proxy/root/"+keySegmentPath+"/12", string(sb12)); err != nil {
		t.Fatal(err)
	}

	_ = startTestMaster("localhost:10004", t)
	time.Sleep(100 * time.Millisecond)
	ps := startTestProxyServer("localhost:10005", "localhost:10004", t)
	//defer ps.Close()
	time.Sleep(100 * time.Millisecond)
	conn, err := grpc.DialContext(ps.ctx, "localhost:10005", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: ps.pulsarAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer pulsarClient.Close()
	reader, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topics:                      ps.readerTopics,
		SubscriptionName:            "reader-group",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	deleter, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       ps.deleteTopic,
		SubscriptionName:            "delete-group",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})

	pctx, _ := context.WithTimeout(ps.ctx, time.Second)
	isbreak := false
	for {
		if isbreak {
			break
		}
		select {
		case <-pctx.Done():
			isbreak = true
			break
		case cm, ok := <-reader.Chan():
			if !ok {
				t.Fatalf("reader closed")
			}
			reader.AckID(cm.ID())
			break
		case cm, ok := <-deleter.Chan():
			if !ok {
				t.Fatalf("deleter closed")
			}
			deleter.AckID(cm.ID())
		}

	}

	ip := pb.InsertParam{
		CollectionName: "col10",
		Schema:         nil,
		RowsData: []*pb.RowData{
			{Blob: uint64ToBytes(1)},
			{Blob: uint64ToBytes(2)},
			{Blob: uint64ToBytes(3)},
			{Blob: uint64ToBytes(4)},
			{Blob: uint64ToBytes(5)},
		},
		EntityIdArray: []int64{1, 2, 3, 4, 5},
		PartitionTag:  "",
		ExtraParams:   nil,
	}

	dp := pb.DeleteByIDParam{
		CollectionName: "deleteCollection",
		IdArray:        []int64{1, 2, 3, 4, 5},
	}

	serverClient := pb.NewMilvusServiceClient(conn)
	ir, err := serverClient.Insert(ps.ctx, &ip)
	if err != nil {
		t.Fatal(err)
	}
	if ir.Status.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("Insert failed, error code = %d, reason = %s", ir.Status.ErrorCode, ir.Status.Reason)
	}
	if len(ir.EntityIdArray) != 5 {
		t.Fatalf("insert failed, len(ir.EntityIdArray) expect to be 5")
	}
	sort.Slice(ir.EntityIdArray, func(i int, j int) bool { return ir.EntityIdArray[i] < ir.EntityIdArray[j] })
	for i := 0; i < 5; i++ {
		if ir.EntityIdArray[i] != int64(i+1) {
			t.Fatalf("insert failed")
		}
	}
	dr, err := serverClient.DeleteByID(ps.ctx, &dp)
	if err != nil {
		t.Fatal(err)
	}
	if dr.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatalf("delete failed, error code = %d, reason = %s", dr.ErrorCode, dr.Reason)
	}

	var primaryKey []uint64
	isbreak = false
	for {
		if isbreak {
			break
		}
		select {
		case <-ps.ctx.Done():
			isbreak = true
			break
		case cm, ok := <-reader.Chan():
			if !ok {
				t.Fatalf("reader closed")
			}
			msg := cm.Message
			var m pb.ManipulationReqMsg
			if err := proto.Unmarshal(msg.Payload(), &m); err != nil {
				t.Fatal(err)
			}
			for i, k := range m.PrimaryKeys {
				primaryKey = append(primaryKey, k)
				rowValue := binary.LittleEndian.Uint64(m.RowsData[i].Blob)
				t.Logf("primary key = %d, rowvalue =%d", k, rowValue)
				if k != rowValue {
					t.Fatalf("key expect equal to row value")
				}
			}
			reader.AckID(cm.ID())
			break
		case cm, ok := <-deleter.Chan():
			if !ok {
				t.Fatalf("deleter closed")
			}
			var m pb.ManipulationReqMsg
			if err := proto.Unmarshal(cm.Message.Payload(), &m); err != nil {
				t.Fatal(err)
			}
			if m.CollectionName != "deleteCollection" {
				t.Fatalf("delete failed, collection name = %s", m.CollectionName)
			}
			if len(m.PrimaryKeys) != 5 {
				t.Fatalf("delete failed,len(m.PrimaryKeys) = %d", len(m.PrimaryKeys))
			}
			for i, v := range m.PrimaryKeys {
				if v != uint64(i+1) {
					t.Fatalf("delete failed")
				}
			}

		}
	}
	if len(primaryKey) != 5 {
		t.Fatalf("Receive from pulsar failed")
	}
	sort.Slice(primaryKey, func(i int, j int) bool { return primaryKey[i] < primaryKey[j] })
	for i := 0; i < 5; i++ {
		if primaryKey[i] != uint64(i+1) {
			t.Fatalf("insert failed")
		}
	}
	t.Logf("m_timestamp = %d", ps.reqSch.m_timestamp)
	if ps.reqSch.m_timestamp != 1300 {
		t.Fatalf("insert failed")
	}
}

func TestProxyServer_Search(t *testing.T) {
	_ = startTestMaster("localhost:10006", t)
	time.Sleep(100 * time.Millisecond)
	ps := startTestProxyServer("localhost:10007", "localhost:10006", t)
	time.Sleep(100 * time.Millisecond)
	conn, err := grpc.DialContext(ps.ctx, "localhost:10007", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: ps.pulsarAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer pulsarClient.Close()
	query, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       ps.queryTopic,
		SubscriptionName:            "query-group",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	result, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: ps.resultTopic})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	pctx, _ := context.WithTimeout(ps.ctx, time.Second)
	func() {
		for {
			select {
			case <-pctx.Done():
				return
			case cm, ok := <-query.Chan():
				if !ok {
					t.Fatal("query topic is closed")
				}
				query.AckID(cm.ID())
			}
		}
	}()

	go func() {
		cm, ok := <-query.Chan()
		query.AckID(cm.ID())
		if !ok {
			t.Fatal("query topic is closed")
		}
		var qm pb.QueryReqMsg
		if err := proto.Unmarshal(cm.Payload(), &qm); err != nil {
			t.Fatal(err)
		}
		if qm.ProxyId != ps.proxyId {
			t.Fatalf("search failed, incorrect proxy id = %d", qm.ProxyId)
		}
		if qm.CollectionName != "collection_search" {
			t.Fatalf("search failed, incorrect collection name = %s", qm.CollectionName)
		}
		r1 := pb.QueryResult{
			Status: &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
			Entities: &pb.Entities{
				Status:   &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
				Ids:      []int64{1, 3, 5},
				ValidRow: []bool{true, true, true},
				RowsData: []*pb.RowData{
					{Blob: uint64ToBytes(1)},
					{Blob: uint64ToBytes(3)},
					{Blob: uint64ToBytes(5)},
				},
			},
			RowNum:      3,
			Scores:      []float32{1, 3, 5},
			Distances:   []float32{1, 3, 5},
			ExtraParams: nil,
			QueryId:     qm.QueryId,
			ProxyId:     qm.ProxyId,
		}

		r2 := pb.QueryResult{
			Status: &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
			Entities: &pb.Entities{
				Status:   &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
				Ids:      []int64{2, 4, 6},
				ValidRow: []bool{true, false, true},
				RowsData: []*pb.RowData{
					{Blob: uint64ToBytes(2)},
					{Blob: uint64ToBytes(4)},
					{Blob: uint64ToBytes(6)},
				},
			},
			RowNum:      3,
			Scores:      []float32{2, 4, 6},
			Distances:   []float32{2, 4, 6},
			ExtraParams: nil,
			QueryId:     qm.QueryId,
			ProxyId:     qm.ProxyId,
		}
		b1, err := proto.Marshal(&r1)
		if err != nil {
			t.Fatal(err)
		}
		b2, err := proto.Marshal(&r2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result.Send(ps.ctx, &pulsar.ProducerMessage{Payload: b1}); err != nil {
			t.Fatal(err)
		}
		if _, err := result.Send(ps.ctx, &pulsar.ProducerMessage{Payload: b2}); err != nil {
			t.Fatal(err)
		}
	}()

	sm := pb.SearchParam{
		CollectionName: "collection_search",
		VectorParam:    nil,
		Dsl:            "",
		PartitionTag:   nil,
		ExtraParams:    nil,
	}

	serverClient := pb.NewMilvusServiceClient(conn)
	qr, err := serverClient.Search(ps.ctx, &sm)
	if err != nil {
		t.Fatal(err)
	}
	if qr.Status.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatal("query failed")
	}
	if qr.Entities.Status.ErrorCode != pb.ErrorCode_SUCCESS {
		t.Fatal("query failed")
	}

	if len(qr.Entities.Ids) != 3 {
		t.Fatal("query failed")
	}
	if qr.Entities.Ids[0] != 6 {
		t.Fatal("query failed")
	}
	if qr.Entities.Ids[1] != 5 {
		t.Fatal("query failed")
	}
	if qr.Entities.Ids[2] != 3 {
		t.Fatal("query failed")
	}

	if len(qr.Entities.ValidRow) != 3 {
		t.Fatal("query failed")
	}
	if qr.Entities.ValidRow[0] != true {
		t.Fatal("query failed")
	}
	if qr.Entities.ValidRow[1] != true {
		t.Fatal("query failed")
	}
	if qr.Entities.ValidRow[2] != true {
		t.Fatal("query failed")
	}

	if len(qr.Entities.RowsData) != 3 {
		t.Fatal("query failed")
	}
	if binary.LittleEndian.Uint64(qr.Entities.RowsData[0].Blob) != 6 {
		t.Fatal("query failed")
	}
	if binary.LittleEndian.Uint64(qr.Entities.RowsData[1].Blob) != 5 {
		t.Fatal("query failed")
	}
	if binary.LittleEndian.Uint64(qr.Entities.RowsData[2].Blob) != 3 {
		t.Fatal("query failed")
	}

	if len(qr.Scores) != 3 {
		t.Fatal("query failed")
	}
	if qr.Scores[0] != 6 {
		t.Fatal("query failed")
	}
	if qr.Scores[1] != 5 {
		t.Fatal("query failed")
	}
	if qr.Scores[2] != 3 {
		t.Fatal("query failed")
	}

	if len(qr.Distances) != 3 {
		t.Fatal("query failed")
	}
	if qr.Distances[0] != 6 {
		t.Fatal("query failed")
	}
	if qr.Distances[1] != 5 {
		t.Fatal("query failed")
	}
	if qr.Distances[2] != 3 {
		t.Fatal("query failed")
	}
}
