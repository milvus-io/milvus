package proxy_node

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	mpb "github.com/czs007/suvlim/pkg/master/grpc/master"
	pb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestProxyNode(t *testing.T) {
	startTestMaster("localhost:11000", t)
	testOpt := ProxyOptions{
		address:                "localhost:11001",
		master_address:         "localhost:11000",
		collectionMetaRootPath: "/collections/meta",
		pulsarAddr:             "pulsar://localhost:6650",
		readerTopicsPrefix:     "reader-",
		numReadTopics:          2,
		deleteTopic:            "deleteT",
		queryTopic:             "queryT",
		resultTopic:            "resultT",
		resultGroup:            "resultG",
		numReaderNode:          2,
		proxyId:                1,
		etcdEndpoints:          []string{"127.0.0.1:2379"},
		tsoRootPath:            "/tso",
		tsoSaveInterval:        200,
		timeTickInterval:       200,
		timeTickTopic:          "timetick",
		timeTickPeerId:         1,
	}
	if err := StartProxy(&testOpt); err != nil {
		t.Fatal(err)
	}

	startTime := uint64(time.Now().UnixNano()) / uint64(1e6)
	t.Logf("start time stamp = %d", startTime)

	etcdClient, err := etcd.New(etcd.Config{Endpoints: testOpt.etcdEndpoints})
	assert.Nil(t, err)
	//defer etcdClient.Close()

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: testOpt.pulsarAddr})
	assert.Nil(t, err)
	defer pulsarClient.Close()

	go func() {
		time.Sleep(time.Second)
		for {
			ts, err := etcdClient.Get(testOpt.ctx, testOpt.tsoRootPath+tsoKeyPath)
			assert.Nil(t, err)
			if len(ts.Kvs) != 1 {
				t.Fatalf("save tso into etcd falied")
			}
			value, err := strconv.ParseUint(string(ts.Kvs[0].Value), 10, 64)
			assert.Nil(t, err)

			curValue, st := testOpt.tso.GetTimestamp(1)
			assert.Equalf(t, st.ErrorCode, pb.ErrorCode_SUCCESS, "%s", st.Reason)

			curTime := ToPhysicalTime(uint64(curValue[0]))
			t.Logf("current time stamp = %d, saved time stamp = %d", curTime, value)
			assert.GreaterOrEqual(t, uint64(curValue[0]), value)
			assert.GreaterOrEqual(t, value, startTime)
			time.Sleep(time.Duration(testOpt.tsoSaveInterval) * time.Millisecond)
		}
	}()

	tickComsumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       testOpt.timeTickTopic,
		SubscriptionName:            testOpt.timeTickTopic + "G",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer tickComsumer.Close()

	reader, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topics:                      testOpt.proxyServer.readerTopics,
		SubscriptionName:            testOpt.readerTopicsPrefix + "G",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer reader.Close()

	query, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       testOpt.queryTopic,
		SubscriptionName:            testOpt.queryTopic + "G",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer query.Close()

	deleteC, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       testOpt.deleteTopic,
		SubscriptionName:            testOpt.deleteTopic + "G",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer deleteC.Close()

	result, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: testOpt.resultTopic})
	assert.Nil(t, err)
	defer result.Close()

	tick := time.Tick(500 * time.Millisecond)
	// read pulsar channel until empty
	func() {
		cnt := 0
		for {
			select {
			case <-tick:
				cnt++
				if cnt >= 3 {
					return
				}

			case cm, ok := <-tickComsumer.Chan():
				assert.Truef(t, ok, "time tick consumer topic has closed")
				tickComsumer.AckID(cm.ID())
			case cm, ok := <-reader.Chan():
				assert.Truef(t, ok, "reader comsumer topic has closed")
				reader.AckID(cm.ID())
			case cm, ok := <-deleteC.Chan():
				assert.Truef(t, ok, "delete topic has closed")
				deleteC.AckID(cm.ID())
			case cm, ok := <-query.Chan():
				assert.Truef(t, ok, "query topic has closed")
				query.AckID(cm.ID())
			}
		}
	}()
	go func() {
		lastT := startTime
		for {
			cm, ok := <-tickComsumer.Chan()
			assert.Truef(t, ok, "time tick consumer topic has closed")
			tickComsumer.AckID(cm.ID())
			var tsm pb.TimeSyncMsg
			if err := proto.Unmarshal(cm.Payload(), &tsm); err != nil {
				t.Fatal(err)
			}
			curT := ToPhysicalTime(tsm.Timestamp)
			t.Logf("time tick = %d", curT)
			assert.Greater(t, curT, lastT)
			lastT = curT
		}
	}()

	cm100 := mpb.Collection{
		Id:            100,
		Name:          "cm100",
		Schema:        nil,
		CreateTime:    0,
		SegmentIds:    []uint64{101, 102},
		PartitionTags: nil,
		Indexes:       nil,
	}
	sm101 := mpb.Segment{
		SegmentId:    101,
		CollectionId: 100,
		ChannelStart: 0,
		ChannelEnd:   1,
		Status:       mpb.SegmentStatus_OPENED,
	}
	sm102 := mpb.Segment{
		SegmentId:    102,
		CollectionId: 100,
		ChannelStart: 1,
		ChannelEnd:   2,
		Status:       mpb.SegmentStatus_OPENED,
	}
	if cm100b, err := json.Marshal(&cm100); err != nil {
		t.Fatal(err)
	} else if _, err := etcdClient.Put(testOpt.ctx, testOpt.collectionMetaRootPath+"/"+keyCollectionPath+"/100", string(cm100b)); err != nil {
		t.Fatal(err)
	}
	if sm101b, err := json.Marshal(&sm101); err != nil {
		t.Fatal(err)
	} else if _, err := etcdClient.Put(testOpt.ctx, testOpt.collectionMetaRootPath+"/"+keySegmentPath+"/101", string(sm101b)); err != nil {
		t.Fatal(err)
	}
	if sm102b, err := json.Marshal(&sm102); err != nil {
		t.Fatal(err)
	} else if _, err := etcdClient.Put(testOpt.ctx, testOpt.collectionMetaRootPath+"/"+keySegmentPath+"/102", string(sm102b)); err != nil {
		t.Fatal(err)
	}

	ctx1, _ := context.WithTimeout(testOpt.ctx, time.Second)
	grpcConn, err := grpc.DialContext(ctx1, testOpt.address, grpc.WithInsecure(), grpc.WithBlock())
	assert.Nil(t, err)
	defer grpcConn.Close()
	proxyClient := pb.NewMilvusServiceClient(grpcConn)

	insertParm := pb.InsertParam{
		CollectionName: "cm100",
		Schema:         nil,
		RowsData: []*pb.RowData{
			{Blob: uint64ToBytes(10)},
			{Blob: uint64ToBytes(11)},
			{Blob: uint64ToBytes(12)},
			{Blob: uint64ToBytes(13)},
			{Blob: uint64ToBytes(14)},
			{Blob: uint64ToBytes(15)},
		},
		EntityIdArray: []int64{10, 11, 12, 13, 14, 15},
		PartitionTag:  "",
		ExtraParams:   nil,
	}
	deleteParm := pb.DeleteByIDParam{
		CollectionName: "cm100",
		IdArray:        []int64{20, 21},
	}

	searchParm := pb.SearchParam{
		CollectionName: "cm100",
		VectorParam:    nil,
		Dsl:            "",
		PartitionTag:   nil,
		ExtraParams:    nil,
	}

	go func() {
		cm, ok := <-query.Chan()
		assert.Truef(t, ok, "query topic has closed")
		query.AckID(cm.ID())
		var qm pb.QueryReqMsg
		if err := proto.Unmarshal(cm.Payload(), &qm); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, qm.ProxyId, testOpt.proxyId)
		assert.Equal(t, qm.CollectionName, "cm100")

		t.Logf("query time stamp = %d", ToPhysicalTime(qm.Timestamp))
		assert.Greater(t, ToPhysicalTime(qm.Timestamp), startTime)

		r1 := pb.QueryResult{
			Status: &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
			Entities: &pb.Entities{
				Status:   &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
				Ids:      []int64{11, 13, 15},
				ValidRow: []bool{true, true, true},
				RowsData: []*pb.RowData{
					{Blob: uint64ToBytes(11)},
					{Blob: uint64ToBytes(13)},
					{Blob: uint64ToBytes(15)},
				},
			},
			RowNum:      3,
			Scores:      []float32{11, 13, 15},
			Distances:   []float32{11, 13, 15},
			ExtraParams: nil,
			QueryId:     qm.QueryId,
			ProxyId:     qm.ProxyId,
		}

		r2 := pb.QueryResult{
			Status: &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
			Entities: &pb.Entities{
				Status:   &pb.Status{ErrorCode: pb.ErrorCode_SUCCESS},
				Ids:      []int64{12, 14, 16},
				ValidRow: []bool{true, false, true},
				RowsData: []*pb.RowData{
					{Blob: uint64ToBytes(12)},
					{Blob: uint64ToBytes(14)},
					{Blob: uint64ToBytes(16)},
				},
			},
			RowNum:      3,
			Scores:      []float32{12, 14, 16},
			Distances:   []float32{12, 14, 16},
			ExtraParams: nil,
			QueryId:     qm.QueryId,
			ProxyId:     qm.ProxyId,
		}
		if b1, err := proto.Marshal(&r1); err != nil {
			t.Fatal(err)
		} else if _, err := result.Send(testOpt.ctx, &pulsar.ProducerMessage{Payload: b1}); err != nil {
			t.Fatal(err)
		}
		if b2, err := proto.Marshal(&r2); err != nil {
			t.Fatal(err)
		} else if _, err := result.Send(testOpt.ctx, &pulsar.ProducerMessage{Payload: b2}); err != nil {
			t.Fatal(err)
		}
	}()

	insertR, err := proxyClient.Insert(testOpt.ctx, &insertParm)
	assert.Nil(t, err)
	assert.Equalf(t, insertR.Status.ErrorCode, pb.ErrorCode_SUCCESS, "%s", insertR.Status.Reason)

	assert.Equal(t, len(insertR.EntityIdArray), 6)

	sort.Slice(insertR.EntityIdArray, func(i, j int) bool {
		return insertR.EntityIdArray[i] < insertR.EntityIdArray[j]
	})
	for i := 0; i < len(insertR.EntityIdArray); i++ {
		assert.Equal(t, insertR.EntityIdArray[i], int64(i+10))
	}

	var insertPrimaryKey []uint64
	readerM1, ok := <-reader.Chan()
	assert.True(t, ok)

	reader.AckID(readerM1.ID())
	var m1 pb.ManipulationReqMsg
	if err := proto.UnmarshalMerge(readerM1.Payload(), &m1); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, m1.CollectionName, "cm100")
	assert.Equal(t, len(m1.PrimaryKeys), len(m1.RowsData))

	t.Logf("reader time stamp = %d", ToPhysicalTime(m1.Timestamp))
	assert.GreaterOrEqual(t, ToPhysicalTime(m1.Timestamp), startTime)

	for i, k := range m1.PrimaryKeys {
		insertPrimaryKey = append(insertPrimaryKey, k)
		rowValue := binary.LittleEndian.Uint64(m1.RowsData[i].Blob)
		t.Logf("insert primary key = %d, row data= %d", k, rowValue)
		assert.Equal(t, k, rowValue)
	}

	readerM2, ok := <-reader.Chan()
	assert.True(t, ok)
	reader.AckID(readerM2.ID())

	var m2 pb.ManipulationReqMsg
	if err := proto.UnmarshalMerge(readerM2.Payload(), &m2); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, m2.CollectionName, "cm100")
	assert.Equal(t, len(m2.PrimaryKeys), len(m2.RowsData))

	t.Logf("read time stamp = %d", ToPhysicalTime(m2.Timestamp))
	assert.GreaterOrEqual(t, ToPhysicalTime(m2.Timestamp), startTime)

	for i, k := range m2.PrimaryKeys {
		insertPrimaryKey = append(insertPrimaryKey, k)
		rowValue := binary.LittleEndian.Uint64(m2.RowsData[i].Blob)
		t.Logf("insert primary key = %d, row data= %d", k, rowValue)
		assert.Equal(t, k, rowValue)
	}
	sort.Slice(insertPrimaryKey, func(i, j int) bool {
		return insertPrimaryKey[i] < insertPrimaryKey[j]
	})

	assert.Equal(t, len(insertPrimaryKey), 6)
	for i := 0; i < len(insertPrimaryKey); i++ {
		assert.Equal(t, insertPrimaryKey[i], uint64(i+10))
	}

	deleteR, err := proxyClient.DeleteByID(testOpt.ctx, &deleteParm)
	assert.Nil(t, err)
	assert.Equal(t, deleteR.ErrorCode, pb.ErrorCode_SUCCESS)

	deleteM, ok := <-deleteC.Chan()
	assert.True(t, ok)
	deleteC.AckID(deleteM.ID())
	var dm pb.ManipulationReqMsg
	if err := proto.UnmarshalMerge(deleteM.Payload(), &dm); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, dm.CollectionName, "cm100")
	assert.Equal(t, len(dm.PrimaryKeys), 2)

	t.Logf("delete time stamp = %d", ToPhysicalTime(dm.Timestamp))
	assert.GreaterOrEqual(t, ToPhysicalTime(dm.Timestamp), startTime)

	for i := 0; i < len(dm.PrimaryKeys); i++ {
		assert.Equal(t, dm.PrimaryKeys[i], uint64(i+20))
	}

	searchR, err := proxyClient.Search(testOpt.ctx, &searchParm)
	assert.Nil(t, err)
	assert.Equal(t, searchR.Status.ErrorCode, pb.ErrorCode_SUCCESS)
	assert.Equal(t, searchR.Entities.Status.ErrorCode, pb.ErrorCode_SUCCESS)

	assert.Equal(t, len(searchR.Entities.Ids), 3)
	assert.Equal(t, searchR.Entities.Ids, []int64{16, 15, 13})

	assert.Equal(t, len(searchR.Entities.ValidRow), 3)
	assert.Equal(t, searchR.Entities.ValidRow, []bool{true, true, true})

	assert.Equal(t, len(searchR.Entities.RowsData), 3)
	assert.Equal(t, searchR.Entities.RowsData, []*pb.RowData{
		{Blob: uint64ToBytes(16)},
		{Blob: uint64ToBytes(15)},
		{Blob: uint64ToBytes(13)},
	})

	assert.Equal(t, len(searchR.Scores), 3)
	assert.Equal(t, searchR.Scores, []float32{16, 15, 13})

	assert.Equal(t, len(searchR.Distances), 3)
	assert.Equal(t, searchR.Distances, []float32{16, 15, 13})

	time.Sleep(time.Second)
}

func TestReadProxyOptionsFromConfig(t *testing.T) {
	conf, err := ReadProxyOptionsFromConfig()
	assert.Nil(t, err)
	t.Log(conf.address)
	t.Log(conf.master_address)
	t.Log(conf.collectionMetaRootPath)
	t.Log(conf.pulsarAddr)
	t.Log(conf.readerTopicsPrefix)
	t.Log(conf.numReadTopics)
	t.Log(conf.deleteTopic)
	t.Log(conf.queryTopic)
	t.Log(conf.resultTopic)
	t.Log(conf.resultGroup)
	t.Log(conf.numReaderNode)
	t.Log(conf.proxyId)
	t.Log(conf.etcdEndpoints)
	t.Log(conf.tsoRootPath)
	t.Log(conf.tsoSaveInterval)
	t.Log(conf.timeTickInterval)
	t.Log(conf.timeTickTopic)
	t.Log(conf.timeTickPeerId)
}
