package proxynode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"go.etcd.io/etcd/clientv3"
)

var ctx context.Context
var cancel func()
var proxyConn *grpc.ClientConn
var proxyClient servicepb.MilvusServiceClient

var proxyServer *Proxy

var masterServer *master.Master

var testNum = 10

func makeNewChannalNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "_test" + strconv.FormatInt(rand.Int63n(100), 10)
	master.Params.DDChannelNames = makeNewChannalNames(master.Params.DDChannelNames, suffix)
	master.Params.WriteNodeTimeTickChannelNames = makeNewChannalNames(master.Params.WriteNodeTimeTickChannelNames, suffix)
	master.Params.InsertChannelNames = makeNewChannalNames(master.Params.InsertChannelNames, suffix)
	master.Params.K2SChannelNames = makeNewChannalNames(master.Params.K2SChannelNames, suffix)
	master.Params.ProxyTimeTickChannelNames = makeNewChannalNames(master.Params.ProxyTimeTickChannelNames, suffix)
}

func startMaster(ctx context.Context) {
	master.Init()
	refreshChannelNames()
	etcdAddr := master.Params.EtcdAddress
	metaRootPath := master.Params.MetaRootPath

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		panic(err)
	}
	_, err = etcdCli.Delete(context.TODO(), metaRootPath, clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}

	svr, err := master.CreateServer(ctx)
	masterServer = svr
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}
	if err := svr.Run(int64(master.Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())

}

func startProxy(ctx context.Context) {

	svr, err := CreateProxy(ctx)
	proxyServer = svr
	if err != nil {
		log.Print("create proxynode failed", zap.Error(err))
	}

	// TODO: change to wait until master is ready
	if err := svr.Start(); err != nil {
		log.Fatal("run proxynode failed", zap.Error(err))
	}
}

func setup() {
	Params.Init()
	ctx, cancel = context.WithCancel(context.Background())

	startMaster(ctx)
	startProxy(ctx)
	proxyAddr := Params.NetworkAddress()
	addr := strings.Split(proxyAddr, ":")
	if addr[0] == "0.0.0.0" {
		proxyAddr = "127.0.0.1:" + addr[1]
	}

	conn, err := grpc.DialContext(ctx, proxyAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Connect to proxynode failed, error= %v", err)
	}
	proxyConn = conn
	proxyClient = servicepb.NewMilvusServiceClient(proxyConn)

}

func shutdown() {
	cancel()
	masterServer.Close()
	proxyServer.Close()
}

func hasCollection(t *testing.T, name string) bool {
	resp, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: name})
	msg := "Has Collection " + name + " should succeed!"
	assert.Nil(t, err, msg)
	return resp.Value
}

func createCollection(t *testing.T, name string) {
	has := hasCollection(t, name)
	if has {
		dropCollection(t, name)
	}

	req := &schemapb.CollectionSchema{
		Name:        name,
		Description: "no description",
		AutoID:      true,
		Fields:      make([]*schemapb.FieldSchema, 2),
	}
	fieldName := "Field1"
	req.Fields[0] = &schemapb.FieldSchema{
		Name:        fieldName,
		Description: "no description",
		DataType:    schemapb.DataType_INT32,
	}
	fieldName = "vec"
	req.Fields[1] = &schemapb.FieldSchema{
		Name:        fieldName,
		Description: "vector",
		DataType:    schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}
	resp, err := proxyClient.CreateCollection(ctx, req)
	assert.Nil(t, err)
	msg := "Create Collection " + name + " should succeed!"
	assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
}

func dropCollection(t *testing.T, name string) {
	req := &servicepb.CollectionName{
		CollectionName: name,
	}
	resp, err := proxyClient.DropCollection(ctx, req)
	assert.Nil(t, err)
	msg := "Drop Collection " + name + " should succeed! err :" + resp.Reason
	assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
}

func createIndex(t *testing.T, collectionName, fieldName string) {

	req := &servicepb.IndexParam{
		CollectionName: collectionName,
		FieldName:      fieldName,
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}

	resp, err := proxyClient.CreateIndex(ctx, req)
	assert.Nil(t, err)
	msg := "Create Index for " + fieldName + " should succeed!"
	assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
}

func TestProxy_CreateCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			println("collectionName:", collectionName)
			createCollection(t, collectionName)
			dropCollection(t, collectionName)
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_HasCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			msg := "Should has Collection " + collectionName
			assert.Equal(t, has, true, msg)
			dropCollection(t, collectionName)
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_DescribeCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			if has {
				resp, err := proxyClient.DescribeCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
				if err != nil {
					t.Error(err)
				}
				msg := "Describe Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				t.Logf("Describe Collection %v: %v", i, resp)
				dropCollection(t, collectionName)
			}
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_ShowCollections(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			if has {
				resp, err := proxyClient.ShowCollections(ctx, &commonpb.Empty{})
				if err != nil {
					t.Error(err)
				}
				msg := "Show collections " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				t.Logf("Show collections %v: %v", i, resp)
				dropCollection(t, collectionName)
			}
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_Insert(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i

		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		req := &servicepb.RowBatch{
			CollectionName: collectionName,
			PartitionTag:   "haha",
			RowData:        make([]*commonpb.Blob, 0),
			HashKeys:       make([]uint32, 0),
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			if has {
				resp, err := proxyClient.Insert(ctx, req)
				if err != nil {
					t.Error(err)
				}
				msg := "Insert into Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				dropCollection(t, collectionName)
			}
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_Search(t *testing.T) {
	var sendWg sync.WaitGroup
	var queryWg sync.WaitGroup
	queryDone := make(chan int)

	sendWg.Add(1)
	go func(group *sync.WaitGroup) {
		defer group.Done()
		queryResultChannels := []string{"QueryResult"}
		bufSize := 1024
		queryResultMsgStream := msgstream.NewPulsarMsgStream(ctx, int64(bufSize))
		pulsarAddress := Params.PulsarAddress()
		queryResultMsgStream.SetPulsarClient(pulsarAddress)
		assert.NotEqual(t, queryResultMsgStream, nil, "query result message stream should not be nil!")
		queryResultMsgStream.CreatePulsarProducers(queryResultChannels)

		i := 0
		for {
			select {
			case <-ctx.Done():
				t.Logf("query result message stream is closed ...")
				queryResultMsgStream.Close()
				return
			case <-queryDone:
				return
			default:
				for j := 0; j < 4; j++ {
					searchResultMsg := &msgstream.SearchResultMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						SearchResults: internalpb2.SearchResults{
							Base: &commonpb.MsgBase{
								MsgType: commonpb.MsgType_kSearchResult,
								MsgID:   int64(i % testNum),
							},
						},
					}
					msgPack := &msgstream.MsgPack{
						Msgs: make([]msgstream.TsMsg, 1),
					}
					msgPack.Msgs[0] = searchResultMsg
					queryResultMsgStream.Produce(msgPack)
				}
				i++
			}
		}
	}(&sendWg)

	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		req := &servicepb.Query{
			CollectionName: collectionName,
		}
		queryWg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			//createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			if !has {
				createCollection(t, collectionName)
			}
			resp, err := proxyClient.Search(ctx, req)
			t.Logf("response of search collection %v: %v", i, resp)
			assert.Nil(t, err)
			dropCollection(t, collectionName)
		}(&queryWg)
	}

	t.Log("wait query to finish...")
	queryWg.Wait()
	t.Log("query finish ...")
	queryDone <- 1
	sendWg.Wait()
}

func TestProxy_AssignSegID(t *testing.T) {
	collectionName := "CreateCollection1"
	createCollection(t, collectionName)
	testNum := 1
	futureTS := tsoutil.ComposeTS(time.Now().Add(time.Second*-1000).UnixNano()/int64(time.Millisecond), 0)
	for i := 0; i < testNum; i++ {
		segID, err := proxyServer.segAssigner.GetSegmentID(collectionName, Params.defaultPartitionTag(), int32(i), 200000, futureTS)
		assert.Nil(t, err)
		fmt.Println("segID", segID)
	}

}

func TestProxy_DropCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collectionName)
			has := hasCollection(t, collectionName)
			if has {
				dropCollection(t, collectionName)
			}
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_PartitionGRPC(t *testing.T) {
	var wg sync.WaitGroup
	collName := "collPartTest"
	createCollection(t, collName)

	for i := 0; i < testNum; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			tag := fmt.Sprintf("partition_%d", i)
			preq := &servicepb.PartitionName{
				CollectionName: collName,
				Tag:            tag,
			}

			stb, err := proxyClient.HasPartition(ctx, preq)
			assert.Nil(t, err)
			assert.Equal(t, stb.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
			assert.Equal(t, stb.Value, false)

			st, err := proxyClient.CreatePartition(ctx, preq)
			assert.Nil(t, err)
			assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

			stb, err = proxyClient.HasPartition(ctx, preq)
			assert.Nil(t, err)
			assert.Equal(t, stb.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
			assert.Equal(t, stb.Value, true)

			//std, err := proxyClient.DescribePartition(ctx, preq)
			//assert.Nil(t, err)
			//assert.Equal(t, std.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

			sts, err := proxyClient.ShowPartitions(ctx, &servicepb.CollectionName{CollectionName: collName})
			assert.Nil(t, err)
			assert.Equal(t, sts.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)
			assert.True(t, len(sts.Values) >= 2)
			assert.True(t, len(sts.Values) <= testNum+1)

			st, err = proxyClient.DropPartition(ctx, preq)
			assert.Nil(t, err)
			assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)
		}()
	}
	wg.Wait()
	dropCollection(t, collName)
}

func TestProxy_CreateIndex(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < testNum; i++ {
		i := i
		collName := "collName" + strconv.FormatInt(int64(i), 10)
		fieldName := "Field1"
		if i%2 == 0 {
			fieldName = "vec"
		}
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collName)
			if i%2 == 0 {
				createIndex(t, collName, fieldName)
			}
			dropCollection(t, collName)
			// dropIndex(t, collectionName, fieldName, indexName)
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_DescribeIndex(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < testNum; i++ {
		i := i
		collName := "collName" + strconv.FormatInt(int64(i), 10)
		fieldName := "Field1"
		if i%2 == 0 {
			fieldName = "vec"
		}
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collName)
			if i%2 == 0 {
				createIndex(t, collName, fieldName)
			}
			req := &servicepb.DescribeIndexRequest{
				CollectionName: collName,
				FieldName:      fieldName,
			}
			resp, err := proxyClient.DescribeIndex(ctx, req)
			assert.Nil(t, err)
			msg := "Describe Index for " + fieldName + "should successed!"
			assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
			dropCollection(t, collName)
		}(&wg)
	}
	wg.Wait()
}

func TestProxy_DescribeIndexProgress(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < testNum; i++ {
		i := i
		collName := "collName" + strconv.FormatInt(int64(i), 10)
		fieldName := "Field1"
		if i%2 == 0 {
			fieldName = "vec"
		}
		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			createCollection(t, collName)
			if i%2 == 0 {
				createIndex(t, collName, fieldName)
			}
			req := &servicepb.DescribeIndexProgressRequest{
				CollectionName: collName,
				FieldName:      fieldName,
			}
			resp, err := proxyClient.DescribeIndexProgress(ctx, req)
			assert.Nil(t, err)
			msg := "Describe Index Progress for " + fieldName + "should succeed!"
			assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
			assert.True(t, resp.Value)
			dropCollection(t, collName)
		}(&wg)
	}
	wg.Wait()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}
