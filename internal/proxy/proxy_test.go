package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

var ctx context.Context
var cancel func()
var proxyAddress = "127.0.0.1:5053"
var proxyConn *grpc.ClientConn
var proxyClient servicepb.MilvusServiceClient

var proxyServer *Proxy

var masterServer *master.Master

var testNum = 10

func startMaster(ctx context.Context) {
	etcdAddr := conf.Config.Etcd.Address
	etcdAddr += ":"
	etcdAddr += strconv.FormatInt(int64(conf.Config.Etcd.Port), 10)
	rootPath := conf.Config.Etcd.Rootpath
	kvRootPath := path.Join(rootPath, "kv")
	metaRootPath := path.Join(rootPath, "meta")
	tsoRootPath := path.Join(rootPath, "timestamp")

	svr, err := master.CreateServer(ctx, kvRootPath, metaRootPath, tsoRootPath, []string{etcdAddr})
	masterServer = svr
	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}

	if err := svr.Run(int64(conf.Config.Master.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	fmt.Println("Waiting for server!", svr.IsServing())

}

func startProxy(ctx context.Context) {

	svr, err := CreateProxy(ctx)
	proxyServer = svr
	if err != nil {
		log.Print("create proxy failed", zap.Error(err))
	}

	// TODO: change to wait until master is ready
	if err := svr.Run(); err != nil {
		log.Fatal("run proxy failed", zap.Error(err))
	}
}

func setup() {
	conf.LoadConfig("config.yaml")
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	ctx, cancel = context.WithCancel(context.Background())

	startMaster(ctx)
	startProxy(ctx)

	conn, err := grpc.DialContext(ctx, proxyAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Connect to proxy failed, error= %v", err)
	}
	proxyConn = conn
	proxyClient = servicepb.NewMilvusServiceClient(proxyConn)

}

func shutdown() {
	cancel()
	masterServer.Close()
	proxyServer.Close()
}

func TestProxy_CreateCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i
		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		req := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "no description",
			AutoID:      true,
			Fields:      make([]*schemapb.FieldSchema, 1),
		}
		fieldName := "Field" + strconv.FormatInt(int64(i), 10)
		req.Fields[0] = &schemapb.FieldSchema{
			Name:        fieldName,
			Description: "no description",
			DataType:    schemapb.DataType_INT32,
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()

			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if !bool.Value {
				resp, err := proxyClient.CreateCollection(ctx, req)
				if err != nil {
					t.Error(err)
				}
				msg := "Create Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
			}
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

			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
			t.Logf("Has Collection %v: %v", i, bool.Value)
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

			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if bool.Value {
				resp, err := proxyClient.DescribeCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
				if err != nil {
					t.Error(err)
				}
				msg := "Describe Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				t.Logf("Describe Collection %v: %v", i, resp)
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

			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if bool.Value {
				resp, err := proxyClient.ShowCollections(ctx, &commonpb.Empty{})
				if err != nil {
					t.Error(err)
				}
				msg := "Show collections " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				t.Logf("Show collections %v: %v", i, resp)
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
			PartitionTag:   "",
			RowData:        make([]*commonpb.Blob, 0),
			HashKeys:       make([]int32, 0),
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if bool.Value {
				resp, err := proxyClient.Insert(ctx, req)
				if err != nil {
					t.Error(err)
				}
				msg := "Insert into Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
			}
		}(&wg)
	}
	wg.Wait()
}

/*
func TestProxy_Search(t *testing.T) {
	var wg sync.WaitGroup
	//buf := make(chan int, testNum)
	buf := make(chan int, 1)

	wg.Add(1)
	func(group *sync.WaitGroup) {
		defer wg.Done()
		queryResultChannels := []string{"QueryResult"}
		bufSize := 1024
		queryResultMsgStream := msgstream.NewPulsarMsgStream(ctx, int64(bufSize))
		pulsarAddress := "pulsar://localhost:6650"
		queryResultMsgStream.SetPulsarCient(pulsarAddress)
		assert.NotEqual(t, queryResultMsgStream, nil, "query result message stream should not be nil!")
		queryResultMsgStream.CreatePulsarProducers(queryResultChannels)

		for {
			select {
			case <-ctx.Done():
				t.Logf("query result message stream is closed ...")
				queryResultMsgStream.Close()
			case i := <- buf:
				log.Printf("receive query request, reqID: %v", i)
				for j := 0; j < 4; j++ {
					searchResultMsg := &msgstream.SearchResultMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []int32{1},
						},
						SearchResult: internalpb.SearchResult{
							MsgType: internalpb.MsgType_kSearchResult,
							ReqID: int64(i),
						},
					}
					msgPack := &msgstream.MsgPack{
						Msgs: make([]*msgstream.TsMsg, 1),
					}
					var tsMsg msgstream.TsMsg = searchResultMsg
					msgPack.Msgs[0] = &tsMsg
					log.Printf("proxy_test, produce message...")
					queryResultMsgStream.Produce(msgPack)
				}
			}
		}
	}(&wg)

	for i := 0; i < testNum; i++ {
		i := i

		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		req := &servicepb.Query{
			CollectionName: collectionName,
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			bool, err := proxyClient.HasCollection(ctx, &servicepb.CollectionName{CollectionName: collectionName})
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if bool.Value {
				log.Printf("Search: %v", collectionName)
				fn := func() error {
					buf <- i
					resp, err := proxyClient.Search(ctx, req)
					t.Logf("response of search collection %v: %v", i, resp)
					return err
				}
				err := Retry(10, time.Millisecond, fn)
				if err != nil {
					t.Error(err)
				}
			}
		}(&wg)
	}

	wg.Wait()
}

*/

func TestProxy_DropCollection(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < testNum; i++ {
		i := i

		collectionName := "CreateCollection" + strconv.FormatInt(int64(i), 10)
		req := &servicepb.CollectionName{
			CollectionName: collectionName,
		}

		wg.Add(1)
		go func(group *sync.WaitGroup) {
			defer group.Done()
			bool, err := proxyClient.HasCollection(ctx, req)
			if err != nil {
				t.Error(err)
			}
			msg := "Has Collection " + strconv.Itoa(i) + " should succeed!"
			assert.Equal(t, bool.Status.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)

			if bool.Value {
				resp, err := proxyClient.DropCollection(ctx, req)
				if err != nil {
					t.Error(err)
				}
				msg := "Drop Collection " + strconv.Itoa(i) + " should succeed!"
				assert.Equal(t, resp.ErrorCode, commonpb.ErrorCode_SUCCESS, msg)
				t.Logf("response of insert collection %v: %v", i, resp)
			}
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
