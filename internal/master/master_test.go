package master

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var testPORT = 53200

func genMasterTestPort() int64 {
	testPORT++
	return int64(testPORT)
}

func refreshMasterAddress() {
	masterPort := genMasterTestPort()
	Params.Port = int(masterPort)
	masterAddr := makeMasterAddress(masterPort)
	Params.Address = masterAddr
}

func makeMasterAddress(port int64) string {
	masterAddr := "127.0.0.1:" + strconv.FormatInt(port, 10)
	return masterAddr
}

func makeNewChannalNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "_test" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannalNames(Params.DDChannelNames, suffix)
	Params.WriteNodeTimeTickChannelNames = makeNewChannalNames(Params.WriteNodeTimeTickChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannalNames(Params.InsertChannelNames, suffix)
	Params.K2SChannelNames = makeNewChannalNames(Params.K2SChannelNames, suffix)
	Params.ProxyTimeTickChannelNames = makeNewChannalNames(Params.ProxyTimeTickChannelNames, suffix)
	Params.QueryNodeStatsChannelName = Params.QueryNodeStatsChannelName + suffix
	Params.MetaRootPath = "/test" + strconv.FormatInt(rand.Int63n(100), 10) + "/root/kv"
}

func receiveTimeTickMsg(stream *ms.MsgStream) bool {
	result := (*stream).Consume()
	return result != nil
}

func getTimeTickMsgPack(ttmsgs [][2]uint64) *ms.MsgPack {
	msgPack := ms.MsgPack{}
	for _, vi := range ttmsgs {
		msgPack.Msgs = append(msgPack.Msgs, getTtMsg(commonpb.MsgType_kTimeTick, UniqueID(vi[0]), Timestamp(vi[1])))
	}
	return &msgPack
}

func mockTimeTickBroadCast(msgStream ms.MsgStream, time Timestamp) error {
	timeTick := [][2]uint64{
		{0, time},
	}
	ttMsgPackForDD := getTimeTickMsgPack(timeTick)
	return msgStream.Broadcast(ttMsgPackForDD)
}

func TestMaster(t *testing.T) {
	Init()
	refreshMasterAddress()
	refreshChannelNames()
	etcdAddr := Params.EtcdAddress
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	_, err = etcdCli.Delete(context.Background(), Params.MetaRootPath, clientv3.WithPrefix())
	assert.Nil(t, err)

	gTestTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{etcdAddr}, Params.MetaRootPath, "tso"))
	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddr}, Params.MetaRootPath, "gid"))
	pulsarAddr := Params.PulsarAddress
	Params.ProxyIDList = []UniqueID{0}
	//Param
	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := CreateServer(ctx)

	if err != nil {
		log.Print("create server failed", zap.Error(err))
	}

	if err := svr.Run(int64(Params.Port)); err != nil {
		log.Fatal("run server failed", zap.Error(err))
	}

	conn, err := grpc.DialContext(ctx, Params.Address, grpc.WithInsecure(), grpc.WithBlock())
	require.Nil(t, err)

	cli := masterpb.NewMasterServiceClient(conn)

	//t.Run("TestConfigTask", func(t *testing.T) {
	//	testKeys := []string{
	//		"/etcd/address",
	//		"/master/port",
	//		"/master/proxyidlist",
	//		"/master/segmentthresholdfactor",
	//		"/pulsar/token",
	//		"/reader/stopflag",
	//		"/proxy/timezone",
	//		"/proxy/network/address",
	//		"/proxy/storage/path",
	//		"/storage/accesskey",
	//	}
	//
	//	testVals := []string{
	//		"localhost",
	//		"53100",
	//		"[1 2]",
	//		"0.75",
	//		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY",
	//		"-1",
	//		"UTC+8",
	//		"0.0.0.0",
	//		"/var/lib/milvus",
	//		"",
	//	}
	//
	//	sc := SysConfig{kv: svr.kvBase}
	//	sc.InitFromFile(".")
	//
	//	configRequest := &internalpb.SysConfigRequest{
	//		MsgType:     commonpb.MsgType_kGetSysConfigs,
	//		ReqID:       1,
	//		Timestamp:   uint64(time.Now().Unix()),
	//		ProxyID:     1,
	//		Keys:        testKeys,
	//		KeyPrefixes: []string{},
	//	}
	//
	//	response, err := cli.GetSysConfigs(ctx, configRequest)
	//	assert.Nil(t, err)
	//	assert.ElementsMatch(t, testKeys, response.Keys)
	//	assert.ElementsMatch(t, testVals, response.Values)
	//	assert.Equal(t, len(response.GetKeys()), len(response.GetValues()))
	//
	//	configRequest = &internalpb.SysConfigRequest{
	//		MsgType:     commonpb.MsgType_kGetSysConfigs,
	//		ReqID:       1,
	//		Timestamp:   uint64(time.Now().Unix()),
	//		ProxyID:     1,
	//		Keys:        []string{},
	//		KeyPrefixes: []string{"/master"},
	//	}
	//
	//	response, err = cli.GetSysConfigs(ctx, configRequest)
	//	assert.Nil(t, err)
	//	for i := range response.GetKeys() {
	//		assert.True(t, strings.HasPrefix(response.GetKeys()[i], "/master"))
	//	}
	//	assert.Equal(t, len(response.GetKeys()), len(response.GetValues()))
	//
	//})
	//
	//t.Run("TestConfigDuplicateKeysAndKeyPrefix", func(t *testing.T) {
	//	configRequest := &internalpb.SysConfigRequest{}
	//	configRequest.Keys = []string{}
	//	configRequest.KeyPrefixes = []string{"/master"}
	//
	//	configRequest.Timestamp = uint64(time.Now().Unix())
	//	resp, err := cli.GetSysConfigs(ctx, configRequest)
	//	require.Nil(t, err)
	//	assert.Equal(t, len(resp.GetKeys()), len(resp.GetValues()))
	//	assert.NotEqual(t, 0, len(resp.GetKeys()))
	//
	//	configRequest.Keys = []string{"/master/port"}
	//	configRequest.KeyPrefixes = []string{"/master"}
	//
	//	configRequest.Timestamp = uint64(time.Now().Unix())
	//	respDup, err := cli.GetSysConfigs(ctx, configRequest)
	//	require.Nil(t, err)
	//	assert.Equal(t, len(respDup.GetKeys()), len(respDup.GetValues()))
	//	assert.NotEqual(t, 0, len(respDup.GetKeys()))
	//	assert.Equal(t, len(respDup.GetKeys()), len(resp.GetKeys()))
	//})

	t.Run("TestCollectionTask", func(t *testing.T) {
		sch := schemapb.CollectionSchema{
			Name:        "col1",
			Description: "test collection",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "col1_f1",
					Description: "test collection filed 1",
					DataType:    schemapb.DataType_VECTOR_FLOAT,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f1_tk1",
							Value: "col1_f1_tv1",
						},
						{
							Key:   "col1_f1_tk2",
							Value: "col1_f1_tv2",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f1_ik1",
							Value: "col1_f1_iv1",
						},
						{
							Key:   "col1_f1_ik2",
							Value: "col1_f1_iv2",
						},
					},
				},
				{
					Name:        "col1_f2",
					Description: "test collection filed 2",
					DataType:    schemapb.DataType_VECTOR_BINARY,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f2_tk1",
							Value: "col1_f2_tv1",
						},
						{
							Key:   "col1_f2_tk2",
							Value: "col1_f2_tv2",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f2_ik1",
							Value: "col1_f2_iv1",
						},
						{
							Key:   "col1_f2_ik2",
							Value: "col1_f2_iv2",
						},
					},
				},
			},
		}
		schemaBytes, err := proto.Marshal(&sch)
		assert.Nil(t, err)

		createCollectionReq := milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreateCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			Schema: schemaBytes,
		}
		log.Printf("... [Create] collection col1\n")
		st, err := cli.CreateCollection(ctx, &createCollectionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// HasCollection
		reqHasCollection := milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}

		// "col1" is true
		log.Printf("... [Has] collection col1\n")
		boolResp, err := cli.HasCollection(ctx, &reqHasCollection)
		assert.Nil(t, err)
		assert.Equal(t, true, boolResp.Value)
		assert.Equal(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// "colNotExist" is false
		reqHasCollection.CollectionName = "colNotExist"
		boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
		assert.Nil(t, err)
		assert.Equal(t, boolResp.Value, false)
		assert.Equal(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// error
		reqHasCollection.Base.Timestamp = Timestamp(0)
		reqHasCollection.CollectionName = "col1"
		boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
		assert.Nil(t, err)
		assert.NotEqual(t, boolResp.Status.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// ShowCollection
		reqShowCollection := milvuspb.ShowCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowCollections,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
		}

		listResp, err := cli.ShowCollections(ctx, &reqShowCollection)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
		assert.Equal(t, 1, len(listResp.CollectionNames))
		assert.Equal(t, "col1", listResp.CollectionNames[0])

		reqShowCollection.Base.Timestamp = Timestamp(0)
		listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)

		// CreateCollection Test
		collMeta, err := svr.metaTable.GetCollectionByName(sch.Name)
		assert.Nil(t, err)
		t.Logf("collection id = %d", collMeta.ID)
		assert.Equal(t, collMeta.Schema.Name, "col1")
		assert.Equal(t, collMeta.Schema.AutoID, false)
		assert.Equal(t, len(collMeta.Schema.Fields), 4)
		assert.Equal(t, collMeta.Schema.Fields[0].Name, "col1_f1")
		assert.Equal(t, collMeta.Schema.Fields[1].Name, "col1_f2")
		assert.Equal(t, collMeta.Schema.Fields[2].Name, "RowID")
		assert.Equal(t, collMeta.Schema.Fields[3].Name, "Timestamp")
		assert.Equal(t, collMeta.Schema.Fields[0].DataType, schemapb.DataType_VECTOR_FLOAT)
		assert.Equal(t, collMeta.Schema.Fields[1].DataType, schemapb.DataType_VECTOR_BINARY)
		assert.Equal(t, collMeta.Schema.Fields[2].DataType, schemapb.DataType_INT64)
		assert.Equal(t, collMeta.Schema.Fields[3].DataType, schemapb.DataType_INT64)
		assert.Equal(t, len(collMeta.Schema.Fields[0].TypeParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[0].IndexParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[1].TypeParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[1].IndexParams), 2)
		assert.Equal(t, int64(100), collMeta.Schema.Fields[0].FieldID)
		assert.Equal(t, int64(101), collMeta.Schema.Fields[1].FieldID)
		assert.Equal(t, int64(0), collMeta.Schema.Fields[2].FieldID)
		assert.Equal(t, int64(1), collMeta.Schema.Fields[3].FieldID)
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Key, "col1_f1_tk1")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Key, "col1_f1_tk2")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Value, "col1_f1_tv1")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Value, "col1_f1_tv2")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Key, "col1_f1_ik1")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Key, "col1_f1_ik2")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Value, "col1_f1_iv1")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Value, "col1_f1_iv2")

		assert.Equal(t, int64(101), collMeta.Schema.Fields[1].FieldID)
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Key, "col1_f2_tk1")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Key, "col1_f2_tk2")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Value, "col1_f2_tv1")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Value, "col1_f2_tv2")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Key, "col1_f2_ik1")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Key, "col1_f2_ik2")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Value, "col1_f2_iv1")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Value, "col1_f2_iv2")

		createCollectionReq.Base.Timestamp = Timestamp(0)
		st, err = cli.CreateCollection(ctx, &createCollectionReq)
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// DescribeCollection Test
		reqDescribe := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDescribeCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}
		des, err := cli.DescribeCollection(ctx, reqDescribe)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)

		assert.Equal(t, "col1", des.Schema.Name)
		assert.Equal(t, false, des.Schema.AutoID)
		assert.Equal(t, 2, len(des.Schema.Fields))
		assert.Equal(t, "col1_f1", des.Schema.Fields[0].Name)
		assert.Equal(t, "col1_f2", des.Schema.Fields[1].Name)
		assert.Equal(t, schemapb.DataType_VECTOR_FLOAT, des.Schema.Fields[0].DataType)
		assert.Equal(t, schemapb.DataType_VECTOR_BINARY, des.Schema.Fields[1].DataType)
		assert.Equal(t, 2, len(des.Schema.Fields[0].TypeParams))
		assert.Equal(t, 2, len(des.Schema.Fields[0].IndexParams))
		assert.Equal(t, 2, len(des.Schema.Fields[1].TypeParams))
		assert.Equal(t, 2, len(des.Schema.Fields[1].IndexParams))
		assert.Equal(t, int64(100), des.Schema.Fields[0].FieldID)
		assert.Equal(t, "col1_f1_tk1", des.Schema.Fields[0].TypeParams[0].Key)
		assert.Equal(t, "col1_f1_tv1", des.Schema.Fields[0].TypeParams[0].Value)
		assert.Equal(t, "col1_f1_ik1", des.Schema.Fields[0].IndexParams[0].Key)
		assert.Equal(t, "col1_f1_iv1", des.Schema.Fields[0].IndexParams[0].Value)
		assert.Equal(t, "col1_f1_tk2", des.Schema.Fields[0].TypeParams[1].Key)
		assert.Equal(t, "col1_f1_tv2", des.Schema.Fields[0].TypeParams[1].Value)
		assert.Equal(t, "col1_f1_ik2", des.Schema.Fields[0].IndexParams[1].Key)
		assert.Equal(t, "col1_f1_iv2", des.Schema.Fields[0].IndexParams[1].Value)

		assert.Equal(t, int64(101), des.Schema.Fields[1].FieldID)
		assert.Equal(t, "col1_f2_tk1", des.Schema.Fields[1].TypeParams[0].Key)
		assert.Equal(t, "col1_f2_tv1", des.Schema.Fields[1].TypeParams[0].Value)
		assert.Equal(t, "col1_f2_ik1", des.Schema.Fields[1].IndexParams[0].Key)
		assert.Equal(t, "col1_f2_iv1", des.Schema.Fields[1].IndexParams[0].Value)
		assert.Equal(t, "col1_f2_tk2", des.Schema.Fields[1].TypeParams[1].Key)
		assert.Equal(t, "col1_f2_tv2", des.Schema.Fields[1].TypeParams[1].Value)
		assert.Equal(t, "col1_f2_ik2", des.Schema.Fields[1].IndexParams[1].Key)
		assert.Equal(t, "col1_f2_iv2", des.Schema.Fields[1].IndexParams[1].Value)

		reqDescribe.CollectionName = "colNotExist"
		des, err = cli.DescribeCollection(ctx, reqDescribe)
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)
		log.Printf(des.Status.Reason)

		reqDescribe.CollectionName = "col1"
		reqDescribe.Base.Timestamp = Timestamp(0)
		des, err = cli.DescribeCollection(ctx, reqDescribe)
		assert.Nil(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_SUCCESS, des.Status.ErrorCode)
		log.Printf(des.Status.Reason)

		// ------------------------------DropCollectionTask---------------------------
		log.Printf("... [Drop] collection col1\n")

		reqDrop := milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}

		// DropCollection
		st, err = cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		collMeta, err = svr.metaTable.GetCollectionByName(sch.Name)
		assert.NotNil(t, err)

		// HasCollection "col1" is false
		reqHasCollection.Base.Timestamp = uint64(time.Now().Unix())
		reqHasCollection.CollectionName = "col1"
		boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
		assert.Nil(t, err)
		assert.Equal(t, false, boolResp.Value)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, boolResp.Status.ErrorCode)

		// ShowCollections
		reqShowCollection.Base.Timestamp = uint64(time.Now().Unix())
		listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
		assert.Equal(t, 0, len(listResp.CollectionNames))

		// Drop again
		st, err = cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.NotEqual(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		// Create "col1"
		createCollectionReq.Base.Timestamp = uint64(time.Now().Unix())

		st, err = cli.CreateCollection(ctx, &createCollectionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		reqHasCollection.Base.Timestamp = uint64(time.Now().Unix())
		boolResp, err = cli.HasCollection(ctx, &reqHasCollection)
		assert.Nil(t, err)
		assert.Equal(t, true, boolResp.Value)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, boolResp.Status.ErrorCode)

		// Create "col2"
		sch.Name = "col2"
		schemaBytes, err = proto.Marshal(&sch)
		assert.Nil(t, err)

		createCollectionReq = milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreateCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			Schema: schemaBytes,
		}
		st, err = cli.CreateCollection(ctx, &createCollectionReq)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

		// Show Collections
		reqShowCollection.Base.Timestamp = uint64(time.Now().Unix())
		listResp, err = cli.ShowCollections(ctx, &reqShowCollection)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, listResp.Status.ErrorCode)
		assert.Equal(t, 2, len(listResp.CollectionNames))
		assert.ElementsMatch(t, []string{"col1", "col2"}, listResp.CollectionNames)

		// Drop Collection

		reqDrop = milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}

		// DropCollection
		st, err = cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		reqDrop = milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col2",
		}

		// DropCollection
		st, err = cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		time.Sleep(1000 * time.Millisecond)
		timestampNow := Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		//consume msg
		ddMs := ms.NewPulsarTtMsgStream(ctx, 1024)
		ddMs.SetPulsarClient(pulsarAddr)
		ddMs.CreatePulsarConsumers(Params.DDChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
		ddMs.Start()

		var consumeMsg ms.MsgStream = ddMs
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				break
			}
		}
		ddMs.Close()
	})

	t.Run("TestPartitionTask", func(t *testing.T) {
		sch := schemapb.CollectionSchema{
			Name:        "col1",
			Description: "test collection",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "col1_f1",
					Description: "test collection filed 1",
					DataType:    schemapb.DataType_VECTOR_FLOAT,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f1_tk1",
							Value: "col1_f1_tv1",
						},
						{
							Key:   "col1_f1_tk2",
							Value: "col1_f1_tv2",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f1_ik1",
							Value: "col1_f1_iv1",
						},
						{
							Key:   "col1_f1_ik2",
							Value: "col1_f1_iv2",
						},
					},
				},
				{
					Name:        "col1_f2",
					Description: "test collection filed 2",
					DataType:    schemapb.DataType_VECTOR_BINARY,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f2_tk1",
							Value: "col1_f2_tv1",
						},
						{
							Key:   "col1_f2_tk2",
							Value: "col1_f2_tv2",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "col1_f2_ik1",
							Value: "col1_f2_iv1",
						},
						{
							Key:   "col1_f2_ik2",
							Value: "col1_f2_iv2",
						},
					},
				},
			},
		}
		schemaBytes, err := proto.Marshal(&sch)
		assert.Nil(t, err)

		createCollectionReq := milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			Schema: schemaBytes,
		}
		st, _ := cli.CreateCollection(ctx, &createCollectionReq)
		assert.NotNil(t, st)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

		createPartitionReq := milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition1",
		}
		st, _ = cli.CreatePartition(ctx, &createPartitionReq)
		assert.NotNil(t, st)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

		createPartitionReq = milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition1",
		}
		st, _ = cli.CreatePartition(ctx, &createPartitionReq)
		assert.NotNil(t, st)
		assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, st.ErrorCode)

		createPartitionReq = milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition2",
		}
		st, _ = cli.CreatePartition(ctx, &createPartitionReq)
		assert.NotNil(t, st)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

		collMeta, err := svr.metaTable.GetCollectionByName(sch.Name)
		assert.Nil(t, err)
		t.Logf("collection id = %d", collMeta.ID)
		assert.Equal(t, collMeta.Schema.Name, "col1")
		assert.Equal(t, collMeta.Schema.AutoID, false)
		assert.Equal(t, len(collMeta.Schema.Fields), 4)
		assert.Equal(t, collMeta.Schema.Fields[0].Name, "col1_f1")
		assert.Equal(t, collMeta.Schema.Fields[1].Name, "col1_f2")
		assert.Equal(t, collMeta.Schema.Fields[2].Name, "RowID")
		assert.Equal(t, collMeta.Schema.Fields[3].Name, "Timestamp")
		assert.Equal(t, collMeta.Schema.Fields[0].DataType, schemapb.DataType_VECTOR_FLOAT)
		assert.Equal(t, collMeta.Schema.Fields[1].DataType, schemapb.DataType_VECTOR_BINARY)
		assert.Equal(t, len(collMeta.Schema.Fields[0].TypeParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[0].IndexParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[1].TypeParams), 2)
		assert.Equal(t, len(collMeta.Schema.Fields[1].IndexParams), 2)
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Key, "col1_f1_tk1")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Key, "col1_f1_tk2")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[0].Value, "col1_f1_tv1")
		assert.Equal(t, collMeta.Schema.Fields[0].TypeParams[1].Value, "col1_f1_tv2")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Key, "col1_f1_ik1")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Key, "col1_f1_ik2")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[0].Value, "col1_f1_iv1")
		assert.Equal(t, collMeta.Schema.Fields[0].IndexParams[1].Value, "col1_f1_iv2")

		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Key, "col1_f2_tk1")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Key, "col1_f2_tk2")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[0].Value, "col1_f2_tv1")
		assert.Equal(t, collMeta.Schema.Fields[1].TypeParams[1].Value, "col1_f2_tv2")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Key, "col1_f2_ik1")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Key, "col1_f2_ik2")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[0].Value, "col1_f2_iv1")
		assert.Equal(t, collMeta.Schema.Fields[1].IndexParams[1].Value, "col1_f2_iv2")
		assert.ElementsMatch(t, []string{"_default", "partition1", "partition2"}, collMeta.PartitionTags)

		showPartitionReq := milvuspb.ShowPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowPartitions,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}

		stringList, err := cli.ShowPartitions(ctx, &showPartitionReq)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []string{"_default", "partition1", "partition2"}, stringList.PartitionNames)

		showPartitionReq = milvuspb.ShowPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowPartitions,
				MsgID:     1,
				Timestamp: 0,
				SourceID:  1,
			},
			CollectionName: "col1",
		}

		stringList, _ = cli.ShowPartitions(ctx, &showPartitionReq)
		assert.NotNil(t, stringList)
		assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, stringList.Status.ErrorCode)

		hasPartitionReq := milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasPartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition1",
		}

		hasPartition, err := cli.HasPartition(ctx, &hasPartitionReq)
		assert.Nil(t, err)
		assert.True(t, hasPartition.Value)

		hasPartitionReq = milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasPartition,
				MsgID:     1,
				Timestamp: 0,
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition1",
		}

		hasPartition, _ = cli.HasPartition(ctx, &hasPartitionReq)
		assert.NotNil(t, hasPartition)
		assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, stringList.Status.ErrorCode)

		hasPartitionReq = milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasPartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition3",
		}

		hasPartition, err = cli.HasPartition(ctx, &hasPartitionReq)
		assert.Nil(t, err)
		assert.False(t, hasPartition.Value)

		deletePartitionReq := milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropPartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition2",
		}

		st, err = cli.DropPartition(ctx, &deletePartitionReq)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_SUCCESS, st.ErrorCode)

		deletePartitionReq = milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropPartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition2",
		}

		st, _ = cli.DropPartition(ctx, &deletePartitionReq)
		assert.NotNil(t, st)
		assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, st.ErrorCode)

		hasPartitionReq = milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kHasPartition,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
			PartitionName:  "partition2",
		}

		hasPartition, err = cli.HasPartition(ctx, &hasPartitionReq)
		assert.Nil(t, err)
		assert.False(t, hasPartition.Value)

		//describePartitionReq := internalpb.DescribePartitionRequest{
		//	MsgType:       commonpb.MsgType_kDescribePartition,
		//	ReqID:         1,
		//	Timestamp:     uint64(time.Now().Unix()),
		//	ProxyID:       1,
		//	PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
		//}
		//
		//describePartition, err := cli.DescribePartition(ctx, &describePartitionReq)
		//assert.Nil(t, err)
		//assert.Equal(t, &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"}, describePartition.Name)
		//
		//describePartitionReq = internalpb.DescribePartitionRequest{
		//	MsgType:       commonpb.MsgType_kDescribePartition,
		//	ReqID:         1,
		//	Timestamp:     0,
		//	ProxyID:       1,
		//	PartitionName: &servicepb.PartitionName{CollectionName: "col1", Tag: "partition1"},
		//}
		//
		//describePartition, _ = cli.DescribePartition(ctx, &describePartitionReq)
		//assert.Equal(t, commonpb.ErrorCode_UNEXPECTED_ERROR, describePartition.Status.ErrorCode)

		// DropCollection
		reqDrop := milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: uint64(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: "col1",
		}
		st, err = cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		//consume msg
		ddMs := ms.NewPulsarTtMsgStream(ctx, 1024)
		ddMs.SetPulsarClient(pulsarAddr)
		ddMs.CreatePulsarConsumers(Params.DDChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
		ddMs.Start()

		time.Sleep(1000 * time.Millisecond)
		timestampNow := Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		var consumeMsg ms.MsgStream = ddMs
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				break
			}
		}
		ddMs.Close()
	})

	t.Run("TestBroadCastRequest", func(t *testing.T) {

		proxyTimeTickStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
		proxyTimeTickStream.SetPulsarClient(pulsarAddr)
		proxyTimeTickStream.CreatePulsarProducers(Params.ProxyTimeTickChannelNames)
		proxyTimeTickStream.Start()

		writeNodeStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
		writeNodeStream.SetPulsarClient(pulsarAddr)
		writeNodeStream.CreatePulsarProducers(Params.WriteNodeTimeTickChannelNames)
		writeNodeStream.Start()

		ddMs := ms.NewPulsarTtMsgStream(ctx, 1024)
		ddMs.SetPulsarClient(pulsarAddr)
		ddMs.CreatePulsarConsumers(Params.DDChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
		ddMs.Start()

		dMMs := ms.NewPulsarTtMsgStream(ctx, 1024)
		dMMs.SetPulsarClient(pulsarAddr)
		dMMs.CreatePulsarConsumers(Params.InsertChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
		dMMs.Start()

		k2sMs := ms.NewPulsarMsgStream(ctx, 1024)
		k2sMs.SetPulsarClient(pulsarAddr)
		k2sMs.CreatePulsarConsumers(Params.K2SChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
		k2sMs.Start()

		ttsoftmsgs := [][2]uint64{
			{0, 10},
		}
		msgSoftPackAddr := getTimeTickMsgPack(ttsoftmsgs)

		err := proxyTimeTickStream.Produce(msgSoftPackAddr)
		assert.Nil(t, err)
		var dMMsgstream ms.MsgStream = dMMs
		assert.True(t, receiveTimeTickMsg(&dMMsgstream))
		var ddMsgstream ms.MsgStream = ddMs
		assert.True(t, receiveTimeTickMsg(&ddMsgstream))

		tthardmsgs := [][2]int{
			{3, 10},
		}

		msghardPackAddr := getMsgPack(tthardmsgs)
		err = writeNodeStream.Produce(msghardPackAddr)
		assert.Nil(t, err)
		var k2sMsgstream ms.MsgStream = k2sMs
		assert.True(t, receiveTimeTickMsg(&k2sMsgstream))

		sch := schemapb.CollectionSchema{
			Name:        "name" + strconv.FormatUint(rand.Uint64(), 10),
			Description: "test collection",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		}

		schemaBytes, err := proto.Marshal(&sch)
		assert.Nil(t, err)

		////////////////////////////CreateCollection////////////////////////
		createCollectionReq := milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreateCollection,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			Schema: schemaBytes,
		}
		st, err := cli.CreateCollection(ctx, &createCollectionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		time.Sleep(1000 * time.Millisecond)
		timestampNow := Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		var consumeMsg ms.MsgStream = ddMs
		var createCollectionMsg *ms.CreateCollectionMsg
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					createCollectionMsg = v.(*ms.CreateCollectionMsg)
				}
				break
			}
		}
		assert.Equal(t, createCollectionReq.Base.MsgType, createCollectionMsg.CreateCollectionRequest.Base.MsgType)
		assert.Equal(t, createCollectionReq.Base.MsgID, createCollectionMsg.CreateCollectionRequest.Base.MsgID)
		assert.Equal(t, createCollectionReq.Base.Timestamp, createCollectionMsg.CreateCollectionRequest.Base.Timestamp)
		assert.Equal(t, createCollectionReq.Base.SourceID, createCollectionMsg.CreateCollectionRequest.Base.SourceID)

		////////////////////////////CreatePartition////////////////////////
		partitionName := "partitionName" + strconv.FormatUint(rand.Uint64(), 10)
		createPartitionReq := milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: sch.Name,
			PartitionName:  partitionName,
		}

		st, err = cli.CreatePartition(ctx, &createPartitionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		time.Sleep(1000 * time.Millisecond)
		timestampNow = Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		var createPartitionMsg *ms.CreatePartitionMsg
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					createPartitionMsg = v.(*ms.CreatePartitionMsg)
				}
				break
			}
		}
		assert.Equal(t, createPartitionReq.Base.MsgType, createPartitionMsg.CreatePartitionRequest.Base.MsgType)
		assert.Equal(t, createPartitionReq.Base.MsgID, createPartitionMsg.CreatePartitionRequest.Base.MsgID)
		assert.Equal(t, createPartitionReq.Base.Timestamp, createPartitionMsg.CreatePartitionRequest.Base.Timestamp)
		assert.Equal(t, createPartitionReq.Base.SourceID, createPartitionMsg.CreatePartitionRequest.Base.SourceID)
		assert.Equal(t, createPartitionReq.CollectionName, createPartitionMsg.CreatePartitionRequest.CollectionName)
		assert.Equal(t, createPartitionReq.PartitionName, createPartitionMsg.CreatePartitionRequest.PartitionName)

		////////////////////////////DropPartition////////////////////////
		dropPartitionReq := milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropPartition,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: sch.Name,
			PartitionName:  partitionName,
		}

		st, err = cli.DropPartition(ctx, &dropPartitionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		time.Sleep(1000 * time.Millisecond)
		timestampNow = Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		var dropPartitionMsg *ms.DropPartitionMsg
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					dropPartitionMsg = v.(*ms.DropPartitionMsg)
				}
				break
			}
		}
		assert.Equal(t, dropPartitionReq.Base.MsgType, dropPartitionMsg.DropPartitionRequest.Base.MsgType)
		assert.Equal(t, dropPartitionReq.Base.MsgID, dropPartitionMsg.DropPartitionRequest.Base.MsgID)
		assert.Equal(t, dropPartitionReq.Base.Timestamp, dropPartitionMsg.DropPartitionRequest.Base.Timestamp)
		assert.Equal(t, dropPartitionReq.Base.SourceID, dropPartitionMsg.DropPartitionRequest.Base.SourceID)
		assert.Equal(t, dropPartitionReq.CollectionName, dropPartitionMsg.DropPartitionRequest.CollectionName)

		////////////////////////////DropCollection////////////////////////
		dropCollectionReq := milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: sch.Name,
		}

		st, err = cli.DropCollection(ctx, &dropCollectionReq)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

		time.Sleep(1000 * time.Millisecond)
		timestampNow = Timestamp(time.Now().Unix())
		err = mockTimeTickBroadCast(svr.timesSyncMsgProducer.ddSyncStream, timestampNow)
		assert.NoError(t, err)

		var dropCollectionMsg *ms.DropCollectionMsg
		for {
			result := consumeMsg.Consume()
			if len(result.Msgs) > 0 {
				msgs := result.Msgs
				for _, v := range msgs {
					dropCollectionMsg = v.(*ms.DropCollectionMsg)
				}
				break
			}
		}
		assert.Equal(t, dropCollectionReq.Base.MsgType, dropCollectionMsg.DropCollectionRequest.Base.MsgType)
		assert.Equal(t, dropCollectionReq.Base.MsgID, dropCollectionMsg.DropCollectionRequest.Base.MsgID)
		assert.Equal(t, dropCollectionReq.Base.Timestamp, dropCollectionMsg.DropCollectionRequest.Base.Timestamp)
		assert.Equal(t, dropCollectionReq.Base.SourceID, dropCollectionMsg.DropCollectionRequest.Base.SourceID)
		assert.Equal(t, dropCollectionReq.CollectionName, dropCollectionMsg.DropCollectionRequest.CollectionName)
	})

	t.Run("TestSegmentManager_RPC", func(t *testing.T) {
		collName := "test_coll"
		partitionName := "test_part"
		schema := &schemapb.CollectionSchema{
			Name:        collName,
			Description: "test coll",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "f1", IsPrimaryKey: false, DataType: schemapb.DataType_INT32},
				{FieldID: 1, Name: "f1", IsPrimaryKey: false, DataType: schemapb.DataType_VECTOR_FLOAT, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			},
		}
		schemaBytes, err := proto.Marshal(schema)
		assert.Nil(t, err)
		_, err = cli.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreateCollection,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			Schema: schemaBytes,
		})
		assert.Nil(t, err)
		_, err = cli.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kCreatePartition,
				MsgID:     2,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: collName,
			PartitionName:  partitionName,
		})
		assert.Nil(t, err)

		resp, err := cli.AssignSegmentID(ctx, &datapb.AssignSegIDRequest{
			NodeID:   1,
			PeerRole: "ProxyNode",
			SegIDRequests: []*datapb.SegIDRequest{
				{Count: 10000, ChannelID: "0", CollName: collName, PartitionName: partitionName},
			},
		})
		assert.Nil(t, err)
		assignments := resp.GetSegIDAssignments()
		assert.EqualValues(t, 1, len(assignments))
		assert.EqualValues(t, commonpb.ErrorCode_SUCCESS, assignments[0].Status.ErrorCode)
		assert.EqualValues(t, collName, assignments[0].CollName)
		assert.EqualValues(t, partitionName, assignments[0].PartitionName)
		assert.EqualValues(t, int32(0), assignments[0].ChannelID)
		assert.EqualValues(t, uint32(10000), assignments[0].Count)

		// test stats
		segID := assignments[0].SegID
		pulsarAddress := Params.PulsarAddress
		msgStream := ms.NewPulsarMsgStream(ctx, 1024)
		msgStream.SetPulsarClient(pulsarAddress)
		msgStream.CreatePulsarProducers([]string{Params.QueryNodeStatsChannelName})
		msgStream.Start()
		defer msgStream.Close()

		err = msgStream.Produce(&ms.MsgPack{
			BeginTs: 102,
			EndTs:   104,
			Msgs: []ms.TsMsg{
				&ms.QueryNodeStatsMsg{
					QueryNodeStats: internalpb2.QueryNodeStats{
						Base: &commonpb.MsgBase{
							MsgType:  commonpb.MsgType_kQueryNodeStats,
							SourceID: 1,
						},
						SegStats: []*internalpb2.SegmentStats{
							{SegmentID: segID, MemorySize: 600000000, NumRows: 1000000, RecentlyModified: true},
						},
					},
					BaseMsg: ms.BaseMsg{
						HashValues: []uint32{0},
					},
				},
			},
		})
		assert.Nil(t, err)

		time.Sleep(500 * time.Millisecond)
		segMeta, err := svr.metaTable.GetSegmentByID(segID)
		assert.Nil(t, err)
		assert.EqualValues(t, 1000000, segMeta.GetNumRows())
		assert.EqualValues(t, int64(600000000), segMeta.GetMemSize())

		reqDrop := milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDropCollection,
				MsgID:     1,
				Timestamp: Timestamp(time.Now().Unix()),
				SourceID:  1,
			},
			CollectionName: collName,
		}

		// DropCollection
		st, err := cli.DropCollection(ctx, &reqDrop)
		assert.Nil(t, err)
		assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)
	})

	cancel()
	conn.Close()
	svr.Close()
}
