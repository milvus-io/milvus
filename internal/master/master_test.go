package master

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
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

func receiveTimeTickMsg(stream *ms.MsgStream) bool {
	for {
		result := (*stream).Consume()
		if len(result.Msgs) > 0 {
			return true
		}
	}
}

func getTimeTickMsgPack(ttmsgs [][2]uint64) *ms.MsgPack {
	msgPack := ms.MsgPack{}
	for _, vi := range ttmsgs {
		msgPack.Msgs = append(msgPack.Msgs, getTtMsg(internalPb.MsgType_kTimeTick, UniqueID(vi[0]), Timestamp(vi[1])))
	}
	return &msgPack
}

func TestMaster(t *testing.T) {
	Init()
	refreshMasterAddress()
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

	proxyTimeTickStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	proxyTimeTickStream.SetPulsarClient(pulsarAddr)
	proxyTimeTickStream.CreatePulsarProducers(Params.ProxyTimeTickChannelNames)
	proxyTimeTickStream.Start()

	writeNodeStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	writeNodeStream.SetPulsarClient(pulsarAddr)
	writeNodeStream.CreatePulsarProducers(Params.WriteNodeTimeTickChannelNames)
	writeNodeStream.Start()

	ddMs := ms.NewPulsarMsgStream(ctx, 1024)
	ddMs.SetPulsarClient(pulsarAddr)
	ddMs.CreatePulsarConsumers(Params.DDChannelNames, "DDStream", ms.NewUnmarshalDispatcher(), 1024)
	ddMs.Start()

	dMMs := ms.NewPulsarMsgStream(ctx, 1024)
	dMMs.SetPulsarClient(pulsarAddr)
	dMMs.CreatePulsarConsumers(Params.InsertChannelNames, "DMStream", ms.NewUnmarshalDispatcher(), 1024)
	dMMs.Start()

	k2sMs := ms.NewPulsarMsgStream(ctx, 1024)
	k2sMs.SetPulsarClient(pulsarAddr)
	k2sMs.CreatePulsarConsumers(Params.K2SChannelNames, "K2SStream", ms.NewUnmarshalDispatcher(), 1024)
	k2sMs.Start()

	ttsoftmsgs := [][2]uint64{
		{0, 10},
	}
	msgSoftPackAddr := getTimeTickMsgPack(ttsoftmsgs)

	proxyTimeTickStream.Produce(msgSoftPackAddr)
	var dMMsgstream ms.MsgStream = dMMs
	assert.True(t, receiveTimeTickMsg(&dMMsgstream))
	var ddMsgstream ms.MsgStream = ddMs
	assert.True(t, receiveTimeTickMsg(&ddMsgstream))

	tthardmsgs := [][2]int{
		{3, 10},
	}

	msghardPackAddr := getMsgPack(tthardmsgs)
	writeNodeStream.Produce(msghardPackAddr)
	var k2sMsgstream ms.MsgStream = k2sMs
	assert.True(t, receiveTimeTickMsg(&k2sMsgstream))

	conn, err := grpc.DialContext(ctx, Params.Address, grpc.WithInsecure(), grpc.WithBlock())
	assert.Nil(t, err)
	defer conn.Close()

	cli := masterpb.NewMasterClient(conn)

	sch := schemapb.CollectionSchema{
		Name:        "name" + strconv.FormatUint(rand.Uint64(), 10),
		Description: "test collection",
		AutoID:      false,
		Fields:      []*schemapb.FieldSchema{},
	}

	schemaBytes, err := proto.Marshal(&sch)
	assert.Nil(t, err)

	createCollectionReq := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}
	st, err := cli.CreateCollection(ctx, &createCollectionReq)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

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
	assert.Equal(t, createCollectionReq.MsgType, createCollectionMsg.CreateCollectionRequest.MsgType)
	assert.Equal(t, createCollectionReq.ReqID, createCollectionMsg.CreateCollectionRequest.ReqID)
	assert.Equal(t, createCollectionReq.Timestamp, createCollectionMsg.CreateCollectionRequest.Timestamp)
	assert.Equal(t, createCollectionReq.ProxyID, createCollectionMsg.CreateCollectionRequest.ProxyID)
	assert.Equal(t, createCollectionReq.Schema.Value, createCollectionMsg.CreateCollectionRequest.Schema.Value)

	////////////////////////////CreatePartition////////////////////////
	partitionName := "partitionName" + strconv.FormatUint(rand.Uint64(), 10)
	createPartitionReq := internalpb.CreatePartitionRequest{
		MsgType:   internalpb.MsgType_kCreatePartition,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		PartitionName: &servicepb.PartitionName{
			CollectionName: sch.Name,
			Tag:            partitionName,
		},
	}

	st, err = cli.CreatePartition(ctx, &createPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

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
	assert.Equal(t, createPartitionReq.MsgType, createPartitionMsg.CreatePartitionRequest.MsgType)
	assert.Equal(t, createPartitionReq.ReqID, createPartitionMsg.CreatePartitionRequest.ReqID)
	assert.Equal(t, createPartitionReq.Timestamp, createPartitionMsg.CreatePartitionRequest.Timestamp)
	assert.Equal(t, createPartitionReq.ProxyID, createPartitionMsg.CreatePartitionRequest.ProxyID)
	assert.Equal(t, createPartitionReq.PartitionName.CollectionName, createPartitionMsg.CreatePartitionRequest.PartitionName.CollectionName)
	assert.Equal(t, createPartitionReq.PartitionName.Tag, createPartitionMsg.CreatePartitionRequest.PartitionName.Tag)

	////////////////////////////DropPartition////////////////////////
	dropPartitionReq := internalpb.DropPartitionRequest{
		MsgType:   internalpb.MsgType_kDropPartition,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		PartitionName: &servicepb.PartitionName{
			CollectionName: sch.Name,
			Tag:            partitionName,
		},
	}

	st, err = cli.DropPartition(ctx, &dropPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

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
	assert.Equal(t, dropPartitionReq.MsgType, dropPartitionMsg.DropPartitionRequest.MsgType)
	assert.Equal(t, dropPartitionReq.ReqID, dropPartitionMsg.DropPartitionRequest.ReqID)
	assert.Equal(t, dropPartitionReq.Timestamp, dropPartitionMsg.DropPartitionRequest.Timestamp)
	assert.Equal(t, dropPartitionReq.ProxyID, dropPartitionMsg.DropPartitionRequest.ProxyID)
	assert.Equal(t, dropPartitionReq.PartitionName.CollectionName, dropPartitionMsg.DropPartitionRequest.PartitionName.CollectionName)

	////////////////////////////DropCollection////////////////////////
	dropCollectionReq := internalpb.DropCollectionRequest{
		MsgType:        internalpb.MsgType_kDropCollection,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: &servicepb.CollectionName{CollectionName: sch.Name},
	}

	st, err = cli.DropCollection(ctx, &dropCollectionReq)
	assert.Nil(t, err)
	assert.Equal(t, st.ErrorCode, commonpb.ErrorCode_SUCCESS)

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
	assert.Equal(t, dropCollectionReq.MsgType, dropCollectionMsg.DropCollectionRequest.MsgType)
	assert.Equal(t, dropCollectionReq.ReqID, dropCollectionMsg.DropCollectionRequest.ReqID)
	assert.Equal(t, dropCollectionReq.Timestamp, dropCollectionMsg.DropCollectionRequest.Timestamp)
	assert.Equal(t, dropCollectionReq.ProxyID, dropCollectionMsg.DropCollectionRequest.ProxyID)
	assert.Equal(t, dropCollectionReq.CollectionName.CollectionName, dropCollectionMsg.DropCollectionRequest.CollectionName.CollectionName)

	cancel()
	svr.Close()
}
