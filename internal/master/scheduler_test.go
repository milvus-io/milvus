package master

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"go.etcd.io/etcd/clientv3"
)

func TestMaster_Scheduler_Collection(t *testing.T) {
	Init()
	etcdAddress := Params.EtcdAddress
	kvRootPath := Params.EtcdRootPath
	pulsarAddr := Params.PulsarAddress

	producerChannels := []string{"ddstream"}
	consumerChannels := []string{"ddstream"}
	consumerSubName := "substream"

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	etcdKV := kv.NewEtcdKV(cli, "/etcd/test/root")

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pulsarDDStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	pulsarDDStream.SetPulsarClient(pulsarAddr)
	pulsarDDStream.CreatePulsarProducers(producerChannels)
	pulsarDDStream.Start()
	defer pulsarDDStream.Close()

	consumeMs := ms.NewPulsarMsgStream(ctx, 1024)
	consumeMs.SetPulsarClient(pulsarAddr)
	consumeMs.CreatePulsarConsumers(consumerChannels, consumerSubName, ms.NewUnmarshalDispatcher(), 1024)
	consumeMs.Start()
	defer consumeMs.Close()

	idAllocator := NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "gid"))
	err = idAllocator.Initialize()
	assert.Nil(t, err)

	scheduler := NewDDRequestScheduler(ctx)
	scheduler.SetDDMsgStream(pulsarDDStream)
	scheduler.SetIDAllocator(func() (UniqueID, error) { return idAllocator.AllocOne() })
	scheduler.Start()
	defer scheduler.Close()

	rand.Seed(time.Now().Unix())
	sch := schemapb.CollectionSchema{
		Name:        "name" + strconv.FormatUint(rand.Uint64(), 10),
		Description: "string",
		AutoID:      true,
		Fields:      nil,
	}

	schemaBytes, err := proto.Marshal(&sch)
	assert.Nil(t, err)

	////////////////////////////CreateCollection////////////////////////
	createCollectionReq := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}

	var createCollectionTask task = &createCollectionTask{
		req: &createCollectionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
	}

	err = scheduler.Enqueue(createCollectionTask)
	assert.Nil(t, err)
	err = createCollectionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

	var consumeMsg ms.MsgStream = consumeMs
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

	////////////////////////////DropCollection////////////////////////
	dropCollectionReq := internalpb.DropCollectionRequest{
		MsgType:        internalpb.MsgType_kDropCollection,
		ReqID:          1,
		Timestamp:      11,
		ProxyID:        1,
		CollectionName: &servicepb.CollectionName{CollectionName: sch.Name},
	}

	var dropCollectionTask task = &dropCollectionTask{
		req: &dropCollectionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
	}

	err = scheduler.Enqueue(dropCollectionTask)
	assert.Nil(t, err)
	err = dropCollectionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

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

}

func TestMaster_Scheduler_Partition(t *testing.T) {
	Init()
	etcdAddress := Params.EtcdAddress
	kvRootPath := Params.EtcdRootPath
	pulsarAddr := Params.PulsarAddress

	producerChannels := []string{"ddstream"}
	consumerChannels := []string{"ddstream"}
	consumerSubName := "substream"

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	etcdKV := kv.NewEtcdKV(cli, "/etcd/test/root")

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pulsarDDStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	pulsarDDStream.SetPulsarClient(pulsarAddr)
	pulsarDDStream.CreatePulsarProducers(producerChannels)
	pulsarDDStream.Start()
	defer pulsarDDStream.Close()

	consumeMs := ms.NewPulsarMsgStream(ctx, 1024)
	consumeMs.SetPulsarClient(pulsarAddr)
	consumeMs.CreatePulsarConsumers(consumerChannels, consumerSubName, ms.NewUnmarshalDispatcher(), 1024)
	consumeMs.Start()
	defer consumeMs.Close()

	idAllocator := NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "gid"))
	err = idAllocator.Initialize()
	assert.Nil(t, err)

	scheduler := NewDDRequestScheduler(ctx)
	scheduler.SetDDMsgStream(pulsarDDStream)
	scheduler.SetIDAllocator(func() (UniqueID, error) { return idAllocator.AllocOne() })
	scheduler.Start()
	defer scheduler.Close()

	rand.Seed(time.Now().Unix())
	sch := schemapb.CollectionSchema{
		Name:        "name" + strconv.FormatUint(rand.Uint64(), 10),
		Description: "string",
		AutoID:      true,
		Fields:      nil,
	}

	schemaBytes, err := proto.Marshal(&sch)
	assert.Nil(t, err)

	////////////////////////////CreateCollection////////////////////////
	createCollectionReq := internalpb.CreateCollectionRequest{
		MsgType:   internalpb.MsgType_kCreateCollection,
		ReqID:     1,
		Timestamp: 11,
		ProxyID:   1,
		Schema:    &commonpb.Blob{Value: schemaBytes},
	}

	var createCollectionTask task = &createCollectionTask{
		req: &createCollectionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
	}

	err = scheduler.Enqueue(createCollectionTask)
	assert.Nil(t, err)
	err = createCollectionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

	var consumeMsg ms.MsgStream = consumeMs
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

	var createPartitionTask task = &createPartitionTask{
		req: &createPartitionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
	}

	err = scheduler.Enqueue(createPartitionTask)
	assert.Nil(t, err)
	err = createPartitionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

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

	var dropPartitionTask task = &dropPartitionTask{
		req: &dropPartitionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
	}

	err = scheduler.Enqueue(dropPartitionTask)
	assert.Nil(t, err)
	err = dropPartitionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

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

}
