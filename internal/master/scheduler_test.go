package master

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"go.etcd.io/etcd/clientv3"
)

func filterSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	cloneSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	// remove system field
	var newFields []*schemapb.FieldSchema
	for _, fieldMeta := range cloneSchema.Fields {
		fieldID := fieldMeta.FieldID
		// todo not hardcode
		if fieldID < 100 {
			continue
		}
		newFields = append(newFields, fieldMeta)
	}
	cloneSchema.Fields = newFields
	return cloneSchema
}

func TestMaster_Scheduler_Collection(t *testing.T) {
	Init()
	etcdAddress := Params.EtcdAddress
	kvRootPath := Params.MetaRootPath
	pulsarAddr := Params.PulsarAddress

	producerChannels := []string{"ddstream"}
	consumerChannels := []string{"ddstream"}
	consumerSubName := "substream"

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

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

	consumeMs := ms.NewPulsarTtMsgStream(ctx, 1024)
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
	createCollectionReq := milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kCreateCollection,
			MsgID:     1,
			Timestamp: 11,
			SourceID:  1,
		},
		Schema: schemaBytes,
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

	err = mockTimeTickBroadCast(pulsarDDStream, Timestamp(12))
	assert.NoError(t, err)

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
	assert.Equal(t, createCollectionReq.Base.MsgType, createCollectionMsg.CreateCollectionRequest.Base.MsgType)
	assert.Equal(t, createCollectionReq.Base.MsgID, createCollectionMsg.CreateCollectionRequest.Base.MsgID)
	assert.Equal(t, createCollectionReq.Base.Timestamp, createCollectionMsg.CreateCollectionRequest.Base.Timestamp)
	assert.Equal(t, createCollectionReq.Base.SourceID, createCollectionMsg.CreateCollectionRequest.Base.SourceID)

	var schema1 schemapb.CollectionSchema
	proto.UnmarshalMerge(createCollectionReq.Schema, &schema1)

	var schema2 schemapb.CollectionSchema
	proto.UnmarshalMerge(createCollectionMsg.CreateCollectionRequest.Schema, &schema2)
	filterSchema2 := filterSchema(&schema2)
	filterSchema2Value, _ := proto.Marshal(filterSchema2)
	fmt.Println("aaaa")
	fmt.Println(schema1.String())
	fmt.Println("bbbb")
	fmt.Println(schema2.String())
	assert.Equal(t, createCollectionReq.Schema, filterSchema2Value)

	////////////////////////////DropCollection////////////////////////
	dropCollectionReq := milvuspb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kDropCollection,
			MsgID:     1,
			Timestamp: 13,
			SourceID:  1,
		},
		CollectionName: sch.Name,
	}

	var dropCollectionTask task = &dropCollectionTask{
		req: &dropCollectionReq,
		baseTask: baseTask{
			sch: scheduler,
			mt:  meta,
			cv:  make(chan error),
		},
		segManager: NewMockSegmentManager(),
	}

	err = scheduler.Enqueue(dropCollectionTask)
	assert.Nil(t, err)
	err = dropCollectionTask.WaitToFinish(ctx)
	assert.Nil(t, err)

	err = mockTimeTickBroadCast(pulsarDDStream, Timestamp(14))
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
	assert.Equal(t, dropCollectionReq.Base.SourceID, dropCollectionMsg.DropCollectionRequest.Base.MsgID)
	assert.Equal(t, dropCollectionReq.CollectionName, dropCollectionMsg.DropCollectionRequest.CollectionName)

}

func TestMaster_Scheduler_Partition(t *testing.T) {
	Init()
	etcdAddress := Params.EtcdAddress
	kvRootPath := Params.MetaRootPath
	pulsarAddr := Params.PulsarAddress

	producerChannels := []string{"ddstream"}
	consumerChannels := []string{"ddstream"}
	consumerSubName := "substream"

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

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

	consumeMs := ms.NewPulsarTtMsgStream(ctx, 1024)
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
	createCollectionReq := milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kCreateCollection,
			MsgID:     1,
			Timestamp: 11,
			SourceID:  1,
		},
		Schema: schemaBytes,
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

	err = mockTimeTickBroadCast(pulsarDDStream, Timestamp(12))
	assert.NoError(t, err)

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
	assert.Equal(t, createCollectionReq.Base.MsgType, createCollectionMsg.CreateCollectionRequest.Base.MsgType)
	assert.Equal(t, createCollectionReq.Base.MsgID, createCollectionMsg.CreateCollectionRequest.Base.MsgID)
	assert.Equal(t, createCollectionReq.Base.Timestamp, createCollectionMsg.CreateCollectionRequest.Base.Timestamp)
	assert.Equal(t, createCollectionReq.Base.SourceID, createCollectionMsg.CreateCollectionRequest.Base.SourceID)
	//assert.Equal(t, createCollectionReq.Schema, createCollectionMsg.CreateCollectionRequest.Schema)

	var schema1 schemapb.CollectionSchema
	proto.UnmarshalMerge(createCollectionReq.Schema, &schema1)

	var schema2 schemapb.CollectionSchema
	proto.UnmarshalMerge(createCollectionMsg.CreateCollectionRequest.Schema, &schema2)
	filterSchema2 := filterSchema(&schema2)
	filterSchema2Value, _ := proto.Marshal(filterSchema2)
	fmt.Println("aaaa")
	fmt.Println(schema1.String())
	fmt.Println("bbbb")
	fmt.Println(schema2.String())
	assert.Equal(t, createCollectionReq.Schema, filterSchema2Value)

	////////////////////////////CreatePartition////////////////////////
	partitionName := "partitionName" + strconv.FormatUint(rand.Uint64(), 10)
	createPartitionReq := milvuspb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kCreatePartition,
			MsgID:     1,
			Timestamp: 13,
			SourceID:  1,
		},
		CollectionName: sch.Name,
		PartitionName:  partitionName,
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

	err = mockTimeTickBroadCast(pulsarDDStream, Timestamp(14))
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
	assert.Equal(t, createPartitionReq.Base.SourceID, createPartitionMsg.CreatePartitionRequest.Base.MsgID)
	assert.Equal(t, createPartitionReq.CollectionName, createPartitionMsg.CreatePartitionRequest.CollectionName)
	assert.Equal(t, createPartitionReq.PartitionName, createPartitionMsg.CreatePartitionRequest.PartitionName)

	////////////////////////////DropPartition////////////////////////
	dropPartitionReq := milvuspb.DropPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kDropPartition,
			MsgID:     1,
			Timestamp: 15,
			SourceID:  1,
		},
		CollectionName: sch.Name,
		PartitionName:  partitionName,
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

	err = mockTimeTickBroadCast(pulsarDDStream, Timestamp(16))
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

}
