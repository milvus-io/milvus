package rmqms

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/allocator"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

func repackFunc(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range msgs {
		keys := hashKeys[i]
		for _, channelID := range keys {
			_, ok := result[channelID]
			if ok == false {
				msgPack := MsgPack{}
				result[channelID] = &msgPack
			}
			result[channelID].Msgs = append(result[channelID].Msgs, request)
		}
	}
	return result, nil
}

func getTsMsg(msgType MsgType, reqID UniqueID, hashValue uint32) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	switch msgType {
	case commonpb.MsgType_kInsert:
		insertRequest := internalpb2.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kInsert,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ChannelID:      "0",
			Timestamps:     []Timestamp{uint64(reqID)},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &msgstream.InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		return insertMsg
	case commonpb.MsgType_kDelete:
		deleteRequest := internalpb2.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDelete,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			ChannelID:      "1",
			Timestamps:     []Timestamp{1},
			PrimaryKeys:    []IntPrimaryKey{1},
		}
		deleteMsg := &msgstream.DeleteMsg{
			BaseMsg:       baseMsg,
			DeleteRequest: deleteRequest,
		}
		return deleteMsg
	case commonpb.MsgType_kSearch:
		searchRequest := internalpb2.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kSearch,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			Query:           nil,
			ResultChannelID: "0",
		}
		searchMsg := &msgstream.SearchMsg{
			BaseMsg:       baseMsg,
			SearchRequest: searchRequest,
		}
		return searchMsg
	case commonpb.MsgType_kSearchResult:
		searchResult := internalpb2.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kSearchResult,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
			ResultChannelID: "0",
		}
		searchResultMsg := &msgstream.SearchResultMsg{
			BaseMsg:       baseMsg,
			SearchResults: searchResult,
		}
		return searchResultMsg
	case commonpb.MsgType_kTimeTick:
		timeTickResult := internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		return timeTickMsg
	case commonpb.MsgType_kQueryNodeStats:
		queryNodeSegStats := internalpb2.QueryNodeStats{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_kQueryNodeStats,
				SourceID: reqID,
			},
		}
		queryNodeSegStatsMsg := &QueryNodeStatsMsg{
			BaseMsg:        baseMsg,
			QueryNodeStats: queryNodeSegStats,
		}
		return queryNodeSegStatsMsg
	}
	return nil
}

func getTimeTickMsg(reqID UniqueID, hashValue uint32, time uint64) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	timeTickResult := internalpb2.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kTimeTick,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
	}
	timeTickMsg := &TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	return timeTickMsg
}

func initRmq(name string) *etcdkv.EtcdKV {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	err = rocksmq.InitRmq(name, idAllocator)

	if err != nil {
		log.Fatalf("InitRmq error = %v", err)
	}
	return etcdKV
}

func Close(rocksdbName string, intputStream, outputStream msgstream.MsgStream, etcdKV *etcdkv.EtcdKV) {
	intputStream.Close()
	outputStream.Close()
	etcdKV.Close()
	err := os.RemoveAll(rocksdbName)
	fmt.Println(err)
}

func initRmqStream(producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	inputStream, _ := newRmqMsgStream(context.Background(), 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	outputStream, _ := newRmqMsgStream(context.Background(), 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return input, output
}

func initRmqTtStream(producerChannels []string,
	consumerChannels []string,
	consumerGroupName string,
	opts ...RepackFunc) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	inputStream, _ := newRmqMsgStream(context.Background(), 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	outputStream, _ := newRmqTtMsgStream(context.Background(), 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerGroupName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return input, output
}

func receiveMsg(outputStream msgstream.MsgStream, msgCount int) {
	receiveCount := 0
	for {
		result, _ := outputStream.Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", v.Type(), ", msg value: ", v)
			}
		}
		if receiveCount >= msgCount {
			break
		}
	}
}

func TestStream_RmqMsgStream_Insert(t *testing.T) {
	ctx := context.Background()
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerGroupName := "InsertGroup"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	rocksdbName := "/tmp/rocksmq_insert"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerGroupName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_Delete(t *testing.T) {
	ctx := context.Background()
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kDelete, 1, 1))

	rocksdbName := "/tmp/rocksmq_delete"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_Search(t *testing.T) {
	ctx := context.Background()
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 3, 3))

	rocksdbName := "/tmp/rocksmq_search"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_SearchResult(t *testing.T) {
	ctx := context.Background()

	producerChannels := []string{"searchResult"}
	consumerChannels := []string{"searchResult"}
	consumerSubName := "subSearchResult"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 3, 3))

	rocksdbName := "/tmp/rocksmq_searchresult"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_TimeTick(t *testing.T) {
	ctx := context.Background()
	producerChannels := []string{"timeTick"}
	consumerChannels := []string{"timeTick"}
	consumerSubName := "subTimeTick"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	rocksdbName := "/tmp/rocksmq_timetick"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_BroadCast(t *testing.T) {
	ctx := context.Background()
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	rocksdbName := "/tmp/rocksmq_broadcast"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Broadcast(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(consumerChannels)*len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_RmqMsgStream_RepackFunc(t *testing.T) {
	ctx := context.Background()

	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	rocksdbName := "/tmp/rocksmq_repackfunc"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqStream(producerChannels, consumerChannels, consumerSubName, repackFunc)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	ctx := context.Background()

	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	rocksdbName := "/tmp/rocksmq_insert_tt"
	etcdKV := initRmq(rocksdbName)
	inputStream, outputStream := initRmqTtStream(producerChannels, consumerChannels, consumerSubName)

	err := inputStream.Broadcast(ctx, &msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(ctx, &msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(ctx, &msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack1.Msgs))
	Close(rocksdbName, inputStream, outputStream, etcdKV)
}
