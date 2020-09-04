package write_node

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"writer/message_client"
	"writer/mock"
	"writer/pb"
)

type SegmentIdInfo struct {
	CollectionName string
	EntityId       int64
	SegmentIds     *[]string
}

type WriteNode struct {
	KvStore       *mock.TikvStore
	MessageClient *message_client.MessageClient
	TimeSync      uint64
}

func NewWriteNode(ctx context.Context,
	address string,
	topics []string,
	timeSync uint64) (*WriteNode, error) {
	kv, err := mock.NewTikvStore()
	mc := &message_client.MessageClient{}
	return &WriteNode{
		KvStore:       kv,
		MessageClient: mc,
		TimeSync:      timeSync,
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*pb.InsertOrDeleteMsg, wg sync.WaitGroup) error {
	var prefixKey string
	var suffixKey string
	var prefixKeys [][]byte
	var suffixKeys [][]byte
	var binaryData [][]byte
	var timeStamp []uint64

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "-" + strconv.FormatUint(uint64(data[i].Uid), 10)
		suffixKey = strconv.FormatUint(uint64(data[i].SegmentId), 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		suffixKeys = append(suffixKeys, []byte(suffixKey))
		binaryData = append(binaryData, []byte(data[i].String()))
		timeStamp = append(timeStamp, uint64(data[i].Timestamp))
	}

	error := (*wn.KvStore).PutRows(ctx, prefixKeys, timeStamp, suffixKeys, binaryData)
	if error != nil {
		fmt.Println("Can't insert data!")
		return error
	}
	wg.Done()
	return nil
}

func (wn *WriteNode) DeleteBatchData(ctx context.Context, data []*pb.InsertOrDeleteMsg, wg sync.WaitGroup) error {
	var segmentInfos []*SegmentIdInfo
	var prefixKey string
	var prefixKeys [][]byte
	var timeStamps []uint64

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "-" + strconv.FormatUint(uint64(data[i].Uid), 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		timeStamps = append(timeStamps, uint64(data[i].Timestamp))
	}
	segmentIds := (*wn.KvStore).GetSegment(ctx, prefixKeys)
	for i := 0; i < len(prefixKeys); i++ {
		segmentInfos = append(segmentInfos, &SegmentIdInfo{
			CollectionName: data[i].CollectionName,
			EntityId:       data[i].Uid,
			SegmentIds:     segmentIds,
		})
	}
	err := (*wn.KvStore).DeleteRows(ctx, prefixKeys, timeStamps)
	if err != nil {
		fmt.Println("Can't delete data")
	}
	wg.Done()
	return nil
}

func (wn *WriteNode) UpdateTimeSync(timeSync uint64) {
	wn.TimeSync = timeSync
}

func (wn *WriteNode) DoWriteNode(ctx context.Context, timeSync uint64, wg sync.WaitGroup) {
	wg.Add(2)
	go wn.InsertBatchData(ctx, wn.MessageClient.InsertMsg, wg)
	go wn.DeleteBatchData(ctx, wn.MessageClient.DeleteMsg, wg)
	wg.Wait()
	wn.UpdateTimeSync(timeSync)
}
