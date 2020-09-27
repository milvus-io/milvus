package write_node

import (
	"context"
	"fmt"
	msgpb "github.com/czs007/suvlim/pkg/master/grpc/message"
	storage "github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/storage/pkg/types"
	"github.com/czs007/suvlim/writer/message_client"
	"strconv"
	"sync"
)

type SegmentIdInfo struct {
	CollectionName string
	EntityId       int64
	SegmentIds     []string
}

type MsgCounter struct {
	InsertCounter int64
	InsertedRecordSize float64
	DeleteCounter int64
}

type WriteNode struct {
	KvStore       *types.Store
	MessageClient *message_client.MessageClient
	TimeSync      uint64
	MsgCounter    *MsgCounter
}

func (wn *WriteNode) Close() {
	wn.MessageClient.Close()
}

func NewWriteNode(ctx context.Context,
	address string,
	topics []string,
	timeSync uint64) (*WriteNode, error) {
	kv, err := storage.NewStore(context.Background(), types.MinIODriver)
	mc := message_client.MessageClient{}

	msgCounter := MsgCounter{
		InsertCounter: 0,
		DeleteCounter: 0,
		InsertedRecordSize: 0,
	}

	return &WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      timeSync,
		MsgCounter:    &msgCounter,
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*msgpb.InsertOrDeleteMsg, wg *sync.WaitGroup) error {
	var prefixKey string
	var suffixKey string
	var prefixKeys [][]byte
	var suffixKeys []string
	var binaryData [][]byte
	var timeStamp []uint64

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "-" + strconv.FormatUint(uint64(data[i].Uid), 10)
		suffixKey = strconv.FormatUint(uint64(data[i].SegmentId), 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		suffixKeys = append(suffixKeys, suffixKey)
		binaryData = append(binaryData, []byte(data[i].String()))
		timeStamp = append(timeStamp, uint64(data[i].Timestamp))
	}

	wn.MsgCounter.InsertCounter += int64(len(timeStamp))
	if len(timeStamp) > 0 {
		// assume each record is same size
		wn.MsgCounter.InsertedRecordSize += float64(len(timeStamp) * len(data[0].RowsData.Blob))
	}
	error := (*wn.KvStore).PutRows(ctx, prefixKeys, binaryData, suffixKeys, timeStamp)
	if error != nil {
		fmt.Println("Can't insert data!")
		wg.Done()
		return error
	}
	wg.Done()
	return nil
}

func (wn *WriteNode) DeleteBatchData(ctx context.Context, data []*msgpb.InsertOrDeleteMsg, wg *sync.WaitGroup) error {
	var prefixKey string
	var prefixKeys [][]byte
	var timeStamps []uint64

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "-" + strconv.FormatUint(uint64(data[i].Uid), 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		timeStamps = append(timeStamps, uint64(data[i].Timestamp))
		segmentString, _ := (*wn.KvStore).GetSegments(ctx, []byte(prefixKey), uint64(data[i].Timestamp))

		var segmentIds []int64
		for _, str := range segmentString {
			id, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				fmt.Println(str, " is not an integer.")
			}
			segmentIds = append(segmentIds, id)
		}

		segmentInfo := msgpb.Key2SegMsg{
			Uid:       data[i].Uid,
			SegmentId: segmentIds,
			Timestamp: data[i].Timestamp,
		}
		wn.MessageClient.Send(ctx, segmentInfo)
	}

	wn.MsgCounter.DeleteCounter += int64(len(timeStamps))

	err := (*wn.KvStore).DeleteRows(ctx, prefixKeys, timeStamps)
	if err != nil {
		fmt.Println("Can't delete data")
		wg.Done()
		return err
	}
	wg.Done()
	return nil
}

func (wn *WriteNode) UpdateTimeSync(timeSync uint64) {
	wn.TimeSync = timeSync
}

func (wn *WriteNode) DoWriteNode(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(2)
	go wn.InsertBatchData(ctx, wn.MessageClient.InsertMsg, wg)
	go wn.DeleteBatchData(ctx, wn.MessageClient.DeleteMsg, wg)
	wg.Wait()
	wn.UpdateTimeSync(wn.MessageClient.TimeSync())
}
