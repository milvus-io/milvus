package writer

import (
	"container/list"
	"context"
	"fmt"
	"github.com/czs007/suvlim/pulsar"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/writer/mock"
	"strconv"
	"sync"
)

//type PartitionMeta struct {
//	collectionName       string
//	partitionName        string
//	openSegmentId        string
//	segmentCloseTime     uint64
//	nextSegmentId        string
//	nextSegmentCloseTime uint64
//}
//
//type CollectionMeta struct {
//	collionName      string
//	partitionMetaMap map[string]*PartitionMeta
//	deleteTimeSync   uint64
//	insertTimeSync   uint64
//}

type WriteNode struct {
	KvStore *mock.TikvStore
	mc                *pulsar.MessageClient
	gtInsertMsgBuffer *list.List
	gtDeleteMsgBuffer *list.List
	deleteTimeSync    uint64
	insertTimeSync    uint64
}

func NewWriteNode(ctx context.Context,
	address string,
	topics []string,
	timeSync uint64) (*WriteNode, error) {
	kv, err := mock.NewTikvStore()
	mc := &pulsar.MessageClient{}
	return &WriteNode{
		KvStore: kv,
		mc:                mc,
		gtInsertMsgBuffer: list.New(),
		gtDeleteMsgBuffer: list.New(),
		insertTimeSync:    timeSync,
		deleteTimeSync:    timeSync,
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*schema.InsertMsg, timeSync uint64, wg sync.WaitGroup) error {
	var prefixKey string
	var suffixKey string
	var prefixKeys [][]byte
	var suffixKeys [][]byte
	var binaryData [][]byte
	var timeStamp []uint64

	wn.AddInsertMsgBufferData(&prefixKeys, &suffixKeys, &binaryData, &timeStamp, timeSync)

	for i := 0; i < len(data); i++ {
		if data[i].Timestamp <= timeSync {
			prefixKey = data[i].CollectionName + "_" + strconv.FormatInt(data[i].EntityId, 10)
			suffixKey = data[i].PartitionTag + "_" + strconv.FormatUint(data[i].SegmentId, 10)
			prefixKeys = append(prefixKeys, []byte(prefixKey))
			suffixKeys = append(suffixKeys, []byte(suffixKey))
			binaryData = append(binaryData, data[i].Serialization())
			timeStamp = append(timeStamp, data[i].Timestamp)
		} else {
			wn.gtInsertMsgBuffer.PushBack(data[i])
		}
	}

	(*wn.KvStore).PutRows(ctx, prefixKeys, timeStamp, suffixKeys, binaryData)
	wn.UpdateInsertTimeSync(timeSync)
	wg.Done()
	return nil
}

func (wn *WriteNode) DeleteBatchData(ctx context.Context, data []*schema.DeleteMsg, timeSync uint64, wg sync.WaitGroup) error {
	var prefixKey string
	var prefixKeys [][]byte
	var timeStamps []uint64

	wn.AddDeleteMsgBufferData(&prefixKeys, &timeStamps, timeSync)

	for i := 0; i < len(data); i++ {
		if data[i].Timestamp <= timeSync {
			prefixKey = data[i].CollectionName + "_" + strconv.FormatInt(data[i].EntityId, 10) + "_"
			prefixKeys = append(prefixKeys, []byte(prefixKey))
			timeStamps = append(timeStamps, data[i].Timestamp)
		} else {
			wn.gtDeleteMsgBuffer.PushBack(data[i])
		}
	}

	err := (*wn.KvStore).DeleteRows(ctx, prefixKeys, timeStamps)
	if err != nil {
		fmt.Println("Can't insert data")
	}
	wn.UpdateDeleteTimeSync(timeSync)
	wg.Done()
	return nil
}

func (wn *WriteNode) UpdateInsertTimeSync(timeSync uint64) {
	wn.insertTimeSync = timeSync
}

func (wn *WriteNode) UpdateDeleteTimeSync(timeSync uint64) {
	wn.deleteTimeSync = timeSync
}

func (wn *WriteNode) AddInsertMsgBufferData(
	prefixKeys *[][]byte,
	suffixKeys *[][]byte,
	data *[][]byte,
	timeStamp *[]uint64,
	timeSync uint64) {
	var prefixKey string
	var suffixKey string
	var selectElement []*list.Element
	for e := wn.gtInsertMsgBuffer.Front(); e != nil; e = e.Next() {
		collectionName := e.Value.(*schema.InsertMsg).CollectionName
		partitionTag := e.Value.(*schema.InsertMsg).PartitionTag
		segmentId := e.Value.(*schema.InsertMsg).SegmentId
		if e.Value.(*schema.InsertMsg).Timestamp <= timeSync {
			prefixKey = collectionName + "_" + strconv.FormatInt(e.Value.(*schema.InsertMsg).EntityId, 10)
			suffixKey = partitionTag + "_" + strconv.FormatUint(segmentId, 10)
			*prefixKeys = append(*prefixKeys, []byte(prefixKey))
			*suffixKeys = append(*suffixKeys, []byte(suffixKey))
			*data = append(*data, e.Value.(*schema.InsertMsg).Serialization())
			*timeStamp = append(*timeStamp, e.Value.(*schema.InsertMsg).Timestamp)
			selectElement = append(selectElement, e)
		}
	}
	for i := 0; i < len(selectElement); i++ {
		wn.gtInsertMsgBuffer.Remove(selectElement[i])
	}
}

func (wn *WriteNode) AddDeleteMsgBufferData(prefixKeys *[][]byte,
	timeStamps *[]uint64,
	timeSync uint64) {
	var prefixKey string
	var selectElement []*list.Element
	for e := wn.gtDeleteMsgBuffer.Front(); e != nil; e = e.Next() {
		collectionName := e.Value.(*schema.InsertMsg).CollectionName
		if e.Value.(*schema.DeleteMsg).Timestamp <= timeSync {
			prefixKey = collectionName + "_" + strconv.FormatInt(e.Value.(*schema.InsertMsg).EntityId, 10) + "_"
			*prefixKeys = append(*prefixKeys, []byte(prefixKey))
			*timeStamps = append(*timeStamps, e.Value.(*schema.DeleteMsg).Timestamp)
			selectElement = append(selectElement, e)
		}
	}
	for i := 0; i < len(selectElement); i++ {
		wn.gtDeleteMsgBuffer.Remove(selectElement[i])
	}
}

func (wn *WriteNode) GetInsertBuffer() *list.List {
	return wn.gtInsertMsgBuffer
}

func (wn *WriteNode) GetDeleteBuffer() *list.List {
	return wn.gtDeleteMsgBuffer
}

func (wn *WriteNode) doWriteNode(ctx context.Context, wg sync.WaitGroup) {
	//deleteTimeSync := make(map[string]uint64)
	//insertTimeSync := make(map[string]uint64)
	//wg.Add(2)
	//go wn.InsertBatchData(ctx, wn.mc.InsertMsg, insertTimeSync, wg)
	//go wn.DeleteBatchData(ctx, wn.mc.DeleteMsg, deleteTimeSync, wg)
	//wg.Wait()
}
