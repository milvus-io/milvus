package writer

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/pulsar"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/writer/mock"
	"strconv"
	"sync"
)

type WriteNode struct {
	KvStore        *mock.TikvStore
	mc             *pulsar.MessageClient
	deleteTimeSync uint64
	insertTimeSync uint64
}

func NewWriteNode(ctx context.Context,
	address string,
	topics []string,
	timeSync uint64) (*WriteNode, error) {
	kv, err := mock.NewTikvStore()
	mc := &pulsar.MessageClient{}
	return &WriteNode{
		KvStore:        kv,
		mc:             mc,
		insertTimeSync: timeSync,
		deleteTimeSync: timeSync,
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*schema.InsertMsg, timeSync uint64, wg sync.WaitGroup) error {
	var prefixKey string
	var suffixKey string
	var prefixKeys [][]byte
	var suffixKeys [][]byte
	var binaryData [][]byte
	var timeStamp []uint64

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "_" + strconv.FormatInt(data[i].EntityId, 10)
		suffixKey = data[i].PartitionTag + "_" + strconv.FormatUint(data[i].SegmentId, 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		suffixKeys = append(suffixKeys, []byte(suffixKey))
		binaryData = append(binaryData, data[i].Serialization())
		timeStamp = append(timeStamp, data[i].Timestamp)
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

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "_" + strconv.FormatInt(data[i].EntityId, 10) + "_"
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		timeStamps = append(timeStamps, data[i].Timestamp)
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

func (wn *WriteNode) doWriteNode(ctx context.Context, deleteTimeSync uint64, insertTimeSync uint64, wg sync.WaitGroup) {
	wg.Add(2)
	go wn.InsertBatchData(ctx, wn.mc.InsertMsg, insertTimeSync, wg)
	go wn.DeleteBatchData(ctx, wn.mc.DeleteMsg, deleteTimeSync, wg)
	wg.Wait()
}
