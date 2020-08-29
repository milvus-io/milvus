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

type CollectionMeta struct {
	collionName          string
	openSegmentId        string
	segmentCloseTime     uint64
	nextSegmentId        string
	nextSegmentCloseTime uint64
	deleteTimeSync       uint64
	insertTimeSync       uint64
}

type WriteNode struct {
	KvStore           *mock.TikvStore
	mc                *pulsar.MessageClient
	collectionMap     map[string]*CollectionMeta
	gtInsertMsgBuffer *list.List
	gtDeleteMsgBuffer *list.List
}

func NewWriteNode(ctx context.Context,
	collectionName []string,
	openSegmentId []string,
	closeTime []uint64,
	nextSegmentId []string,
	nextCloseSegmentTime []uint64,
	timeSync []uint64,
	mc *pulsar.MessageClient) (*WriteNode, error) {
	kv, err := mock.NewTikvStore()
	collectionMap := make(map[string]*CollectionMeta)
	for i := 0; i < len(collectionName); i++ {
		collectionMap[collectionName[i]] = &CollectionMeta{
			collionName:          collectionName[i],
			openSegmentId:        openSegmentId[i],
			segmentCloseTime:     closeTime[i],
			nextSegmentId:        nextSegmentId[i],
			nextSegmentCloseTime: nextCloseSegmentTime[i],
			deleteTimeSync:       timeSync[i],
			insertTimeSync:       timeSync[i],
		}
	}
	return &WriteNode{
		KvStore:           kv,
		mc:                mc,
		collectionMap:     collectionMap,
		gtInsertMsgBuffer: list.New(),
		gtDeleteMsgBuffer: list.New(),
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*schema.InsertMsg, timeSync map[string]uint64, wg sync.WaitGroup) error {
	var storeKey string
	keyMap := make(map[string][][]byte)
	binaryDataMap := make(map[string][][]byte)
	timeStampMap := make(map[string][]uint64)

	keyMap, binaryDataMap, timeStampMap = wn.AddInsertMsgBufferData(keyMap, binaryDataMap, timeStampMap, timeSync)

	for i := 0; i < len(data); i++ {
		if data[i].Timestamp <= timeSync[data[i].CollectionName] {
			CollectionName := data[i].CollectionName
			storeKey = data[i].CollectionName + strconv.FormatInt(data[i].EntityId, 10)
			keyMap[CollectionName] = append(keyMap[CollectionName], []byte(storeKey))
			binaryDataMap[CollectionName] = append(binaryDataMap[CollectionName], data[i].Serialization())
			timeStampMap[CollectionName] = append(timeStampMap[CollectionName], data[i].Timestamp)
		} else {
			wn.gtInsertMsgBuffer.PushBack(data[i])
		}
	}

	for k, v := range wn.collectionMap {
		if v.segmentCloseTime < timeSync[k] {
			v.openSegmentId = v.nextSegmentId
			v.segmentCloseTime = v.nextSegmentCloseTime
		}
	}

	for k, v := range keyMap {
		err := (*wn.KvStore).PutRows(ctx, v, binaryDataMap[k], wn.collectionMap[k].openSegmentId, timeStampMap[k])
		if err != nil {
			fmt.Println("Can't insert data")
		}
	}
	wn.UpdateInsertTimeSync(timeSync)
	wg.Done()
	return nil
}

func (wn *WriteNode) DeleteBatchData(ctx context.Context, data []*schema.DeleteMsg, timeSyncMap map[string]uint64, wg sync.WaitGroup) error {
	var storeKey string
	keyMap := make(map[string][][]byte)
	timeStampMap := make(map[string][]uint64)

	keyMap, timeStampMap = wn.AddDeleteMsgBufferData(keyMap, timeStampMap, timeSyncMap)

	for i := 0; i < len(data); i++ {
		if data[i].Timestamp <= timeSyncMap[data[i].CollectionName] {
			CollectionName := data[i].CollectionName
			storeKey = data[i].CollectionName + strconv.FormatInt(data[i].EntityId, 10)
			keyMap[CollectionName] = append(keyMap[CollectionName], []byte(storeKey))
			timeStampMap[CollectionName] = append(timeStampMap[CollectionName], data[i].Timestamp)
		} else {
			wn.gtDeleteMsgBuffer.PushBack(data[i])
		}
	}

	for k, v := range wn.collectionMap {
		if v.segmentCloseTime < timeSyncMap[k] {
			v.openSegmentId = v.nextSegmentId
			v.segmentCloseTime = v.nextSegmentCloseTime
		}
	}

	for k, v := range keyMap {
		err := (*wn.KvStore).DeleteRows(ctx, v, timeStampMap[k])
		if err != nil {
			fmt.Println("Can't insert data")
		}
	}
	wn.UpdateDeleteTimeSync(timeSyncMap)
	wg.Done()
	return nil
}

func (wn *WriteNode) AddNextSegment(collectionName string, segmentId string, closeSegmentTime uint64) {
	wn.collectionMap[collectionName].nextSegmentId = segmentId
	wn.collectionMap[collectionName].nextSegmentCloseTime = closeSegmentTime
}

func (wn *WriteNode) UpdateInsertTimeSync(timeSyncMap map[string]uint64) {
	for k, v := range wn.collectionMap {
		v.insertTimeSync = timeSyncMap[k]
	}
}

func (wn *WriteNode) UpdateDeleteTimeSync(timeSyncMap map[string]uint64) {
	for k, v := range wn.collectionMap {
		v.deleteTimeSync = timeSyncMap[k]
	}
}

func (wn *WriteNode) UpdateCloseTime(collectionName string, closeTime uint64) {
	wn.collectionMap[collectionName].segmentCloseTime = closeTime
}

func (wn *WriteNode) AddInsertMsgBufferData(keyMap map[string][][]byte,
	dataMap map[string][][]byte,
	timeStampMap map[string][]uint64,
	timeSyncMap map[string]uint64) (map[string][][]byte, map[string][][]byte, map[string][]uint64) {
	var storeKey string
	var selectElement []*list.Element
	for e := wn.gtInsertMsgBuffer.Front(); e != nil; e = e.Next() {
		collectionName := e.Value.(*schema.InsertMsg).CollectionName
		if e.Value.(*schema.InsertMsg).Timestamp <= timeSyncMap[collectionName] {
			storeKey = collectionName +
				strconv.FormatInt(e.Value.(*schema.InsertMsg).EntityId, 10)
			keyMap[collectionName] = append(keyMap[collectionName], []byte(storeKey))
			dataMap[collectionName] = append(dataMap[collectionName], e.Value.(*schema.InsertMsg).Serialization())
			timeStampMap[collectionName] = append(timeStampMap[collectionName], e.Value.(*schema.InsertMsg).Timestamp)
			selectElement = append(selectElement, e)
		}
	}
	for i := 0; i < len(selectElement); i++ {
		wn.gtInsertMsgBuffer.Remove(selectElement[i])
	}
	return keyMap, dataMap, timeStampMap
}

func (wn *WriteNode) AddDeleteMsgBufferData(keyMap map[string][][]byte,
	timeStampMap map[string][]uint64,
	timeSyncMap map[string]uint64) (map[string][][]byte, map[string][]uint64) {
	var storeKey string
	var selectElement []*list.Element
	for e := wn.gtDeleteMsgBuffer.Front(); e != nil; e = e.Next() {
		collectionName := e.Value.(*schema.InsertMsg).CollectionName
		if e.Value.(*schema.InsertMsg).Timestamp <= timeSyncMap[collectionName] {
			storeKey = collectionName +
				strconv.FormatInt(e.Value.(*schema.InsertMsg).EntityId, 10)
			keyMap[collectionName] = append(keyMap[collectionName], []byte(storeKey))
			timeStampMap[collectionName] = append(timeStampMap[collectionName], e.Value.(*schema.InsertMsg).Timestamp)
			selectElement = append(selectElement, e)
		}
	}
	for i := 0; i < len(selectElement); i++ {
		wn.gtDeleteMsgBuffer.Remove(selectElement[i])
	}
	return keyMap, timeStampMap
}

func (wn *WriteNode) AddCollection(collectionName string,
	openSegmentId string,
	closeTime uint64,
	nextSegmentId string,
	nextSegmentCloseTime uint64,
	timeSync uint64) {
	wn.collectionMap[collectionName] = &CollectionMeta{
		collionName:          collectionName,
		openSegmentId:        openSegmentId,
		segmentCloseTime:     closeTime,
		nextSegmentId:        nextSegmentId,
		nextSegmentCloseTime: nextSegmentCloseTime,
		deleteTimeSync:       timeSync,
		insertTimeSync:       timeSync,
	}
}

func (wn *WriteNode) DeleteCollection(collectionName string) {
	delete(wn.collectionMap, collectionName)
	var deleteMsg []*list.Element
	var insertMsg []*list.Element
	for e := wn.gtInsertMsgBuffer.Front(); e != nil; e = e.Next() {
		if e.Value.(*schema.InsertMsg).CollectionName == collectionName {
			insertMsg = append(insertMsg, e)
		}
	}
	for e := wn.gtDeleteMsgBuffer.Front(); e != nil; e = e.Next() {
		if e.Value.(*schema.DeleteMsg).CollectionName == collectionName {
			deleteMsg = append(deleteMsg, e)
		}
	}
	for i := 0; i < len(insertMsg); i++ {
		wn.gtInsertMsgBuffer.Remove(insertMsg[i])
	}
	for i := 0; i < len(deleteMsg); i++ {
		wn.gtDeleteMsgBuffer.Remove(deleteMsg[i])
	}
}

func (wn *WriteNode) doWriteNode(ctx context.Context, wg sync.WaitGroup) {
	deleteTimeSync := make(map[string]uint64)
	insertTimeSync := make(map[string]uint64)
	wg.Add(2)
	go wn.InsertBatchData(ctx, wn.mc.InsertMsg, insertTimeSync, wg)
	go wn.DeleteBatchData(ctx, wn.mc.DeleteMsg, deleteTimeSync, wg)
	wg.Wait()
}
