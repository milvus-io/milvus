package writer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	msgpb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/storage"
	storagetype "github.com/zilliztech/milvus-distributed/internal/storage/type"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type SegmentIdInfo struct {
	CollectionName string
	EntityId       int64
	SegmentIds     []string
}

type MsgCounter struct {
	InsertCounter int64
	InsertTime    time.Time
	// InsertedRecordSize float64

	DeleteCounter int64
	DeleteTime    time.Time
}

type InsertLog struct {
	MsgLength              int
	DurationInMilliseconds int64
	InsertTime             time.Time
	NumSince               int64
	Speed                  float64
}

type WriteNode struct {
	KvStore       *storagetype.Store
	MessageClient *msgclient.WriterMessageClient
	TimeSync      uint64
	MsgCounter    *MsgCounter
	InsertLogs    []InsertLog
}

func (wn *WriteNode) Close() {
	wn.MessageClient.Close()
}

func NewWriteNode(ctx context.Context,
	address string,
	topics []string,
	timeSync uint64) (*WriteNode, error) {
	kv, err := storage.NewStore(context.Background(), storagetype.MinIODriver)
	mc := msgclient.WriterMessageClient{}

	msgCounter := MsgCounter{
		InsertCounter: 0,
		InsertTime:    time.Now(),
		DeleteCounter: 0,
		DeleteTime:    time.Now(),
		// InsertedRecordSize: 0,
	}

	return &WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      timeSync,
		MsgCounter:    &msgCounter,
		InsertLogs:    make([]InsertLog, 0),
	}, err
}

func (wn *WriteNode) InsertBatchData(ctx context.Context, data []*msgpb.InsertOrDeleteMsg, wg *sync.WaitGroup) error {
	var prefixKey string
	var suffixKey string
	var prefixKeys [][]byte
	var suffixKeys []string
	var binaryData [][]byte
	var timeStamp []uint64
	byteArr := make([]byte, 8)
	intData := uint64(0)
	binary.BigEndian.PutUint64(byteArr, intData)

	for i := 0; i < len(data); i++ {
		prefixKey = data[i].CollectionName + "-" + strconv.FormatUint(uint64(data[i].Uid), 10)
		suffixKey = strconv.FormatUint(uint64(data[i].SegmentId), 10)
		prefixKeys = append(prefixKeys, []byte(prefixKey))
		suffixKeys = append(suffixKeys, suffixKey)
		binaryData = append(binaryData, byteArr)
		timeStamp = append(timeStamp, uint64(data[i].Timestamp))
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

func (wn *WriteNode) DeleteBatchData(ctx context.Context, data []*msgpb.InsertOrDeleteMsg) error {
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
		return err
	}
	return nil
}

func (wn *WriteNode) UpdateTimeSync(timeSync uint64) {
	wn.TimeSync = timeSync
}

func (wn *WriteNode) DoWriteNode(ctx context.Context) {
	numInsertData := len(wn.MessageClient.InsertMsg)
	numGoRoute := conf.Config.Writer.Parallelism
	batchSize := numInsertData / numGoRoute
	if numInsertData%numGoRoute != 0 {
		batchSize += 1
	}
	start := 0
	end := 0
	wg := sync.WaitGroup{}
	for end < numInsertData {
		if end+batchSize >= numInsertData {
			end = numInsertData
		} else {
			end = end + batchSize
		}
		wg.Add(1)
		go wn.InsertBatchData(ctx, wn.MessageClient.InsertMsg[start:end], &wg)
		start = end
	}
	wg.Wait()
	wn.WriterLog(numInsertData)
	wn.DeleteBatchData(ctx, wn.MessageClient.DeleteMsg)
	wn.UpdateTimeSync(wn.MessageClient.TimeSync())
}

func (wn *WriteNode) WriterLog(length int) {
	wn.MsgCounter.InsertCounter += int64(length)
	timeNow := time.Now()
	duration := timeNow.Sub(wn.MsgCounter.InsertTime)
	speed := float64(length) / duration.Seconds()

	insertLog := InsertLog{
		MsgLength:              length,
		DurationInMilliseconds: duration.Milliseconds(),
		InsertTime:             timeNow,
		NumSince:               wn.MsgCounter.InsertCounter,
		Speed:                  speed,
	}

	wn.InsertLogs = append(wn.InsertLogs, insertLog)
	wn.MsgCounter.InsertTime = timeNow
}

func (wn *WriteNode) WriteWriterLog() {
	f, err := os.OpenFile("/tmp/write_node_insert.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// write logs
	for _, insertLog := range wn.InsertLogs {
		insertLogJson, err := json.Marshal(&insertLog)
		if err != nil {
			log.Fatal(err)
		}

		writeString := string(insertLogJson) + "\n"
		//fmt.Println(writeString)

		_, err2 := f.WriteString(writeString)
		if err2 != nil {
			log.Fatal(err2)
		}
	}

	// reset InsertLogs buffer
	wn.InsertLogs = make([]InsertLog, 0)

	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("write write node log done")
}
