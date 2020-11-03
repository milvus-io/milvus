package reader

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

func TestInsertAndDelete_MessagesPreprocess(t *testing.T) {
	ctx := context.Background()

	node := NewQueryNode(ctx, 0, 0)
	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_INSERT,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	assert.Equal(t, len(node.insertData.insertIDs), msgLength)
	assert.Equal(t, len(node.insertData.insertTimestamps), msgLength)
	assert.Equal(t, len(node.insertData.insertRecords), msgLength)
	assert.Equal(t, len(node.insertData.insertOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.Close()
}

// NOTE: start pulsar before test
func TestInsertAndDelete_WriterDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_DELETE,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	for i := 0; i < msgLength; i++ {
		key2SegMsg := msgPb.Key2SegMsg{
			Uid:       int64(i),
			Timestamp: uint64(i + 1000),
			SegmentId: []int64{int64(i)},
		}
		node.messageClient.Key2SegChan <- &key2SegMsg
	}

	assert.Equal(t, len(node.deleteData.deleteIDs), 0)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), 0)
	assert.Equal(t, len(node.deleteData.deleteOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength)
	assert.Equal(t, node.deletePreprocessData.count, int32(msgLength))

	node.WriterDelete()

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength)
	assert.Equal(t, node.deletePreprocessData.count, int32(0))

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.Close()
}

// NOTE: start pulsar before test
func TestInsertAndDelete_PreInsertAndDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength/2; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_INSERT,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	for i := 0; i < msgLength/2; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i + msgLength/2),
			ChannelId:    0,
			Op:           msgPb.OpType_DELETE,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	for i := 0; i < msgLength; i++ {
		key2SegMsg := msgPb.Key2SegMsg{
			Uid:       int64(i),
			Timestamp: uint64(i + 1000),
			SegmentId: []int64{int64(i)},
		}
		node.messageClient.Key2SegChan <- &key2SegMsg
	}

	assert.Equal(t, len(node.insertData.insertIDs), msgLength/2)
	assert.Equal(t, len(node.insertData.insertTimestamps), msgLength/2)
	assert.Equal(t, len(node.insertData.insertRecords), msgLength/2)
	assert.Equal(t, len(node.insertData.insertOffset), 0)

	assert.Equal(t, len(node.deleteData.deleteIDs), 0)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), 0)
	assert.Equal(t, len(node.deleteData.deleteOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength/2)
	assert.Equal(t, node.deletePreprocessData.count, int32(msgLength/2))

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.WriterDelete()

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength/2)
	assert.Equal(t, node.deletePreprocessData.count, int32(0))

	node.PreInsertAndDelete()

	assert.Equal(t, len(node.insertData.insertOffset), msgLength/2)

	assert.Equal(t, len(node.deleteData.deleteIDs), msgLength/2)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), msgLength/2)
	assert.Equal(t, len(node.deleteData.deleteOffset), msgLength/2)

	node.Close()
}

func TestInsertAndDelete_DoInsert(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	node := CreateQueryNode(ctx, 0, 0, &mc)

	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_INSERT,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	assert.Equal(t, len(node.insertData.insertIDs), msgLength)
	assert.Equal(t, len(node.insertData.insertTimestamps), msgLength)
	assert.Equal(t, len(node.insertData.insertRecords), msgLength)
	assert.Equal(t, len(node.insertData.insertOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.PreInsertAndDelete()

	assert.Equal(t, len(node.insertData.insertOffset), msgLength)

	wg := sync.WaitGroup{}
	for segmentID := range node.insertData.insertRecords {
		wg.Add(1)
		go node.DoInsert(segmentID, &wg)
	}
	wg.Wait()

	node.Close()
}

// NOTE: start pulsar before test
func TestInsertAndDelete_DoDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_DELETE,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	for i := 0; i < msgLength; i++ {
		key2SegMsg := msgPb.Key2SegMsg{
			Uid:       int64(i),
			Timestamp: uint64(i + 1000),
			SegmentId: []int64{int64(i)},
		}
		node.messageClient.Key2SegChan <- &key2SegMsg
	}

	assert.Equal(t, len(node.deleteData.deleteIDs), 0)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), 0)
	assert.Equal(t, len(node.deleteData.deleteOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength)
	assert.Equal(t, node.deletePreprocessData.count, int32(msgLength))

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.WriterDelete()

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength)
	assert.Equal(t, node.deletePreprocessData.count, int32(0))

	node.PreInsertAndDelete()

	assert.Equal(t, len(node.deleteData.deleteIDs), msgLength)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), msgLength)
	assert.Equal(t, len(node.deleteData.deleteOffset), msgLength)

	wg := sync.WaitGroup{}
	for segmentID, deleteIDs := range node.deleteData.deleteIDs {
		if segmentID < 0 {
			continue
		}
		wg.Add(1)
		var deleteTimestamps = node.deleteData.deleteTimestamps[segmentID]
		go node.DoDelete(segmentID, &deleteIDs, &deleteTimestamps, &wg)
	}
	wg.Wait()

	node.Close()
}

// NOTE: start pulsar before test
func TestInsertAndDelete_DoInsertAndDelete(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	var collection = node.NewCollection(0, "collection0", "")
	_ = collection.NewPartition("partition0")

	const msgLength = 10
	const DIM = 16
	const N = 3

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	insertDeleteMessages := make([]*msgPb.InsertOrDeleteMsg, 0)

	for i := 0; i < msgLength/2; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i),
			ChannelId:    0,
			Op:           msgPb.OpType_INSERT,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	for i := 0; i < msgLength/2; i++ {
		msg := msgPb.InsertOrDeleteMsg{
			CollectionName: "collection0",
			RowsData: &msgPb.RowData{
				Blob: rawData,
			},
			Uid:          int64(i),
			PartitionTag: "partition0",
			Timestamp:    uint64(i + 1000),
			SegmentId:    int64(i + msgLength/2),
			ChannelId:    0,
			Op:           msgPb.OpType_DELETE,
			ClientId:     0,
			ExtraParams:  nil,
		}
		insertDeleteMessages = append(insertDeleteMessages, &msg)
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)

	node.MessagesPreprocess(insertDeleteMessages, timeRange)

	for i := 0; i < msgLength; i++ {
		key2SegMsg := msgPb.Key2SegMsg{
			Uid:       int64(i),
			Timestamp: uint64(i + 1000),
			SegmentId: []int64{int64(i)},
		}
		node.messageClient.Key2SegChan <- &key2SegMsg
	}

	assert.Equal(t, len(node.insertData.insertIDs), msgLength/2)
	assert.Equal(t, len(node.insertData.insertTimestamps), msgLength/2)
	assert.Equal(t, len(node.insertData.insertRecords), msgLength/2)
	assert.Equal(t, len(node.insertData.insertOffset), 0)

	assert.Equal(t, len(node.deleteData.deleteIDs), 0)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), 0)
	assert.Equal(t, len(node.deleteData.deleteOffset), 0)

	assert.Equal(t, len(node.buffer.InsertDeleteBuffer), 0)
	assert.Equal(t, len(node.buffer.validInsertDeleteBuffer), 0)

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength/2)
	assert.Equal(t, node.deletePreprocessData.count, int32(msgLength/2))

	assert.Equal(t, len(node.SegmentsMap), 10)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 10)

	node.WriterDelete()

	assert.Equal(t, len(node.deletePreprocessData.deleteRecords), msgLength/2)
	assert.Equal(t, node.deletePreprocessData.count, int32(0))

	node.PreInsertAndDelete()

	assert.Equal(t, len(node.insertData.insertOffset), msgLength/2)

	assert.Equal(t, len(node.deleteData.deleteIDs), msgLength/2)
	assert.Equal(t, len(node.deleteData.deleteTimestamps), msgLength/2)
	assert.Equal(t, len(node.deleteData.deleteOffset), msgLength/2)

	status := node.DoInsertAndDelete()

	assert.Equal(t, status.ErrorCode, msgPb.ErrorCode_SUCCESS)

	node.Close()
}
