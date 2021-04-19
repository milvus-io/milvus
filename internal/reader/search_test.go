package reader

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

// NOTE: start pulsar before test
func TestSearch_Search(t *testing.T) {
	conf.LoadConfig("config.yaml")

	ctx, _ := context.WithCancel(context.Background())

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

	var queryRawData = make([]float32, 0)
	for i := 0; i < DIM; i++ {
		queryRawData = append(queryRawData, float32(i))
	}

	var queryJson = "{\"field_name\":\"fakevec\",\"num_queries\":1,\"topK\":10}"
	searchMsg1 := msgPb.SearchMsg{
		CollectionName: "collection0",
		Records: &msgPb.VectorRowRecord{
			FloatData: queryRawData,
		},
		PartitionTag: []string{"partition0"},
		Uid:          int64(0),
		Timestamp:    uint64(0),
		ClientId:     int64(0),
		ExtraParams:  nil,
		Json:         []string{queryJson},
	}
	searchMessages := []*msgPb.SearchMsg{&searchMsg1}

	node.queryNodeTimeSync.updateSearchServiceTime(timeRange)
	assert.Equal(t, node.queryNodeTimeSync.ServiceTimeSync, timeRange.timestampMax)

	status := node.Search(searchMessages)
	assert.Equal(t, status.ErrorCode, msgPb.ErrorCode_SUCCESS)

	node.Close()
}
