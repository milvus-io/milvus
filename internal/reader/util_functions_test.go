package reader

//import (
//	"context"
//	"strconv"
//	"testing"
//	"time"
//
//	"github.com/zilliztech/milvus-distributed/internal/conf"
//	"github.com/zilliztech/milvus-distributed/internal/msgclient"
//	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
//
//	"github.com/stretchr/testify/assert"
//)
//
//// NOTE: start pulsar before test
//func TestUtilFunctions_GetKey2Segments(t *testing.T) {
//	conf.LoadConfig("config.yaml")
//
//	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
//	ctx, cancel := context.WithDeadline(context.Background(), d)
//	defer cancel()
//
//	mc := msgclient.ReaderMessageClient{}
//	pulsarAddr := "pulsar://"
//	pulsarAddr += conf.Config.Pulsar.Address
//	pulsarAddr += ":"
//	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
//
//	mc.InitClient(ctx, pulsarAddr)
//	mc.ReceiveMessage()
//
//	node := CreateQueryNode(ctx, 0, 0, &mc)
//
//	node.messageClient.PrepareKey2SegmentMsg()
//
//	const msgLength = 10
//
//	for i := 0; i < msgLength; i++ {
//		key2SegMsg := msgPb.Key2SegMsg{
//			Uid:       int64(i),
//			Timestamp: uint64(i + 1000),
//			SegmentId: []int64{int64(i)},
//		}
//		node.messageClient.Key2SegMsg = append(node.messageClient.Key2SegMsg, &key2SegMsg)
//	}
//
//	entityIDs, timestamps, segmentIDs := node.GetKey2Segments()
//
//	assert.Equal(t, len(*entityIDs), msgLength)
//	assert.Equal(t, len(*timestamps), msgLength)
//	assert.Equal(t, len(*segmentIDs), msgLength)
//
//	node.Close()
//}
//
//func TestUtilFunctions_GetCollectionByID(t *testing.T) {
//	ctx := context.Background()
//
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.PartitionName, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	c := node.getCollectionByID(int64(0))
//	assert.Equal(t, c.CollectionName, "collection0")
//
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestUtilFunctions_GetCollectionByCollectionName(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, and collections
//	node := NewQueryNode(ctx, 0, 0)
//	var _ = node.newCollection(0, "collection0", "")
//
//	// 2. Get collection by collectionName
//	var c0, err = node.getCollectionByCollectionName("collection0")
//	assert.NoError(t, err)
//	assert.Equal(t, c0.CollectionName, "collection0")
//
//	c0 = node.getCollectionByID(0)
//	assert.NotNil(t, c0)
//	assert.Equal(t, c0.CollectionID, uint64(0))
//
//	node.Close()
//}
//
//func TestUtilFunctions_GetSegmentBySegmentID(t *testing.T) {
//	ctx := context.Background()
//
//	// 1. Construct node, collection, partition and segment
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//	node.SegmentsMap[0] = segment
//
//	// 2. Get segment by segment id
//	var s0, err = node.getSegmentBySegmentID(0)
//	assert.NoError(t, err)
//	assert.Equal(t, s0.SegmentID, int64(0))
//
//	node.Close()
//}
//
//func TestUtilFunctions_FoundSegmentBySegmentID(t *testing.T) {
//	ctx := context.Background()
//
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.PartitionName, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	b1 := node.foundSegmentBySegmentID(int64(0))
//	assert.Equal(t, b1, true)
//
//	b2 := node.foundSegmentBySegmentID(int64(1))
//	assert.Equal(t, b2, false)
//
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestUtilFunctions_GetPartitionByName(t *testing.T) {
//	ctx := context.Background()
//
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//
//	var p = collection.getPartitionByName("partition0")
//	assert.Equal(t, p.PartitionName, "partition0")
//
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	node.Close()
//}
//
//// NOTE: start pulsar before test
//func TestUtilFunctions_PrepareBatchMsg(t *testing.T) {
//	conf.LoadConfig("config.yaml")
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	mc := msgclient.ReaderMessageClient{}
//	pulsarAddr := "pulsar://"
//	pulsarAddr += conf.Config.Pulsar.Address
//	pulsarAddr += ":"
//	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
//
//	mc.InitClient(ctx, pulsarAddr)
//	mc.ReceiveMessage()
//
//	node := CreateQueryNode(ctx, 0, 0, &mc)
//
//	node.PrepareBatchMsg()
//	node.Close()
//}
//
//func TestUtilFunctions_QueryJson2Info(t *testing.T) {
//	ctx := context.Background()
//	node := NewQueryNode(ctx, 0, 0)
//
//	var queryJSON = "{\"field_name\":\"age\",\"num_queries\":1,\"topK\":10}"
//	info := node.queryJSON2Info(&queryJSON)
//
//	assert.Equal(t, info.FieldName, "age")
//	assert.Equal(t, info.NumQueries, int64(1))
//	assert.Equal(t, info.TopK, 10)
//
//	node.Close()
//}
