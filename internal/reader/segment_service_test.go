package reader

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
)

//func TestSegmentManagement_SegmentsManagement(t *testing.T) {
//	// Construct node, collection, partition and segment
//	ctx := context.Background()
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.NewCollection(0, "collection0", "")
//	var partition = collection.NewPartition("partition0")
//	var segment = partition.NewSegment(0)
//	node.SegmentsMap[0] = segment
//
//	node.SegmentsManagement()
//
//	node.Close()
//}

//func TestSegmentManagement_SegmentService(t *testing.T) {
//	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
//	ctx, _ := context.WithDeadline(context.Background(), d)
//
//	// Construct node, collection, partition and segment
//	node := NewQueryNode(ctx, 0, 0)
//	var collection = node.NewCollection(0, "collection0", "")
//	var partition = collection.NewPartition("partition0")
//	var segment = partition.NewSegment(0)
//	node.SegmentsMap[0] = segment
//
//	node.SegmentManagementService()
//
//	node.Close()
//}

// NOTE: start pulsar before test
func TestSegmentManagement_SegmentStatistic(t *testing.T) {
	conf.LoadConfig("config.yaml")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)

	mc.InitClient(ctx, pulsarAddr)
	mc.ReceiveMessage()

	node := CreateQueryNode(ctx, 0, 0, &mc)

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentStatistic(1000)

	node.Close()
}

// NOTE: start pulsar before test
func TestSegmentManagement_SegmentStatisticService(t *testing.T) {
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

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentStatisticService()

	node.Close()
}
