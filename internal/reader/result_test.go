package reader

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"strconv"
	"testing"
	"time"

	masterPb "github.com/zilliztech/milvus-distributed/internal/proto/master"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

// NOTE: start pulsar before test
func TestResult_PublishSearchResult(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	node := CreateQueryNode(ctx, 0, 0, &mc)

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	const N = 10
	var entityIDs = msgPb.Entities{
		Ids: make([]int64, N),
	}
	var result = msgPb.QueryResult{
		Entities:  &entityIDs,
		Distances: make([]float32, N),
	}
	for i := 0; i < N; i++ {
		result.Entities.Ids = append(result.Entities.Ids, int64(i))
		result.Distances = append(result.Distances, float32(i))
	}
	node.PublishSearchResult(&result)
	node.Close()
}

// NOTE: start pulsar before test
func TestResult_PublishFailedSearchResult(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	node := CreateQueryNode(ctx, 0, 0, &mc)

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: start pulsar server
	node.PublishFailedSearchResult()

	node.Close()
}

// NOTE: start pulsar before test
func TestResult_PublicStatistic(t *testing.T) {
	conf.LoadConfig("config.yaml")

	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	node := CreateQueryNode(ctx, 0, 0, &mc)

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	var statisticData = make([]masterPb.SegmentStat, 0)

	for segmentID, segment := range node.SegmentsMap {
		currentMemSize := segment.GetMemSize()
		memIncreaseRate := float32(0)
		stat := masterPb.SegmentStat{
			SegmentId:  uint64(segmentID),
			MemorySize: currentMemSize,
			MemoryRate: memIncreaseRate,
		}
		statisticData = append(statisticData, stat)
	}

	// TODO: start pulsar server
	node.PublicStatistic(&statisticData)

	node.Close()
}
