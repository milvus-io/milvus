package reader

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"strconv"
	"testing"
	"time"

	//masterPb "github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
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

	status := node.PublishSearchResult(&result)
	assert.Equal(t, status.ErrorCode, msgPb.ErrorCode_SUCCESS)

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

	status := node.PublishFailedSearchResult()
	assert.Equal(t, status.ErrorCode, msgPb.ErrorCode_SUCCESS)

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

	var statisticData = make([]internalpb.SegmentStatistics, 0)

	for segmentID, segment := range node.SegmentsMap {
		currentMemSize := segment.GetMemSize()
		stat := internalpb.SegmentStatistics{
			SegmentId:  segmentID,
			MemorySize: currentMemSize,
		}
		statisticData = append(statisticData, stat)
	}

	status := node.PublicStatistic(&statisticData)
	assert.Equal(t, status.ErrorCode, msgPb.ErrorCode_SUCCESS)

	node.Close()
}
