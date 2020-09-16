package reader

import (
	masterPb "github.com/czs007/suvlim/pkg/master/grpc/master"
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"testing"
)

func TestResult_PublishSearchResult(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: start pulsar server
	const N = 10
	var entityIDs = msgPb.Entities {
		Ids: make([]int64, N),
	}
	var result = msgPb.QueryResult {
		Entities: &entityIDs,
		Distances: make([]float32, N),
	}
	for i := 0; i < N; i++ {
		result.Entities.Ids = append(result.Entities.Ids, int64(i))
		result.Distances = append(result.Distances, float32(i))
	}
	node.PublishSearchResult(&result)
}

func TestResult_PublishFailedSearchResult(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: start pulsar server
	node.PublishFailedSearchResult()
}

func TestResult_PublicStatistic(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
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
}
