package reader

import (
	msgPb "github.com/czs007/suvlim/pkg/message"
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
	// TODO: fix result PublishSearchResult
	const N = 10
	var entityIDs = msgPb.Entities {
		Ids: make([]int64, N),
	}
	var results = msgPb.QueryResult {
		Entities: &entityIDs,
		Distances: make([]float32, N),
	}
	for i := 0; i < N; i++ {
		results.Entities.Ids = append(results.Entities.Ids, int64(i))
		results.Distances = append(results.Distances, float32(i))
	}
	node.PublishSearchResult(&results, 0)
}
