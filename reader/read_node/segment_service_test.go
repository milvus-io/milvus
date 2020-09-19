package reader

import (
	"testing"
)

func TestSegmentManagement_SegmentsManagement(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection(0, "collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: fix segment management
	node.SegmentsManagement()
}

func TestSegmentManagement_SegmentService(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection(0, "collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: fix segment service
	node.SegmentManagementService()
}

func TestSegmentManagement_SegmentStatistic(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection(0, "collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: start pulsar server
	node.SegmentStatistic(1000)
}

func TestSegmentManagement_SegmentStatisticService(t *testing.T) {
	// Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection(0, "collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// TODO: start pulsar server
	node.SegmentStatisticService()
}
