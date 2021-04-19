package reader

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConstructorAndDestructor(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegmentInsert(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] uint64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	var _, err = SegmentInsert(segment, &ids, &timestamps, nil)
	assert.NoError(t, err)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegmentDelete(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] uint64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	var _, err = SegmentDelete(segment, &ids, &timestamps)
	assert.NoError(t, err)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegmentSearch(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] uint64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	var _, insertErr = SegmentInsert(segment, &ids, &timestamps, nil)
	assert.NoError(t, insertErr)

	var searchRes, searchErr = SegmentSearch(segment, "fake query string", &timestamps, nil)
	assert.NoError(t, searchErr)
	fmt.Println(searchRes)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_GetStatus(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	var status = segment.GetStatus()
	assert.Equal(t, status, SegmentOpened)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_Close(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	var err = segment.Close()
	assert.NoError(t, err)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_GetRowCount(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] uint64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	var _, err = SegmentInsert(segment, &ids, &timestamps, nil)
	assert.NoError(t, err)

	var rowCount = segment.GetRowCount()
	assert.Equal(t, rowCount, int64(len(ids)))

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_GetDeletedCount(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] uint64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	var _, err = SegmentDelete(segment, &ids, &timestamps)
	assert.NoError(t, err)

	var deletedCount = segment.GetDeletedCount()
	// TODO: assert.Equal(t, deletedCount, len(ids))
	assert.Equal(t, deletedCount, int64(0))

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_TimestampGetterAndSetter(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	const MinTimestamp = 100
	const MaxTimestamp = 200

	segment.SetMinTimestamp(MinTimestamp)
	var minTimestamp = segment.GetMinTimestamp()
	assert.Equal(t, minTimestamp, uint64(MinTimestamp))

	segment.SetMaxTimestamp(MaxTimestamp)
	var maxTimestamp = segment.GetMaxTimestamp()
	assert.Equal(t, maxTimestamp, uint64(MaxTimestamp))

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentIDGetterAndSetter(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	const SegmentID = 1

	segment.SetSegmentID(SegmentID)
	var segmentID = segment.GetSegmentID()
	assert.Equal(t, segmentID, uint64(SegmentID))

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}
