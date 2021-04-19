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
