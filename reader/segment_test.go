package reader

import (
	//"github.com/realistschuckle/testify/assert"
	"testing"
)

func TestConstructorAndDestructor(t *testing.T) {
	node := NewQueryNode(0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}
