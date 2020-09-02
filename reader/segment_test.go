package reader

import (
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

//func TestSegmentInsert(t *testing.T) {
//	node := NewQueryNode(0, 0)
//	var collection = node.NewCollection("collection0", "fake schema")
//	var partition = collection.NewPartition("partition0")
//	var segment = partition.NewSegment(0)
//
//	const DIM = 4
//	const N = 3
//
//	var ids = [N]uint64{1, 2, 3}
//	var timestamps = [N]uint64{0, 0, 0}
//
//	var vec = [DIM]float32{1.1, 2.2, 3.3, 4.4}
//	var rawData []int8
//
//	for i := 0; i <= N; i++ {
//		for _, ele := range vec {
//			rawData=append(rawData, int8(ele))
//		}
//		rawData=append(rawData, int8(i))
//	}
//
//	const sizeofPerRow = 4 + DIM * 4
//	var res = Insert(segment, N, (*C.ulong)(&ids[0]), (*C.ulong)(&timestamps[0]), unsafe.Pointer(&rawData[0]), C.int(sizeofPerRow), C.long(N))
//	assert.Equal()
//
//	partition.DeleteSegment(segment)
//	collection.DeletePartition(partition)
//	node.DeleteCollection(collection)
//}

func TestSegmentDelete(t *testing.T) {
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	ids :=[] int64{1, 2, 3}
	timestamps :=[] uint64 {0, 0, 0}

	SegmentDelete(segment, &ids, &timestamps)

	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

//func TestSegmentSearch(t *testing.T) {
//	node := NewQueryNode(0, 0)
//	var collection = node.NewCollection("collection0", "fake schema")
//	var partition = collection.NewPartition("partition0")
//	var segment = partition.NewSegment(0)
//
//	const DIM = 4
//	const N = 3
//
//	var ids = [N]uint64{1, 2, 3}
//	var timestamps = [N]uint64{0, 0, 0}
//
//	var vec = [DIM]float32{1.1, 2.2, 3.3, 4.4}
//	var rawData []int8
//
//	for i := 0; i <= N; i++ {
//		for _, ele := range vec {
//			rawData=append(rawData, int8(ele))
//		}
//		rawData=append(rawData, int8(i))
//	}
//
//	const sizeofPerRow = 4 + DIM * 4
//	SegmentSearch(segment, "fake query string", &timestamps, nil)
//
//	partition.DeleteSegment(segment)
//	collection.DeletePartition(partition)
//	node.DeleteCollection(collection)
//}
