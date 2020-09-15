package reader

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestSegment_ConstructorAndDestructor(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentInsert(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Create ids and timestamps
	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	// 3. Create records, use schema below:
	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
	// schema_tmp->AddField("age", DataType::INT32);
	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	// 4. Do PreInsert
	var offset = segment.SegmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 5. Do Insert
	var err = segment.SegmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	// 6. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentDelete(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Create ids and timestamps
	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	// 3. Do PreDelete
	var offset = segment.SegmentPreDelete(10)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 4. Do Delete
	var err = segment.SegmentDelete(offset, &ids, &timestamps)
	assert.NoError(t, err)

	// 5. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentSearch(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Create ids and timestamps
	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	// 3. Create records, use schema below:
	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
	// schema_tmp->AddField("age", DataType::INT32);
	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	// 4. Do PreInsert
	var offset = segment.SegmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 5. Do Insert
	var err = segment.SegmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	// 6. Do search
	var searchRes, searchErr = segment.SegmentSearch("fake query string", timestamps[0], nil)
	assert.NoError(t, searchErr)
	fmt.Println(searchRes)

	// 7. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentPreInsert(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Do PreInsert
	var offset = segment.SegmentPreInsert(10)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 3. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_SegmentPreDelete(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Do PreDelete
	var offset = segment.SegmentPreDelete(10)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 3. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

//  Segment util functions test
////////////////////////////////////////////////////////////////////////////
func TestSegment_GetStatus(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Get segment status
	var status = segment.GetStatus()
	assert.Equal(t, status, SegmentOpened)

	// 3. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_Close(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Close segment
	var err = segment.Close()
	assert.NoError(t, err)

	// 3. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_GetRowCount(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Create ids and timestamps
	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	// 3. Create records, use schema below:
	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
	// schema_tmp->AddField("age", DataType::INT32);
	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records [][]byte
	for i := 0; i < N; i++ {
		records = append(records, rawData)
	}

	// 4. Do PreInsert
	var offset = segment.SegmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 5. Do Insert
	var err = segment.SegmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	// 6. Get segment row count
	var rowCount = segment.GetRowCount()
	assert.Equal(t, rowCount, int64(len(ids)))

	// 7. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}

func TestSegment_GetDeletedCount(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)

	// 2. Create ids and timestamps
	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	// 3. Do PreDelete
	var offset = segment.SegmentPreDelete(10)
	assert.GreaterOrEqual(t, offset, int64(0))

	// 4. Do Delete
	var err = segment.SegmentDelete(offset, &ids, &timestamps)
	assert.NoError(t, err)

	// 5. Get segment deleted count
	var deletedCount = segment.GetDeletedCount()
	// TODO: assert.Equal(t, deletedCount, len(ids))
	assert.Equal(t, deletedCount, int64(0))

	// 6. Destruct node, collection, and segment
	partition.DeleteSegment(segment)
	collection.DeletePartition(partition)
	node.DeleteCollection(collection)
}
