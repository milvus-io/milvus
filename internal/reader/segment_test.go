package reader

//import (
//	"context"
//	"encoding/binary"
//	"fmt"
//	"math"
//	"testing"
//
//	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
//
//	"github.com/stretchr/testify/assert"
//	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
//)
//
//func TestSegment_ConstructorAndDestructor(t *testing.T) {
//	// 1. Construct node, collection, partition and segment
//	ctx := context.Background()
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_SegmentInsert(t *testing.T) {
//	// 1. Construct node, collection, partition and segment
//	ctx := context.Background()
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Create records, use schema below:
//	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
//	// schema_tmp->AddField("age", DataType::INT32);
//	const DIM = 16
//	const N = 3
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	var rawData []byte
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		rawData = append(rawData, buf...)
//	}
//	bs := make([]byte, 4)
//	binary.LittleEndian.PutUint32(bs, 1)
//	rawData = append(rawData, bs...)
//	var records []*commonpb.Blob
//	for i := 0; i < N; i++ {
//		blob := &commonpb.Blob{
//			Value: rawData,
//		}
//		records = append(records, blob)
//	}
//
//	// 4. Do PreInsert
//	var offset = segment.segmentPreInsert(N)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 5. Do Insert
//	var err = segment.segmentInsert(offset, &ids, &timestamps, &records)
//	assert.NoError(t, err)
//
//	// 6. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_SegmentDelete(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Do PreDelete
//	var offset = segment.segmentPreDelete(10)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 4. Do Delete
//	var err = segment.segmentDelete(offset, &ids, &timestamps)
//	assert.NoError(t, err)
//
//	// 5. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_SegmentSearch(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := make([]int64, 0)
//	timestamps := make([]uint64, 0)
//
//	// 3. Create records, use schema below:
//	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
//	// schema_tmp->AddField("age", DataType::INT32);
//	const DIM = 16
//	const N = 100
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	var rawData []byte
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		rawData = append(rawData, buf...)
//	}
//	bs := make([]byte, 4)
//	binary.LittleEndian.PutUint32(bs, 1)
//	rawData = append(rawData, bs...)
//	var records []*commonpb.Blob
//	for i := 0; i < N; i++ {
//		blob := &commonpb.Blob{
//			Value: rawData,
//		}
//		ids = append(ids, int64(i))
//		timestamps = append(timestamps, uint64(i+1))
//		records = append(records, blob)
//	}
//
//	// 4. Do PreInsert
//	var offset = segment.segmentPreInsert(N)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 5. Do Insert
//	var err = segment.segmentInsert(offset, &ids, &timestamps, &records)
//	assert.NoError(t, err)
//
//	// 6. Do search
//	var queryJSON = "{\"field_name\":\"fakevec\",\"num_queries\":1,\"topK\":10}"
//	var queryRawData = make([]float32, 0)
//	for i := 0; i < 16; i++ {
//		queryRawData = append(queryRawData, float32(i))
//	}
//	var vectorRecord = msgPb.VectorRowRecord{
//		FloatData: queryRawData,
//	}
//
//	sService := searchService{}
//	query := sService.queryJSON2Info(&queryJSON)
//	var searchRes, searchErr = segment.segmentSearch(query, timestamps[N/2], &vectorRecord)
//	assert.NoError(t, searchErr)
//	fmt.Println(searchRes)
//
//	// 7. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_SegmentPreInsert(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Do PreInsert
//	var offset = segment.segmentPreInsert(10)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 3. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_SegmentPreDelete(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Do PreDelete
//	var offset = segment.segmentPreDelete(10)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 3. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_GetRowCount(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Create records, use schema below:
//	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
//	// schema_tmp->AddField("age", DataType::INT32);
//	const DIM = 16
//	const N = 3
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	var rawData []byte
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		rawData = append(rawData, buf...)
//	}
//	bs := make([]byte, 4)
//	binary.LittleEndian.PutUint32(bs, 1)
//	rawData = append(rawData, bs...)
//	var records []*commonpb.Blob
//	for i := 0; i < N; i++ {
//		blob := &commonpb.Blob{
//			Value: rawData,
//		}
//		records = append(records, blob)
//	}
//
//	// 4. Do PreInsert
//	var offset = segment.segmentPreInsert(N)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 5. Do Insert
//	var err = segment.segmentInsert(offset, &ids, &timestamps, &records)
//	assert.NoError(t, err)
//
//	// 6. Get segment row count
//	var rowCount = segment.getRowCount()
//	assert.Equal(t, rowCount, int64(len(ids)))
//
//	// 7. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_GetDeletedCount(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Do PreDelete
//	var offset = segment.segmentPreDelete(10)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 4. Do Delete
//	var err = segment.segmentDelete(offset, &ids, &timestamps)
//	assert.NoError(t, err)
//
//	// 5. Get segment deleted count
//	var deletedCount = segment.getDeletedCount()
//	// TODO: assert.Equal(t, deletedCount, len(ids))
//	assert.Equal(t, deletedCount, int64(0))
//
//	// 6. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
//
//func TestSegment_GetMemSize(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Create records, use schema below:
//	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
//	// schema_tmp->AddField("age", DataType::INT32);
//	const DIM = 16
//	const N = 3
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	var rawData []byte
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		rawData = append(rawData, buf...)
//	}
//	bs := make([]byte, 4)
//	binary.LittleEndian.PutUint32(bs, 1)
//	rawData = append(rawData, bs...)
//	var records []*commonpb.Blob
//	for i := 0; i < N; i++ {
//		blob := &commonpb.Blob{
//			Value: rawData,
//		}
//		records = append(records, blob)
//	}
//
//	// 4. Do PreInsert
//	var offset = segment.segmentPreInsert(N)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 5. Do Insert
//	var err = segment.segmentInsert(offset, &ids, &timestamps, &records)
//	assert.NoError(t, err)
//
//	// 6. Get memory usage in bytes
//	var memSize = segment.getMemSize()
//	assert.Equal(t, memSize, int64(2785280))
//
//	// 7. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}

//func TestSegment_RealSchemaTest(t *testing.T) {
//	ctx := context.Background()
//	// 1. Construct node, collection, partition and segment
//	var schemaString = "id: 6875229265736357360\nname: \"collection0\"\nschema: \u003c\n  " +
//		"field_metas: \u003c\n    field_name: \"field_3\"\n    type: INT32\n    dim: 1\n  \u003e\n  " +
//		"field_metas: \u003c\n    field_name: \"field_vec\"\n    type: VECTOR_FLOAT\n    dim: 16\n  " +
//		"\u003e\n\u003e\ncreate_time: 1600764055\nsegment_ids: 6875229265736357360\npartition_tags: \"default\"\n"
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//	var collection = node.newCollection(0, "collection0", schemaString)
//	var partition = collection.newPartition("partition0")
//	var segment = partition.newSegment(0)
//
//	node.SegmentsMap[int64(0)] = segment
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, segment.SegmentID, int64(0))
//	assert.Equal(t, len(node.SegmentsMap), 1)
//
//	// 2. Create ids and timestamps
//	ids := []int64{1, 2, 3}
//	timestamps := []uint64{0, 0, 0}
//
//	// 3. Create records, use schema below:
//	// schema_tmp->AddField("fakeVec", DataType::VECTOR_FLOAT, 16);
//	// schema_tmp->AddField("age", DataType::INT32);
//	const DIM = 16
//	const N = 3
//	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
//	var rawData []byte
//	for _, ele := range vec {
//		buf := make([]byte, 4)
//		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
//		rawData = append(rawData, buf...)
//	}
//	bs := make([]byte, 4)
//	binary.LittleEndian.PutUint32(bs, 1)
//	rawData = append(rawData, bs...)
//	var records []*commonpb.Blob
//	for i := 0; i < N; i++ {
//		blob := &commonpb.Blob {
//			Value: rawData,
//		}
//		records = append(records, blob)
//	}
//
//	// 4. Do PreInsert
//	var offset = segment.segmentPreInsert(N)
//	assert.GreaterOrEqual(t, offset, int64(0))
//
//	// 5. Do Insert
//	var err = segment.segmentInsert(offset, &ids, &timestamps, &records)
//	assert.NoError(t, err)
//
//	// 6. Destruct collection, partition and segment
//	partition.deleteSegment(node, segment)
//	collection.deletePartition(node, partition)
//	node.deleteCollection(collection)
//
//	assert.Equal(t, len(node.Collections), 0)
//	assert.Equal(t, len(node.SegmentsMap), 0)
//
//	node.Close()
//}
