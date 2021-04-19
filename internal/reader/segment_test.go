package reader

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/stretchr/testify/assert"
)

//-------------------------------------------------------------------------------------- constructor and destructor
func TestSegment_newSegment(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)
}

func TestSegment_deleteSegment(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	deleteSegment(segment)
}

//-------------------------------------------------------------------------------------- stats functions
func TestSegment_getRowCount(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offset = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	err := segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	rowCount := segment.getRowCount()
	assert.Equal(t, int64(N), rowCount)
}

func TestSegment_getDeletedCount(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offsetInsert = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	var err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	err = segment.segmentDelete(offsetDelete, &ids, &timestamps)
	assert.NoError(t, err)

	var deletedCount = segment.getDeletedCount()
	// TODO: assert.Equal(t, deletedCount, len(ids))
	assert.Equal(t, deletedCount, int64(0))
}

func TestSegment_getMemSize(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offset = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	err := segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var memSize = segment.getMemSize()
	assert.Equal(t, memSize, int64(2785280))
}

//-------------------------------------------------------------------------------------- dm & search functions
func TestSegment_segmentInsert(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offset = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))

	err := segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)
}

func TestSegment_segmentDelete(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offsetInsert = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	var err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	err = segment.segmentDelete(offsetDelete, &ids, &timestamps)
	assert.NoError(t, err)
}

//func TestSegment_segmentSearch(t *testing.T) {
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

//-------------------------------------------------------------------------------------- preDm functions
func TestSegment_segmentPreInsert(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offset = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offset, int64(0))
}

func TestSegment_segmentPreDelete(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(&collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

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
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	var offsetInsert = segment.segmentPreInsert(N)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	var err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))
}
