package querynode

import (
	"encoding/binary"
	"log"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

//-------------------------------------------------------------------------------------- constructor and destructor
func TestSegment_newSegment(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
	assert.Equal(t, segmentID, segment.segmentID)
	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_deleteSegment(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
	assert.Equal(t, segmentID, segment.segmentID)

	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- stats functions
func TestSegment_getRowCount(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_getDeletedCount(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteCollection(collection)
}

func TestSegment_getMemSize(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- dm & search functions
func TestSegment_segmentInsert(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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
	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentDelete(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteCollection(collection)
}

func TestSegment_segmentSearch(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"

	var searchRawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		searchRawData = append(searchRawData, buf...)
	}
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData},
	}

	placeholderGroup := servicepb.PlaceholderGroup{
		Placeholders: []*servicepb.PlaceholderValue{&placeholderValue},
	}

	placeHolderGroupBlob, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	searchTimestamp := Timestamp(1020)
	plan, err := createPlan(*collection, dslString)
	assert.NoError(t, err)
	holder, err := parserPlaceholderGroup(plan, placeHolderGroupBlob)
	assert.NoError(t, err)
	placeholderGroups := make([]*PlaceholderGroup, 0)
	placeholderGroups = append(placeholderGroups, holder)

	_, err = segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp})
	assert.Nil(t, err)

	plan.delete()
	holder.delete()
	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- preDm functions
func TestSegment_segmentPreInsert(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentPreDelete(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)
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

	deleteSegment(segment)
	deleteCollection(collection)
}
