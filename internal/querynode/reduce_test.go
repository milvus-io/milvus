package querynode

import (
	"encoding/binary"
	"log"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

func TestReduce_AllFunc(t *testing.T) {
	fieldVec := schemapb.FieldSchema{
		Name:         "vec",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name:   "collection0",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
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
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// start search service
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
	var searchRawData1 []byte
	var searchRawData2 []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData1 = append(searchRawData1, buf...)
	}
	for i, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
		searchRawData2 = append(searchRawData2, buf...)
	}
	placeholderValue := servicepb.PlaceholderValue{
		Tag:    "$0",
		Type:   servicepb.PlaceholderType_VECTOR_FLOAT,
		Values: [][]byte{searchRawData1, searchRawData2},
	}

	placeholderGroup := servicepb.PlaceholderGroup{
		Placeholders: []*servicepb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	plan := createPlan(*collection, dslString)
	holder := parserPlaceholderGroup(plan, placeGroupByte)
	placeholderGroups := make([]*PlaceholderGroup, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResults := make([]*SearchResult, 0)
	searchResult, err := segment.segmentSearch(plan, placeholderGroups, []Timestamp{0})
	assert.Nil(t, err)
	searchResults = append(searchResults, searchResult)

	reducedSearchResults := reduceSearchResults(searchResults, 1)
	assert.NotNil(t, reducedSearchResults)

	marshaledHits := reducedSearchResults.reorganizeQueryResults(plan, placeholderGroups)
	assert.NotNil(t, marshaledHits)

	hitsBlob, err := marshaledHits.getHitsBlob()
	assert.Nil(t, err)

	var offset int64 = 0
	for index := range placeholderGroups {
		hitBolbSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
		assert.Nil(t, err)
		for _, len := range hitBolbSizePeerQuery {
			marshaledHit := hitsBlob[offset : offset+len]
			unMarshaledHit := servicepb.Hits{}
			err = proto.Unmarshal(marshaledHit, &unMarshaledHit)
			assert.Nil(t, err)
			log.Println("hits msg  = ", unMarshaledHit)
			offset += len
		}
	}

	plan.delete()
	holder.delete()
	deleteSearchResults(searchResults)
	deleteSearchResults([]*SearchResult{reducedSearchResults})
	deleteMarshaledHits(marshaledHits)
	deleteSegment(segment)
	deleteCollection(collection)
}
