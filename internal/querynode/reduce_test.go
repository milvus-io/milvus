package querynodeimp

import (
	"encoding/binary"
	"log"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

func TestReduce_AllFunc(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	segmentID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	segment := newSegment(collection, segmentID, Params.DefaultPartitionTag, collectionID)

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

	plan, err := createPlan(*collection, dslString)
	assert.NoError(t, err)
	holder, err := parserPlaceholderGroup(plan, placeGroupByte)
	assert.NoError(t, err)
	placeholderGroups := make([]*PlaceholderGroup, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResults := make([]*SearchResult, 0)
	matchedSegment := make([]*Segment, 0)
	searchResult, err := segment.segmentSearch(plan, placeholderGroups, []Timestamp{0})
	assert.Nil(t, err)
	searchResults = append(searchResults, searchResult)
	matchedSegment = append(matchedSegment, segment)

	testReduce := make([]bool, len(searchResults))
	err = reduceSearchResults(searchResults, 1, testReduce)
	assert.Nil(t, err)
	err = fillTargetEntry(plan, searchResults, matchedSegment, testReduce)
	assert.Nil(t, err)

	marshaledHits, err := reorganizeQueryResults(plan, placeholderGroups, searchResults, 1, testReduce)
	assert.NotNil(t, marshaledHits)
	assert.Nil(t, err)

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
	deleteMarshaledHits(marshaledHits)
	deleteSegment(segment)
	deleteCollection(collection)
}
