// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"encoding/binary"
	"log"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func TestReduce_AllFunc(t *testing.T) {
	collectionID := UniqueID(0)
	segmentID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)

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
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData1, searchRawData2},
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	plan, err := createSearchPlan(collection, dslString)
	assert.NoError(t, err)
	holder, err := parseSearchRequest(plan, placeGroupByte)
	assert.NoError(t, err)
	placeholderGroups := make([]*searchRequest, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResults := make([]*SearchResult, 0)
	searchResult, err := segment.search(plan, placeholderGroups, []Timestamp{0})
	assert.Nil(t, err)
	searchResults = append(searchResults, searchResult)

	err = reduceSearchResultsAndFillData(plan, searchResults, 1)
	assert.Nil(t, err)

	marshaledHits, err := reorganizeSearchResults(searchResults, 1)
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
			unMarshaledHit := milvuspb.Hits{}
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

func TestReduce_nilPlan(t *testing.T) {
	plan := &SearchPlan{}
	err := reduceSearchResultsAndFillData(plan, nil, 1)
	assert.Error(t, err)
}
