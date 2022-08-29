// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

import (
	"context"
	"log"
	"math"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func TestReduce_parseSliceInfo(t *testing.T) {
	originNQs := []int64{2, 3, 2}
	originTopKs := []int64{10, 5, 20}
	nqPerSlice := int64(2)
	sInfo := parseSliceInfo(originNQs, originTopKs, nqPerSlice)

	expectedSliceNQs := []int64{2, 2, 1, 2}
	expectedSliceTopKs := []int64{10, 5, 5, 20}
	assert.True(t, funcutil.SliceSetEqual(sInfo.sliceNQs, expectedSliceNQs))
	assert.True(t, funcutil.SliceSetEqual(sInfo.sliceTopKs, expectedSliceTopKs))
}

func TestReduce_AllFunc(t *testing.T) {
	nq := int64(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	collection, err := node.metaReplica.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)

	segment, err := node.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
	assert.NoError(t, err)

	// TODO: replace below by genPlaceholderGroup(nq)
	vec := generateFloatVectors(1, defaultDim)
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: [][]byte{},
	}

	for i := 0; i < int(nq); i++ {
		placeholderValue.Values = append(placeholderValue.Values, searchRawData)
	}

	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"

	plan, err := createSearchPlan(collection, dslString)
	assert.NoError(t, err)
	searchReq, err := parseSearchRequest(plan, placeGroupByte)
	searchReq.timestamp = 0
	assert.NoError(t, err)

	searchResult, err := segment.search(searchReq)
	assert.NoError(t, err)

	err = checkSearchResult(nq, plan, searchResult)
	assert.NoError(t, err)

	searchReq.delete()
	deleteSegment(segment)
	deleteCollection(collection)
}

func TestReduce_Invalid(t *testing.T) {
	t.Run("nil plan", func(t *testing.T) {
		plan := &SearchPlan{}
		_, err := reduceSearchResultsAndFillData(plan, nil, 1, nil, nil)
		assert.Error(t, err)
	})

	t.Run("nil search result", func(t *testing.T) {
		collection := newCollection(defaultCollectionID, genTestCollectionSchema())
		searchReq, err := genSearchPlanAndRequests(collection, IndexHNSW, 10)
		assert.NoError(t, err)
		searchResults := make([]*SearchResult, 0)
		searchResults = append(searchResults, nil)
		_, err = reduceSearchResultsAndFillData(searchReq.plan, searchResults, 1, []int64{10}, []int64{10})
		assert.Error(t, err)
	})
}
