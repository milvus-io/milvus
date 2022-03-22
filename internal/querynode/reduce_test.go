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

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func TestReduce_AllFunc(t *testing.T) {
	nq := int64(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	collection, err := node.historical.replica.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)

	segment, err := node.historical.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	// TODO: replace below by genPlaceholderGroup(nq)
	vec := genSimpleFloatVectors()
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{},
	}

	for i := 0; i < int(nq); i++ {
		placeholderValue.Values = append(placeholderValue.Values, searchRawData)
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	dslString, err := genSimpleDSL()
	assert.NoError(t, err)

	plan, err := createSearchPlan(collection, dslString)
	assert.NoError(t, err)
	holder, err := parseSearchRequest(plan, placeGroupByte)
	assert.NoError(t, err)

	placeholderGroups := make([]*searchRequest, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResult, err := segment.search(plan, placeholderGroups, []Timestamp{0})
	assert.NoError(t, err)

	err = checkSearchResult(nq, plan, searchResult)
	assert.NoError(t, err)

	plan.delete()
	holder.delete()
	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSliceInfo(t *testing.T) {
	originNQs := []int64{3, 2, 3}
	nq := int64(2)
	originReqIDs := []UniqueID{100, 200, 300}
	sInfo, err := parseSliceInfo(originNQs, nq, originReqIDs)
	assert.NoError(t, err)

	expectedSlices := []int32{2, 1, 2, 2, 1}
	expectedReqIDs := []UniqueID{100, 100, 200, 300, 300}
	expectedReqNum := map[UniqueID]int64{100: 2, 200: 1, 300: 2}

	assert.Equal(t, len(expectedSlices), len(sInfo.slices))
	assert.Equal(t, len(expectedReqIDs), len(sInfo.reqIDs))
	for i := 0; i < len(expectedSlices); i++ {
		assert.Equal(t, expectedSlices[i], sInfo.slices[i])
		assert.Equal(t, expectedReqIDs[i], sInfo.reqIDs[i])
	}
	assert.Equal(t, len(expectedReqNum), len(sInfo.reqNum))
	for id, num := range expectedReqNum {
		assert.Equal(t, num, sInfo.reqNum[id])
	}

	expectedSliceOffset := make(map[UniqueID]int64)
	for i := 0; i < len(expectedSlices); i++ {
		num := sInfo.getSliceNum(i)
		reqID := sInfo.reqIDs[i]
		expectedSliceOffset[reqID]++
		assert.Equal(t, expectedReqNum[reqID], num)
		assert.Equal(t, expectedSliceOffset[reqID], sInfo.getSliceOffset(i))
	}
}

func TestReduce_nilPlan(t *testing.T) {
	plan := &SearchPlan{}
	err := reduceSearchResultsAndFillData(plan, nil, 1)
	assert.Error(t, err)
}
