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
	node, err := genSimpleQueryNode(ctx, t)
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

	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"

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

func TestReduce_nilPlan(t *testing.T) {
	plan := &SearchPlan{}
	err := reduceSearchResultsAndFillData(plan, nil, 1)
	assert.Error(t, err)
}
