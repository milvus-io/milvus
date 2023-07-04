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

package segments

import (
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/common"
)

type PlanSuite struct {
	suite.Suite

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
}

func (suite *PlanSuite) SetupTest() {
	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1
	schema := GenTestCollectionSchema("plan-suite", schemapb.DataType_Int64)
	suite.collection = NewCollection(suite.collectionID, schema, GenTestIndexMeta(suite.collectionID, schema), querypb.LoadType_LoadCollection)
	suite.collection.AddPartition(suite.partitionID)
}

func (suite *PlanSuite) TearDownTest() {
	DeleteCollection(suite.collection)
}

func (suite *PlanSuite) TestPlanDSL() {
	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"

	plan, err := createSearchPlan(suite.collection, dslString, "")
	defer plan.delete()
	suite.NoError(err)
	suite.NotEqual(plan, nil)
	topk := plan.getTopK()
	suite.Equal(int(topk), 10)
	metricType := plan.getMetricType()
	suite.Equal(metricType, "L2")
}

func (suite *PlanSuite) TestPlanCreateByExpr() {
	planNode := &planpb.PlanNode{
		OutputFieldIds: []int64{rowIDFieldID},
	}
	expr, err := proto.Marshal(planNode)
	suite.NoError(err)

	_, err = createSearchPlanByExpr(suite.collection, expr, "")
	suite.Error(err)
}

func (suite *PlanSuite) TestPlanFail() {
	collection := &Collection{
		id: -1,
	}

	_, err := createSearchPlan(collection, "", "")
	suite.Error(err)

	_, err = createSearchPlanByExpr(collection, nil, "")
	suite.Error(err)
}

func (suite *PlanSuite) TestPlanPlaceholderGroup() {
	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"
	plan, err := createSearchPlan(suite.collection, dslString, "")
	suite.NoError(err)
	suite.NotNil(plan)

	var searchRawData1 []byte
	var searchRawData2 []byte
	var vec = generateFloatVectors(1, defaultDim)
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData1 = append(searchRawData1, buf...)
	}
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
		searchRawData2 = append(searchRawData2, buf...)
	}
	placeholderValue := commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData1, searchRawData2},
	}

	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	suite.Nil(err)
	holder, err := parseSearchRequest(plan, placeGroupByte)
	suite.NoError(err)
	suite.NotNil(holder)
	numQueries := holder.getNumOfQuery()
	suite.Equal(int(numQueries), 2)

	holder.Delete()
}

func (suite *PlanSuite) TestPlanNewSearchRequest() {
	nq := int64(10)

	iReq, _ := genSearchRequest(nq, IndexHNSW, suite.collection)
	req := &querypb.SearchRequest{
		Req:             iReq,
		DmlChannels:     []string{"dml"},
		SegmentIDs:      []int64{suite.segmentID},
		FromShardLeader: true,
		Scope:           querypb.DataScope_Historical,
	}
	searchReq, err := NewSearchRequest(suite.collection, req, req.Req.GetPlaceholderGroup())
	suite.NoError(err)

	suite.EqualValues(nq, searchReq.getNumOfQuery())

	searchReq.Delete()
}

func TestPlan(t *testing.T) {
	suite.Run(t, new(PlanSuite))
}
