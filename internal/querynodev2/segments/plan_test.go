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
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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

	_, err := createSearchPlanByExpr(collection, nil, "")
	suite.Error(err)
}

func (suite *PlanSuite) TestQueryPlanCollectionReleased() {
	collection := &Collection{id: suite.collectionID}
	_, err := NewRetrievePlan(collection, nil, 0, 0)
	suite.Error(err)
}

func TestPlan(t *testing.T) {
	suite.Run(t, new(PlanSuite))
}
