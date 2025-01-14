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

package segcore_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type PlanSuite struct {
	suite.Suite

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *segcore.CCollection
}

func (suite *PlanSuite) SetupTest() {
	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1
	schema := mock_segcore.GenTestCollectionSchema("plan-suite", schemapb.DataType_Int64, true)
	var err error
	suite.collection, err = segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		Schema:    schema,
		IndexMeta: mock_segcore.GenTestIndexMeta(suite.collectionID, schema),
	})
	if err != nil {
		panic(err)
	}
}

func (suite *PlanSuite) TearDownTest() {
	suite.collection.Release()
}

func (suite *PlanSuite) TestPlanCreateByExpr() {
	planNode := &planpb.PlanNode{
		OutputFieldIds: []int64{0},
	}
	expr, err := proto.Marshal(planNode)
	suite.NoError(err)

	_, err = segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			SerializedExprPlan: expr,
		},
	}, nil)
	suite.Error(err)
}

func (suite *PlanSuite) TestQueryPlanCollectionReleased() {
	suite.collection.Release()
	_, err := segcore.NewRetrievePlan(suite.collection, nil, 0, 0)
	suite.Error(err)
}

func TestPlan(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(PlanSuite))
}
