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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	_, err := segcore.NewRetrievePlan(suite.collection, nil, 0, 0, 0, 0, nil)
	suite.Error(err)
}

// --------------- helper functions ---------------

// genRetrievePlanExpr builds a serialized PlanNode with a TermExpr on the PK field.
func (suite *PlanSuite) genRetrievePlanExpr() []byte {
	pkField, err := typeutil.GetPrimaryFieldSchema(suite.collection.Schema())
	suite.Require().NoError(err)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  pkField.FieldID,
							DataType: pkField.DataType,
						},
						Values: []*planpb.GenericValue{
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
							{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
						},
					},
				},
			},
		},
		OutputFieldIds: []int64{pkField.FieldID},
	}
	data, err := proto.Marshal(planNode)
	suite.Require().NoError(err)
	return data
}

// genSearchPlanExpr builds a serialized vector_anns PlanNode for search tests.
func (suite *PlanSuite) genSearchPlanExpr() []byte {
	var vecFieldID int64
	var metricType string
	for _, f := range suite.collection.Schema().Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			vecFieldID = f.FieldID
			for _, p := range f.IndexParams {
				if p.Key == common.MetricTypeKey {
					metricType = p.Value
				}
			}
			break
		}
	}
	suite.Require().NotZero(vecFieldID)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId: vecFieldID,
				QueryInfo: &planpb.QueryInfo{
					Topk:         10,
					RoundDecimal: 6,
					MetricType:   metricType,
					SearchParams: `{"nprobe": 10}`,
				},
				PlaceholderTag: "$0",
			},
		},
	}
	data, err := proto.Marshal(planNode)
	suite.Require().NoError(err)
	return data
}

// genPlaceholderGroup builds a serialized PlaceholderGroup with nq float vectors.
func (suite *PlanSuite) genPlaceholderGroup(nq int) []byte {
	const dim = 128
	pv := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0, nq),
	}
	for i := 0; i < nq; i++ {
		rawData := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			buf := make([]byte, 4)
			common.Endian.PutUint32(buf, math.Float32bits(rand.Float32()))
			rawData = append(rawData, buf...)
		}
		pv.Values = append(pv.Values, rawData)
	}
	data, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{pv},
	})
	suite.Require().NoError(err)
	return data
}

// buildHintList constructs a SegmentPkHintList from segment-id → pk-values mapping.
func buildHintList(segPks map[int64][]int64) *planpb.SegmentPkHintList {
	hints := make([]*planpb.SegmentPkHint, 0, len(segPks))
	for segID, pks := range segPks {
		values := make([]*planpb.GenericValue, 0, len(pks))
		for _, pk := range pks {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{Int64Val: pk},
			})
		}
		hints = append(hints, &planpb.SegmentPkHint{
			SegmentId:        segID,
			FilteredPkValues: values,
		})
	}
	return &planpb.SegmentPkHintList{Hints: hints}
}

// makeSearchRequest is a helper that builds a segcore.SearchRequest with the given hints.
func (suite *PlanSuite) makeSearchRequest(hints *planpb.SegmentPkHintList) *segcore.SearchRequest {
	return suite.makeSearchRequestWithMetricType(hints, "")
}

// makeSearchRequestWithMetricType builds a SearchRequest allowing the caller to override MetricType.
func (suite *PlanSuite) makeSearchRequestWithMetricType(hints *planpb.SegmentPkHintList, metricType string) *segcore.SearchRequest {
	planExpr := suite.genSearchPlanExpr()
	placeholderGrp := suite.genPlaceholderGroup(1)

	req, err := segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:               &commonpb.MsgBase{MsgType: commonpb.MsgType_Search, MsgID: 42},
			CollectionID:       suite.collectionID,
			SerializedExprPlan: planExpr,
			PlaceholderGroup:   placeholderGrp,
			Nq:                 1,
			MetricType:         metricType,
			MvccTimestamp:      1000,
		},
		DmlChannels:    []string{fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID)},
		SegmentIDs:     []int64{suite.segmentID},
		SegmentPkHints: hints,
	}, placeholderGrp)
	suite.Require().NoError(err)
	return req
}

// --------------- Retrieve plan hint tests ---------------

func (suite *PlanSuite) TestRetrievePlanWithNilHints() {
	expr := suite.genRetrievePlanExpr()
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 1, 0, 0, nil)
	suite.NoError(err)
	suite.NotNil(plan)
	plan.Delete()
}

func (suite *PlanSuite) TestRetrievePlanWithEmptyHintList() {
	expr := suite.genRetrievePlanExpr()
	emptyHints := &planpb.SegmentPkHintList{Hints: nil}
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 1, 0, 0, emptyHints)
	suite.NoError(err)
	suite.NotNil(plan)
	plan.Delete()
}

func (suite *PlanSuite) TestRetrievePlanWithValidHints() {
	expr := suite.genRetrievePlanExpr()
	hints := buildHintList(map[int64][]int64{
		1001: {1, 2},
		1002: {3},
	})
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 1, 0, 0, hints)
	suite.NoError(err)
	suite.NotNil(plan)
	plan.Delete()
}

func (suite *PlanSuite) TestRetrievePlanWithEmptyFilteredValues() {
	expr := suite.genRetrievePlanExpr()
	hints := &planpb.SegmentPkHintList{
		Hints: []*planpb.SegmentPkHint{
			{SegmentId: 1001, FilteredPkValues: nil},
		},
	}
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 1, 0, 0, hints)
	suite.NoError(err)
	suite.NotNil(plan)
	plan.Delete()
}

func (suite *PlanSuite) TestRetrievePlanWithLargeHintList() {
	expr := suite.genRetrievePlanExpr()
	segPks := make(map[int64][]int64, 100)
	for i := int64(0); i < 100; i++ {
		pks := make([]int64, 50)
		for j := range pks {
			pks[j] = i*1000 + int64(j)
		}
		segPks[i+1000] = pks
	}
	hints := buildHintList(segPks)
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 1, 0, 0, hints)
	suite.NoError(err)
	suite.NotNil(plan)
	plan.Delete()
}

// --------------- Retrieve plan getter tests ---------------

func (suite *PlanSuite) TestRetrievePlanGetters() {
	expr := suite.genRetrievePlanExpr()
	plan, err := segcore.NewRetrievePlan(suite.collection, expr, 1000, 42, 0, 0, nil)
	suite.Require().NoError(err)
	defer plan.Delete()

	// MsgID
	suite.Equal(int64(42), plan.MsgID())

	// ShouldIgnoreNonPk — depends on plan content, just verify no panic
	_ = plan.ShouldIgnoreNonPk()

	// SetIgnoreNonPk / IsIgnoreNonPk
	suite.False(plan.IsIgnoreNonPk())
	plan.SetIgnoreNonPk(true)
	suite.True(plan.IsIgnoreNonPk())
	plan.SetIgnoreNonPk(false)
	suite.False(plan.IsIgnoreNonPk())
}

// --------------- Search request hint tests ---------------

func (suite *PlanSuite) TestSearchRequestWithNilHints() {
	req := suite.makeSearchRequest(nil)
	defer req.Delete()

	suite.Nil(req.GetSegmentPkHints())
}

func (suite *PlanSuite) TestSearchRequestWithHints() {
	hints := buildHintList(map[int64][]int64{
		2001: {10, 20},
		2002: {30},
	})
	req := suite.makeSearchRequest(hints)
	defer req.Delete()

	suite.NotNil(req.GetSegmentPkHints())
	suite.Len(req.GetSegmentPkHints().GetHints(), 2)
}

func (suite *PlanSuite) TestSearchRequestWithEmptyHintList() {
	emptyHints := &planpb.SegmentPkHintList{Hints: nil}
	req := suite.makeSearchRequest(emptyHints)
	defer req.Delete()

	// Empty hint list is stored as-is (marshal was skipped because len == 0)
	suite.NotNil(req.GetSegmentPkHints())
	suite.Empty(req.GetSegmentPkHints().GetHints())
}

func (suite *PlanSuite) TestSearchRequestHintsPreservedAfterCreation() {
	hints := buildHintList(map[int64][]int64{
		3001: {100, 200, 300},
	})
	req := suite.makeSearchRequest(hints)
	defer req.Delete()

	got := req.GetSegmentPkHints()
	suite.Require().NotNil(got)
	suite.Require().Len(got.GetHints(), 1)
	suite.Equal(int64(3001), got.GetHints()[0].GetSegmentId())
	suite.Len(got.GetHints()[0].GetFilteredPkValues(), 3)
}

// --------------- Search request getter tests ---------------

func (suite *PlanSuite) TestSearchRequestGetters() {
	req := suite.makeSearchRequest(nil)
	defer req.Delete()

	// GetNumOfQuery — we created with nq=1
	suite.Equal(int64(1), req.GetNumOfQuery())

	// MVCC
	suite.Equal(uint64(1000), req.MVCC())

	// Plan — returns the underlying SearchPlan
	plan := req.Plan()
	suite.NotNil(plan)

	// GetTopK — should match the plan's topK (10 in genSearchPlanExpr)
	suite.Equal(int64(10), plan.GetTopK())

	// SearchFieldID — should be the float vector field ID
	suite.NotZero(req.SearchFieldID())
}

// --------------- Search request error path tests ---------------

func (suite *PlanSuite) TestSearchRequestEmptyPlaceholderGroup() {
	planExpr := suite.genSearchPlanExpr()

	_, err := segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:               &commonpb.MsgBase{MsgType: commonpb.MsgType_Search},
			SerializedExprPlan: planExpr,
			Nq:                 1,
		},
	}, nil)
	suite.Error(err)
	suite.Contains(err.Error(), "empty search request")
}

func (suite *PlanSuite) TestSearchRequestMetricTypeMismatch() {
	planExpr := suite.genSearchPlanExpr()
	placeholderGrp := suite.genPlaceholderGroup(1)

	// The plan uses L2, so pass a mismatched metric type
	_, err := segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:               &commonpb.MsgBase{MsgType: commonpb.MsgType_Search},
			SerializedExprPlan: planExpr,
			PlaceholderGroup:   placeholderGrp,
			Nq:                 1,
			MetricType:         "IP",
		},
	}, placeholderGrp)
	suite.Error(err)
	suite.Contains(err.Error(), "metric type not match")
}

func (suite *PlanSuite) TestSearchRequestInvalidPlaceholderGroup() {
	planExpr := suite.genSearchPlanExpr()
	garbage := []byte{0xff, 0xfe, 0xfd}

	_, err := segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:               &commonpb.MsgBase{MsgType: commonpb.MsgType_Search},
			SerializedExprPlan: planExpr,
			PlaceholderGroup:   garbage,
			Nq:                 1,
		},
	}, garbage)
	suite.Error(err)
	suite.Contains(err.Error(), "parser searchRequest failed")
}

func TestPlan(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(PlanSuite))
}
