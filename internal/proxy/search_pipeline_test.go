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

package proxy

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/function/highlight"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

func TestSearchPipeline(t *testing.T) {
	suite.Run(t, new(SearchPipelineSuite))
}

type SearchPipelineSuite struct {
	suite.Suite
	span trace.Span
}

func (s *SearchPipelineSuite) SetupTest() {
	_, sp := otel.Tracer("test").Start(context.Background(), "Proxy-Search-PostExecute")
	s.span = sp
}

func (s *SearchPipelineSuite) TearDownTest() {
	s.span.End()
}

func (s *SearchPipelineSuite) TestSearchReduceOp() {
	nq := int64(2)
	topk := int64(10)
	pk := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	data := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, false)
	op := searchReduceOperator{
		context.Background(),
		pk,
		nq,
		topk,
		0,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}},
	}
	_, err := op.run(context.Background(), s.span, []*internalpb.SearchResults{data})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestHybridSearchReduceOp() {
	nq := int64(2)
	topk := int64(10)
	pk := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	data1 := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, true)
	data1.SubResults[0].ReqIndex = 0
	data2 := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, true)
	data2.SubResults[0].ReqIndex = 1

	subReqs := []*internalpb.SubSearchRequest{
		{
			Nq:     2,
			Topk:   10,
			Offset: 0,
		},
		{
			Nq:     2,
			Topk:   10,
			Offset: 0,
		},
	}

	op := hybridSearchReduceOperator{
		context.Background(),
		subReqs,
		pk,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}, {}},
	}
	_, err := op.run(context.Background(), s.span, []*internalpb.SearchResults{data1, data2})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestRerankOp() {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
			{FieldID: 102, Name: "ts", DataType: schemapb.DataType_Int64},
		},
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"ts"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	funcScore, err := rerank.NewFunctionScore(schema, &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)

	nq := int64(2)
	topk := int64(10)
	offset := int64(0)

	reduceOp := searchReduceOperator{
		context.Background(),
		schema.Fields[0],
		nq,
		topk,
		offset,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}},
	}

	data := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, false)
	reduced, err := reduceOp.run(context.Background(), s.span, []*internalpb.SearchResults{data})
	s.NoError(err)

	op := rerankOperator{
		nq:            nq,
		topK:          topk,
		offset:        offset,
		roundDecimal:  10,
		functionScore: funcScore,
	}

	_, err = op.run(context.Background(), s.span, reduced[0], []string{"IP"})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestRequeryOp() {
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f1.FieldId = 101

	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	op := requeryOperator{
		traceCtx:         context.Background(),
		outputFieldNames: []string{"int64"},
	}
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2},
			},
		},
	}
	_, err := op.run(context.Background(), s.span, ids, segcore.StorageCost{})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestOrganizeOp() {
	op := organizeOperator{
		traceCtx:           context.Background(),
		primaryFieldSchema: &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		collectionID:       1,
	}
	fields := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
						},
					},
				},
			},
		}, {
			Type:      schemapb.DataType_Int64,
			FieldName: "int64",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
						},
					},
				},
			},
		},
	}

	ids := []*schemapb.IDs{
		{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 4, 5, 9, 10},
				},
			},
		},
		{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 6, 7, 8, 9, 10},
				},
			},
		},
	}
	ret, err := op.run(context.Background(), s.span, fields, ids)
	s.NoError(err)
	fmt.Println(ret)
}

func (s *SearchPipelineSuite) TestHighlightOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &Proxy{}
	proxy.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	sched, err := newTaskScheduler(ctx, proxy.tsoAllocator)
	s.Require().NoError(err)

	err = sched.Start()
	s.Require().NoError(err)
	defer sched.Close()
	proxy.sched = sched

	collName := "test_coll_highlight"
	fieldName2Types := map[string]schemapb.DataType{
		testVarCharField: schemapb.DataType_VarChar,
	}
	schema := constructCollectionSchemaByDataType(collName, fieldName2Types, testVarCharField, false)

	req := &milvuspb.SearchRequest{
		CollectionName: collName,
		DbName:         "default",
	}

	highlightTasks := map[int64]*highlightTask{
		100: {
			HighlightTask: &querypb.HighlightTask{
				Texts:     []string{"target text"},
				FieldName: testVarCharField,
				FieldId:   100,
			},
			preTags:  [][]byte{[]byte(DefaultPreTag)},
			postTags: [][]byte{[]byte(DefaultPostTag)},
		},
	}

	mockLb := shardclient.NewMockLBPolicy(s.T())
	searchTask := &searchTask{
		node: proxy,
		highlighter: &LexicalHighlighter{
			tasks: highlightTasks,
		},
		lb:             mockLb,
		schema:         newSchemaInfo(schema),
		request:        req,
		collectionName: collName,
		SearchRequest: &internalpb.SearchRequest{
			CollectionID: 0,
		},
	}

	op, err := opFactory[highlightOp](searchTask, map[string]any{})
	s.Require().NoError(err)

	// mockery
	mockLb.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload shardclient.CollectionWorkLoad) {
		qn := mocks.NewMockQueryNodeClient(s.T())
		qn.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(
			&querypb.GetHighlightResponse{
				Status:  merr.Success(),
				Results: []*querypb.HighlightResult{},
			}, nil)
		workload.Exec(ctx, 0, qn, "test_chan")
	}).Return(nil)

	_, err = op.run(ctx, s.span, &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			TopK:  3,
			Topks: []int64{1},
			FieldsData: []*schemapb.FieldData{{
				FieldName: testVarCharField,
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"match text"},
							},
						},
					},
				},
			}},
		},
	})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOp() {
	ctx := context.Background()

	// Mock SemanticHighlight methods
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{
					{"<em>highlighted</em> text 1"},
					{"<em>highlighted</em> text 2"},
					{"<em>highlighted</em> text 3"},
				}, [][]float32{
					{0.9},
					{0.8},
					{0.7},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).To(func(h *highlight.SemanticHighlight) []int64 {
		return []int64{101}
	}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		return testVarCharField
	}).Build()
	defer mockGetFieldName.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with text data
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1", "text 2", "text 3"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)
	s.Len(results, 1)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 1)

	highlightResult := result.Results.HighlightResults[0]
	s.Equal(testVarCharField, highlightResult.FieldName)
	s.Len(highlightResult.Datas, 3)
	s.Equal([]string{"<em>highlighted</em> text 1"}, highlightResult.Datas[0].Fragments)
	s.Equal([]string{"<em>highlighted</em> text 2"}, highlightResult.Datas[1].Fragments)
	s.Equal([]string{"<em>highlighted</em> text 3"}, highlightResult.Datas[2].Fragments)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMissingField() {
	ctx := context.Background()

	// Mock FieldIDs to return field 999 (not in results)
	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{999}).Build()
	defer mockFieldIDs.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results without the expected field
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator and expect error
	_, err := op.run(ctx, s.span, searchResults)
	s.Error(err)
	s.Contains(err.Error(), "text field not in output field")
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMultipleFields() {
	ctx := context.Background()

	// Use a counter to return different results for different calls
	callCount := 0
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			callCount++
			return [][]string{
					{fmt.Sprintf("<em>highlighted</em> text field%d-1", callCount)},
					{fmt.Sprintf("<em>highlighted</em> text field%d-2", callCount)},
				}, [][]float32{
					{0.9},
					{0.8},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101, 102}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		if id == 101 {
			return "field1"
		}
		return "field2"
	}).Build()
	defer mockGetFieldName.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with multiple text fields
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: "field1",
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1", "text 2"},
								},
							},
						},
					},
				},
				{
					FieldId:   102,
					FieldName: "field2",
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"another text 1", "another text 2"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 2)

	// Verify first field
	s.Equal("field1", result.Results.HighlightResults[0].FieldName)
	s.Len(result.Results.HighlightResults[0].Datas, 2)

	// Verify second field
	s.Equal("field2", result.Results.HighlightResults[1].FieldName)
	s.Len(result.Results.HighlightResults[1].Datas, 2)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpEmptyResults() {
	ctx := context.Background()

	// Mock Process to return empty results
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{}, [][]float32{}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		return testVarCharField
	}).Build()
	defer mockGetFieldName.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create empty search results
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       0,
			Topks:      []int64{0},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 1)
	s.Equal(testVarCharField, result.Results.HighlightResults[0].FieldName)
	s.Len(result.Results.HighlightResults[0].Datas, 0)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpDynamicField() {
	ctx := context.Background()

	// Mock SemanticHighlight methods for schema fields
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{
					{"<em>dynamic</em> content 1"},
					{"<em>dynamic</em> content 2"},
				}, [][]float32{
					{0.95},
					{0.85},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{}).Build()
	defer mockFieldIDs.UnPatch()

	mockHasDynamicFields := mockey.Mock((*highlight.SemanticHighlight).HasDynamicFields).Return(true).Build()
	defer mockHasDynamicFields.UnPatch()

	mockDynamicFieldNames := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldNames).Return([]string{"dyn_content"}).Build()
	defer mockDynamicFieldNames.UnPatch()

	mockDynamicFieldID := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldID).Return(int64(102)).Build()
	defer mockDynamicFieldID.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with $meta JSON field data
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Scores:     []float32{0.9, 0.8},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   102,
					FieldName: "$meta",
					Type:      schemapb.DataType_JSON,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{
									Data: [][]byte{
										[]byte(`{"dyn_content": "dynamic content 1"}`),
										[]byte(`{"dyn_content": "dynamic content 2"}`),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 1)

	highlightResult := result.Results.HighlightResults[0]
	s.Equal("dyn_content", highlightResult.FieldName)
	s.Len(highlightResult.Datas, 2)
	s.Equal([]string{"<em>dynamic</em> content 1"}, highlightResult.Datas[0].Fragments)
	s.Equal([]string{"<em>dynamic</em> content 2"}, highlightResult.Datas[1].Fragments)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMixedFields() {
	ctx := context.Background()

	// Track calls to distinguish schema field vs dynamic field processing
	callCount := 0
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			callCount++
			if callCount == 1 {
				// Schema field
				return [][]string{{"<em>schema</em> text"}}, [][]float32{{0.95}}, nil
			}
			// Dynamic field
			return [][]string{{"<em>dynamic</em> text"}}, [][]float32{{0.90}}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).Return(testVarCharField).Build()
	defer mockGetFieldName.UnPatch()

	mockHasDynamicFields := mockey.Mock((*highlight.SemanticHighlight).HasDynamicFields).Return(true).Build()
	defer mockHasDynamicFields.UnPatch()

	mockDynamicFieldNames := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldNames).Return([]string{"dyn_content"}).Build()
	defer mockDynamicFieldNames.UnPatch()

	mockDynamicFieldID := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldID).Return(int64(102)).Build()
	defer mockDynamicFieldID.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with both schema field and $meta JSON field
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Scores:     []float32{0.9},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"schema text"},
								},
							},
						},
					},
				},
				{
					FieldId:   102,
					FieldName: "$meta",
					Type:      schemapb.DataType_JSON,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{
									Data: [][]byte{
										[]byte(`{"dyn_content": "dynamic text"}`),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 2)

	// Schema field result
	s.Equal(testVarCharField, result.Results.HighlightResults[0].FieldName)
	s.Equal([]string{"<em>schema</em> text"}, result.Results.HighlightResults[0].Datas[0].Fragments)

	// Dynamic field result
	s.Equal("dyn_content", result.Results.HighlightResults[1].FieldName)
	s.Equal([]string{"<em>dynamic</em> text"}, result.Results.HighlightResults[1].Datas[0].Fragments)
}

func (s *SearchPipelineSuite) TestExtractMultipleDynamicFieldTexts() {
	// Test normal extraction with multiple fields
	jsonData := [][]byte{
		[]byte(`{"field1": "value1", "field2": "value2"}`),
		[]byte(`{"field1": "value3", "field2": "value4"}`),
	}
	result, err := extractMultipleDynamicFieldTexts(jsonData, []string{"field1", "field2"})
	s.NoError(err)
	s.Equal([]string{"value1", "value3"}, result["field1"])
	s.Equal([]string{"value2", "value4"}, result["field2"])

	// Test single field extraction
	result2, err := extractMultipleDynamicFieldTexts(jsonData, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", "value3"}, result2["field1"])

	// Test missing field (graceful degradation)
	jsonData2 := [][]byte{
		[]byte(`{"field1": "value1"}`),
		[]byte(`{"other": "value"}`),
	}
	result3, err := extractMultipleDynamicFieldTexts(jsonData2, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", ""}, result3["field1"])

	// Test empty JSON
	jsonData3 := [][]byte{
		[]byte(`{"field1": "value1"}`),
		[]byte(``),
	}
	result4, err := extractMultipleDynamicFieldTexts(jsonData3, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", ""}, result4["field1"])

	// Test non-string value
	jsonData4 := [][]byte{
		[]byte(`{"field1": 123}`),
	}
	_, err = extractMultipleDynamicFieldTexts(jsonData4, []string{"field1"})
	s.Error(err)
	s.Contains(err.Error(), "is not a string type")

	// Test invalid JSON
	jsonData5 := [][]byte{
		[]byte(`{invalid json}`),
	}
	_, err = extractMultipleDynamicFieldTexts(jsonData5, []string{"field1"})
	s.Error(err)
	s.Contains(err.Error(), "failed to unmarshal")

	// Test multiple fields with partial data
	jsonData6 := [][]byte{
		[]byte(`{"field1": "a", "field2": "b"}`),
		[]byte(`{"field1": "c"}`), // field2 missing
		[]byte(`{"field2": "d"}`), // field1 missing
	}
	result5, err := extractMultipleDynamicFieldTexts(jsonData6, []string{"field1", "field2"})
	s.NoError(err)
	s.Equal([]string{"a", "c", ""}, result5["field1"])
	s.Equal([]string{"b", "", "d"}, result5["field2"])
}

func (s *SearchPipelineSuite) TestSearchPipeline() {
	collectionName := "test"
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collectionName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
				},
			},
			pkField: &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
	}

	pipeline, err := newPipeline(searchPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	sr := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{sr}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 250})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(100), storageCost.ScannedRemoteBytes)
	s.Equal(int64(250), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
	fmt.Println(results)
}

func (s *SearchPipelineSuite) TestSearchPipelineWithRequery() {
	collectionName := "test_collection"
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collectionName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
				},
			},
			pkField: &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
	}

	// Mock requery operation
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(searchWithRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{
		genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false),
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(200), storageCost.ScannedRemoteBytes)
	s.Equal(int64(400), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestSearchWithRerankPipe() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"intField"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScore, err := rerank.NewFunctionScore(schema, &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
		functionScore:          funcScore,
	}

	pipeline, err := newPipeline(searchWithRerankPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	searchResults := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{searchResults}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestSearchWithRerankRequeryPipe() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"intField"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScore, err := rerank.NewFunctionScore(schema, &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
		functionScore:          funcScore,
	}
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(searchWithRerankRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	searchResults := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{searchResults}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(0), storageCost.ScannedRemoteBytes)
	s.Equal(int64(0), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestHybridSearchPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{},
	)

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	f1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	f2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{f1, f2}, segcore.StorageCost{ScannedRemoteBytes: 900, ScannedTotalBytes: 2000})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.Equal(int64(900), storageCost.ScannedRemoteBytes)
	s.Equal(int64(2000), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestFilterFieldOperatorWithStructArrayFields() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "floatField", DataType: schemapb.DataType_Float},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "structArray",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 104, Name: "structArrayField", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{FieldID: 105, Name: "structVectorField", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
				},
			},
		},
	}

	task := &searchTask{
		schema: &schemaInfo{
			CollectionSchema: schema,
		},
		translatedOutputFields: []string{"intField", "floatField", "structArrayField", "structVectorField"},
	}

	op, err := newEndOperator(task, nil)
	s.NoError(err)

	// Create mock search results with fields including struct array fields
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			FieldsData: []*schemapb.FieldData{
				{FieldId: 101}, // intField
				{FieldId: 102}, // floatField
				{FieldId: 104}, // structArrayField
				{FieldId: 105}, // structVectorField
			},
		},
	}

	results, err := op.run(context.Background(), s.span, searchResults, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{AllSearchCount: 0}}})
	s.NoError(err)
	s.NotNil(results)

	resultData := results[0].(*milvuspb.SearchResults)
	s.NotNil(resultData.Results.FieldsData)
	s.Len(resultData.Results.FieldsData, 4)

	// Verify all fields including struct array fields got their names and types set
	for _, field := range resultData.Results.FieldsData {
		switch field.FieldId {
		case 101:
			s.Equal("intField", field.FieldName)
			s.Equal(schemapb.DataType_Int64, field.Type)
			s.False(field.IsDynamic)
		case 102:
			s.Equal("floatField", field.FieldName)
			s.Equal(schemapb.DataType_Float, field.Type)
			s.False(field.IsDynamic)
		case 104:
			// Struct array field should be handled by GetAllFieldSchemas
			s.Equal("structArrayField", field.FieldName)
			s.Equal(schemapb.DataType_Array, field.Type)
			s.False(field.IsDynamic)
		case 105:
			// Struct array vector field should be handled by GetAllFieldSchemas
			s.Equal("structVectorField", field.FieldName)
			s.Equal(schemapb.DataType_ArrayOfVector, field.Type)
			s.False(field.IsDynamic)
		}
	}
}

func (s *SearchPipelineSuite) TestHybridSearchWithRequeryAndRerankByDataPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{"intField"},
	)

	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(hybridSearchWithRequeryAndRerankByFieldDataPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	d1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	d2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{d1, d2}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestHybridSearchWithRequeryPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{"intField"},
	)

	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(hybridSearchWithRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, filterFieldNode)

	d1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	d2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{d1, d2}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func getHybridSearchTask(collName string, data [][]string, outputFields []string) *searchTask {
	subReqs := []*milvuspb.SubSearchRequest{}
	for _, item := range data {
		subReq := &milvuspb.SubSearchRequest{
			SearchParams: []*commonpb.KeyValuePair{
				{Key: TopKKey, Value: "10"},
			},
			Nq: int64(len(item)),
		}
		subReqs = append(subReqs, subReq)
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "rrf"},
		},
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScore, _ := rerank.NewFunctionScore(schema, &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			Topk:       10,
			Nq:         2,
			IsAdvanced: true,
			SubReqs: []*internalpb.SubSearchRequest{
				{
					Topk: 10,
					Nq:   2,
				},
				{
					Topk: 10,
					Nq:   2,
				},
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: collName,
			SubReqs:        subReqs,
			SearchParams: []*commonpb.KeyValuePair{
				{Key: LimitKey, Value: "10"},
			},
			FunctionScore: &schemapb.FunctionScore{
				Functions: []*schemapb.FunctionSchema{functionSchema},
			},
			OutputFields: outputFields,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		mixCoord: nil,
		tr:       timerecord.NewTimeRecorder("test-search"),
		rankParams: &rankParams{
			limit:        10,
			offset:       0,
			roundDecimal: 0,
		},
		queryInfos:             []*planpb.QueryInfo{{}, {}},
		functionScore:          funcScore,
		translatedOutputFields: outputFields,
	}
	return task
}

func (s *SearchPipelineSuite) TestMergeIDsFunc() {
	{
		ids1 := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 5},
				},
			},
		}

		ids2 := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 4, 5, 6},
				},
			},
		}
		rets := []*milvuspb.SearchResults{
			{
				Results: &schemapb.SearchResultData{
					Ids: ids1,
				},
			},
			{
				Results: &schemapb.SearchResultData{
					Ids: ids2,
				},
			},
		}
		allIDs, err := mergeIDsFunc(context.Background(), s.span, rets)
		s.NoError(err)
		sortedIds := allIDs[0].(*schemapb.IDs).GetIntId().GetData()
		slices.Sort(sortedIds)
		s.Equal(sortedIds, []int64{1, 2, 3, 4, 5, 6})
	}
	{
		ids1 := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b", "e"},
				},
			},
		}

		ids2 := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b", "c", "d"},
				},
			},
		}
		rets := []*milvuspb.SearchResults{
			{
				Results: &schemapb.SearchResultData{
					Ids: ids1,
				},
			},
		}
		rets = append(rets, &milvuspb.SearchResults{
			Results: &schemapb.SearchResultData{
				Ids: ids2,
			},
		})
		allIDs, err := mergeIDsFunc(context.Background(), s.span, rets)
		s.NoError(err)
		sortedIds := allIDs[0].(*schemapb.IDs).GetStrId().GetData()
		slices.Sort(sortedIds)
		s.Equal(sortedIds, []string{"a", "b", "c", "d", "e"})
	}
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithHighlightDynamicFields() {
	// Test that highlight dynamic fields are added to requery output
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	// Mock highlighter with dynamic field names
	mockHighlighter := &SemanticHighlighter{highlight: &highlight.SemanticHighlight{}}
	mockDynFields := mockey.Mock((*SemanticHighlighter).DynamicFieldNames).To(func(h *SemanticHighlighter) []string {
		return []string{"dyn_field1", "dyn_field2"}
	}).Build()
	defer mockDynFields.UnPatch()

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            mockHighlighter,
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify that dynamic fields from highlighter are included in output fields
	s.Contains(reqOp.outputFieldNames, "title")
	s.Contains(reqOp.outputFieldNames, "dyn_field1")
	s.Contains(reqOp.outputFieldNames, "dyn_field2")
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithoutHighlighter() {
	// Test that without highlighter, only translated output fields are included
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            nil, // No highlighter
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify only translated output fields are included
	s.Equal([]string{"title"}, reqOp.outputFieldNames)
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithHighlighterNoDynamicFields() {
	// Test that highlighter with empty dynamic fields doesn't affect output
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	// Mock highlighter with empty dynamic field names
	mockHighlighter := &SemanticHighlighter{highlight: &highlight.SemanticHighlight{}}
	mockDynFields := mockey.Mock((*SemanticHighlighter).DynamicFieldNames).To(func(h *SemanticHighlighter) []string {
		return []string{} // Empty dynamic fields
	}).Build()
	defer mockDynFields.UnPatch()

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            mockHighlighter,
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify only translated output fields are included (no dynamic fields added)
	s.Equal([]string{"title"}, reqOp.outputFieldNames)
}
