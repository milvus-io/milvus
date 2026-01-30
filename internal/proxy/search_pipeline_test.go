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
	pipeline.AddNodes(task, endNode)

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
		request:                &milvuspb.SearchRequest{Namespace: nil},
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
	pipeline.AddNodes(task, endNode)

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
	pipeline.AddNodes(task, endNode)

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
		request:                &milvuspb.SearchRequest{Namespace: nil},
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
	pipeline.AddNodes(task, endNode)

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
	pipeline.AddNodes(task, endNode)

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
	pipeline.AddNodes(task, endNode)
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
	pipeline.AddNodes(task, endNode)

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

// Test parseOrderByFields function
func (s *SearchPipelineSuite) TestParseOrderByFields() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64},
			{Name: "score", DataType: schemapb.DataType_Float},
			{Name: "name", DataType: schemapb.DataType_VarChar},
			{Name: "active", DataType: schemapb.DataType_Bool},
			{Name: "vector", DataType: schemapb.DataType_FloatVector},
		},
	}

	// Test empty order_by_fields
	params := []*commonpb.KeyValuePair{}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test single field ascending (default)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.True(result[0].Ascending)

	// Test single field with explicit asc
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.True(result[0].Ascending)

	// Test single field descending
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)

	// Test multiple fields
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc,name:asc,id"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 3)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)
	s.Equal("name", result[1].FieldName)
	s.True(result[1].Ascending)
	s.Equal("id", result[2].FieldName)
	s.True(result[2].Ascending)

	// Test invalid direction
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:invalid"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "invalid order direction")

	// Test non-existent field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "nonexistent"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "does not exist")

	// Test unsortable type (vector)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "vector"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")

	// Test empty field name
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ":asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "empty field name")
}

// Test parseOrderByFields with various input formats and edge cases
func (s *SearchPipelineSuite) TestParseOrderByFieldsEdgeCases() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64},
			{Name: "score", DataType: schemapb.DataType_Float},
			{Name: "name", DataType: schemapb.DataType_VarChar},
		},
	}

	// Test whitespace handling - spaces around field name
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "  score  "}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test whitespace handling - spaces around colon
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score : desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)

	// Test whitespace handling - spaces around comma
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc , name:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)
	s.Equal("score", result[0].FieldName)
	s.Equal("name", result[1].FieldName)

	// Test case insensitivity - uppercase ASC
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:ASC"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.True(result[0].Ascending)

	// Test case insensitivity - uppercase DESC
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:DESC"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test case insensitivity - mixed case
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:Desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test "ascending" keyword
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:ascending"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.True(result[0].Ascending)

	// Test "descending" keyword
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:descending"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test empty string value
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ""}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test trailing comma - should skip empty part
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc,"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test leading comma - should skip empty part
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ",score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test multiple commas - should skip empty parts
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc,,name:desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)

	// Test only commas - should return nil
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ",,,"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test key not present - should return nil
	params = []*commonpb.KeyValuePair{{Key: "other_key", Value: "score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test all supported sortable types
	schemaAllTypes := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "f_bool", DataType: schemapb.DataType_Bool},
			{Name: "f_int8", DataType: schemapb.DataType_Int8},
			{Name: "f_int16", DataType: schemapb.DataType_Int16},
			{Name: "f_int32", DataType: schemapb.DataType_Int32},
			{Name: "f_int64", DataType: schemapb.DataType_Int64},
			{Name: "f_float", DataType: schemapb.DataType_Float},
			{Name: "f_double", DataType: schemapb.DataType_Double},
			{Name: "f_string", DataType: schemapb.DataType_String},
			{Name: "f_varchar", DataType: schemapb.DataType_VarChar},
		},
	}

	// All these should be valid
	for _, fieldName := range []string{"f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_float", "f_double", "f_string", "f_varchar"} {
		params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: fieldName}}
		result, err = parseOrderByFields(params, schemaAllTypes)
		s.NoError(err, "field %s should be sortable", fieldName)
		s.Len(result, 1)
		s.Equal(fieldName, result[0].FieldName)
	}

	// Test unsortable types
	// Note: JSON without path syntax is not directly sortable; use path syntax like field["key"] instead
	schemaUnsortable := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "f_float_vector", DataType: schemapb.DataType_FloatVector},
			{Name: "f_binary_vector", DataType: schemapb.DataType_BinaryVector},
			{Name: "f_array", DataType: schemapb.DataType_Array},
			{Name: "f_json", DataType: schemapb.DataType_JSON},
		},
	}

	for _, fieldName := range []string{"f_float_vector", "f_binary_vector", "f_array", "f_json"} {
		params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: fieldName}}
		_, err = parseOrderByFields(params, schemaUnsortable)
		s.Error(err, "field %s should not be sortable", fieldName)
		s.Contains(err.Error(), "unsortable type")
	}
}

// Test isSortableFieldType function
func (s *SearchPipelineSuite) TestIsSortableFieldType() {
	// Sortable types
	s.True(isSortableFieldType(schemapb.DataType_Bool))
	s.True(isSortableFieldType(schemapb.DataType_Int8))
	s.True(isSortableFieldType(schemapb.DataType_Int16))
	s.True(isSortableFieldType(schemapb.DataType_Int32))
	s.True(isSortableFieldType(schemapb.DataType_Int64))
	s.True(isSortableFieldType(schemapb.DataType_Float))
	s.True(isSortableFieldType(schemapb.DataType_Double))
	s.True(isSortableFieldType(schemapb.DataType_String))
	s.True(isSortableFieldType(schemapb.DataType_VarChar))

	// Non-sortable types
	// Note: JSON type requires path syntax (e.g., field["key"]) for sorting
	s.False(isSortableFieldType(schemapb.DataType_JSON))
	s.False(isSortableFieldType(schemapb.DataType_FloatVector))
	s.False(isSortableFieldType(schemapb.DataType_BinaryVector))
	s.False(isSortableFieldType(schemapb.DataType_Array))
}

// Test compareFieldDataAt function
func (s *SearchPipelineSuite) TestCompareFieldDataAt() {
	// Helper to call compareFieldDataAt and assert no error
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	// Test Int64 comparison
	int64Field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "int64_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 15}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(int64Field, 0, 1)) // 10 < 20
	s.Equal(1, mustCompare(int64Field, 1, 0))  // 20 > 10
	s.Equal(0, mustCompare(int64Field, 0, 0))  // 10 == 10

	// Test Float comparison
	floatField := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "float_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{1.5, 2.5, 1.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(floatField, 0, 1)) // 1.5 < 2.5
	s.Equal(1, mustCompare(floatField, 1, 0))  // 2.5 > 1.5
	s.Equal(0, mustCompare(floatField, 0, 2))  // 1.5 == 1.5

	// Test String comparison
	stringField := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "string_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"apple", "banana", "apple"}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(stringField, 0, 1)) // "apple" < "banana"
	s.Equal(1, mustCompare(stringField, 1, 0))  // "banana" > "apple"
	s.Equal(0, mustCompare(stringField, 0, 2))  // "apple" == "apple"

	// Test Bool comparison (false < true)
	boolField := &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "bool_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{false, true, false}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(boolField, 0, 1)) // false < true
	s.Equal(1, mustCompare(boolField, 1, 0))  // true > false
	s.Equal(0, mustCompare(boolField, 0, 2))  // false == false
}

// Test compareFieldDataAt with null handling
func (s *SearchPipelineSuite) TestCompareFieldDataAtWithNulls() {
	// Helper to call compareFieldDataAt and assert no error
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	// Test with ValidData (nullable field)
	nullableField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // index 1 is null
	}

	// null vs non-null: null should be first (return -1)
	s.Equal(-1, mustCompare(nullableField, 1, 0)) // null < 10
	s.Equal(1, mustCompare(nullableField, 0, 1))  // 10 > null

	// null vs null: equal
	nullableField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field2",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{false, false, true}, // index 0 and 1 are null
	}
	s.Equal(0, mustCompare(nullableField2, 0, 1)) // null == null

	// non-null vs non-null: normal comparison
	s.Equal(-1, mustCompare(nullableField, 0, 2)) // 10 < 30
}

// Test isSameGroupByValue function
func (s *SearchPipelineSuite) TestIsSameGroupByValue() {
	// Test Int64
	int64Field := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 1, 2, 2, 3}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(int64Field, 0, 1))  // 1 == 1
	s.False(isSameGroupByValue(int64Field, 1, 2)) // 1 != 2
	s.True(isSameGroupByValue(int64Field, 2, 3))  // 2 == 2

	// Test String
	stringField := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "a", "b"}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(stringField, 0, 1))  // "a" == "a"
	s.False(isSameGroupByValue(stringField, 1, 2)) // "a" != "b"

	// Test Bool
	boolField := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, true, false}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(boolField, 0, 1))  // true == true
	s.False(isSameGroupByValue(boolField, 1, 2)) // true != false
}

// Test orderByOperator sorting
func (s *SearchPipelineSuite) TestOrderByOperator() {
	// Create test search result
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4}, // nq=1, 4 results
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	// Test ascending sort by price
	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort by price: 10, 20, 30, 40
	// Original order: 30(id=1), 10(id=2), 40(id=3), 20(id=4)
	// Sorted order: 10(id=2), 20(id=4), 30(id=1), 40(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 20, 30, 40}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

// Test orderByOperator with nq>1 (multiple queries)
// Each query's results should be sorted independently
func (s *SearchPipelineSuite) TestOrderByOperatorMultipleQueries() {
	// Create test search result with nq=2, topk=3 each
	// Query 1: ids [1,2,3] with prices [30,10,20] -> sorted: [2,3,1] with prices [10,20,30]
	// Query 2: ids [4,5,6] with prices [60,40,50] -> sorted: [5,6,4] with prices [40,50,60]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{3, 3}, // nq=2, each query has 3 results
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20, 60, 40, 50}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Each query should be sorted independently
	// Query 1: [10,20,30] -> ids [2,3,1]
	// Query 2: [40,50,60] -> ids [5,6,4]
	expectedIds := []int64{2, 3, 1, 5, 6, 4}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 20, 30, 40, 50, 60}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)

	// Verify Topks unchanged
	s.Equal([]int64{3, 3}, sortedResult.Results.Topks)
}

// Test orderByOperator with nq>1 and group_by
// Each query's groups should be sorted independently
func (s *SearchPipelineSuite) TestOrderByOperatorMultipleQueriesWithGroupBy() {
	// nq=2, each query has 2 groups with 2 results each
	// Query 1: group A [ids 1,2], group B [ids 3,4] with prices [20,25,10,15]
	//   -> After sort by first row price: group B (10) comes before group A (20)
	//   -> Result: [3,4,1,2] with prices [10,15,20,25]
	// Query 2: group C [ids 5,6], group D [ids 7,8] with prices [50,55,30,35]
	//   -> After sort by first row price: group D (30) comes before group C (50)
	//   -> Result: [7,8,5,6] with prices [30,35,50,55]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7, 8}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55},
			Topks:  []int64{4, 4}, // nq=2, each query has 4 results (2 groups x 2)
			GroupByFieldValue: &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "category",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							// Query 1: groups A(100), A(100), B(200), B(200)
							// Query 2: groups C(300), C(300), D(400), D(400)
							LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 200, 300, 300, 400, 400}},
						},
					},
				},
			},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{20, 25, 10, 15, 50, 55, 30, 35}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 1, // Enable group_by
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Query 1: group B (price 10) before group A (price 20) -> [3,4,1,2]
	// Query 2: group D (price 30) before group C (price 50) -> [7,8,5,6]
	expectedIds := []int64{3, 4, 1, 2, 7, 8, 5, 6}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 15, 20, 25, 30, 35, 50, 55}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

// Test orderByOperator with descending sort
func (s *SearchPipelineSuite) TestOrderByOperatorDescending() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: false},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After descending sort by price: 40, 30, 20, 10
	expectedIds := []int64{3, 1, 4, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator validates missing fields
func (s *SearchPipelineSuite) TestOrderByOperatorMissingField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2}},
				},
			},
			Scores:     []float32{0.9, 0.8},
			FieldsData: []*schemapb.FieldData{},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "nonexistent", Ascending: true},
		},
		groupByFieldId: -1,
	}

	_, err := op.run(context.Background(), s.span, result)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

// Test orderByOperator with empty results
func (s *SearchPipelineSuite) TestOrderByOperatorEmptyResults() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids:        nil,
			Scores:     []float32{},
			FieldsData: []*schemapb.FieldData{},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	s.NotNil(outputs)
}

// Test orderByOperator with no order_by fields (passthrough)
func (s *SearchPipelineSuite) TestOrderByOperatorNoOrderBy() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Should be unchanged
	s.Equal([]int64{1, 2, 3}, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with group_by - critical test for sortGroupsByOrderByFields
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupBy() {
	// Create result with GroupByFieldValue where groups have varying sizes
	// Group 1: ids [1,2] with group value "A", prices [30, 25]
	// Group 2: ids [3] with group value "B", price [10]
	// Group 3: ids [4,5,6] with group value "C", prices [20, 15, 18]
	// After order_by price:asc (by first row in each group), groups should be ordered: B(10), C(20), A(30)
	// Final order: [3, 4, 5, 6, 1, 2]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65},
			Topks:  []int64{6},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 25, 10, 20, 15, 18}},
							},
						},
					},
				},
			},
			// GroupByFieldValue indicates group membership
			// "A", "A", "B", "C", "C", "C"
			GroupByFieldValue: &schemapb.FieldData{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"A", "A", "B", "C", "C", "C"}},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100, // Any value >= 0 indicates group_by mode
		groupSize:      3,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting by first row's price in each group:
	// Group B (price=10) -> Group C (price=20) -> Group A (price=30)
	// Final order: [3, 4, 5, 6, 1, 2]
	expectedIds := []int64{3, 4, 5, 6, 1, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	// Verify group by values are also reordered correctly
	expectedGroupValues := []string{"B", "C", "C", "C", "A", "A"}
	actualGroupValues := sortedResult.Results.GroupByFieldValue.GetScalars().GetStringData().Data
	s.Equal(expectedGroupValues, actualGroupValues)
}

// Test orderByOperator with multiple sort fields (tie-breaking)
func (s *SearchPipelineSuite) TestOrderByOperatorMultiField() {
	// Test tie-breaking: sort by category:asc, then by price:desc
	// Same category should be ordered by price descending
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5},
			Topks:  []int64{5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_VarChar,
					FieldName: "category",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"B", "A", "B", "A", "A"}},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{100, 200, 150, 50, 300}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "category", Ascending: true}, // First sort by category asc
			{FieldName: "price", Ascending: false},   // Then by price desc
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Category A: ids 2(200), 4(50), 5(300) -> sorted by price desc: 5(300), 2(200), 4(50)
	// Category B: ids 1(100), 3(150) -> sorted by price desc: 3(150), 1(100)
	// Final order: A first (5, 2, 4), then B (3, 1)
	expectedIds := []int64{5, 2, 4, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	// Verify categories are in order
	expectedCategories := []string{"A", "A", "A", "B", "B"}
	actualCategories := sortedResult.Results.FieldsData[0].GetScalars().GetStringData().Data
	s.Equal(expectedCategories, actualCategories)

	// Verify prices within each category are descending
	expectedPrices := []int64{300, 200, 50, 150, 100}
	actualPrices := sortedResult.Results.FieldsData[1].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

// Test orderByOperator with string IDs
func (s *SearchPipelineSuite) TestOrderByOperatorStringIds() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{"id_a", "id_b", "id_c", "id_d"}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "priority",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{3, 1, 4, 2}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "priority", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting by priority asc: 1, 2, 3, 4
	// Original: id_a(3), id_b(1), id_c(4), id_d(2)
	// Sorted: id_b(1), id_d(2), id_a(3), id_c(4)
	expectedIds := []string{"id_b", "id_d", "id_a", "id_c"}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetStrId().Data)

	// Verify scores are also reordered
	expectedScores := []float32{0.8, 0.6, 0.9, 0.7}
	s.Equal(expectedScores, sortedResult.Results.Scores)
}

// Test JSON field comparison (byte-level ordering)
func (s *SearchPipelineSuite) TestCompareFieldDataAtJSON() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: [][]byte{
						[]byte(`{"a": 2}`),
						[]byte(`{"a": 10}`),
						[]byte(`{"b": 1}`),
					}},
				},
			},
		},
	}

	// JSON comparison is byte-level, so "2" > "1" (comparing '2' vs '1' in "10")
	// This tests the documented behavior that JSON sorting is lexicographic
	cmp, err := compareFieldDataAt(jsonField, 0, 1)
	s.NoError(err)
	s.Greater(cmp, 0) // {"a": 2} > {"a": 10} in byte comparison because '2' > '1'

	// Different keys
	cmp, err = compareFieldDataAt(jsonField, 0, 2)
	s.NoError(err)
	s.Less(cmp, 0) // {"a": ...} < {"b": ...} because 'a' < 'b'
}

// Test vector field reordering (FloatVector)
func (s *SearchPipelineSuite) TestReorderFieldDataFloatVector() {
	dim := 4
	// 3 vectors, each with dim=4
	vectorField := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "embedding",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: []float32{
							1.0, 1.1, 1.2, 1.3, // vector 0
							2.0, 2.1, 2.2, 2.3, // vector 1
							3.0, 3.1, 3.2, 3.3, // vector 2
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(vectorField, indices)
	s.NoError(err)

	expectedData := []float32{
		3.0, 3.1, 3.2, 3.3, // was vector 2
		1.0, 1.1, 1.2, 1.3, // was vector 0
		2.0, 2.1, 2.2, 2.3, // was vector 1
	}
	actualData := vectorField.GetVectors().GetFloatVector().GetData()
	s.Equal(expectedData, actualData)
}

// Test Double comparison in compareFieldDataAt
func (s *SearchPipelineSuite) TestCompareFieldDataAtDouble() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	doubleField := &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "score",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, 2.5, 1.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(doubleField, 0, 1)) // 1.5 < 2.5
	s.Equal(1, mustCompare(doubleField, 1, 0))  // 2.5 > 1.5
	s.Equal(0, mustCompare(doubleField, 0, 2))  // 1.5 == 1.5
}

// Test Int32 comparison in compareFieldDataAt
func (s *SearchPipelineSuite) TestCompareFieldDataAtInt32() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	int32Field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "count",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 20, 10}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(int32Field, 0, 1)) // 10 < 20
	s.Equal(1, mustCompare(int32Field, 1, 0))  // 20 > 10
	s.Equal(0, mustCompare(int32Field, 0, 2))  // 10 == 10
}

// Test extractJSONValue function
func (s *SearchPipelineSuite) TestExtractJSONValue() {
	jsonData := []byte(`{"price": 99.99, "name": "product", "active": true, "count": 42}`)

	// Extract number
	result := extractJSONValue(jsonData, "/price")
	s.True(result.Exists())
	s.Equal(99.99, result.Float())

	// Extract string
	result = extractJSONValue(jsonData, "/name")
	s.True(result.Exists())
	s.Equal("product", result.String())

	// Extract boolean
	result = extractJSONValue(jsonData, "/active")
	s.True(result.Exists())
	s.True(result.Bool())

	// Extract integer
	result = extractJSONValue(jsonData, "/count")
	s.True(result.Exists())
	s.Equal(int64(42), result.Int())

	// Non-existent path
	result = extractJSONValue(jsonData, "/nonexistent")
	s.False(result.Exists())

	// Empty path returns empty result
	result = extractJSONValue(jsonData, "")
	s.False(result.Exists())

	// Nested path
	nestedJSON := []byte(`{"user": {"profile": {"age": 30}}}`)
	result = extractJSONValue(nestedJSON, "/user/profile/age")
	s.True(result.Exists())
	s.Equal(int64(30), result.Int())

	// Key with literal slash (escaped as ~1 in JSON Pointer)
	jsonWithSlash := []byte(`{"key/with/slash": 100}`)
	result = extractJSONValue(jsonWithSlash, "/key~1with~1slash")
	s.True(result.Exists())
	s.Equal(int64(100), result.Int())

	// Key with literal tilde (escaped as ~0 in JSON Pointer)
	jsonWithTilde := []byte(`{"key~tilde": 200}`)
	result = extractJSONValue(jsonWithTilde, "/key~0tilde")
	s.True(result.Exists())
	s.Equal(int64(200), result.Int())

	// Key with dot (should be escaped for gjson)
	jsonWithDot := []byte(`{"key.with.dot": 300}`)
	result = extractJSONValue(jsonWithDot, "/key.with.dot")
	s.True(result.Exists())
	s.Equal(int64(300), result.Int())
}

// Test compareJSONValues function
func (s *SearchPipelineSuite) TestCompareJSONValues() {
	// Number comparison
	a := extractJSONValue([]byte(`{"v": 10}`), "/v")
	b := extractJSONValue([]byte(`{"v": 20}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // 10 < 20
	s.Equal(1, compareJSONValues(b, a))  // 20 > 10
	s.Equal(0, compareJSONValues(a, a))  // 10 == 10

	// String comparison
	a = extractJSONValue([]byte(`{"v": "apple"}`), "/v")
	b = extractJSONValue([]byte(`{"v": "banana"}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // "apple" < "banana"
	s.Equal(1, compareJSONValues(b, a))  // "banana" > "apple"

	// Boolean comparison (false < true)
	a = extractJSONValue([]byte(`{"v": false}`), "/v")
	b = extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // false < true
	s.Equal(1, compareJSONValues(b, a))  // true > false

	// Non-existent values (nulls first)
	a = extractJSONValue([]byte(`{}`), "/v")
	b = extractJSONValue([]byte(`{"v": 10}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // null < 10
	s.Equal(1, compareJSONValues(b, a))  // 10 > null

	// Both non-existent
	a = extractJSONValue([]byte(`{}`), "/v")
	b = extractJSONValue([]byte(`{}`), "/v")
	s.Equal(0, compareJSONValues(a, b)) // null == null

	// Explicit JSON null value (type gjson.Null)
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{"v": 10}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // null < 10
	s.Equal(1, compareJSONValues(b, a))  // 10 > null

	// Both explicit null
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{"v": null}`), "/v")
	s.Equal(0, compareJSONValues(a, b)) // null == null

	// Explicit null vs non-existent (both treated as null)
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{}`), "/v")
	s.Equal(0, compareJSONValues(a, b)) // null == null
}

// Test orderByOperator with JSON subfield path
func (s *SearchPipelineSuite) TestOrderByOperatorWithJSONPath() {
	// Create result with JSON field containing nested data
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "metadata",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"price": 30, "category": "A"}`),
									[]byte(`{"price": 10, "category": "B"}`),
									[]byte(`{"price": 40, "category": "A"}`),
									[]byte(`{"price": 20, "category": "B"}`),
								}},
							},
						},
					},
				},
			},
		},
	}

	// Sort by metadata["price"] ascending
	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort by price: 10, 20, 30, 40
	// Original order: 30(id=1), 10(id=2), 40(id=3), 20(id=4)
	// Sorted order: 10(id=2), 20(id=4), 30(id=1), 40(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with JSON path - descending order
func (s *SearchPipelineSuite) TestOrderByOperatorWithJSONPathDescending() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "data",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"score": 100}`),
									[]byte(`{"score": 300}`),
									[]byte(`{"score": 200}`),
								}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "data", FieldID: 100, JSONPath: "/score", Ascending: false},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After descending sort: 300, 200, 100
	// Original: 100(id=1), 300(id=2), 200(id=3)
	// Sorted: 300(id=2), 200(id=3), 100(id=1)
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with missing JSON path values (nulls first)
func (s *SearchPipelineSuite) TestOrderByOperatorWithMissingJSONPath() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "metadata",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"price": 30}`),
									[]byte(`{}`), // missing price - should sort first
									[]byte(`{"price": 10}`),
									[]byte(`{"other": 99}`), // missing price - should sort first
								}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Nulls first, then ascending: null, null, 10, 30
	// IDs with missing price (2, 4) should come first, then 3(10), 1(30)
	// Note: stable sort preserves relative order of equal elements
	resultIds := sortedResult.Results.Ids.GetIntId().Data
	// First two should be the ones with missing price (2 and 4)
	s.Contains([]int64{2, 4}, resultIds[0])
	s.Contains([]int64{2, 4}, resultIds[1])
	// Last two should be 3(10) and 1(30) in that order
	s.Equal(int64(3), resultIds[2])
	s.Equal(int64(1), resultIds[3])
}

// Test parseOrderByFields with JSON path syntax
func (s *SearchPipelineSuite) TestParseOrderByFieldsWithJSONPath() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "metadata", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "score", DataType: schemapb.DataType_Float},
		},
	}

	// Test JSON path with ascending
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["price"]:asc`}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal(int64(101), result[0].FieldID)
	s.Equal("/price", result[0].JSONPath)
	s.True(result[0].Ascending)
	s.Equal("metadata", result[0].OutputFieldName) // Regular JSON: request whole field
	s.False(result[0].IsDynamicField)              // Not a dynamic field

	// Test JSON path with descending
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["rating"]:desc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal("/rating", result[0].JSONPath)
	s.False(result[0].Ascending)
	s.Equal("metadata", result[0].OutputFieldName) // Regular JSON: request whole field
	s.False(result[0].IsDynamicField)

	// Test mixed: regular field + JSON path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `score:desc,metadata["price"]:asc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)
	// First field: regular
	s.Equal("score", result[0].FieldName)
	s.Equal("", result[0].JSONPath)
	s.False(result[0].Ascending)
	// Second field: JSON path
	s.Equal("metadata", result[1].FieldName)
	s.Equal("/price", result[1].JSONPath)
	s.True(result[1].Ascending)

	// Test nested JSON path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["user"]["age"]:asc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal("/user/age", result[0].JSONPath)
}

// Test parseOrderByFields with dynamic fields
func (s *SearchPipelineSuite) TestParseOrderByFieldsWithDynamicField() {
	schema := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}

	// Test dynamic field key without brackets (age -> $meta["age"])
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "age:asc"}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("$meta", result[0].FieldName)
	s.Equal(int64(102), result[0].FieldID)
	s.Equal("/age", result[0].JSONPath)
	s.True(result[0].Ascending)
	s.Equal("age", result[0].OutputFieldName) // Dynamic field: use original key for requery
	s.True(result[0].IsDynamicField)          // Is a dynamic field - QueryNode extracts subfield

	// Test dynamic field with explicit path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `$meta["category"]:desc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("$meta", result[0].FieldName)
	s.Equal("/category", result[0].JSONPath)
	s.False(result[0].Ascending)
	s.Equal(`$meta["category"]`, result[0].OutputFieldName) // Explicit path for requery
	s.True(result[0].IsDynamicField)
}

// Test splitOrderByFieldAndDirection helper
func (s *SearchPipelineSuite) TestSplitOrderByFieldAndDirection() {
	// Simple field
	field, dir := splitOrderByFieldAndDirection("name:asc")
	s.Equal("name", field)
	s.Equal("asc", dir)

	// Field without direction
	field, dir = splitOrderByFieldAndDirection("name")
	s.Equal("name", field)
	s.Equal("", dir)

	// JSON path with direction
	field, dir = splitOrderByFieldAndDirection(`metadata["price"]:desc`)
	s.Equal(`metadata["price"]`, field)
	s.Equal("desc", dir)

	// JSON path without direction
	field, dir = splitOrderByFieldAndDirection(`metadata["price"]`)
	s.Equal(`metadata["price"]`, field)
	s.Equal("", dir)

	// Nested JSON path with direction
	field, dir = splitOrderByFieldAndDirection(`data["user"]["age"]:asc`)
	s.Equal(`data["user"]["age"]`, field)
	s.Equal("asc", dir)

	// JSON path with colon in value (edge case - not typical but should handle)
	field, dir = splitOrderByFieldAndDirection(`metadata["key:with:colons"]:desc`)
	s.Equal(`metadata["key:with:colons"]`, field)
	s.Equal("desc", dir)
}

// Test jsonPointerToGjsonPath conversion
func (s *SearchPipelineSuite) TestJSONPointerToGjsonPath() {
	// Simple path
	s.Equal("price", jsonPointerToGjsonPath("/price"))

	// Nested path
	s.Equal("user.profile.age", jsonPointerToGjsonPath("/user/profile/age"))

	// Key with literal slash (escaped as ~1)
	s.Equal("key/with/slash", jsonPointerToGjsonPath("/key~1with~1slash"))

	// Key with literal tilde (escaped as ~0)
	s.Equal("key~tilde", jsonPointerToGjsonPath("/key~0tilde"))

	// Key with dot (escaped for gjson)
	s.Equal(`key\.with\.dot`, jsonPointerToGjsonPath("/key.with.dot"))

	// Combined: nested path with special characters
	s.Equal(`user.profile\.v2.score`, jsonPointerToGjsonPath("/user/profile.v2/score"))

	// Empty path
	s.Equal("", jsonPointerToGjsonPath(""))
	s.Equal("", jsonPointerToGjsonPath("/"))

	// Path without leading slash
	s.Equal("price", jsonPointerToGjsonPath("price"))
}

// Test parseOrderByFields error cases for better coverage
func (s *SearchPipelineSuite) TestParseOrderByFieldsErrors() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "score", DataType: schemapb.DataType_Float},
			{FieldID: 103, Name: "metadata", DataType: schemapb.DataType_JSON},
		},
	}

	// Test invalid direction
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:invalid"}}
	_, err := parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "invalid order direction")

	// Test non-JSON field with brackets
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `score["key"]:asc`}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "not a JSON type")

	// Test unknown field with brackets and no dynamic field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `unknown["key"]:asc`}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "not found")

	// Test unknown field without brackets and no dynamic field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "unknown_field:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "does not exist")

	// Test unsortable field type (vector)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "vector:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")

	// Test JSON field without path - not allowed, must use path syntax like metadata["key"]
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "metadata:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")
	s.Contains(err.Error(), "JSON")
}

// Test compareFieldDataAt for different data types
func (s *SearchPipelineSuite) TestCompareFieldDataAtAllTypes() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	// Test Int8/Int16/Int32 comparison
	intField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "int_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{30, 10, 20}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(intField, 1, 2)) // 10 < 20
	s.Equal(1, mustCompare(intField, 0, 1))  // 30 > 10
	s.Equal(0, mustCompare(intField, 0, 0))  // 30 == 30

	// Test Float comparison
	floatField := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "float_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{3.5, 1.5, 2.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(floatField, 1, 2)) // 1.5 < 2.5
	s.Equal(1, mustCompare(floatField, 0, 1))  // 3.5 > 1.5
	s.Equal(0, mustCompare(floatField, 0, 0))  // 3.5 == 3.5

	// Test Double comparison
	doubleField := &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "double_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{3.5, 1.5, 2.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(doubleField, 1, 2)) // 1.5 < 2.5
	s.Equal(1, mustCompare(doubleField, 0, 1))  // 3.5 > 1.5
	s.Equal(0, mustCompare(doubleField, 0, 0))  // 3.5 == 3.5

	// Test Bool comparison
	boolField := &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "bool_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}},
				},
			},
		},
	}
	s.Equal(1, mustCompare(boolField, 0, 1))  // true > false
	s.Equal(-1, mustCompare(boolField, 1, 0)) // false < true
	s.Equal(0, mustCompare(boolField, 0, 2))  // true == true

	// Test out of bounds returns error
	_, err := compareFieldDataAt(intField, 10, 20)
	s.Error(err)
	_, err = compareFieldDataAt(floatField, 10, 20)
	s.Error(err)
	_, err = compareFieldDataAt(doubleField, 10, 20)
	s.Error(err)
	_, err = compareFieldDataAt(boolField, 10, 20)
	s.Error(err)
}

// Test compareFieldDataAt with nullable fields (ValidData)
func (s *SearchPipelineSuite) TestCompareFieldDataAtNullable() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j)
		s.NoError(err)
		return cmp
	}

	// Create field with ValidData (nullable)
	nullableField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // Index 1 is null
	}

	// null vs non-null: null should come first (NULLS FIRST)
	s.Equal(1, mustCompare(nullableField, 0, 1))  // 100 > null
	s.Equal(-1, mustCompare(nullableField, 1, 0)) // null < 100
	s.Equal(-1, mustCompare(nullableField, 1, 2)) // null < 300

	// Create field where both are null
	nullableField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field2",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
				},
			},
		},
		ValidData: []bool{false, false, true}, // Index 0 and 1 are null
	}
	s.Equal(0, mustCompare(nullableField2, 0, 1)) // null == null
}

// Test isSameGroupByValue for different types
func (s *SearchPipelineSuite) TestIsSameGroupByValueAllTypes() {
	// Test Int8/Int16/Int32
	intField := &schemapb.FieldData{
		Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 10, 20}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(intField, 0, 1))  // 10 == 10
	s.False(isSameGroupByValue(intField, 0, 2)) // 10 != 20

	// Test Int64
	int64Field := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 100, 200}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(int64Field, 0, 1))  // 100 == 100
	s.False(isSameGroupByValue(int64Field, 0, 2)) // 100 != 200

	// Test Bool
	boolField := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, true, false}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(boolField, 0, 1))  // true == true
	s.False(isSameGroupByValue(boolField, 0, 2)) // true != false

	// Test VarChar (already covered but verify)
	stringField := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"A", "A", "B"}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(stringField, 0, 1))  // "A" == "A"
	s.False(isSameGroupByValue(stringField, 0, 2)) // "A" != "B"

	// Test out of bounds returns false
	s.False(isSameGroupByValue(intField, 10, 20))
	s.False(isSameGroupByValue(int64Field, 10, 20))
	s.False(isSameGroupByValue(boolField, 10, 20))
	s.False(isSameGroupByValue(stringField, 10, 20))

	// Test unsupported type (Float) returns false
	floatField := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{1.0, 1.0}},
				},
			},
		},
	}
	s.False(isSameGroupByValue(floatField, 0, 1)) // Float not supported
}

// Test compareJSONValues with boolean values
func (s *SearchPipelineSuite) TestCompareJSONValuesBool() {
	// Test bool: false < true
	a := extractJSONValue([]byte(`{"v": false}`), "/v")
	b := extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(-1, compareJSONValues(a, b)) // false < true
	s.Equal(1, compareJSONValues(b, a))  // true > false

	// Test both true
	a = extractJSONValue([]byte(`{"v": true}`), "/v")
	b = extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(0, compareJSONValues(a, b)) // true == true

	// Test both false
	a = extractJSONValue([]byte(`{"v": false}`), "/v")
	b = extractJSONValue([]byte(`{"v": false}`), "/v")
	s.Equal(0, compareJSONValues(a, b)) // false == false
}

// Test compareJSONValues with mixed types (fallback to raw comparison)
func (s *SearchPipelineSuite) TestCompareJSONValuesMixedTypes() {
	// Number vs String - falls back to raw comparison
	// Raw: "10" vs "\"hello\"" - quote char '"' (34) < '1' (49)
	a := extractJSONValue([]byte(`{"v": 10}`), "/v")
	b := extractJSONValue([]byte(`{"v": "hello"}`), "/v")
	cmp := compareJSONValues(a, b)
	// String (with quotes) should sort before number due to quote ASCII
	s.Equal(1, cmp) // "10" > "\"hello\"" because '1' > '"'
}

// Test orderByOperator with Float field type
func (s *SearchPipelineSuite) TestOrderByOperatorFloatField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Float,
					FieldName: "rating",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_FloatData{
								FloatData: &schemapb.FloatArray{Data: []float32{3.5, 1.5, 4.5, 2.5}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "rating", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 1.5, 2.5, 3.5, 4.5
	// Original: 3.5(id=1), 1.5(id=2), 4.5(id=3), 2.5(id=4)
	// Sorted: 1.5(id=2), 2.5(id=4), 3.5(id=1), 4.5(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with Bool field type
func (s *SearchPipelineSuite) TestOrderByOperatorBoolField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Bool,
					FieldName: "active",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_BoolData{
								BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, false}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "active", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: false, false, true, true
	// Original: true(id=1), false(id=2), true(id=3), false(id=4)
	// Sorted (stable): false(id=2), false(id=4), true(id=1), true(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator group_by with Int64 group values
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupByInt64() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7},
			Topks:  []int64{5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 25, 10, 20, 15}},
							},
						},
					},
				},
			},
			// GroupByFieldValue with Int64 type
			// Group 100: ids [1,2], prices [30,25]
			// Group 200: ids [3], price [10]
			// Group 300: ids [4,5], prices [20,15]
			GroupByFieldValue: &schemapb.FieldData{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 300, 300}},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100,
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting groups by first row's price:
	// Group 200 (price=10) -> Group 300 (price=20) -> Group 100 (price=30)
	// Final order: [3, 4, 5, 1, 2]
	expectedIds := []int64{3, 4, 5, 1, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test sortGroupsByOrderByFields with no GroupByFieldValue (fallback to regular sort)
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupByNoGroupValue() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20}},
							},
						},
					},
				},
			},
			// No GroupByFieldValue - should fallback to regular sort
			GroupByFieldValue: nil,
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100, // Says group_by but no GroupByFieldValue
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Should fallback to regular sort: 10, 20, 30
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test reorderFieldData for Int8/Int16/Int32 scalar types
func (s *SearchPipelineSuite) TestReorderFieldDataInt32() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "int_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 20, 30, 40}},
				},
			},
		},
	}

	// Reorder: [2, 0, 3, 1] means new[0]=old[2], new[1]=old[0], etc.
	indices := []int{2, 0, 3, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []int32{30, 10, 40, 20}
	s.Equal(expected, field.GetScalars().GetIntData().Data)
}

// Test reorderFieldData for Array type
func (s *SearchPipelineSuite) TestReorderFieldDataArray() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "array_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6}}}},
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	result := field.GetScalars().GetArrayData().Data
	s.Len(result, 3)
	s.Equal([]int32{5, 6}, result[0].GetIntData().Data)
	s.Equal([]int32{1, 2}, result[1].GetIntData().Data)
	s.Equal([]int32{3, 4}, result[2].GetIntData().Data)
}

// Test reorderFieldData for BinaryVector type
func (s *SearchPipelineSuite) TestReorderFieldDataBinaryVector() {
	// 3 vectors, dim=16 (2 bytes per vector)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: "binary_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 16,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}, // 3 vectors * 2 bytes
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{0x05, 0x06, 0x01, 0x02, 0x03, 0x04}
	s.Equal(expected, field.GetVectors().GetBinaryVector())
}

// Test reorderFieldData for Float16Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataFloat16Vector() {
	// 3 vectors, dim=2 (4 bytes per vector: 2 bytes per float16 * 2 dims)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: "fp16_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: []byte{
						0x01, 0x02, 0x03, 0x04, // vector 0
						0x05, 0x06, 0x07, 0x08, // vector 1
						0x09, 0x0A, 0x0B, 0x0C, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x09, 0x0A, 0x0B, 0x0C, // was vector 2
		0x01, 0x02, 0x03, 0x04, // was vector 0
		0x05, 0x06, 0x07, 0x08, // was vector 1
	}
	s.Equal(expected, field.GetVectors().GetFloat16Vector())
}

// Test reorderFieldData for BFloat16Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataBFloat16Vector() {
	// 3 vectors, dim=2 (4 bytes per vector: 2 bytes per bfloat16 * 2 dims)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_BFloat16Vector,
		FieldName: "bf16_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: []byte{
						0x11, 0x12, 0x13, 0x14, // vector 0
						0x21, 0x22, 0x23, 0x24, // vector 1
						0x31, 0x32, 0x33, 0x34, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [1, 2, 0]
	indices := []int{1, 2, 0}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x21, 0x22, 0x23, 0x24, // was vector 1
		0x31, 0x32, 0x33, 0x34, // was vector 2
		0x11, 0x12, 0x13, 0x14, // was vector 0
	}
	s.Equal(expected, field.GetVectors().GetBfloat16Vector())
}

// Test reorderFieldData for SparseFloatVector type
func (s *SearchPipelineSuite) TestReorderFieldDataSparseFloatVector() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: "sparse_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 100,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{
							{0x01, 0x02}, // sparse vector 0
							{0x03, 0x04}, // sparse vector 1
							{0x05, 0x06}, // sparse vector 2
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	result := field.GetVectors().GetSparseFloatVector().GetContents()
	s.Len(result, 3)
	s.Equal([]byte{0x05, 0x06}, result[0])
	s.Equal([]byte{0x01, 0x02}, result[1])
	s.Equal([]byte{0x03, 0x04}, result[2])
}

// Test reorderFieldData for Int8Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataInt8Vector() {
	// 3 vectors, dim=4 (4 bytes per vector)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int8Vector,
		FieldName: "int8_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: []byte{
						0x01, 0x02, 0x03, 0x04, // vector 0
						0x05, 0x06, 0x07, 0x08, // vector 1
						0x09, 0x0A, 0x0B, 0x0C, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x09, 0x0A, 0x0B, 0x0C, // was vector 2
		0x01, 0x02, 0x03, 0x04, // was vector 0
		0x05, 0x06, 0x07, 0x08, // was vector 1
	}
	s.Equal(expected, field.GetVectors().GetInt8Vector())
}

// Test reorderFieldData with ValidData (nullable fields)
func (s *SearchPipelineSuite) TestReorderFieldDataWithValidData() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // index 1 is null
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expectedData := []int64{30, 10, 20}
	expectedValid := []bool{true, true, false} // null moves to index 2
	s.Equal(expectedData, field.GetScalars().GetLongData().Data)
	s.Equal(expectedValid, field.ValidData)
}

// Test orderByOperator with Int32 field to cover reorderFieldData Int32 branch
func (s *SearchPipelineSuite) TestOrderByOperatorInt32Field() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int32,
					FieldName: "count",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{Data: []int32{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "count", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 10, 20, 30, 40
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedCounts := []int32{10, 20, 30, 40}
	s.Equal(expectedCounts, sortedResult.Results.FieldsData[0].GetScalars().GetIntData().Data)
}

// Test orderByOperator with Double field
func (s *SearchPipelineSuite) TestOrderByOperatorDoubleField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Double,
					FieldName: "value",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_DoubleData{
								DoubleData: &schemapb.DoubleArray{Data: []float64{3.14, 1.41, 2.72}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "value", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 1.41, 2.72, 3.14
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedValues := []float64{1.41, 2.72, 3.14}
	s.Equal(expectedValues, sortedResult.Results.FieldsData[0].GetScalars().GetDoubleData().Data)
}

// Test orderByOperator with VarChar field
func (s *SearchPipelineSuite) TestOrderByOperatorVarCharField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_VarChar,
					FieldName: "name",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"charlie", "alice", "bob"}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "name", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: alice, bob, charlie
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedNames := []string{"alice", "bob", "charlie"}
	s.Equal(expectedNames, sortedResult.Results.FieldsData[0].GetScalars().GetStringData().Data)
}

// Test compareNullsFirst helper function
func (s *SearchPipelineSuite) TestCompareNullsFirst() {
	// Empty ValidData - should return (0, false)
	cmp, handled := compareNullsFirst(nil, 0, 1)
	s.Equal(0, cmp)
	s.False(handled)

	cmp, handled = compareNullsFirst([]bool{}, 0, 1)
	s.Equal(0, cmp)
	s.False(handled)

	// Both non-null - should return (0, false)
	validData := []bool{true, true, true}
	cmp, handled = compareNullsFirst(validData, 0, 1)
	s.Equal(0, cmp)
	s.False(handled)

	// First is null, second is not - should return (-1, true) (nulls first)
	validData = []bool{false, true, true}
	cmp, handled = compareNullsFirst(validData, 0, 1)
	s.Equal(-1, cmp)
	s.True(handled)

	// First is not null, second is null - should return (1, true)
	cmp, handled = compareNullsFirst(validData, 1, 0)
	s.Equal(1, cmp)
	s.True(handled)

	// Both are null - should return (0, true)
	validData = []bool{false, false, true}
	cmp, handled = compareNullsFirst(validData, 0, 1)
	s.Equal(0, cmp)
	s.True(handled)
}

// Test buildJSONValueCache and getCachedJSONValue
func (s *SearchPipelineSuite) TestBuildJSONValueCache() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{
							[]byte(`{"score": 100}`),
							[]byte(`{"score": 200}`),
							[]byte(`{"score": 150}`),
						},
					},
				},
			},
		},
	}

	fieldMap := map[string]*schemapb.FieldData{
		"metadata": jsonField,
	}
	orderByFields := []OrderByField{
		{FieldName: "metadata", JSONPath: "/score"},
	}

	cache := buildJSONValueCache(fieldMap, orderByFields, []int{0, 1, 2})

	// Verify cache contains extracted values
	val0 := cache.getCachedJSONValue("metadata", "/score", 0)
	s.True(val0.Exists())
	s.Equal(float64(100), val0.Float())

	val1 := cache.getCachedJSONValue("metadata", "/score", 1)
	s.True(val1.Exists())
	s.Equal(float64(200), val1.Float())

	val2 := cache.getCachedJSONValue("metadata", "/score", 2)
	s.True(val2.Exists())
	s.Equal(float64(150), val2.Float())

	// Non-existent field/path returns empty result
	valMissing := cache.getCachedJSONValue("nonexistent", "/score", 0)
	s.False(valMissing.Exists())

	valMissingPath := cache.getCachedJSONValue("metadata", "/nonexistent", 0)
	s.False(valMissingPath.Exists())

	// Out of bounds index returns empty result
	valOOB := cache.getCachedJSONValue("metadata", "/score", 100)
	s.False(valOOB.Exists())
}

// Test buildJSONValueCache skips non-JSON fields
func (s *SearchPipelineSuite) TestBuildJSONValueCacheSkipsNonJSON() {
	intField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "age",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
	}

	fieldMap := map[string]*schemapb.FieldData{
		"age": intField,
	}
	orderByFields := []OrderByField{
		{FieldName: "age", JSONPath: ""}, // No JSON path, should be skipped
	}

	cache := buildJSONValueCache(fieldMap, orderByFields, []int{0, 1, 2})
	s.Empty(cache) // No entries should be cached for non-JSON fields
}

// Test compareOrderByField with nullable JSON field
func (s *SearchPipelineSuite) TestCompareOrderByFieldNullableJSON() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{
							[]byte(`{"score": 100}`),
							[]byte(`{"score": 200}`),
							[]byte(`{"score": 150}`),
						},
					},
				},
			},
		},
		ValidData: []bool{true, false, true}, // Index 1 is null
	}

	fieldMap := map[string]*schemapb.FieldData{
		"metadata": jsonField,
	}
	orderBy := OrderByField{FieldName: "metadata", JSONPath: "/score"}
	cache := buildJSONValueCache(fieldMap, []OrderByField{orderBy}, []int{0, 1, 2})

	// null vs non-null: null should come first
	cmp, err := compareOrderByField(jsonField, orderBy, 1, 0, cache)
	s.NoError(err)
	s.Equal(-1, cmp) // null < 100

	cmp, err = compareOrderByField(jsonField, orderBy, 0, 1, cache)
	s.NoError(err)
	s.Equal(1, cmp) // 100 > null

	// non-null vs non-null: normal comparison
	cmp, err = compareOrderByField(jsonField, orderBy, 0, 2, cache)
	s.NoError(err)
	s.Equal(-1, cmp) // 100 < 150

	cmp, err = compareOrderByField(jsonField, orderBy, 2, 0, cache)
	s.NoError(err)
	s.Equal(1, cmp) // 150 > 100
}

// Test reorderFieldData returns error for unhandled data type
func (s *SearchPipelineSuite) TestReorderFieldDataUnhandledType() {
	// Create a field with an unhandled type (None type is not handled)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_None,
		FieldName: "unknown",
	}

	indices := []int{0, 1, 2}
	err := reorderFieldData(field, indices)
	s.Error(err)
	s.Contains(err.Error(), "unhandled data type")
}

// TestNewSearchPipelineWithOrderBy tests that newSearchPipeline correctly routes
// to searchWithOrderByPipe when orderByFields is set
func (s *SearchPipelineSuite) TestNewSearchPipelineWithOrderBy() {
	collectionName := "test_collection"

	// Mock requery operation
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200}, nil).Build()
	defer mocker.UnPatch()

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
			IsAdvanced:   false, // Regular search, not hybrid
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
		request:                &milvuspb.SearchRequest{Namespace: nil},
		// Set orderByFields to trigger searchWithOrderByPipe
		orderByFields: []OrderByField{
			{FieldName: "intField", OutputFieldName: "intField", Ascending: true},
		},
	}

	// Use newSearchPipeline (the actual entry point)
	pipeline, err := newSearchPipeline(task)
	s.NoError(err)
	s.NotNil(pipeline)

	// Run the pipeline with test data
	sr := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{sr}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
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
