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
	"encoding/json"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/highlight"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/zilliz"
)

func TestHighlighter(t *testing.T) {
	suite.Run(t, new(HighlighterSuite))
}

type HighlighterSuite struct {
	suite.Suite
}

// ============== LexicalHighlighter Tests ==============

func (s *HighlighterSuite) TestLexicalHighlighter_RequiredFieldIDs() {
	h := &LexicalHighlighter{
		tasks: map[int64]*highlightTask{
			101: {HighlightTask: nil},
			102: {HighlightTask: nil},
		},
		extraFields: []int64{201, 202},
	}

	// RequiredFieldIDs should return the same as FieldIDs for LexicalHighlighter
	fieldIDs := h.FieldIDs()
	requiredFieldIDs := h.RequiredFieldIDs()

	s.ElementsMatch(fieldIDs, requiredFieldIDs)
	s.Len(requiredFieldIDs, 4) // 2 tasks + 2 extraFields
}

func (s *HighlighterSuite) TestLexicalHighlighter_DynamicFieldNames() {
	h := &LexicalHighlighter{
		tasks:       map[int64]*highlightTask{},
		extraFields: []int64{},
	}

	// LexicalHighlighter always returns empty slice for DynamicFieldNames
	dynamicFields := h.DynamicFieldNames()
	s.NotNil(dynamicFields)
	s.Empty(dynamicFields)
}

func (s *HighlighterSuite) TestLexicalHighlighter_AsSearchPipelineOperator() {
	h := &LexicalHighlighter{
		tasks: map[int64]*highlightTask{
			101: {HighlightTask: nil},
		},
		extraFields: []int64{},
	}

	// Mock newLexicalHighlightOperator to avoid nil pointer dereference
	mockOp := mockey.Mock(newLexicalHighlightOperator).To(func(t *searchTask, tasks []*highlightTask) (operator, error) {
		return &lexicalHighlightOperator{tasks: tasks}, nil
	}).Build()
	defer mockOp.UnPatch()

	op, err := h.AsSearchPipelineOperator(nil)
	s.NoError(err)
	s.NotNil(op)
	_, ok := op.(*lexicalHighlightOperator)
	s.True(ok)
}

// ============== SemanticHighlighter Tests ==============

func (s *HighlighterSuite) TestSemanticHighlighter_RequiredFieldIDs_SchemaFieldsOnly() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "content", DataType: schemapb.DataType_Text},
		},
		EnableDynamicField: false,
	}

	queries := []string{"test query"}
	inputFields := []string{"title", "content"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	// When there are no dynamic fields, RequiredFieldIDs should equal FieldIDs
	fieldIDs := h.FieldIDs()
	requiredFieldIDs := h.RequiredFieldIDs()

	s.Equal(fieldIDs, requiredFieldIDs)
	s.Equal([]int64{101, 102}, requiredFieldIDs)
}

func (s *HighlighterSuite) TestSemanticHighlighter_RequiredFieldIDs_WithDynamicFields() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			{FieldID: 200, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
		EnableDynamicField: true,
	}

	queries := []string{"test query"}
	inputFields := []string{"title", "dynamic_field1", "dynamic_field2"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	// FieldIDs should only contain schema fields
	fieldIDs := h.FieldIDs()
	s.Equal([]int64{101}, fieldIDs)

	// RequiredFieldIDs should include $meta field
	requiredFieldIDs := h.RequiredFieldIDs()
	s.Len(requiredFieldIDs, 2)
	s.Contains(requiredFieldIDs, int64(101)) // schema field
	s.Contains(requiredFieldIDs, int64(200)) // $meta field
}

func (s *HighlighterSuite) TestSemanticHighlighter_RequiredFieldIDs_DoesNotMutateFieldIDs() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "content", DataType: schemapb.DataType_Text},
			{FieldID: 200, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
		EnableDynamicField: true,
	}

	queries := []string{"test query"}
	inputFields := []string{"title", "content", "dynamic_field"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	// Get original FieldIDs and make a copy
	originalFieldIDs := h.FieldIDs()
	originalCopy := make([]int64, len(originalFieldIDs))
	copy(originalCopy, originalFieldIDs)

	// Call RequiredFieldIDs multiple times
	result1 := h.RequiredFieldIDs()
	result2 := h.RequiredFieldIDs()
	result3 := h.RequiredFieldIDs()

	// Verify FieldIDs was NOT mutated
	s.Equal(originalCopy, h.FieldIDs(), "FieldIDs should not be mutated after calling RequiredFieldIDs")

	// Verify each call returns expected results
	s.Len(result1, 3) // 2 schema fields + 1 $meta
	s.Len(result2, 3)
	s.Len(result3, 3)

	// Verify modifying result1 doesn't affect result2
	if len(result1) > 0 {
		result1[0] = 9999
		s.NotEqual(result1[0], result2[0], "Returned slices should be independent")
	}
}

func (s *HighlighterSuite) TestSemanticHighlighter_DynamicFieldNames_Empty() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
		},
		EnableDynamicField: false,
	}

	queries := []string{"test query"}
	inputFields := []string{"title"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	dynamicFields := h.DynamicFieldNames()
	s.NotNil(dynamicFields)
	s.Empty(dynamicFields)
}

func (s *HighlighterSuite) TestSemanticHighlighter_DynamicFieldNames_WithDynamicFields() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			{FieldID: 200, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
		EnableDynamicField: true,
	}

	queries := []string{"test query"}
	inputFields := []string{"title", "dyn_field1", "dyn_field2"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	dynamicFields := h.DynamicFieldNames()
	s.Len(dynamicFields, 2)
	s.Contains(dynamicFields, "dyn_field1")
	s.Contains(dynamicFields, "dyn_field2")
}

func (s *HighlighterSuite) TestSemanticHighlighter_AsSearchPipelineOperator() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
		},
		EnableDynamicField: false,
	}

	queries := []string{"test query"}
	inputFields := []string{"title"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	h := &SemanticHighlighter{highlight: semanticHighlight}

	// AsSearchPipelineOperator should return a semanticHighlightOperator
	op, err := h.AsSearchPipelineOperator(nil)
	s.NoError(err)
	s.NotNil(op)
	_, ok := op.(*semanticHighlightOperator)
	s.True(ok)
}

// ============== Highlighter Interface Tests ==============

func (s *HighlighterSuite) TestHighlighterInterface_LexicalHighlighter() {
	var h Highlighter = &LexicalHighlighter{
		tasks:       map[int64]*highlightTask{101: {}},
		extraFields: []int64{},
	}

	// Verify LexicalHighlighter implements Highlighter interface
	s.NotNil(h.FieldIDs())
	s.NotNil(h.RequiredFieldIDs())
	s.NotNil(h.DynamicFieldNames())

	// Mock newLexicalHighlightOperator to avoid nil pointer dereference
	mockOp := mockey.Mock(newLexicalHighlightOperator).To(func(t *searchTask, tasks []*highlightTask) (operator, error) {
		return &lexicalHighlightOperator{}, nil
	}).Build()
	defer mockOp.UnPatch()

	op, err := h.AsSearchPipelineOperator(nil)
	s.NoError(err)
	s.NotNil(op)
}

func (s *HighlighterSuite) TestHighlighterInterface_SemanticHighlighter() {
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
		},
	}

	queries := []string{"test"}
	inputFields := []string{"title"}
	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: "queries", Value: string(queriesJSON)},
		{Key: "input_fields", Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{"endpoint": "localhost:8080"}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}

	semanticHighlight, err := highlight.NewSemanticHighlight(schema, params, conf, extraInfo)
	s.NoError(err)

	var h Highlighter = &SemanticHighlighter{highlight: semanticHighlight}

	// Verify SemanticHighlighter implements Highlighter interface
	s.NotNil(h.FieldIDs())
	s.NotNil(h.RequiredFieldIDs())
	s.NotNil(h.DynamicFieldNames())

	op, err := h.AsSearchPipelineOperator(nil)
	s.NoError(err)
	s.NotNil(op)
}
