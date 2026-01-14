/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package highlight

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/zilliz"
)

func TestSemanticHighlight(t *testing.T) {
	suite.Run(t, new(SemanticHighlightSuite))
}

type SemanticHighlightSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *SemanticHighlightSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "content", DataType: schemapb.DataType_Text},
			{FieldID: 103, Name: "description", DataType: schemapb.DataType_VarChar},
			{FieldID: 104, Name: "embedding", DataType: schemapb.DataType_FloatVector},
		},
	}
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_Success() {
	queries := []string{"machine learning", "artificial intelligence"}
	inputFields := []string{"title", "content"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.NoError(err)
	s.NotNil(highlight)
	s.Equal([]int64{101, 102}, highlight.FieldIDs())
	s.Equal(queries, highlight.queries)
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_MissingQueries() {
	inputFields := []string{"title"}
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "queries is required")
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_MissingInputFields() {
	queries := []string{"machine learning"}
	queriesJSON, _ := json.Marshal(queries)

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "input_field is required")
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_InvalidQueriesJSON() {
	inputFields := []string{"title"}
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: "invalid json"},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "Parse queries failed")
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_InvalidInputFieldsJSON() {
	queries := []string{"machine learning"}
	queriesJSON, _ := json.Marshal(queries)

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: "invalid json"},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "Parse input_field failed")
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_FieldNotFound() {
	queries := []string{"machine learning"}
	inputFields := []string{"nonexistent_field"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "not found")
}

func (s *SemanticHighlightSuite) TestNewSemanticHighlight_InvalidFieldType() {
	queries := []string{"machine learning"}
	inputFields := []string{"embedding"} // FloatVector, not VarChar or Text

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)

	s.Error(err)
	s.Nil(highlight)
	s.Contains(err.Error(), "is not a VarChar or Text field")
}

func (s *SemanticHighlightSuite) TestProcessOneQuery_Success() {
	queries := []string{"machine learning"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	expectedHighlights := [][]string{
		{"machine learning"},
		{"machine"},
	}
	expectedScores := [][]float32{
		{0.95},
		{0.80},
	}

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*zilliz.ZillizClient).Highlight).To(func(_ *zilliz.ZillizClient, _ context.Context, _ string, _ []string, _ map[string]string) ([][]string, [][]float32, error) {
		return expectedHighlights, expectedScores, nil
	}).Build()
	defer mock2.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{"Machine learning is a subset of AI", "Machine learning is powerful"}
	highlights, scores, err := highlight.processOneQuery(ctx, "machine learning", data)

	s.NoError(err)
	s.Equal(expectedHighlights, highlights)
	s.Equal(expectedScores, scores)
}

func (s *SemanticHighlightSuite) TestProcessOneQuery_Error() {
	queries := []string{"test query"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	expectedError := errors.New("highlight service error")

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*zilliz.ZillizClient).Highlight).To(func(_ *zilliz.ZillizClient, _ context.Context, _ string, _ []string, _ map[string]string) ([][]string, [][]float32, error) {
		return nil, nil, expectedError
	}).Build()
	defer mock2.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{"test document"}
	highlights, scores, err := highlight.processOneQuery(ctx, "test query", data)

	s.Error(err)
	s.Nil(highlights)
	s.Nil(scores)
	s.Equal(expectedError, err)
}

func (s *SemanticHighlightSuite) TestProcess_Success() {
	queries := []string{"machine learning", "deep learning"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	expectedHighlights1 := [][]string{
		{"machine learning", "deep learning"},
	}
	expectedScores1 := [][]float32{
		{0.90},
	}
	expectedHighlights2 := [][]string{
		{"deep learning", "machine learning"},
	}
	expectedScores2 := [][]float32{
		{0.85},
	}

	callCount := 0
	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*zilliz.ZillizClient).Highlight).To(func(_ *zilliz.ZillizClient, _ context.Context, query string, _ []string, _ map[string]string) ([][]string, [][]float32, error) {
		callCount++
		if query == "machine learning" {
			return expectedHighlights1, expectedScores1, nil
		}
		return expectedHighlights2, expectedScores2, nil
	}).Build()
	defer mock2.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{"Machine learning document", "Deep learning document"}
	highlights, scores, err := highlight.Process(ctx, []int64{1, 1}, data)

	s.NoError(err)
	s.NotNil(highlights)
	s.Equal(2, callCount, "Should call highlight twice for two queries")
	s.NotNil(scores)
	s.Equal(2, len(scores))
	s.Equal(1, len(scores[0]))
	s.Equal(1, len(scores[1]))
}

func (s *SemanticHighlightSuite) TestProcess_NqMismatch() {
	queries := []string{"machine learning"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{"test document"}
	highlights, scores, err := highlight.Process(ctx, []int64{1, 1, 1}, data) // nq=3 but queries has only 1

	s.Error(err)
	s.Nil(highlights)
	s.Contains(err.Error(), "nq must equal to queries size")
	s.Nil(scores)
}

func (s *SemanticHighlightSuite) TestProcess_ProviderError() {
	queries := []string{"test query"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	expectedError := errors.New("provider error")

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*zilliz.ZillizClient).Highlight).To(func(_ *zilliz.ZillizClient, _ context.Context, _ string, _ []string, _ map[string]string) ([][]string, [][]float32, error) {
		return nil, nil, expectedError
	}).Build()
	defer mock2.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{"test document"}
	highlights, scores, err := highlight.Process(ctx, []int64{1}, data)

	s.Error(err)
	s.Nil(highlights)
	s.Equal(expectedError, err)
	s.Nil(scores)
}

func (s *SemanticHighlightSuite) TestProcess_EmptyData() {
	queries := []string{"test query", "test query 2", "test query 3"}
	inputFields := []string{"title"}

	queriesJSON, _ := json.Marshal(queries)
	inputFieldsJSON, _ := json.Marshal(inputFields)

	mock1 := mockey.Mock(zilliz.NewZilliClient).To(func(_ string, _ string, _ string, _ map[string]string) (*zilliz.ZillizClient, error) {
		return &zilliz.ZillizClient{}, nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*zilliz.ZillizClient).Highlight).To(func(_ *zilliz.ZillizClient, _ context.Context, _ string, texts []string, _ map[string]string) ([][]string, [][]float32, error) {
		scores := make([][]float32, len(texts))
		for i := range texts {
			scores[i] = []float32{0.75}
		}
		return [][]string{texts}, scores, nil
	}).Build()
	defer mock2.UnPatch()

	params := []*commonpb.KeyValuePair{
		{Key: queryKeyName, Value: string(queriesJSON)},
		{Key: inputFieldKeyName, Value: string(inputFieldsJSON)},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
	}

	conf := map[string]string{
		"endpoint": "localhost:8080",
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	highlight, err := NewSemanticHighlight(s.schema, params, conf, extraInfo)
	s.NoError(err)

	ctx := context.Background()
	data := []string{}
	highlights, scores, err := highlight.Process(ctx, []int64{0, 0, 0}, data)

	s.NoError(err)
	s.NotNil(highlights)
	s.Equal(0, len(highlights))
	s.NotNil(scores)
	s.Equal(0, len(scores))

	data2 := []string{"test document"}

	highlights2, scores2, err := highlight.Process(ctx, []int64{0, 1, 0}, data2)

	s.NoError(err)
	s.Equal(1, len(highlights2))
	s.Equal([][]string{{"test document"}}, highlights2)
	s.NotNil(scores2)
	s.Equal(1, len(scores2))
	s.Equal(1, len(scores2[0]))
	s.Equal(float32(0.75), scores2[0][0])
}

func (s *SemanticHighlightSuite) TestBaseSemanticHighlightProvider_MaxBatch() {
	provider := &baseSemanticHighlightProvider{batchSize: 128}
	s.Equal(128, provider.maxBatch())

	provider2 := &baseSemanticHighlightProvider{batchSize: 32}
	s.Equal(32, provider2.maxBatch())
}
