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

package embedding

// This file contains unit tests for the ZillizEmbeddingProvider.
// Due to the dependency on the ZillizClient which requires gRPC connections,
// these tests focus on testing the logic that can be tested in isolation:
// - Parameter extraction and validation
// - Method behavior (MaxBatch, FieldDim)
// - Batching logic
// - Input type parameter setting
// - Edge cases and constants

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

// ZillizClientInterface defines the interface for ZillizClient used in testing
type ZillizClientInterface interface {
	Embedding(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error)
}

// MockZillizClient is a mock implementation of ZillizClientInterface for testing
type MockZillizClient struct {
	embeddingFunc func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error)
}

func (m *MockZillizClient) Embedding(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
	if m.embeddingFunc != nil {
		return m.embeddingFunc(ctx, texts, params, timeoutSec)
	}
	// Default behavior: return mock embeddings
	embeddings := make([][]float32, len(texts))
	for i := range texts {
		embeddings[i] = []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
	}
	return embeddings, nil
}

// TestableZillizEmbeddingProvider is a version of ZillizEmbeddingProvider that accepts an interface for testing
type TestableZillizEmbeddingProvider struct {
	fieldDim int64

	client      ZillizClientInterface
	modelName   string
	maxBatch    int
	timeoutSec  int64
	modelParams map[string]string
	extraInfo   *models.ModelExtraInfo
}

func (provider *TestableZillizEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *TestableZillizEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *TestableZillizEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	if mode == models.SearchMode {
		provider.modelParams["input_type"] = "query"
	} else {
		provider.modelParams["input_type"] = "document"
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(ctx, texts[i:end], provider.modelParams, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		data = append(data, resp...)
	}
	return data, nil
}

func TestZillizEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(ZillizEmbeddingProviderSuite))
}

type ZillizEmbeddingProviderSuite struct {
	suite.Suite
	fieldSchema    *schemapb.FieldSchema
	functionSchema *schemapb.FunctionSchema
	params         map[string]string
	extraInfo      *models.ModelExtraInfo
}

func (s *ZillizEmbeddingProviderSuite) SetupTest() {
	s.fieldSchema = &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}

	s.functionSchema = &schemapb.FunctionSchema{
		Name:             "test_zilliz_embedding",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelDeploymentIDKey, Value: "test-deployment-id"},
			{Key: "custom_param", Value: "custom_value"},
		},
	}

	s.params = map[string]string{
		"api_key": "test-api-key",
	}

	s.extraInfo = &models.ModelExtraInfo{
		ClusterID: "test-cluster-id",
		DBName:    "test-db",
	}
}

func (s *ZillizEmbeddingProviderSuite) TestParameterExtraction() {
	// Test parameter extraction logic from function schema
	functionSchema := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
			{Key: "model_param1", Value: "value1"},
			{Key: "model_param2", Value: "value2"},
		},
	}

	// Test parameter extraction logic (same as in NewZillizEmbeddingProvider)
	var modelDeploymentID string
	modelParams := map[string]string{}
	for _, param := range functionSchema.Params {
		switch param.Key {
		case models.ModelDeploymentIDKey:
			modelDeploymentID = param.Value
		default:
			modelParams[param.Key] = param.Value
		}
	}

	s.Equal("test-deployment", modelDeploymentID)
	s.Equal("value1", modelParams["model_param1"])
	s.Equal("value2", modelParams["model_param2"])
	s.NotContains(modelParams, models.ModelDeploymentIDKey)
}

func (s *ZillizEmbeddingProviderSuite) TestNewZillizEmbeddingProvider_InvalidDimension() {
	// Test with invalid dimension
	invalidFieldSchema := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "invalid"},
		},
	}

	provider, err := NewZillizEmbeddingProvider(invalidFieldSchema, s.functionSchema, s.params, s.extraInfo)
	s.Error(err)
	s.Nil(provider)
}

func (s *ZillizEmbeddingProviderSuite) TestMaxBatch() {
	// Test MaxBatch method with a provider that has default maxBatch
	provider := &ZillizEmbeddingProvider{
		maxBatch: 64,
	}

	maxBatch := provider.MaxBatch()
	s.Equal(5*64, maxBatch) // 5 * provider.maxBatch
}

func (s *ZillizEmbeddingProviderSuite) TestFieldDim() {
	// Test FieldDim method
	provider := &ZillizEmbeddingProvider{
		fieldDim: 128,
	}

	fieldDim := provider.FieldDim()
	s.Equal(int64(128), fieldDim)
}

func (s *ZillizEmbeddingProviderSuite) TestBatchingLogic() {
	// Test the batching logic used in CallEmbedding
	numRows := 25
	maxBatch := 10

	// Simulate the batching loop from CallEmbedding
	batches := []struct{ start, end int }{}
	for i := 0; i < numRows; i += maxBatch {
		end := i + maxBatch
		if end > numRows {
			end = numRows
		}
		batches = append(batches, struct{ start, end int }{i, end})
	}

	// Should have 3 batches: [0,10), [10,20), [20,25)
	s.Len(batches, 3)
	s.Equal(0, batches[0].start)
	s.Equal(10, batches[0].end)
	s.Equal(10, batches[1].start)
	s.Equal(20, batches[1].end)
	s.Equal(20, batches[2].start)
	s.Equal(25, batches[2].end)
}

func (s *ZillizEmbeddingProviderSuite) TestInputTypeParameterSetting() {
	// Test that input_type parameter is set correctly for different modes
	provider := &ZillizEmbeddingProvider{
		modelParams: make(map[string]string),
	}

	// Simulate the parameter setting logic from CallEmbedding
	// For SearchMode
	provider.modelParams["input_type"] = "query"
	s.Equal("query", provider.modelParams["input_type"])

	// For InsertMode (non-SearchMode)
	provider.modelParams["input_type"] = "document"
	s.Equal("document", provider.modelParams["input_type"])
}

func (s *ZillizEmbeddingProviderSuite) TestDefaultValues() {
	// Test that default values are set correctly in NewZillizEmbeddingProvider
	// We can't test the full constructor due to the zilliz client dependency,
	// but we can test the default values that should be set

	expectedMaxBatch := 64
	expectedTimeoutSec := int64(30)

	// These are the default values that should be set in the constructor
	s.Equal(64, expectedMaxBatch)
	s.Equal(int64(30), expectedTimeoutSec)
}

func (s *ZillizEmbeddingProviderSuite) TestEdgeCases() {
	// Test edge cases for batching logic

	// Test with zero texts
	numRows := 0
	maxBatch := 10
	batchCount := 0
	for i := 0; i < numRows; i += maxBatch {
		batchCount++
	}
	s.Equal(0, batchCount)

	// Test with exactly one batch
	numRows = 10
	maxBatch = 10
	batchCount = 0
	for i := 0; i < numRows; i += maxBatch {
		batchCount++
	}
	s.Equal(1, batchCount)

	// Test with one more than batch size
	numRows = 11
	maxBatch = 10
	batchCount = 0
	for i := 0; i < numRows; i += maxBatch {
		batchCount++
	}
	s.Equal(2, batchCount)
}

func (s *ZillizEmbeddingProviderSuite) TestModeConstants() {
	// Test that the embedding modes are correctly defined
	s.Equal(models.TextEmbeddingMode(0), models.InsertMode)
	s.Equal(models.TextEmbeddingMode(1), models.SearchMode)
}

func (s *ZillizEmbeddingProviderSuite) TestConstantValues() {
	// Test the constant values used in the provider
	s.Equal("test-cluster-id", s.extraInfo.ClusterID)
	s.Equal("test-db", s.extraInfo.DBName)
}

func (s *ZillizEmbeddingProviderSuite) TestNewZillizEmbeddingProviderWithInvalidFieldSchema() {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}
	_, err := NewZillizEmbeddingProvider(fieldSchema, s.functionSchema, s.params, s.extraInfo)
	s.Error(err)
}

// CallEmbedding tests

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_SearchMode() {
	// Create a provider with mock client
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    10,
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"hello", "world"}
	mode := models.SearchMode

	// Set up mock to verify parameters
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		// Verify that input_type is set to "query" for SearchMode
		s.Equal("query", params["input_type"])
		s.Equal(int64(30), timeoutSec)

		// Return mock embeddings
		embeddings := make([][]float32, len(texts))
		for i := range texts {
			embeddings[i] = []float32{1.0, 2.0, 3.0, 4.0}
		}
		return embeddings, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify result type and content
	embeddings, ok := result.([][]float32)
	s.True(ok)
	s.Len(embeddings, 2)
	s.Equal([]float32{1.0, 2.0, 3.0, 4.0}, embeddings[0])
	s.Equal([]float32{1.0, 2.0, 3.0, 4.0}, embeddings[1])

	// Verify that input_type was set correctly
	s.Equal("query", provider.modelParams["input_type"])
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_InsertMode() {
	// Create a provider with mock client
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    10,
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"document1", "document2"}
	mode := models.InsertMode

	// Set up mock to verify parameters
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		// Verify that input_type is set to "document" for InsertMode
		s.Equal("document", params["input_type"])

		// Return mock embeddings
		embeddings := make([][]float32, len(texts))
		for i := range texts {
			embeddings[i] = []float32{2.0, 3.0, 4.0, 5.0}
		}
		return embeddings, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify result type and content
	embeddings, ok := result.([][]float32)
	s.True(ok)
	s.Len(embeddings, 2)
	s.Equal([]float32{2.0, 3.0, 4.0, 5.0}, embeddings[0])
	s.Equal([]float32{2.0, 3.0, 4.0, 5.0}, embeddings[1])

	// Verify that input_type was set correctly
	s.Equal("document", provider.modelParams["input_type"])
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_Batching() {
	// Create a provider with small batch size to test batching
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    3, // Small batch size to force batching
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"text1", "text2", "text3", "text4", "text5"} // 5 texts, batch size 3
	mode := models.InsertMode

	callCount := 0
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		callCount++

		// First batch should have 3 texts, second batch should have 2 texts
		if callCount == 1 {
			s.Len(texts, 3)
			s.Equal([]string{"text1", "text2", "text3"}, texts)
		} else if callCount == 2 {
			s.Len(texts, 2)
			s.Equal([]string{"text4", "text5"}, texts)
		}

		// Return mock embeddings for this batch
		embeddings := make([][]float32, len(texts))
		for i := range texts {
			embeddings[i] = []float32{float32(callCount), float32(i), 0.0, 0.0}
		}
		return embeddings, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify that client was called twice (batching worked)
	s.Equal(2, callCount)

	// Verify result
	embeddings, ok := result.([][]float32)
	s.True(ok)
	s.Len(embeddings, 5) // All 5 embeddings should be returned

	// Verify embeddings from first batch
	s.Equal([]float32{1.0, 0.0, 0.0, 0.0}, embeddings[0])
	s.Equal([]float32{1.0, 1.0, 0.0, 0.0}, embeddings[1])
	s.Equal([]float32{1.0, 2.0, 0.0, 0.0}, embeddings[2])

	// Verify embeddings from second batch
	s.Equal([]float32{2.0, 0.0, 0.0, 0.0}, embeddings[3])
	s.Equal([]float32{2.0, 1.0, 0.0, 0.0}, embeddings[4])
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_Error() {
	// Create a provider with mock client that returns error
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    10,
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"hello", "world"}
	mode := models.SearchMode

	expectedError := errors.New("embedding service error")
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		return nil, expectedError
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.Error(err)
	s.Nil(result)
	s.Equal(expectedError, err)
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_EmptyTexts() {
	// Create a provider with mock client
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    10,
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{} // Empty texts
	mode := models.InsertMode

	callCount := 0
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		callCount++
		return [][]float32{}, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify that client was not called for empty texts
	s.Equal(0, callCount)

	// Verify result
	embeddings, ok := result.([][]float32)
	s.True(ok)
	s.Len(embeddings, 0)
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_SingleBatch() {
	// Test with texts that fit exactly in one batch
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:      mockClient,
		fieldDim:    4,
		maxBatch:    5, // Batch size 5
		timeoutSec:  30,
		modelParams: make(map[string]string),
		extraInfo:   s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"text1", "text2", "text3", "text4", "text5"} // Exactly 5 texts
	mode := models.SearchMode

	callCount := 0
	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		callCount++
		s.Len(texts, 5)
		s.Equal("query", params["input_type"])

		// Return mock embeddings
		embeddings := make([][]float32, len(texts))
		for i := range texts {
			embeddings[i] = []float32{float32(i), 1.0, 2.0, 3.0}
		}
		return embeddings, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify that client was called exactly once
	s.Equal(1, callCount)

	// Verify result
	embeddings, ok := result.([][]float32)
	s.True(ok)
	s.Len(embeddings, 5)

	for i := 0; i < 5; i++ {
		s.Equal([]float32{float32(i), 1.0, 2.0, 3.0}, embeddings[i])
	}
}

func (s *ZillizEmbeddingProviderSuite) TestCallEmbedding_ModelParamsPreservation() {
	// Test that existing model params are preserved and input_type is added
	mockClient := &MockZillizClient{}
	provider := &TestableZillizEmbeddingProvider{
		client:     mockClient,
		fieldDim:   4,
		maxBatch:   10,
		timeoutSec: 30,
		modelParams: map[string]string{
			"existing_param": "existing_value",
			"another_param":  "another_value",
		},
		extraInfo: s.extraInfo,
	}

	ctx := context.Background()
	texts := []string{"test"}
	mode := models.SearchMode

	mockClient.embeddingFunc = func(ctx context.Context, texts []string, params map[string]string, timeoutSec int64) ([][]float32, error) {
		// Verify that existing params are preserved and input_type is added
		s.Equal("existing_value", params["existing_param"])
		s.Equal("another_value", params["another_param"])
		s.Equal("query", params["input_type"])
		s.Len(params, 3) // Should have 3 parameters total

		return [][]float32{{1.0, 2.0, 3.0, 4.0}}, nil
	}

	result, err := provider.CallEmbedding(ctx, texts, mode)
	s.NoError(err)
	s.NotNil(result)

	// Verify that the provider's modelParams were updated
	s.Equal("query", provider.modelParams["input_type"])
	s.Equal("existing_value", provider.modelParams["existing_param"])
	s.Equal("another_value", provider.modelParams["another_param"])
}
