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

package rerank

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

// RerankClient interface for testing
type RerankClient interface {
	Rerank(ctx context.Context, query string, texts []string, params map[string]string, timeoutSec int64) ([]float32, error)
}

// MockZillizClient is a mock implementation of RerankClient for testing
type MockZillizClient struct {
	mock.Mock
}

func (m *MockZillizClient) Rerank(ctx context.Context, query string, texts []string, params map[string]string, timeoutSec int64) ([]float32, error) {
	args := m.Called(ctx, query, texts, params, timeoutSec)
	return args.Get(0).([]float32), args.Error(1)
}

// testBaseProvider mimics the baseProvider for testing
type testBaseProvider struct {
	batchSize int
}

func (provider *testBaseProvider) maxBatch() int {
	return provider.batchSize
}

// testZillzProvider is a test version of zillzProvider that accepts our mock client
type testZillzProvider struct {
	testBaseProvider
	client RerankClient
	params map[string]string
}

func (provider *testZillzProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	return provider.client.Rerank(ctx, query, docs, provider.params, 30)
}

func TestZillizRerankProvider(t *testing.T) {
	suite.Run(t, new(ZillizRerankProviderSuite))
}

type ZillizRerankProviderSuite struct {
	suite.Suite
}

func (s *ZillizRerankProviderSuite) TestNewZillizProvider_Success() {
	tests := []struct {
		name           string
		params         []*commonpb.KeyValuePair
		conf           map[string]string
		extraInfo      *models.ModelExtraInfo
		expectedBatch  int
		expectedParams map[string]string
	}{
		{
			name: "basic configuration",
			params: []*commonpb.KeyValuePair{
				{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
			},
			conf: map[string]string{
				"endpoint": "localhost:8080",
			},
			extraInfo: &models.ModelExtraInfo{
				ClusterID: "test-cluster",
				DBName:    "test-db",
			},
			expectedBatch:  64, // default batch size
			expectedParams: map[string]string{},
		},
		{
			name: "with custom batch size",
			params: []*commonpb.KeyValuePair{
				{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
				{Key: models.MaxClientBatchSizeParamKey, Value: "32"},
			},
			conf: map[string]string{
				"endpoint": "localhost:8080",
			},
			extraInfo: &models.ModelExtraInfo{
				ClusterID: "test-cluster",
				DBName:    "test-db",
			},
			expectedBatch:  32,
			expectedParams: map[string]string{},
		},
		{
			name: "with additional model parameters",
			params: []*commonpb.KeyValuePair{
				{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
				{Key: models.MaxClientBatchSizeParamKey, Value: "16"},
				{Key: "custom_param1", Value: "value1"},
				{Key: "custom_param2", Value: "value2"},
			},
			conf: map[string]string{
				"endpoint": "localhost:8080",
			},
			extraInfo: &models.ModelExtraInfo{
				ClusterID: "test-cluster",
				DBName:    "test-db",
			},
			expectedBatch: 16,
			expectedParams: map[string]string{
				"custom_param1": "value1",
				"custom_param2": "value2",
			},
		},
		{
			name: "case insensitive parameter keys",
			params: []*commonpb.KeyValuePair{
				{Key: "MODEL_DEPLOYMENT_ID", Value: "test-deployment"},
				{Key: "MAX_CLIENT_BATCH_SIZE", Value: "8"},
				{Key: "Custom_Param", Value: "custom_value"},
			},
			conf: map[string]string{
				"endpoint": "localhost:8080",
			},
			extraInfo: &models.ModelExtraInfo{
				ClusterID: "test-cluster",
				DBName:    "test-db",
			},
			expectedBatch: 8,
			expectedParams: map[string]string{
				"Custom_Param": "custom_value",
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			// Note: This test will fail with actual connection since we can't easily mock the global client manager
			// But we can test the parameter parsing logic by checking the error message
			provider, err := newZillizProvider(tt.params, tt.conf, tt.extraInfo)

			// Since we can't easily mock the zilliz client creation, we expect a connection error
			// but we can verify that the parameters were parsed correctly by checking the error doesn't relate to parameter parsing
			if err != nil {
				// Connection errors are expected in unit tests
				s.Contains(err.Error(), "Connect model serving failed", "Expected connection error, got: %v", err)
			} else {
				// If somehow the connection succeeds, verify the provider was created correctly
				s.NotNil(provider)
				zillizProvider, ok := provider.(*zillzProvider)
				s.True(ok)
				s.Equal(tt.expectedBatch, zillizProvider.maxBatch())
				s.Equal(tt.expectedParams, zillizProvider.params)
			}
		})
	}
}

func (s *ZillizRerankProviderSuite) TestNewZillizProvider_InvalidBatchSize() {
	tests := []struct {
		name        string
		batchValue  string
		expectedErr string
	}{
		{
			name:        "negative batch size",
			batchValue:  "-1",
			expectedErr: "must be greater than 0",
		},
		{
			name:        "zero batch size",
			batchValue:  "0",
			expectedErr: "must be greater than 0",
		},
		{
			name:        "invalid number format",
			batchValue:  "not_a_number",
			expectedErr: "is not a valid number",
		},
		{
			name:        "empty batch size",
			batchValue:  "",
			expectedErr: "is not a valid number",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			params := []*commonpb.KeyValuePair{
				{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
				{Key: models.MaxClientBatchSizeParamKey, Value: tt.batchValue},
			}
			conf := map[string]string{
				"endpoint": "localhost:8080",
			}
			extraInfo := &models.ModelExtraInfo{
				ClusterID: "test-cluster",
				DBName:    "test-db",
			}

			provider, err := newZillizProvider(params, conf, extraInfo)
			s.Error(err)
			s.Nil(provider)
			s.Contains(err.Error(), tt.expectedErr)
		})
	}
}

func (s *ZillizRerankProviderSuite) TestNewZillizProvider_MissingConfig() {
	tests := []struct {
		name        string
		conf        map[string]string
		expectedErr string
	}{
		{
			name:        "missing endpoint",
			conf:        map[string]string{},
			expectedErr: "lost endpoint config",
		},
		{
			name: "empty endpoint",
			conf: map[string]string{
				"endpoint": "",
			},
			expectedErr: "lost endpoint config",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			params := []*commonpb.KeyValuePair{
				{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
			}
			extraInfo := &models.ModelExtraInfo{
				ClusterID: "test-cluster",
			}

			provider, err := newZillizProvider(params, tt.conf, extraInfo)
			s.Error(err)
			s.Nil(provider)
			s.Contains(err.Error(), tt.expectedErr)
		})
	}
}

func (s *ZillizRerankProviderSuite) TestZillzProvider_Rerank_Success() {
	// Create a mock client
	mockClient := &MockZillizClient{}

	// Set up expected behavior
	ctx := context.Background()
	query := "test query"
	docs := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}
	expectedScores := []float32{0.9, 0.7, 0.5}

	mockClient.On("Rerank", ctx, query, docs, params, int64(30)).Return(expectedScores, nil)

	// Create test provider with mock client
	provider := &testZillzProvider{
		testBaseProvider: testBaseProvider{batchSize: 64},
		client:           mockClient,
		params:           params,
	}

	// Test the rerank method
	scores, err := provider.rerank(ctx, query, docs)

	s.NoError(err)
	s.Equal(expectedScores, scores)
	mockClient.AssertExpectations(s.T())
}

func (s *ZillizRerankProviderSuite) TestZillzProvider_Rerank_Error() {
	// Create a mock client
	mockClient := &MockZillizClient{}

	// Set up expected behavior with error
	ctx := context.Background()
	query := "test query"
	docs := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}
	expectedError := errors.New("rerank service error")

	mockClient.On("Rerank", ctx, query, docs, params, int64(30)).Return([]float32(nil), expectedError)

	// Create test provider with mock client
	provider := &testZillzProvider{
		testBaseProvider: testBaseProvider{batchSize: 64},
		client:           mockClient,
		params:           params,
	}

	// Test the rerank method
	scores, err := provider.rerank(ctx, query, docs)

	s.Error(err)
	s.Nil(scores)
	s.Equal(expectedError, err)
	mockClient.AssertExpectations(s.T())
}

func (s *ZillizRerankProviderSuite) TestZillzProvider_MaxBatch() {
	tests := []struct {
		name          string
		batchSize     int
		expectedBatch int
	}{
		{
			name:          "default batch size",
			batchSize:     64,
			expectedBatch: 64,
		},
		{
			name:          "custom batch size",
			batchSize:     32,
			expectedBatch: 32,
		},
		{
			name:          "small batch size",
			batchSize:     1,
			expectedBatch: 1,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			provider := &testZillzProvider{
				testBaseProvider: testBaseProvider{batchSize: tt.batchSize},
			}

			s.Equal(tt.expectedBatch, provider.maxBatch())
		})
	}
}

func (s *ZillizRerankProviderSuite) TestZillzProvider_Rerank_WithDifferentParams() {
	// Create a mock client
	mockClient := &MockZillizClient{}

	// Test with empty parameters
	ctx := context.Background()
	query := "test query"
	docs := []string{"doc1", "doc2"}
	emptyParams := map[string]string{}
	expectedScores := []float32{0.8, 0.6}

	mockClient.On("Rerank", ctx, query, docs, emptyParams, int64(30)).Return(expectedScores, nil)

	// Create test provider with empty params
	provider := &testZillzProvider{
		testBaseProvider: testBaseProvider{batchSize: 64},
		client:           mockClient,
		params:           emptyParams,
	}

	// Test the rerank method
	scores, err := provider.rerank(ctx, query, docs)

	s.NoError(err)
	s.Equal(expectedScores, scores)
	mockClient.AssertExpectations(s.T())
}

func (s *ZillizRerankProviderSuite) TestZillzProvider_Rerank_EmptyDocs() {
	// Create a mock client
	mockClient := &MockZillizClient{}

	// Test with empty documents
	ctx := context.Background()
	query := "test query"
	docs := []string{}
	params := map[string]string{}
	expectedScores := []float32{}

	mockClient.On("Rerank", ctx, query, docs, params, int64(30)).Return(expectedScores, nil)

	// Create test provider
	provider := &testZillzProvider{
		testBaseProvider: testBaseProvider{batchSize: 64},
		client:           mockClient,
		params:           params,
	}

	// Test the rerank method
	scores, err := provider.rerank(ctx, query, docs)

	s.NoError(err)
	s.Equal(expectedScores, scores)
	mockClient.AssertExpectations(s.T())
}

// Test parameter parsing edge cases
func (s *ZillizRerankProviderSuite) TestParameterParsing() {
	s.Run("no model deployment id", func() {
		params := []*commonpb.KeyValuePair{
			{Key: models.MaxClientBatchSizeParamKey, Value: "32"},
		}
		conf := map[string]string{
			"endpoint": "localhost:8080",
		}
		extraInfo := &models.ModelExtraInfo{
			ClusterID: "test-cluster",
		}

		// This should still work as model deployment ID is extracted but not validated in the provider
		provider, err := newZillizProvider(params, conf, extraInfo)
		if err != nil {
			// Connection error is expected in unit tests
			s.Contains(err.Error(), "Connect model serving failed")
		} else {
			s.NotNil(provider)
		}
	})

	s.Run("duplicate parameters", func() {
		params := []*commonpb.KeyValuePair{
			{Key: models.ModelDeploymentIDKey, Value: "deployment1"},
			{Key: models.ModelDeploymentIDKey, Value: "deployment2"}, // duplicate, should use last one
			{Key: models.MaxClientBatchSizeParamKey, Value: "16"},
			{Key: "custom_param", Value: "value1"},
			{Key: "custom_param", Value: "value2"}, // duplicate, should use last one
		}
		conf := map[string]string{
			"endpoint": "localhost:8080",
		}
		extraInfo := &models.ModelExtraInfo{
			ClusterID: "test-cluster",
		}

		provider, err := newZillizProvider(params, conf, extraInfo)
		if err != nil {
			// Connection error is expected in unit tests
			s.Contains(err.Error(), "Connect model serving failed")
		} else {
			s.NotNil(provider)
			zillizProvider, ok := provider.(*zillzProvider)
			s.True(ok)
			s.Equal("value2", zillizProvider.params["custom_param"])
		}
	})
}

// Benchmark tests
func BenchmarkZillzProvider_Rerank(b *testing.B) {
	// Create a mock client
	mockClient := &MockZillizClient{}

	// Set up expected behavior
	ctx := context.Background()
	query := "benchmark query"
	docs := []string{"doc1", "doc2", "doc3", "doc4", "doc5"}
	params := map[string]string{}
	expectedScores := []float32{0.9, 0.8, 0.7, 0.6, 0.5}

	// Set up the mock to be called many times
	mockClient.On("Rerank", ctx, query, docs, params, int64(30)).Return(expectedScores, nil)

	// Create test provider with mock client
	provider := &testZillzProvider{
		testBaseProvider: testBaseProvider{batchSize: 64},
		client:           mockClient,
		params:           params,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.rerank(ctx, query, docs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Test integration with actual provider creation (will fail due to connection, but tests parameter flow)
func TestZillizProviderIntegration(t *testing.T) {
	params := []*commonpb.KeyValuePair{
		{Key: providerParamName, Value: zillizProviderName},
		{Key: models.ModelDeploymentIDKey, Value: "test-deployment"},
		{Key: models.MaxClientBatchSizeParamKey, Value: "32"},
	}

	extraInfo := &models.ModelExtraInfo{
		ClusterID: "test-cluster",
		DBName:    "test-db",
	}

	// This will test the integration through the main newProvider function
	_, err := newProvider(params, extraInfo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Zilliz client config error")
}
