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

package zilliz

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/pkg/v2/proto/modelservicepb"
)

const bufSize = 1024 * 1024

// Mock server for testing
type mockTextEmbeddingServer struct {
	modelservicepb.UnimplementedTextEmbeddingServiceServer
	response *modelservicepb.TextEmbeddingResponse
	err      error
}

func (m *mockTextEmbeddingServer) Embedding(ctx context.Context, req *modelservicepb.TextEmbeddingRequest) (*modelservicepb.TextEmbeddingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

type mockRerankServer struct {
	modelservicepb.UnimplementedRerankServiceServer
	response *modelservicepb.TextRerankResponse
	err      error
}

func (m *mockRerankServer) Rerank(ctx context.Context, req *modelservicepb.TextRerankRequest) (*modelservicepb.TextRerankResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

type mockHighlightServer struct {
	modelservicepb.UnimplementedHighlightServiceServer
	response *modelservicepb.HighlightResponse
	err      error
}

func (m *mockHighlightServer) Highlight(ctx context.Context, req *modelservicepb.HighlightRequest) (*modelservicepb.HighlightResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		expectError bool
		expected    *clientConfig
	}{
		{
			name: "valid config without TLS",
			config: map[string]string{
				"endpoint": "localhost:8080",
			},
			expectError: false,
			expected: &clientConfig{
				endpoint:           "localhost:8080",
				enableTLS:          false,
				caPemPath:          "",
				serverNameOverride: "",
				MaxRecvMsgSize:     1024 * 1024 * 100,
				MaxSendMsgSize:     1024 * 1024 * 100,
				Timeout:            10 * time.Second,
				KeepAliveTime:      30 * time.Second,
			},
		},
		{
			name: "valid config with TLS",
			config: map[string]string{
				"endpoint":           "localhost:8080",
				"enableTLS":          "true",
				"certFile":           "/path/to/cert.pem",
				"serverNameOverride": "example.com",
			},
			expectError: false,
			expected: &clientConfig{
				endpoint:           "localhost:8080",
				enableTLS:          true,
				caPemPath:          "/path/to/cert.pem",
				serverNameOverride: "example.com",
				MaxRecvMsgSize:     1024 * 1024 * 100,
				MaxSendMsgSize:     1024 * 1024 * 100,
				Timeout:            10 * time.Second,
				KeepAliveTime:      30 * time.Second,
			},
		},
		{
			name: "missing endpoint",
			config: map[string]string{
				"enableTLS": "false",
			},
			expectError: true,
		},
		{
			name: "invalid enableTLS value",
			config: map[string]string{
				"endpoint":  "localhost:8080",
				"enableTLS": "invalid",
			},
			expectError: true,
		},
		{
			name: "enableTLS false string",
			config: map[string]string{
				"endpoint":  "localhost:8080",
				"enableTLS": "false",
			},
			expectError: false,
			expected: &clientConfig{
				endpoint:           "localhost:8080",
				enableTLS:          false,
				caPemPath:          "",
				serverNameOverride: "",
				MaxRecvMsgSize:     1024 * 1024 * 100,
				MaxSendMsgSize:     1024 * 1024 * 100,
				Timeout:            10 * time.Second,
				KeepAliveTime:      30 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := loadConfig(tt.config)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, config)
			}
		})
	}
}

func TestDefaultClientConfig(t *testing.T) {
	endpoint := "localhost:8080"
	enableTLS := true
	caPemPath := "/path/to/cert.pem"
	serverNameOverride := "example.com"

	config := defaultClientConfig(endpoint, enableTLS, caPemPath, serverNameOverride)

	assert.Equal(t, endpoint, config.endpoint)
	assert.Equal(t, enableTLS, config.enableTLS)
	assert.Equal(t, caPemPath, config.caPemPath)
	assert.Equal(t, serverNameOverride, config.serverNameOverride)
	assert.Equal(t, 1024*1024*100, config.MaxRecvMsgSize)
	assert.Equal(t, 1024*1024*100, config.MaxSendMsgSize)
	assert.Equal(t, 10*time.Second, config.Timeout)
	assert.Equal(t, 30*time.Second, config.KeepAliveTime)
}

func TestClientManager_GetConn(t *testing.T) {
	manager := &clientManager{}

	// Test that manager initializes properly
	t.Run("config validation", func(t *testing.T) {
		assert.NotNil(t, manager)
		assert.Nil(t, manager.conn)
		assert.Nil(t, manager.config)
	})

	t.Run("close connection", func(t *testing.T) {
		err := manager.Close()
		assert.NoError(t, err)
	})
}

func TestGetClientManager(t *testing.T) {
	// Test singleton pattern
	manager1 := getClientManager()
	manager2 := getClientManager()

	assert.NotNil(t, manager1)
	assert.Equal(t, manager1, manager2)
}

func setupMockServer(t *testing.T) (*grpc.Server, *bufconn.Listener, func(context.Context, string) (net.Conn, error)) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	return s, lis, dialer
}

func TestZillizClient_setMeta(t *testing.T) {
	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
	}

	ctx := context.Background()
	newCtx := client.setMeta(ctx)

	md, ok := metadata.FromOutgoingContext(newCtx)
	require.True(t, ok)

	assert.Equal(t, []string{"test-cluster"}, md.Get("instance-id"))
	assert.Equal(t, []string{"test-deployment"}, md.Get("model-deployment-id"))
}

func TestZillizClient_Embedding(t *testing.T) {
	// Setup mock server
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	// Create test embedding data
	embeddingData1 := make([]byte, 8)                              // 2 float32 values
	binary.LittleEndian.PutUint32(embeddingData1[0:4], 0x3f800000) // 1.0
	binary.LittleEndian.PutUint32(embeddingData1[4:8], 0x40000000) // 2.0

	embeddingData2 := make([]byte, 8)                              // 2 float32 values
	binary.LittleEndian.PutUint32(embeddingData2[0:4], 0x40400000) // 3.0
	binary.LittleEndian.PutUint32(embeddingData2[4:8], 0x40800000) // 4.0

	mockServer := &mockTextEmbeddingServer{
		response: &modelservicepb.TextEmbeddingResponse{
			Status: &modelservicepb.Status{Code: 0, Msg: "success"},
			Results: []*modelservicepb.EmbeddingResult{
				{
					Dense: &modelservicepb.DenseVector{
						Dtype: modelservicepb.DenseVector_DTYPE_FLOAT,
						Data:  embeddingData1,
						Dim:   2,
					},
				},
				{
					Dense: &modelservicepb.DenseVector{
						Dtype: modelservicepb.DenseVector_DTYPE_FLOAT,
						Data:  embeddingData2,
						Dim:   2,
					},
				},
			},
		},
	}

	modelservicepb.RegisterTextEmbeddingServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test successful embedding
	ctx := context.Background()
	texts := []string{"hello", "world"}
	params := map[string]string{"param1": "value1"}

	embeddings, err := client.Embedding(ctx, texts, params)
	assert.NoError(t, err)
	assert.Len(t, embeddings, 2)
	assert.Equal(t, []float32{1.0, 2.0}, embeddings[0])
	assert.Equal(t, []float32{3.0, 4.0}, embeddings[1])
}

func TestZillizClient_Embedding_Error(t *testing.T) {
	// Setup mock server with error
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockTextEmbeddingServer{
		err: assert.AnError,
	}

	modelservicepb.RegisterTextEmbeddingServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test embedding with error
	ctx := context.Background()
	texts := []string{"hello", "world"}
	params := map[string]string{"param1": "value1"}

	embeddings, err := client.Embedding(ctx, texts, params)
	assert.Error(t, err)
	assert.Nil(t, embeddings)
}

func TestZillizClient_Rerank(t *testing.T) {
	// Setup mock server
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockRerankServer{
		response: &modelservicepb.TextRerankResponse{
			Status: &modelservicepb.Status{Code: 0, Msg: "success"},
			Scores: []float32{0.9, 0.7, 0.5},
		},
	}

	modelservicepb.RegisterRerankServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test successful rerank
	ctx := context.Background()
	query := "test query"
	texts := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}

	scores, err := client.Rerank(ctx, query, texts, params)
	assert.NoError(t, err)
	assert.Equal(t, []float32{0.9, 0.7, 0.5}, scores)
}

func TestZillizClient_Rerank_Error(t *testing.T) {
	// Setup mock server with error
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockRerankServer{
		err: assert.AnError,
	}

	modelservicepb.RegisterRerankServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test rerank with error
	ctx := context.Background()
	query := "test query"
	texts := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}

	scores, err := client.Rerank(ctx, query, texts, params)
	assert.Error(t, err)
	assert.Nil(t, scores)
}

func TestNewZilliClient(t *testing.T) {
	tests := []struct {
		name        string
		info        map[string]string
		expectError bool
	}{
		{
			name: "missing endpoint",
			info: map[string]string{
				"enableTLS": "false",
			},
			expectError: true,
		},
		{
			name: "invalid enableTLS",
			info: map[string]string{
				"endpoint":  "localhost:8080",
				"enableTLS": "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewZilliClient("test-deployment", "test-cluster", "test-db", tt.info)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

func TestNewZilliClient_WithMockServer(t *testing.T) {
	// Setup mock server for successful connection test
	s, lis, _ := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// We need to test the client creation with a working connection
	// Since NewZilliClient uses the global client manager, we need to test it differently
	t.Run("valid config with mock server", func(t *testing.T) {
		// Test config parsing without actual connection
		info := map[string]string{
			"endpoint": "bufnet",
		}

		// This will create the client but won't actually connect until first RPC
		client, err := NewZilliClient("test-deployment", "test-cluster", "test-db", info)
		// The client creation should succeed even if connection fails
		// because grpc.NewClient creates lazy connections
		if err != nil {
			// Connection error is expected since we can't easily mock the global client manager
			assert.Contains(t, err.Error(), "Connect model serving failed")
		} else {
			assert.NotNil(t, client)
			assert.Equal(t, "test-deployment", client.modelDeploymentID)
			assert.Equal(t, "test-cluster", client.clusterID)
		}
	})
}

// Test edge cases and error conditions
func TestZillizClient_Embedding_EmptyResponse(t *testing.T) {
	// Setup mock server with empty results
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockTextEmbeddingServer{
		response: &modelservicepb.TextEmbeddingResponse{
			Status:  &modelservicepb.Status{Code: 0, Msg: "success"},
			Results: []*modelservicepb.EmbeddingResult{}, // Empty results
		},
	}

	modelservicepb.RegisterTextEmbeddingServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test embedding with empty response
	ctx := context.Background()
	texts := []string{"hello"}
	params := map[string]string{}

	embeddings, err := client.Embedding(ctx, texts, params)
	assert.NoError(t, err)
	assert.Empty(t, embeddings)
}

func TestZillizClient_Highlight(t *testing.T) {
	// Setup mock server
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockHighlightServer{
		response: &modelservicepb.HighlightResponse{
			Status: &modelservicepb.Status{Code: 0, Msg: "success"},
			Results: []*modelservicepb.HighlightResult{
				{
					Sentences: []string{"highlight1", "highlight2"},
					Scores:    []float32{0.9, 0.8},
				},
				{
					Sentences: []string{"highlight3", "highlight4"},
					Scores:    []float32{0.8, 0.7},
				},
			},
		},
	}

	modelservicepb.RegisterHighlightServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test successful highlight
	ctx := context.Background()
	query := "test query"
	texts := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}
	highlights, scores, err := client.Highlight(ctx, query, texts, params)
	assert.NoError(t, err)
	assert.Equal(t, [][]string{{"highlight1", "highlight2"}, {"highlight3", "highlight4"}}, highlights)
	assert.Equal(t, [][]float32{{0.9, 0.8}, {0.8, 0.7}}, scores)
}

func TestZillizClient_Highlight_Error(t *testing.T) {
	// Setup mock server with error
	s, lis, dialer := setupMockServer(t)
	defer lis.Close()
	defer s.Stop()

	mockServer := &mockHighlightServer{
		err: assert.AnError,
	}

	modelservicepb.RegisterHighlightServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v\n", err)
		}
	}()

	// Create connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &ZillizClient{
		modelDeploymentID: "test-deployment",
		clusterID:         "test-cluster",
		conn:              conn,
	}

	// Test highlight with error
	ctx := context.Background()
	query := "test query"
	texts := []string{"doc1", "doc2", "doc3"}
	params := map[string]string{"param1": "value1"}
	highlights, scores, err := client.Highlight(ctx, query, texts, params)
	assert.Error(t, err)
	assert.Nil(t, highlights)
	assert.Nil(t, scores)
}

func TestZillizClient_Highlight_MismatchLength(t *testing.T) {
	tests := []struct {
		name           string
		response       *modelservicepb.HighlightResponse
		expectedErrMsg string
	}{
		{
			name: "more sentences than scores",
			response: &modelservicepb.HighlightResponse{
				Status: &modelservicepb.Status{Code: 0, Msg: "success"},
				Results: []*modelservicepb.HighlightResult{
					{
						Sentences: []string{"highlight1", "highlight2", "highlight3"},
						Scores:    []float32{0.9, 0.8},
					},
				},
			},
			expectedErrMsg: "sentences length 3 does not match scores length 2",
		},
		{
			name: "more scores than sentences",
			response: &modelservicepb.HighlightResponse{
				Status: &modelservicepb.Status{Code: 0, Msg: "success"},
				Results: []*modelservicepb.HighlightResult{
					{
						Sentences: []string{"highlight1"},
						Scores:    []float32{0.9, 0.8, 0.7},
					},
				},
			},
			expectedErrMsg: "sentences length 1 does not match scores length 3",
		},
		{
			name: "mismatch in second result",
			response: &modelservicepb.HighlightResponse{
				Status: &modelservicepb.Status{Code: 0, Msg: "success"},
				Results: []*modelservicepb.HighlightResult{
					{
						Sentences: []string{"highlight1", "highlight2"},
						Scores:    []float32{0.9, 0.8},
					},
					{
						Sentences: []string{"highlight3"},
						Scores:    []float32{0.7, 0.6},
					},
				},
			},
			expectedErrMsg: "sentences length 1 does not match scores length 2",
		},
		{
			name: "nil sentences",
			response: &modelservicepb.HighlightResponse{
				Status: &modelservicepb.Status{Code: 0, Msg: "success"},
				Results: []*modelservicepb.HighlightResult{
					{},
					{
						Sentences: []string{"highlight3"},
						Scores:    []float32{0.7, 0.6},
					},
				},
			},
			expectedErrMsg: "sentences length 1 does not match scores length 2",
		},
		{
			name: "nil scores",
			response: &modelservicepb.HighlightResponse{
				Status: &modelservicepb.Status{Code: 0, Msg: "success"},
				Results: []*modelservicepb.HighlightResult{
					{
						Sentences: []string{"highlight1", "highlight2"},
						Scores:    nil,
					},
				},
			},
			expectedErrMsg: "sentences length 2 does not match scores length 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, lis, dialer := setupMockServer(t)
			defer lis.Close()
			defer s.Stop()

			mockServer := &mockHighlightServer{
				response: tt.response,
			}

			modelservicepb.RegisterHighlightServiceServer(s, mockServer)

			go func() {
				if err := s.Serve(lis); err != nil {
					fmt.Printf("Server exited with error: %v\n", err)
				}
			}()

			conn, err := grpc.DialContext(
				context.Background(),
				"bufnet",
				grpc.WithContextDialer(dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			defer conn.Close()

			client := &ZillizClient{
				modelDeploymentID: "test-deployment",
				clusterID:         "test-cluster",
				conn:              conn,
			}

			ctx := context.Background()
			highlights, scores, err := client.Highlight(ctx, "test query", []string{"doc1"}, map[string]string{})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErrMsg)
			assert.Nil(t, highlights)
			assert.Nil(t, scores)
		})
	}
}
