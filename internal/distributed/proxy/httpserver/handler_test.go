package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockProxyComponent struct {
	// wrap the interface to avoid implement not used func.
	// and to let not implemented call panics
	// implement the method you want to mock
	types.ProxyComponent
}

func (mockProxyComponent) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, nil
}

var emptyBody = &gin.H{}
var testStatus = &commonpb.Status{Reason: "ok"}

func (mockProxyComponent) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{Status: testStatus}, nil
}

func (mockProxyComponent) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return &milvuspb.GetCollectionStatisticsResponse{Status: testStatus}, nil
}

func (mockProxyComponent) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{Status: testStatus}, nil
}

func (mockProxyComponent) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{Status: testStatus}, nil
}

func (mockProxyComponent) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return testStatus, nil
}
func (mockProxyComponent) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return testStatus, nil
}
func (mockProxyComponent) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return &milvuspb.GetPartitionStatisticsResponse{Status: testStatus}, nil
}

func (mockProxyComponent) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{Status: testStatus}, nil
}

func (mockProxyComponent) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return &milvuspb.DescribeIndexResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return &milvuspb.GetIndexStateResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return &milvuspb.GetIndexBuildProgressResponse{Status: testStatus}, nil
}

func (mockProxyComponent) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	if request.CollectionName == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.MutationResult{Acknowledged: true}, nil
}

func (mockProxyComponent) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	if request.Expr == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.MutationResult{Acknowledged: true}, nil
}

var searchResult = milvuspb.SearchResults{
	Results: &schemapb.SearchResultData{
		TopK: 10,
	},
}

func (mockProxyComponent) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if request.Dsl == "" {
		return nil, errors.New("body parse err")
	}
	return &searchResult, nil
}

var queryResult = milvuspb.QueryResults{
	CollectionName: "test",
}

func (mockProxyComponent) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if request.Expr == "" {
		return nil, errors.New("body parse err")
	}
	return &queryResult, nil
}

var flushResult = milvuspb.FlushResponse{
	DbName: "default",
}

func (mockProxyComponent) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	if len(request.CollectionNames) < 1 {
		return nil, errors.New("body parse err")
	}
	return &flushResult, nil
}

var calcDistanceResult = milvuspb.CalcDistanceResults{
	Array: &milvuspb.CalcDistanceResults_IntDist{
		IntDist: &schemapb.IntArray{
			Data: []int32{1, 2, 3},
		},
	},
}

func (mockProxyComponent) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	if len(request.Params) < 1 {
		return nil, errors.New("body parse err")
	}
	return &calcDistanceResult, nil
}

func (mockProxyComponent) GetFlushState(ctx context.Context, request *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return &milvuspb.GetPersistentSegmentInfoResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return &milvuspb.GetQuerySegmentInfoResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetReplicas(ctx context.Context, request *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return &milvuspb.GetReplicasResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{Status: testStatus}, nil
}

func (mockProxyComponent) LoadBalance(ctx context.Context, request *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) GetCompactionState(ctx context.Context, request *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetCompactionStateWithPlans(ctx context.Context, request *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{Status: testStatus}, nil
}

func (mockProxyComponent) ManualCompaction(ctx context.Context, request *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{Status: testStatus}, nil
}

func (mockProxyComponent) Import(ctx context.Context, request *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return &milvuspb.ImportResponse{Status: testStatus}, nil
}

func (mockProxyComponent) GetImportState(ctx context.Context, request *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return &milvuspb.GetImportStateResponse{Status: testStatus}, nil
}

func (mockProxyComponent) ListImportTasks(ctx context.Context, request *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return &milvuspb.ListImportTasksResponse{Status: testStatus}, nil
}

func (mockProxyComponent) CreateCredential(ctx context.Context, request *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) UpdateCredential(ctx context.Context, request *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) DeleteCredential(ctx context.Context, request *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (mockProxyComponent) ListCredUsers(ctx context.Context, request *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return &milvuspb.ListCredUsersResponse{Status: testStatus}, nil
}

func TestHandlers(t *testing.T) {
	mockProxy := &mockProxyComponent{}
	h := NewHandlers(mockProxy)
	testEngine := gin.New()
	h.RegisterRoutesTo(testEngine)

	t.Run("handleGetHealth default json ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, w.Body.Bytes(), []byte(`{"status":"ok"}`))
	})
	t.Run("handleGetHealth accept yaml ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header = http.Header{
			"Accept": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, w.Body.Bytes(), []byte("status: ok\n"))
	})
	t.Run("handlePostDummy parsejson failed 400", func(t *testing.T) {
		bodyBytes := []byte("---")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy parseyaml failed 400", func(t *testing.T) {
		bodyBytes := []byte("{")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy default json ok", func(t *testing.T) {
		bodyBytes := []byte("")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("handlePostDummy yaml ok", func(t *testing.T) {
		bodyBytes := []byte("")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	type testCase struct {
		httpMethod     string
		path           string
		body           interface{}
		expectedStatus int
		expectedBody   interface{}
	}
	testCases := []testCase{
		{
			http.MethodPost, "/collection", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodDelete, "/collection", emptyBody,
			http.StatusOK, &testStatus,
		},
		{
			http.MethodGet, "/collection/existence", emptyBody,
			http.StatusOK, &milvuspb.BoolResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/collection", emptyBody,
			http.StatusOK, &milvuspb.DescribeCollectionResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/collection/load", emptyBody,
			http.StatusOK, &testStatus,
		},
		{
			http.MethodDelete, "/collection/load", emptyBody,
			http.StatusOK, &testStatus,
		},
		{
			http.MethodGet, "/collection/statistics", emptyBody,
			http.StatusOK, &milvuspb.GetCollectionStatisticsResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/collections", emptyBody,
			http.StatusOK, &milvuspb.ShowCollectionsResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/partition", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodDelete, "/partition", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodGet, "/partition/existence", emptyBody,
			http.StatusOK, &milvuspb.BoolResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/partitions/load", emptyBody,
			http.StatusOK, &testStatus,
		},
		{
			http.MethodDelete, "/partitions/load", emptyBody,
			http.StatusOK, &testStatus,
		},
		{
			http.MethodGet, "/partition/statistics", emptyBody,
			http.StatusOK, milvuspb.GetPartitionStatisticsResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/partitions", emptyBody,
			http.StatusOK, &milvuspb.ShowPartitionsResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/alias", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodDelete, "/alias", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodPatch, "/alias", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodPost, "/index", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodGet, "/index", emptyBody,
			http.StatusOK, &milvuspb.DescribeIndexResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/index/state", emptyBody,
			http.StatusOK, &milvuspb.GetIndexStateResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/index/progress", emptyBody,
			http.StatusOK, &milvuspb.GetIndexBuildProgressResponse{Status: testStatus},
		},
		{
			http.MethodDelete, "/index", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodPost, "/entities", &milvuspb.InsertRequest{CollectionName: "c1"},
			http.StatusOK, &milvuspb.MutationResult{Acknowledged: true},
		},
		{
			http.MethodDelete, "/entities", milvuspb.DeleteRequest{Expr: "some expr"},
			http.StatusOK, &milvuspb.MutationResult{Acknowledged: true},
		},
		{
			http.MethodPost, "/search", milvuspb.SearchRequest{Dsl: "some dsl"},
			http.StatusOK, &searchResult,
		},
		{
			http.MethodPost, "/query", milvuspb.QueryRequest{Expr: "some expr"},
			http.StatusOK, &queryResult,
		},
		{
			http.MethodPost, "/persist", milvuspb.FlushRequest{CollectionNames: []string{"c1"}},
			http.StatusOK, flushResult,
		},
		{
			http.MethodGet, "/distance", milvuspb.CalcDistanceRequest{
				Params: []*commonpb.KeyValuePair{
					{Key: "key", Value: "val"},
				}},
			http.StatusOK, calcDistanceResult,
		},
		{
			http.MethodGet, "/persist/state", emptyBody,
			http.StatusOK, &milvuspb.GetFlushStateResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/persist/segment-info", emptyBody,
			http.StatusOK, &milvuspb.GetPersistentSegmentInfoResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/query-segment-info", emptyBody,
			http.StatusOK, &milvuspb.GetQuerySegmentInfoResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/replicas", emptyBody,
			http.StatusOK, &milvuspb.GetReplicasResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/metrics", emptyBody,
			http.StatusOK, &milvuspb.GetMetricsResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/load-balance", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodGet, "/compaction/state", emptyBody,
			http.StatusOK, &milvuspb.GetCompactionStateResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/compaction/plans", emptyBody,
			http.StatusOK, &milvuspb.GetCompactionPlansResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/compaction", emptyBody,
			http.StatusOK, &milvuspb.ManualCompactionResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/import", emptyBody,
			http.StatusOK, &milvuspb.ImportResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/import/state", emptyBody,
			http.StatusOK, &milvuspb.GetImportStateResponse{Status: testStatus},
		},
		{
			http.MethodGet, "/import/tasks", emptyBody,
			http.StatusOK, &milvuspb.ListImportTasksResponse{Status: testStatus},
		},
		{
			http.MethodPost, "/credential", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodPatch, "/credential", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodDelete, "/credential", emptyBody,
			http.StatusOK, testStatus,
		},
		{
			http.MethodGet, "/credential/users", emptyBody,
			http.StatusOK, &milvuspb.ListCredUsersResponse{Status: testStatus},
		},
	}
	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%s %s %d", tt.httpMethod, tt.path, tt.expectedStatus), func(t *testing.T) {
			body := []byte{}
			if tt.body != nil {
				body, _ = json.Marshal(tt.body)
			}
			req := httptest.NewRequest(tt.httpMethod, tt.path, bytes.NewReader(body))
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)
			if tt.expectedBody != nil {
				bodyBytes, err := json.Marshal(tt.expectedBody)
				assert.NoError(t, err)
				assert.Equal(t, bodyBytes, w.Body.Bytes())
			}
			// test marshal failed
			req = httptest.NewRequest(tt.httpMethod, tt.path, bytes.NewReader([]byte("bad request")))
			w = httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}
