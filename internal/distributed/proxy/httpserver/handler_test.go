package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

func Test_WrappedInsertRequest_JSONMarshal_AsInsertRequest(t *testing.T) {
	// https://github.com/milvus-io/milvus/issues/20415
	insertRaw := []byte(`{
		"collection_name": "seller_tag",
		"fields_data": [{ "field_name":"kid","type":5,"field":[9999999999999999] },{ "field_name":"seller_id","type":5,"field":[5625300123280813090] },{ "field_name":"vector","type":101,"field":[[0.08090433, 0.19154754, 0.16858263, 0.027101958, 0.07229418, 0.15223257, -0.024227709, 0.13302892, 0.05951315, 0.03572949, -0.015721956, -0.21992287, 0.08134472, 0.18640009, -0.09814235, -0.11117617, 0.10464557, -0.092037976, -0.19489805, -0.069008306, -0.039415136, -0.17841195, 0.076126315, 0.031378396, 0.22680397, 0.045089707, 0.12307317, 0.06711619, 0.15067382, -0.213569, 0.066602595, -0.021743167, -0.2727193, -0.112709574, 0.09504322, 0.02386695, 0.04574049, -0.055642836, -0.16812482, -0.051256, -0.11399734, 0.29519975, 0.109542266, 0.18452083, 0.05543076, -0.064969495, -0.14457555, -0.034600936, 0.045484997, -0.15677887, -0.12983392, 0.20921704, -0.049788076, 0.050687622, -0.23369887, -0.022488454, 0.06089106, 0.14699098, -0.08140416, -0.008949298, -0.14867777, 0.07415456, -0.0027948048, 0.0060837376]] }],
		"num_rows": 1
		}`)
	wrappedInsert := new(WrappedInsertRequest)
	err := json.Unmarshal(insertRaw, &wrappedInsert)
	assert.NoError(t, err)
	insertReq, err := wrappedInsert.AsInsertRequest()
	assert.NoError(t, err)
	assert.Equal(t, int64(9999999999999999), insertReq.FieldsData[0].GetScalars().GetLongData().Data[0])
}

type mockProxyComponent struct {
	// wrap the interface to avoid implement not used func.
	// and to let not implemented call panics
	// implement the method you want to mock
	types.ProxyComponent
}

func (m *mockProxyComponent) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, nil
}

var emptyBody = &gin.H{}
var testStatus = &commonpb.Status{Reason: "ok"}

func (m *mockProxyComponent) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return &milvuspb.GetCollectionStatisticsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return testStatus, nil
}
func (m *mockProxyComponent) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return testStatus, nil
}
func (m *mockProxyComponent) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return &milvuspb.GetPartitionStatisticsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return &milvuspb.DescribeIndexResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetIndexStatistics(ctx context.Context, request *milvuspb.GetIndexStatisticsRequest) (*milvuspb.GetIndexStatisticsResponse, error) {
	return &milvuspb.GetIndexStatisticsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return &milvuspb.GetIndexStateResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return &milvuspb.GetIndexBuildProgressResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	if request.CollectionName == "" {
		return nil, errors.New("body parse err")
	}
	return &milvuspb.MutationResult{Acknowledged: true}, nil
}

func (m *mockProxyComponent) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
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

func (m *mockProxyComponent) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if request.Dsl == "" {
		return nil, errors.New("body parse err")
	}
	return &searchResult, nil
}

var queryResult = milvuspb.QueryResults{
	CollectionName: "test",
}

func (m *mockProxyComponent) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if request.Expr == "" {
		return nil, errors.New("body parse err")
	}
	return &queryResult, nil
}

var flushResult = milvuspb.FlushResponse{
	DbName: "default",
}

func (m *mockProxyComponent) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
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

func (m *mockProxyComponent) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	if len(request.Params) < 1 {
		return nil, errors.New("body parse err")
	}
	return &calcDistanceResult, nil
}

func (m *mockProxyComponent) GetFlushState(ctx context.Context, request *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return &milvuspb.GetPersistentSegmentInfoResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return &milvuspb.GetQuerySegmentInfoResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetReplicas(ctx context.Context, request *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return &milvuspb.GetReplicasResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) LoadBalance(ctx context.Context, request *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) GetCompactionState(ctx context.Context, request *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetCompactionStateWithPlans(ctx context.Context, request *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) ManualCompaction(ctx context.Context, request *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) Import(ctx context.Context, request *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return &milvuspb.ImportResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) GetImportState(ctx context.Context, request *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return &milvuspb.GetImportStateResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) ListImportTasks(ctx context.Context, request *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return &milvuspb.ListImportTasksResponse{Status: testStatus}, nil
}

func (m *mockProxyComponent) CreateCredential(ctx context.Context, request *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) UpdateCredential(ctx context.Context, request *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) DeleteCredential(ctx context.Context, request *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return testStatus, nil
}

func (m *mockProxyComponent) ListCredUsers(ctx context.Context, request *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
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
