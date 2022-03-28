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
		req := httptest.NewRequest(http.MethodPost, "/dummy", nil)
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy parseyaml failed 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/dummy", nil)
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEYAML},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
	t.Run("handlePostDummy default json ok", func(t *testing.T) {
		bodyBytes := []byte("{}")
		req := httptest.NewRequest(http.MethodPost, "/dummy", bytes.NewReader(bodyBytes))
		req.Header = http.Header{
			"Content-Type": []string{binding.MIMEJSON},
		}
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("handlePostDummy yaml ok", func(t *testing.T) {
		bodyBytes := []byte("---")
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
			http.MethodPost, "/entities", &milvuspb.InsertRequest{CollectionName: "c1"},
			http.StatusOK, &milvuspb.MutationResult{Acknowledged: true},
		},
		{
			http.MethodPost, "/entities", []byte("bad request"),
			http.StatusBadRequest, nil,
		},
		{
			http.MethodDelete, "/entities", milvuspb.DeleteRequest{Expr: "some expr"},
			http.StatusOK, &milvuspb.MutationResult{Acknowledged: true},
		},
		{
			http.MethodDelete, "/entities", []byte("bad request"),
			http.StatusBadRequest, nil,
		},
		{
			http.MethodPost, "/search", milvuspb.SearchRequest{Dsl: "some dsl"},
			http.StatusOK, &searchResult,
		},
		{
			http.MethodPost, "/search", []byte("bad request"),
			http.StatusBadRequest, nil,
		},
		{
			http.MethodPost, "/query", milvuspb.QueryRequest{Expr: "some expr"},
			http.StatusOK, &queryResult,
		},
		{
			http.MethodPost, "/query", []byte("bad request"),
			http.StatusBadRequest, nil,
		},
		{
			http.MethodPost, "/persist", milvuspb.FlushRequest{CollectionNames: []string{"c1"}},
			http.StatusOK, flushResult,
		},
		{
			http.MethodPost, "/persist", []byte("bad request"),
			http.StatusBadRequest, nil,
		},
		{
			http.MethodGet, "/distance", milvuspb.CalcDistanceRequest{
				Params: []*commonpb.KeyValuePair{
					{Key: "key", Value: "val"},
				}},
			http.StatusOK, calcDistanceResult,
		},
		{
			http.MethodGet, "/distance", []byte("bad request"),
			http.StatusBadRequest, nil,
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
		})
	}
}
