package httpserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
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

func TestHandlers(t *testing.T) {
	mockProxy := mockProxyComponent{}
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
}
