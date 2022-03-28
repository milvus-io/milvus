package httpserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestWrapHandler(t *testing.T) {
	var testWrapFunc = func(c *gin.Context) (interface{}, error) {
		Case := c.Param("case")
		switch Case {
		case "0":
			return gin.H{"status": "ok"}, nil
		case "1":
			return nil, errBadRequest
		case "2":
			return nil, errors.New("internal err")
		}
		panic("shall not reach")
	}
	wrappedHandler := wrapHandler(testWrapFunc)
	testEngine := gin.New()
	testEngine.GET("/test/:case", wrappedHandler)

	t.Run("status ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test/0", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("err notfound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("err bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test/1", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("err internal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test/2", nil)
		w := httptest.NewRecorder()
		testEngine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

}
