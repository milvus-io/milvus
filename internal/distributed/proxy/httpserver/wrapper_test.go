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

package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestWrapHandler(t *testing.T) {
	testWrapFunc := func(c *gin.Context) (interface{}, error) {
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
		req := httptest.NewRequest(http.MethodGet, "/test/0?verbose=false", nil)
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
