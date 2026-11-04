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

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The RESTful metrics are labeled with the route a request matched, never the
// raw path: the raw path is the client's to vary, and the middleware runs ahead
// of authentication and for requests that match no route.
func TestMetricsHandlerFuncLabelsTheMatchedRoute(t *testing.T) {
	paramtable.Init()
	gin.SetMode(gin.TestMode)
	defer metrics.RestfulFunctionCall.Reset()
	defer metrics.RestfulReqLatency.Reset()
	defer metrics.RestfulReceiveBytes.Reset()
	defer metrics.RestfulSendBytes.Reset()

	engine := gin.New()
	engine.Use(MetricsHandlerFunc)
	engine.GET("/v2/vectordb/entities/:id", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	for _, path := range []string{"/v2/vectordb/entities/1", "/v2/vectordb/entities/2", "/leak/1", "/leak/2"} {
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	for _, vec := range []*prometheus.CounterVec{metrics.RestfulFunctionCall, metrics.RestfulReceiveBytes} {
		assert.Equal(t, 1, vec.DeletePartialMatch(prometheus.Labels{"path": "/v2/vectordb/entities/:id"}))
		assert.Equal(t, 1, vec.DeletePartialMatch(prometheus.Labels{"path": metricsUnmatchedPath}))
		assert.Equal(t, 0, vec.DeletePartialMatch(prometheus.Labels{}), "no series may be labeled with a raw path")
	}
	assert.Equal(t, 1, metrics.RestfulReqLatency.DeletePartialMatch(prometheus.Labels{"path": "/v2/vectordb/entities/:id"}))
	assert.Equal(t, 1, metrics.RestfulReqLatency.DeletePartialMatch(prometheus.Labels{"path": metricsUnmatchedPath}))
	assert.Equal(t, 0, metrics.RestfulReqLatency.DeletePartialMatch(prometheus.Labels{}))
}
