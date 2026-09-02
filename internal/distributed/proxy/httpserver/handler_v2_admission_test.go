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
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type dqlFullProxy struct {
	mockProxyComponent
	full bool
}

func (m *dqlFullProxy) IsDQLQueueFull() bool { return m.full }

func postJSONBody(server http.Handler, path string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	return w
}

func TestDQLAdmissionRejectsBeforeDecode(t *testing.T) {
	server := initHTTPServerV2(&dqlFullProxy{full: true}, false)
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	// admission runs outside restfulSizeMiddleware: a rejected request must
	// not add its client-declared size to the restful byte accounting
	receiveBytes := metrics.ProxyReceiveBytes.WithLabelValues("0", "", "", "")
	receiveBytesBefore := testutil.ToFloat64(receiveBytes)
	for _, action := range []string{QueryAction, GetAction, SearchAction, AdvancedSearchAction, HybridSearchAction} {
		methodTag := routeToMethod[versionalV2(EntityCategory, action)]
		total := metrics.ProxyFunctionCall.WithLabelValues(nodeID, methodTag, metrics.TotalLabel, metrics.CauseNA, "", "")
		rejected := metrics.ProxyFunctionCall.WithLabelValues(nodeID, methodTag, metrics.RejectedLabel, metrics.CauseSystem, "", "")
		totalBefore, rejectedBefore := testutil.ToFloat64(total), testutil.ToFloat64(rejected)

		// "{" is not decodable JSON: a decoded request would fail with
		// ErrIncorrectParameterFormat, so a 429 proves admission fired first
		w := postJSONBody(server, versionalV2(EntityCategory, action), "{")
		assert.Equal(t, http.StatusTooManyRequests, w.Code, action)
		assert.Equal(t, "1", w.Header().Get("Retry-After"), action)
		// Retry-After is not CORS-safelisted: browser callers need it exposed
		assert.Equal(t, "Retry-After", w.Header().Get("Access-Control-Expose-Headers"), action)
		returnBody := &ReturnErrMsg{}
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody), action)
		assert.Equal(t, merr.Code(merr.ErrServiceTooManyRequests), returnBody.Code, action)

		// the rejection is visible in the request counters wrapperPost never reached
		assert.Equal(t, totalBefore+1, testutil.ToFloat64(total), action)
		assert.Equal(t, rejectedBefore+1, testutil.ToFloat64(rejected), action)
	}
	assert.Equal(t, receiveBytesBefore, testutil.ToFloat64(receiveBytes))
}

func TestDQLAdmissionSparesNonDQLRoutes(t *testing.T) {
	server := initHTTPServerV2(&dqlFullProxy{full: true}, false)

	// DDL is served normally by the mock component
	w := postJSONBody(server, versionalV2(DataBaseCategory, ListAction), "{}")
	assert.Equal(t, http.StatusOK, w.Code)

	// DML falls through to body decoding instead of being rejected
	w = postJSONBody(server, versionalV2(EntityCategory, InsertAction), "{")
	assert.Equal(t, http.StatusOK, w.Code)
	returnBody := &ReturnErrMsg{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody))
	assert.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), returnBody.Code)
}

func TestDQLAdmissionPassesWhenNotFull(t *testing.T) {
	server := initHTTPServerV2(&dqlFullProxy{full: false}, false)
	w := postJSONBody(server, versionalV2(EntityCategory, SearchAction), "{")
	assert.Equal(t, http.StatusOK, w.Code)
	returnBody := &ReturnErrMsg{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody))
	assert.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), returnBody.Code)
}

func TestDQLAdmissionDisabled(t *testing.T) {
	key := paramtable.Get().HTTPCfg.DQLAdmissionEnabled.Key
	paramtable.Get().Save(key, "false")
	defer paramtable.Get().Reset(key)

	server := initHTTPServerV2(&dqlFullProxy{full: true}, false)
	w := postJSONBody(server, versionalV2(EntityCategory, SearchAction), "{")
	assert.Equal(t, http.StatusOK, w.Code)
	returnBody := &ReturnErrMsg{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody))
	assert.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), returnBody.Code)
}

func TestDQLAdmissionDBLabelStaysEmpty(t *testing.T) {
	// the DB-Name header is client-controlled and not authoritative (the body
	// value wins after decode): it must neither mint a series nor shift
	// rejection counts between databases
	server := initHTTPServerV2(&dqlFullProxy{full: true}, false)
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	headerDB := metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.RejectedLabel, metrics.CauseSystem, "my_db", "")
	empty := metrics.ProxyFunctionCall.WithLabelValues(nodeID, "Search", metrics.RejectedLabel, metrics.CauseSystem, "", "")
	headerDBBefore, emptyBefore := testutil.ToFloat64(headerDB), testutil.ToFloat64(empty)

	req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), strings.NewReader("{"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HTTPHeaderDBName, "my_db")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, headerDBBefore, testutil.ToFloat64(headerDB), "header value must not become a label")
	assert.Equal(t, emptyBefore+1, testutil.ToFloat64(empty))
}

func TestDQLAdmissionWithoutProbe(t *testing.T) {
	// a component that does not expose IsDQLQueueFull admits everything
	server := initHTTPServerV2(&mockProxyComponent{}, false)
	w := postJSONBody(server, versionalV2(EntityCategory, SearchAction), "{")
	assert.Equal(t, http.StatusOK, w.Code)
	returnBody := &ReturnErrMsg{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), returnBody))
	assert.Equal(t, merr.Code(merr.ErrIncorrectParameterFormat), returnBody.Code)
}
