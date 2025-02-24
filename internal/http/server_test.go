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

package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type HTTPServerTestSuite struct {
	suite.Suite
}

func (suite *HTTPServerTestSuite) SetupSuite() {
	paramtable.Init()
	ServeHTTP()
	conn, err := net.DialTimeout("tcp", "localhost:"+DefaultListenPort, time.Second*5)
	if err != nil {
		time.Sleep(time.Second)
		conn, err = net.DialTimeout("tcp", "localhost:"+DefaultListenPort, time.Second*5)
	}
	suite.Equal(nil, err)
	conn.Close()
}

func (suite *HTTPServerTestSuite) TearDownSuite() {
	defer server.Close()
	metricsServer = nil
}

func (suite *HTTPServerTestSuite) TestGetHTTPAddr() {
	suite.Equal(getHTTPAddr(), ":"+DefaultListenPort)
	testPort := "9092"
	os.Setenv(ListenPortEnvKey, testPort)
	suite.Equal(getHTTPAddr(), ":"+testPort)
}

func (suite *HTTPServerTestSuite) TestDefaultLogHandler() {
	log.SetLevel(zap.DebugLevel)
	suite.Equal(zap.DebugLevel, log.GetLevel())

	// replace global logger, log change will not be affected.
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := log.InitLogger(conf)
	log.ReplaceGlobals(logger, p)
	suite.Equal(zap.InfoLevel, log.GetLevel())

	// change log level through http
	payload, err := json.Marshal(map[string]any{"level": "error"})
	suite.Require().NoError(err)

	url := "http://localhost:" + DefaultListenPort + "/log/level"
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	suite.Require().NoError(err)

	client := http.Client{}
	resp, err := client.Do(req)
	suite.Require().NoError(err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	suite.Require().NoError(err)
	suite.Equal("{\"level\":\"error\"}\n", string(body))
	suite.Equal(zap.ErrorLevel, log.GetLevel())
}

func (suite *HTTPServerTestSuite) TestHealthzHandler() {
	url := "http://localhost:" + DefaultListenPort + "/healthz"
	client := http.Client{}

	healthz.SetComponentNum(1)
	healthz.Register(&MockIndicator{"m1", commonpb.StateCode_Healthy})

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	suite.Equal("OK", string(body))

	req, _ = http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ = io.ReadAll(resp.Body)
	suite.Equal("{\"state\":\"OK\",\"detail\":[{\"name\":\"m1\",\"code\":1}]}", string(body))

	healthz.SetComponentNum(2)
	healthz.Register(&MockIndicator{"m2", commonpb.StateCode_Abnormal})
	req, _ = http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ = io.ReadAll(resp.Body)
	respObj := &healthz.HealthResponse{}
	err = json.Unmarshal(body, respObj)
	suite.NoError(err)
	suite.NotEqual("OK", respObj.State)
}

func (suite *HTTPServerTestSuite) TestEventlogHandler() {
	url := "http://localhost:" + DefaultListenPort + EventLogRouterPath
	client := http.Client{}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	suite.True(strings.HasPrefix(string(body), "{\"status\":200,\"port\":"))
}

func (suite *HTTPServerTestSuite) TestPprofHandler() {
	client := http.Client{}
	testCases := []struct {
		enable     bool
		path       string
		statusCode int
		resp       []byte
	}{
		{true, "/debug/pprof/<script>scripty<script>", http.StatusNotFound, []byte("Unknown profile\n")},
		{true, "/debug/pprof/heap", http.StatusOK, nil},
		{true, "/debug/pprof/heap?debug=1", http.StatusOK, nil},
		{true, "/debug/pprof/cmdline", http.StatusOK, nil},
		{true, "/debug/pprof/profile?seconds=1", http.StatusOK, nil},
		{true, "/debug/pprof/symbol", http.StatusOK, nil},
		{true, "/debug/pprof/trace", http.StatusOK, nil},
		{true, "/debug/pprof/mutex", http.StatusOK, nil},
		{true, "/debug/pprof/block?seconds=1", http.StatusOK, nil},
		{true, "/debug/pprof/goroutine?seconds=1", http.StatusOK, nil},
		{true, "/debug/pprof/", http.StatusOK, []byte("Types of profiles available:")},
		{false, "/debug/pprof/<script>scripty<script>", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/heap", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/heap?debug=1", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/cmdline", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/profile?seconds=1", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/symbol", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/trace", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/mutex", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/block?seconds=1", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/goroutine?seconds=1", http.StatusNotFound, []byte("404 page not found\n")},
		{false, "/debug/pprof/", http.StatusNotFound, []byte("404 page not found\n")},
	}
	for _, tc := range testCases {
		if tc.enable != paramtable.Get().HTTPCfg.EnablePprof.GetAsBool() {
			continue
		}
		req, _ := http.NewRequest(http.MethodGet, "http://localhost:"+DefaultListenPort+tc.path, nil)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		suite.Nil(err)
		if err == nil {
			defer resp.Body.Close()
			suite.Equal(tc.statusCode, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			suite.Nil(err)
			if resp.StatusCode != http.StatusOK {
				suite.True(bytes.Equal(tc.resp, body))
			}
		} else {
			fmt.Println(err.Error())
		}
	}
}

func (suite *HTTPServerTestSuite) TestExprHandler() {
	expr.Init()
	expr.Register("foo", "hello")
	suite.Run("fail", func() {
		url := "http://localhost:" + DefaultListenPort + ExprPath + "?code=foo"
		client := http.Client{}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := client.Do(req)
		suite.Nil(err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		suite.True(strings.Contains(string(body), "failed to execute"))
	})
	suite.Run("success", func() {
		url := "http://localhost:" + DefaultListenPort + ExprPath + "?auth=by-dev&code=foo"
		client := http.Client{}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := client.Do(req)
		suite.Nil(err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		suite.True(strings.Contains(string(body), "hello"))
	})
}

func TestHTTPServerSuite(t *testing.T) {
	suite.Run(t, new(HTTPServerTestSuite))
}

type MockIndicator struct {
	name string
	code commonpb.StateCode
}

func (m *MockIndicator) Health(ctx context.Context) commonpb.StateCode {
	return m.code
}

func (m *MockIndicator) GetName() string {
	return m.name
}

func TestRegisterWebUIHandler(t *testing.T) {
	// Initialize the HTTP server
	func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("May the handler has been registered!", err)
			}
		}()
		RegisterWebUIHandler()
	}()

	// Create a test server
	ts := httptest.NewServer(http.DefaultServeMux)
	defer ts.Close()

	// Test cases
	tests := []struct {
		url          string
		expectedCode int
		expectedBody string
	}{
		{"/webui/", http.StatusOK, "<!doctype html>"},
		{"/webui/index.html", http.StatusOK, "<!doctype html>"},
		{"/webui/unknown", http.StatusOK, "<!doctype html>"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			req, err := http.NewRequest("GET", ts.URL+tt.url, nil)
			assert.NoError(t, err)
			req.Header.Set("Accept", "text/html")
			resp, err := ts.Client().Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedCode, resp.StatusCode)

			body := make([]byte, len(tt.expectedBody))
			_, err = resp.Body.Read(body)
			assert.NoError(t, err)
			assert.Contains(t, strings.ToLower(string(body)), tt.expectedBody)
		})
	}
}

func TestHandleNotFound(t *testing.T) {
	mainHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	fallbackHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Fallback"))
	})

	handler := handleNotFound(mainHandler, fallbackHandler)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	resp := w.Result()
	body := make([]byte, 8)
	resp.Body.Read(body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Fallback", string(body))
}

func TestServeFile(t *testing.T) {
	fs := http.FS(staticFiles)
	handler := serveFile("unknown", fs)

	// No Accept in http header
	{
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		resp := w.Result()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	}

	// unknown request file
	{
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", "text/html")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)
		resp := w.Result()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	}
}
