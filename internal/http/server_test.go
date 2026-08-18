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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type HTTPServerTestSuite struct {
	suite.Suite
}

func TestManagementHTTPHandlerPreservesLegacyMuxOnlyWhileGateIsOff(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.CommonCfg.AdminAuthEnabled.Key
	t.Cleanup(func() { params.Reset(key) })

	previousMetricsServer := metricsServer
	previousDefaultServeMux := http.DefaultServeMux
	t.Cleanup(func() {
		metricsServer = previousMetricsServer
		http.DefaultServeMux = previousDefaultServeMux
	})

	metricsServer = http.NewServeMux()
	metricsServer.HandleFunc(RootPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	metricsServer.HandleFunc("/management/test", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	http.DefaultServeMux = http.NewServeMux()
	http.DefaultServeMux.HandleFunc("/debug/vars", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	request := func(handler http.Handler, path string) int {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder.Code
	}

	require.NoError(t, params.Save(key, "false"))
	legacyCompatible := managementHTTPHandler(true)
	assert.Equal(t, http.StatusNoContent, request(legacyCompatible, "/debug/vars"),
		"flag-off mode must preserve the more-specific DefaultServeMux route")
	assert.Equal(t, http.StatusAccepted, request(legacyCompatible, "/management/test"),
		"a Milvus-owned route must not be displaced by the legacy mux")

	require.NoError(t, params.Save(key, "true"))
	assert.Equal(t, http.StatusTeapot, request(legacyCompatible, "/debug/vars"),
		"flag-on mode must not expose a DefaultServeMux bypass")

	require.NoError(t, params.Save(key, "false"))
	assert.Equal(t, http.StatusTeapot, request(managementHTTPHandler(false), "/debug/vars"),
		"pprof-disabled mode historically used only the private mux")
}

func TestConfigureEventlogListenerModeFollowsFlag(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.CommonCfg.AdminAuthEnabled.Key
	t.Cleanup(func() { params.Reset(key) })
	require.NoError(t, params.Save(key, "false"))

	applied := make(chan bool, 8)
	// A distinct identifier, unregistered on cleanup: the dispatcher removes by
	// identifier, so reusing ServeHTTP's would either leave this handler
	// running for the rest of the binary -- rebinding the process eventlog
	// listener behind every later config change in this package -- or deregister
	// the one ServeHTTP installed.
	handler := configureEventlogListenerMode("eventlog.listener.mode.test", func(localOnly bool) error {
		// Non-blocking anyway: a blocking send would park a goroutine if the
		// handler ever outlived the test.
		select {
		case applied <- localOnly:
		default:
		}
		return nil
	})
	t.Cleanup(func() { params.Unwatch(key, handler) })
	require.False(t, <-applied, "startup must apply the current flag value")

	// Turning the gate on writes a key that did not exist in etcd, so the event
	// is a CREATE carrying the separator-free alias. ParamItem.RegisterCallback
	// forwards neither, which is why this watches the dispatcher directly.
	require.NoError(t, params.Save(key, "true"))
	paramtable.GetBaseTable().Manager().Dispatcher.Dispatch(&config.Event{
		EventType: config.CreateType,
		Key:       "commonsecurityadminauthenabled",
		Value:     "true",
	})

	assert.True(t, <-applied, "enabling the gate must switch the listener to loopback")
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
	mlog.SetLevel(mlog.DebugLevel)
	suite.Equal(mlog.DebugLevel, mlog.GetLevel())

	// replace global logger, log change will not be affected.
	conf := &mlog.Config{Level: "info", File: mlog.FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := mlog.InitLogger(conf)
	mlog.ReplaceGlobals(logger, p)
	suite.Equal(mlog.InfoLevel, mlog.GetLevel())

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
	suite.Equal(mlog.ErrorLevel, mlog.GetLevel())
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

	// Register() now always uses a private ServeMux instead of opportunistically
	// falling back to http.DefaultServeMux when pprof is enabled, so the test
	// server must be backed by the package-level metricsServer that
	// RegisterWebUIHandler populates.
	ts := httptest.NewServer(metricsServer)
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

// installVerifier points exactly one management verifier slot at fn and
// restores every slot afterwards. The empty slot name installs no verifier at
// all, which is what a node looks like before any component has registered
// one. It returns a restore func.
func installVerifier(t require.TestingT, slot string, fn CredentialVerifier) (restore func()) {
	passwordVerifyMu.Lock()
	prevVerifiers, prevPrimary := managementVerifiers, passwordVerifyFunc
	managementVerifiers, passwordVerifyFunc = [numManagementVerifierSlots]CredentialVerifier{}, nil
	switch slot {
	case "":
	case "proxy":
		managementVerifiers[VerifierSlotProxy] = fn
	case "coordinator":
		managementVerifiers[VerifierSlotCoordinator] = fn
	case "worker":
		managementVerifiers[VerifierSlotWorker] = fn
	default:
		passwordVerifyMu.Unlock()
		require.FailNow(t, "unknown verifier slot "+slot)
		return func() {}
	}
	passwordVerifyMu.Unlock()
	return func() {
		passwordVerifyMu.Lock()
		defer passwordVerifyMu.Unlock()
		managementVerifiers, passwordVerifyFunc = prevVerifiers, prevPrimary
	}
}

// rootOnlyVerifier accepts root/s3cr3t, reports any other password as a
// mismatch, and reports "coord is gone" for the unavailable user so the 503
// path is reachable from a real verifier rather than only from a nil slot.
func rootOnlyVerifier(_ context.Context, username, password string) error {
	if username == "unavailable" {
		return errors.New("credential store unreachable")
	}
	if username == util.UserRoot && password == "s3cr3t" {
		return nil
	}
	return NewAuthenticationError("invalid root password")
}

// TestAdminAuthGatesManagementPlane exercises the gate against the real server
// on the metrics port, covering exactly what was reported: /management/stop (the
// unauthenticated DoS) and /log/level (the log-level mutation), plus /eventlog.
//
// It runs once per verifier slot, because which slot is filled is precisely
// what differs between a proxy, a coordinator and a worker node — and the
// worker slot is the only one that can answer 503.
//
// It also pins the other half of the contract — that the liveness surface stays
// open — because gating /healthz or /management/check/ready would take down
// every k8s probe in the fleet, a far worse outage than the bug being fixed.
func (suite *HTTPServerTestSuite) TestAdminAuthGatesManagementPlane() {
	RegisterStopComponent(func(role string) error { return nil })
	RegisterCheckComponentReady(func(role string) error { return nil })

	params := paramtable.Get()
	suite.NoError(params.Save(params.CommonCfg.AdminAuthEnabled.Key, "true"))
	defer params.Reset(params.CommonCfg.AdminAuthEnabled.Key)

	base := "http://localhost:" + DefaultListenPort
	gated := []string{RouteTriggerStopPath, LogLevelRouterPath, EventLogRouterPath, "/debug/pprof/"}

	get := func(path, user, pass string) *http.Response {
		req, err := http.NewRequest(http.MethodGet, base+path, nil)
		suite.Require().NoError(err)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(req)
		suite.Require().NoError(err, path)
		return resp
	}

	for _, slot := range []string{"proxy", "coordinator", "worker"} {
		suite.Run(slot, func() {
			defer installVerifier(suite.T(), slot, rootOnlyVerifier)()

			for _, path := range gated {
				resp := get(path, "", "")
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				suite.Equal(http.StatusUnauthorized, resp.StatusCode,
					"%s must reject unauthenticated callers, got body %q", path, string(body))

				// A non-root user is rejected with 403, not 401 — retrying
				// with a different password cannot help.
				resp = get(path, "alice", "s3cr3t")
				resp.Body.Close()
				suite.Equal(http.StatusForbidden, resp.StatusCode, "%s with non-root user", path)

				// Correct root credentials get past the gate. Handlers' own
				// status codes vary, so assert only that auth stopped blocking.
				resp = get(path, util.UserRoot, "s3cr3t")
				resp.Body.Close()
				suite.NotEqual(http.StatusUnauthorized, resp.StatusCode, "%s with root creds", path)
				suite.NotEqual(http.StatusForbidden, resp.StatusCode, "%s with root creds", path)
			}
		})
	}

	// A verifier that cannot reach its credential store must render 503, not
	// 401: telling an operator their correct password is wrong while the
	// cluster is half-down is the worst possible message at that moment.
	for _, slot := range []string{"proxy", "coordinator", "worker"} {
		suite.Run(slot+"/unverifiable", func() {
			defer installVerifier(suite.T(), slot, func(_ context.Context, _, _ string) error {
				return errors.New("credential store unreachable")
			})()

			resp := get(RouteTriggerStopPath, util.UserRoot, "s3cr3t")
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			suite.Equal(http.StatusServiceUnavailable, resp.StatusCode)
			suite.NotContains(string(body), "unreachable\": ",
				"the cause belongs in the log, not in a reply to an unauthenticated caller")
		})
	}

	// No verifier at all is also 503 rather than a silent pass.
	suite.Run("no verifier", func() {
		defer installVerifier(suite.T(), "", nil)()

		resp := get(RouteTriggerStopPath, util.UserRoot, "s3cr3t")
		resp.Body.Close()
		suite.Equal(http.StatusServiceUnavailable, resp.StatusCode)
	})

	// Probe endpoints must remain reachable with no credentials at all.
	for _, path := range []string{HealthzRouterPath, LivezRouterPath, RouteCheckComponentReady} {
		resp := get(path, "", "")
		resp.Body.Close()
		suite.NotEqual(http.StatusUnauthorized, resp.StatusCode,
			"%s must stay open for k8s probes", path)
		suite.NotEqual(http.StatusServiceUnavailable, resp.StatusCode,
			"%s must stay open for k8s probes", path)
	}
}

// TestAdminAuthDisabledKeepsManagementPlaneOpen is the back-compat half: with
// the flag at its default of false, nothing on the management plane starts
// demanding credentials.
func (suite *HTTPServerTestSuite) TestAdminAuthDisabledKeepsManagementPlaneOpen() {
	params := paramtable.Get()
	suite.False(params.CommonCfg.AdminAuthEnabled.GetAsBool(),
		"adminAuthEnabled must default to false so upgrades are transparent")

	base := "http://localhost:" + DefaultListenPort
	for _, path := range []string{LogLevelRouterPath, EventLogRouterPath} {
		resp, err := http.Get(base + path)
		suite.Require().NoError(err, path)
		resp.Body.Close()
		suite.NotEqual(http.StatusUnauthorized, resp.StatusCode,
			"%s must not require auth while adminAuthEnabled is false", path)
	}
}
