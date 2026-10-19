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

package healthz

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
)

type testIndicator struct {
	name string
	code atomic.Int32
}

func (*testIndicator) GetName() string                           { panic("patched by mockey") }
func (*testIndicator) Health(context.Context) commonpb.StateCode { panic("patched by mockey") }

func setupHealthHandler(t *testing.T) *mockey.Mocker {
	t.Helper()
	defaultHandler = HealthHandler{}
	t.Cleanup(func() { defaultHandler = HealthHandler{} })
	name := mockey.Mock((*testIndicator).GetName).To(func(in *testIndicator) string { return in.name }).Build()
	health := mockey.Mock((*testIndicator).Health).To(func(in *testIndicator, _ context.Context) commonpb.StateCode {
		return commonpb.StateCode(in.code.Load())
	}).Build()
	t.Cleanup(func() { name.UnPatch() })
	t.Cleanup(func() { health.UnPatch() })
	return health
}

func newTestIndicator(name string, code commonpb.StateCode) *testIndicator {
	in := &testIndicator{name: name}
	in.code.Store(int32(code))
	return in
}

func checkHealth(t *testing.T, status int, state string, detail ...*IndicatorState) {
	t.Helper()
	for _, contentType := range []string{ContentTypeText, ContentTypeJSON} {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		req.Header.Set(ContentTypeHeader, contentType)
		recorder := httptest.NewRecorder()
		Handler().ServeHTTP(recorder, req)
		require.Equal(t, status, recorder.Code, recorder.Body.String())
		if contentType == ContentTypeText {
			assert.Equal(t, state, recorder.Body.String())
		} else {
			var response HealthResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, state, response.State)
			assert.Equal(t, detail, response.Detail)
		}
	}
}

func TestHealthHandlerStartup(t *testing.T) {
	setupHealthHandler(t)
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 0/0")
	SetComponentNum(2)
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 0/2")
	proxy := newTestIndicator("proxy", commonpb.StateCode_Healthy)
	Register(proxy)
	proxyDetail := &IndicatorState{Name: "proxy", Code: commonpb.StateCode_Healthy}
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 1/2", proxyDetail)
	coord := newTestIndicator("mixcoord", commonpb.StateCode_Initializing)
	Register(coord)
	checkHealth(t, http.StatusInternalServerError, "Not all components are healthy, 1/2",
		proxyDetail, &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_Initializing})
	coord.code.Store(int32(commonpb.StateCode_StandBy))
	checkHealth(t, http.StatusOK, "OK", proxyDetail, &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_StandBy})
	coord.code.Store(int32(commonpb.StateCode_Healthy))
	checkHealth(t, http.StatusOK, "OK", proxyDetail, &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_Healthy})
	proxy.code.Store(int32(commonpb.StateCode_Abnormal))
	checkHealth(t, http.StatusInternalServerError, "Not all components are healthy, 1/2",
		&IndicatorState{Name: "proxy", Code: commonpb.StateCode_Abnormal}, &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_Healthy})
}

func TestHealthHandlerComponentCount(t *testing.T) {
	setupHealthHandler(t)
	Register(newTestIndicator("proxy", commonpb.StateCode_Healthy))
	detail := &IndicatorState{Name: "proxy", Code: commonpb.StateCode_Healthy}
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 1/0", detail)
	SetComponentNum(1)
	checkHealth(t, http.StatusOK, "OK", detail)
	SetComponentNum(2)
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 1/2", detail)
	Register(newTestIndicator("mixcoord", commonpb.StateCode_Healthy))
	SetComponentNum(1)
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 2/1",
		detail, &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_Healthy})
}

func TestHealthHandlerUnregister(t *testing.T) {
	setupHealthHandler(t)
	SetComponentNum(2)
	Register(newTestIndicator("proxy", commonpb.StateCode_Abnormal))
	UnRegister("proxy")
	UnRegister("proxy")
	UnRegister("unknown")
	// Excluding a stopped role (or an unknown name) cannot replace a missing registration.
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 1/2")
	Register(newTestIndicator("mixcoord", commonpb.StateCode_StandBy))
	checkHealth(t, http.StatusOK, "OK", &IndicatorState{Name: "mixcoord", Code: commonpb.StateCode_StandBy})
	UnRegister("mixcoord")
	checkHealth(t, http.StatusOK, "OK")
}

func TestHealthHandlerConcurrentRegistration(t *testing.T) {
	setupHealthHandler(t)
	const count = 32
	SetComponentNum(count + 1)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("component-%d", i)
			Register(newTestIndicator(name, commonpb.StateCode_Healthy))
			SetComponentNum(count + 1)
			UnRegister(name)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 10; j++ {
				recorder := httptest.NewRecorder()
				Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
				assert.Equal(t, http.StatusInternalServerError, recorder.Code)
			}
		}()
	}
	close(start)
	wg.Wait()
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 32/33")
	Register(newTestIndicator("last", commonpb.StateCode_Healthy))
	checkHealth(t, http.StatusOK, "OK", &IndicatorState{Name: "last", Code: commonpb.StateCode_Healthy})
}

func TestLivenessDuringStartup(t *testing.T) {
	setupHealthHandler(t)
	SetComponentNum(1)
	checkHealth(t, http.StatusInternalServerError, "Not all components are registered, 0/1")
	recorder := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "Milvus Process Active\n", recorder.Body.String())
}

func TestHealthHandlerRegistrationWhileChecking(t *testing.T) {
	health := setupHealthHandler(t)
	health.UnPatch()
	entered, release := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*testIndicator).Health).To(func(*testIndicator, context.Context) commonpb.StateCode {
		close(entered)
		<-release
		return commonpb.StateCode_Healthy
	}).Build()
	defer patch.UnPatch()
	SetComponentNum(2)
	Register(newTestIndicator("first", commonpb.StateCode_Healthy))
	var wg sync.WaitGroup
	wg.Add(1)
	recorder := httptest.NewRecorder()
	defer func() {
		close(release)
		wg.Wait()
		assert.Equal(t, http.StatusInternalServerError, recorder.Code)
		assert.Equal(t, "Not all components are registered, 1/2", recorder.Body.String())
	}()
	go func() {
		defer wg.Done()
		Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	}()
	<-entered
	registered := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		Register(newTestIndicator("second", commonpb.StateCode_Healthy))
		UnRegister("first")
		close(registered)
	}()
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("registration must not wait for a component health check")
	}
}

func TestHealthHandlerHTTPStartup(t *testing.T) {
	setupHealthHandler(t)
	SetComponentNum(1)
	server := httptest.NewServer(Handler())
	defer server.Close()
	check := func(status int, state string) {
		t.Helper()
		resp, err := server.Client().Get(server.URL + "/healthz")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, status, resp.StatusCode)
		assert.Equal(t, state, string(body))
	}
	check(http.StatusInternalServerError, "Not all components are registered, 0/1")
	proxy := newTestIndicator("proxy", commonpb.StateCode_Initializing)
	Register(proxy)
	check(http.StatusInternalServerError, "Not all components are healthy, 0/1")
	proxy.code.Store(int32(commonpb.StateCode_Healthy))
	check(http.StatusOK, "OK")
}
