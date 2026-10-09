//go:build ignore

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

// File-list-only characterization. PASS reproduces an existing timeout gap.
// Run with timeout_middleware.go, json_render.go, and
// timeout_baseline_probe_test.go. This file does not change production code.

package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type encodeProbeResult struct {
	ended time.Time
	err   error
}

// All rows reuse one immutable vector to keep fixture construction cheap;
// Sonic must still serialize every element into the response buffer.
func encodeProbeData(rows, dimensions int) [][]float64 {
	vector := make([]float64, dimensions)
	for i := range vector {
		vector[i] = 0.125
	}
	data := make([][]float64, rows)
	for i := range data {
		data[i] = vector
	}
	return data
}

func TestBaseline_TimeoutDoesNotStopLargeJSONEncoding(t *testing.T) {
	configureProbe(t)
	const budget = 20 * time.Millisecond
	const rows = 32768
	const dimensions = 128
	payload := encodeProbeData(rows, dimensions)
	// Warm the same concrete response type outside the timed request.
	if _, err := json.Marshal(payload[:1]); err != nil {
		t.Fatal(err)
	}
	if err := paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, strconv.FormatInt(budget.Milliseconds(), 10)); err != nil {
		t.Fatal(err)
	}

	var profileFile *os.File
	if path := os.Getenv("MILVUS_TIMEOUT_ENCODE_CPU_PROFILE"); path != "" {
		var err error
		profileFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = profileFile.Close() })
	}

	requestContext := make(chan context.Context, 1)
	encodeStarted := make(chan time.Time, 1)
	encodeDone := make(chan encodeProbeResult, 1)
	handlerDone := make(chan struct{})
	outerDone := make(chan struct{})
	engine := gin.New()
	engine.GET("/probe", timeoutMiddleware(func(c *gin.Context) {
		defer close(handlerDone)
		requestContext <- c.Request.Context()
		encodeStarted <- time.Now()
		c.Render(http.StatusOK, jsonRender{Data: payload})
		var err error
		if last := c.Errors.Last(); last != nil {
			err = last.Err
		}
		encodeDone <- encodeProbeResult{ended: time.Now(), err: err}
	}))
	response := httptest.NewRecorder()
	requestStarted := time.Now()
	go func() {
		defer close(outerDone)
		engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/probe", nil))
	}()
	// Wait for both workers before the baseline fixture restores configuration.
	t.Cleanup(func() {
		awaitDecodeProbe(t, handlerDone, "encoding handler cleanup")
		awaitDecodeProbe(t, outerDone, "encoding middleware cleanup")
	})
	ctx := awaitDecodeProbe(t, requestContext, "encoding request context")
	started := awaitDecodeProbe(t, encodeStarted, "real renderer entry")
	deadline, ok := ctx.Deadline()
	if !ok || !started.Before(deadline) {
		t.Fatal("probe inconclusive: renderer did not start before request deadline")
	}
	awaitDecodeProbe(t, ctx.Done(), "request cancellation during encoding")
	cancelObserved := time.Now()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("unexpected request context error: %v", ctx.Err())
	}
	select {
	case result := <-encodeDone:
		t.Fatalf("probe inconclusive: renderer already returned by cancellation observation: %+v", result)
	default:
	}
	// Collect optional CPU samples only after the deadline. A profile sample
	// under Sonic's encoder gives stronger evidence than wall time alone.
	var profileStarted time.Time
	if profileFile != nil {
		if err := pprof.StartCPUProfile(profileFile); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
		profileStarted = time.Now()
	}
	awaitDecodeProbe(t, outerDone, "timeout response during encoding")
	responseEnded := time.Now()
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("HTTP status = %d, want 408", response.Code)
	}
	result := awaitDecodeProbe(t, encodeDone, "renderer completion")
	awaitDecodeProbe(t, handlerDone, "encoding handler completion")
	if !result.ended.After(deadline) || !result.ended.After(cancelObserved) || !result.ended.After(responseEnded) {
		t.Fatalf("expected renderer to outlive deadline, cancellation observation, and 408; renderer ended %v", result.ended)
	}
	if profileFile != nil && !result.ended.After(profileStarted) {
		t.Fatal("probe inconclusive: renderer returned before post-cancel profiling began")
	}
	// A closed timeout recorder can reject the final Write after the encoder
	// finishes. Preserve that result as evidence, without misclassifying it as
	// an encoding error or pretending the full response was sent.
	t.Logf("renderer=internal/json (Sonic) go=%s platform=%s/%s rows=%d dimensions=%d budget=%v renderStart=%v canceled=%v response408=%v renderEnd=%v afterDeadline=%v renderError=%v",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, rows, dimensions, budget,
		started.Sub(requestStarted), cancelObserved.Sub(requestStarted), responseEnded.Sub(requestStarted),
		result.ended.Sub(requestStarted), result.ended.Sub(deadline), result.err)
}
