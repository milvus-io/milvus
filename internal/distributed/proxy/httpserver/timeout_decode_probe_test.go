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

// File-list-only characterization, not a passing timeout acceptance test.
// Compile alongside the ORIGINAL middleware/renderer and the baseline helpers:
// LOCAL_STORAGE_SIZE=1 go test -tags dynamic,test,sonic,bytedance_tango \
//   -ldflags=-checklinkname=0 -gcflags='all=-N -l' -count=1 -v \
//   timeout_middleware.go json_render.go \
//   timeout_baseline_probe_test.go timeout_decode_probe_test.go \
//   -run '^TestBaseline_TimeoutDoesNotStopCompleteBodyDecoding$'
// Use -tags dynamic,test without the linker flag for Gin's standard codec.
// Optional MILVUS_TIMEOUT_PROBE_CPU_PROFILE names a NEW file for CPU samples
// collected only after request-context cancellation. Never overwrite a profile.

package httpserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	ginjson "github.com/gin-gonic/gin/codec/json"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Match CollectionDataReq's JSON shape at the reviewed revision. Importing
// request_v2.go would pull in unavailable Milvus native libraries. No custom
// UnmarshalJSON, slow reader, artificial delay, or mock decoder is used.
type decodeProbeRequest struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" binding:"required"`
	RlsPrincipal   string                   `json:"rlsPrincipal"`
	SkipRls        bool                     `json:"skipRls"`
	PartitionName  string                   `json:"partitionName"`
	Data           []map[string]interface{} `json:"data" binding:"required"`
	PartialUpdate  bool                     `json:"partialUpdate"`
	FieldOps       []struct {
		FieldName string `json:"fieldName"`
		Op        string `json:"op"`
		Path      string `json:"path"`
	} `json:"fieldOps"`
}

type decodeProbeResult struct {
	started time.Time
	ended   time.Time
	err     error
}

// Observe the exact Decode call, excluding Gin's subsequent validation. All
// codec operations, including UseNumber/DisallowUnknownFields, still delegate
// to the build-selected real codec. These tests must not run in parallel.
type decodeProbeCodec struct {
	ginjson.Core
	started chan time.Time
	done    chan decodeProbeResult
}

func (c decodeProbeCodec) NewDecoder(reader io.Reader) ginjson.Decoder {
	return decodeProbeDecoder{c.Core.NewDecoder(reader), c.started, c.done}
}

type decodeProbeDecoder struct {
	ginjson.Decoder
	started chan time.Time
	done    chan decodeProbeResult
}

func (d decodeProbeDecoder) Decode(value any) error {
	started := time.Now()
	d.started <- started
	err := d.Decoder.Decode(value)
	d.done <- decodeProbeResult{started: started, ended: time.Now(), err: err}
	return err
}

func decodeProbeBody(rows int) []byte {
	const prefix = `{"collectionName":"timeout_probe","data":[`
	const suffix = `]}`
	row := append([]byte(`{"id":1,"vector":[`), bytes.Repeat([]byte("0.125,"), 63)...)
	row = append(row, []byte(`0.125],"label":"payload"}`)...)
	var body bytes.Buffer
	body.Grow(len(prefix) + rows*(len(row)+1) + len(suffix))
	body.WriteString(prefix)
	for i := 0; i < rows; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		body.Write(row)
	}
	body.WriteString(suffix)
	return body.Bytes()
}

func awaitDecodeProbe[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(30 * time.Second):
		t.Fatalf("did not observe %s within diagnostic hang guard", label)
		var zero T
		return zero
	}
}

func TestBaseline_TimeoutDoesNotStopCompleteBodyDecoding(t *testing.T) {
	configureProbe(t)
	const budget = 20 * time.Millisecond
	const rows = 32768
	body := decodeProbeBody(rows) // Entire body exists before the request begins.
	var warm decodeProbeRequest
	if err := binding.JSON.BindBody(decodeProbeBody(1), &warm); err != nil {
		t.Fatal(err)
	}
	// Warm codec/type initialization and validation before timing.
	if err := paramtable.Get().Save(paramtable.Get().HTTPCfg.RequestTimeoutMs.Key, strconv.FormatInt(budget.Milliseconds(), 10)); err != nil {
		t.Fatal(err)
	}

	var profileFile *os.File
	if path := os.Getenv("MILVUS_TIMEOUT_PROBE_CPU_PROFILE"); path != "" {
		var err error
		profileFile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = profileFile.Close() })
	}
	decodeStarted := make(chan time.Time, 1)
	decodeDone := make(chan decodeProbeResult, 1)
	originalCodec := ginjson.API
	ginjson.API = decodeProbeCodec{originalCodec, decodeStarted, decodeDone}
	t.Cleanup(func() { ginjson.API = originalCodec })

	requestContext := make(chan context.Context, 1)
	handlerDone := make(chan struct{})
	outerDone := make(chan struct{})
	var payload decodeProbeRequest
	var bindErr error
	engine := gin.New()
	engine.POST("/probe", timeoutMiddleware(func(c *gin.Context) {
		defer close(handlerDone)
		// Enter Gin's normal cached-body path: no socket/body read is involved.
		c.Set(gin.BodyBytesKey, body)
		requestContext <- c.Request.Context()
		bindErr = c.ShouldBindBodyWith(&payload, binding.JSON)
	}))
	response := httptest.NewRecorder()
	requestStarted := time.Now()
	go func() {
		defer close(outerDone)
		engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/probe", nil))
	}()
	// On every assertion-failure path, wait for the real decoder and middleware
	// before restoring the global codec/config. No leaked test worker is allowed.
	t.Cleanup(func() {
		awaitDecodeProbe(t, handlerDone, "handler cleanup")
		awaitDecodeProbe(t, outerDone, "middleware cleanup")
	})
	ctx := awaitDecodeProbe(t, requestContext, "request context")
	started := awaitDecodeProbe(t, decodeStarted, "real Decode entry")
	deadline, ok := ctx.Deadline()
	if !ok || !started.Before(deadline) {
		t.Fatal("probe inconclusive: Decode did not start before the request deadline")
	}
	awaitDecodeProbe(t, ctx.Done(), "request cancellation")
	cancelObserved := time.Now()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("unexpected request context error: %v", ctx.Err())
	}
	select {
	case result := <-decodeDone:
		t.Fatalf("probe inconclusive: Decode already returned by cancellation observation: %+v", result)
	default:
	}
	// Optional profiling is deliberately started AFTER cancellation. Profiles
	// are process-wide; decoder stack samples, not total CPU, establish that
	// decoding itself kept consuming CPU after the request expired.
	var profileStarted time.Time
	if profileFile != nil {
		if err := pprof.StartCPUProfile(profileFile); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
		profileStarted = time.Now()
	}
	awaitDecodeProbe(t, outerDone, "timeout response")
	responseEnded := time.Now()
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("HTTP status = %d, want 408", response.Code)
	}
	result := awaitDecodeProbe(t, decodeDone, "real Decode completion")
	awaitDecodeProbe(t, handlerDone, "binding and handler completion")
	if result.err != nil || bindErr != nil {
		t.Fatalf("unexpected early termination: Decode=%v, binding=%v", result.err, bindErr)
	}
	if !result.ended.After(deadline) || !result.ended.After(cancelObserved) || !result.ended.After(responseEnded) {
		t.Fatalf("expected Decode to outlive deadline, cancellation observation, and 408; Decode ended %v", result.ended)
	}
	if profileFile != nil && !result.ended.After(profileStarted) {
		t.Fatal("probe inconclusive: Decode finished before post-cancel profiling started")
	}
	if payload.CollectionName != "timeout_probe" || len(payload.Data) != rows {
		t.Fatalf("decoded collection=%q rows=%d, want timeout_probe and %d", payload.CollectionName, len(payload.Data), rows)
	}
	for _, row := range []map[string]interface{}{payload.Data[0], payload.Data[rows-1]} {
		vector, ok := row["vector"].([]interface{})
		if !ok || len(vector) != 64 || vector[0] != float64(0.125) || vector[63] != float64(0.125) || row["label"] != "payload" {
			t.Fatal("decoded first/last row differs from the literal fixture")
		}
	}
	t.Logf("codec=%s go=%s platform=%s/%s bytes=%d rows=%d budget=%v decodeStart=%v canceled=%v response408=%v decodeEnd=%v decodeDuration=%v afterDeadline=%v afterCancelObserved=%v",
		ginjson.Package, runtime.Version(), runtime.GOOS, runtime.GOARCH, len(body), rows, budget,
		started.Sub(requestStarted), cancelObserved.Sub(requestStarted), responseEnded.Sub(requestStarted), result.ended.Sub(requestStarted),
		result.ended.Sub(result.started), result.ended.Sub(deadline), result.ended.Sub(cancelObserved))
}
