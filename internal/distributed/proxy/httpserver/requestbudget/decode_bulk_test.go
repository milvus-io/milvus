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

package requestbudget

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type countingJSONReader struct {
	reader io.Reader
	read   int
	onRead func()
	once   sync.Once
}

func (r *countingJSONReader) Read(buf []byte) (int, error) {
	n, err := r.reader.Read(buf)
	r.read += n
	if n > 0 && r.onRead != nil {
		r.once.Do(r.onRead)
	}
	return n, err
}

func TestBufferedLargeBodyStopsDecodeAfterCancellation(t *testing.T) {
	row := `{"text":"` + strings.Repeat("x", 1024) + `"}`
	body := `{"collectionName":"c","data":[` + strings.Repeat(row+",", 4095) + row + `]}`
	newContext, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	newInput := &countingJSONReader{reader: strings.NewReader(body), onRead: cancelNew}
	_, err := DecodeBulkJSON(newContext, newInput, MaxJSONUnitBytes, MaxJSONBodyBytes, func([]byte) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("budgeted decoder error = %v", err)
	}
	if newInput.read >= len(body) {
		t.Fatalf("budgeted decoder read all %d bytes after cancellation", len(body))
	}

	oldContext, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	oldInput := &countingJSONReader{reader: strings.NewReader(body), onRead: cancelOld}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = (&http.Request{Body: io.NopCloser(oldInput)}).WithContext(oldContext)
	var oldRequest struct {
		Data []map[string]any `json:"data"`
	}
	if err := c.ShouldBindBodyWith(&oldRequest, binding.JSON); err != nil {
		t.Fatalf("legacy Gin decode: %v", err)
	}
	if oldContext.Err() != context.Canceled || oldInput.read != len(body) || len(oldRequest.Data) != 4096 {
		t.Fatalf("legacy decoder unexpectedly stopped: context=%v bytes=%d rows=%d", oldContext.Err(), oldInput.read, len(oldRequest.Data))
	}
}

func TestConcurrentBufferedBulkWorkersExitAfterCancellation(t *testing.T) {
	const workers = 8
	row := `{"text":"` + strings.Repeat("x", 1024) + `"}`
	body := `{"collectionName":"c","data":[` + strings.Repeat(row+",", 8191) + row + `]}`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	done := make(chan error, workers)
	for range workers {
		go func() {
			input := &countingJSONReader{reader: strings.NewReader(body), onRead: func() {
				started <- struct{}{}
				<-release
			}}
			_, err := DecodeBulkJSON(ctx, input, MaxJSONUnitBytes, MaxJSONBodyBytes, func([]byte) error { return nil })
			done <- err
		}()
	}
	for range workers {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			cancel()
			close(release)
			t.Fatal("workers did not enter body decoding")
		}
	}
	cancel()
	cutoff := time.Now()
	close(release)
	for range workers {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("worker error = %v, want context canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("worker remained active after cancellation")
		}
	}
	t.Logf("%d buffered-body decode workers exited within %s after cancellation", workers, time.Since(cutoff))
}

func TestDecodeBulkJSONStopsAfterCanceledRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := `{"collectionName":"c","data":[{"id":1},{"id":2},` + strings.Repeat(`{"id":3},`, 3000) + `{"id":4}]}`
	input := &countingJSONReader{reader: strings.NewReader(body)}
	called := 0
	_, err := DecodeBulkJSON(ctx, input, 4<<20, 8<<20, func(row []byte) error {
		called++
		if string(row) != `{"id":1}` {
			t.Fatalf("row = %s, want first row", row)
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if called != 1 {
		t.Fatalf("decoded %d rows after cancellation, want 1", called)
	}
	if input.read >= len(body) {
		t.Fatalf("read full %d-byte body after cancellation", len(body))
	}
}

func TestDecodeBulkJSONPreservesMetadataAndRows(t *testing.T) {
	input := `{"collectionName":"c","data":[{"text":"a}\\\"b"},{"id":2}],"partitionName":"p"}`
	var rows []string
	metadata, err := DecodeBulkJSON(context.Background(), strings.NewReader(input), 4<<20, 8<<20, func(row []byte) error {
		rows = append(rows, string(row))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(metadata), `{"collectionName":"c","data":[{}],"partitionName":"p"}`; got != want {
		t.Fatalf("metadata = %s, want %s", got, want)
	}
	if len(rows) != 2 || rows[0] != `{"text":"a}\\\"b"}` || rows[1] != `{"id":2}` {
		t.Fatalf("rows = %q", rows)
	}
}

func TestDecodeBulkJSONRejectsOversizeRowBeforeReadingTail(t *testing.T) {
	body := `{"data":[{"text":"` + strings.Repeat("x", 200000) + `"},{"id":2}]}`
	input := &countingJSONReader{reader: strings.NewReader(body)}
	_, err := DecodeBulkJSON(context.Background(), input, 1024, len(body)+1, func([]byte) error {
		t.Fatal("oversized row reached callback")
		return nil
	})
	if !errors.Is(err, merr.ErrParameterTooLarge) {
		t.Fatalf("error = %v, want parameter too large", err)
	}
	if input.read >= len(body) {
		t.Fatalf("read full %d-byte body before rejecting oversized row", len(body))
	}
}

func TestDecodeBulkJSONRejectsTrailingJSON(t *testing.T) {
	_, err := DecodeBulkJSON(context.Background(), strings.NewReader(`{"data":[{"id":1}]}true`), 4<<20, 8<<20, func([]byte) error {
		return nil
	})
	if !errors.Is(err, merr.ErrParameterInvalid) {
		t.Fatalf("error = %v, want invalid JSON", err)
	}
}
