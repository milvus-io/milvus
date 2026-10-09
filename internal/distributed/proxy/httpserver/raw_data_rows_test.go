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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestRawDataRowsPreservesRawSpelling(t *testing.T) {
	body := []byte(`{"other":{"data":[0]},"d\u0061ta":[{"x":1e2},{"x":"\\u0041"}]}`)
	rows, err := rawDataRows(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Raw != `{"x":1e2}` || rows[1].Raw != `{"x":"\\u0041"}` {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestRawDataRowsHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rawDataRows(ctx, []byte(`{"data":[{"x":1}]}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
}

type cancelRawRowsAfterChecks struct {
	context.Context
	checks int
}

func (c *cancelRawRowsAfterChecks) Err() error {
	c.checks++
	if c.checks >= 3 {
		return context.Canceled
	}
	return nil
}

func TestRawDataRowsChecksBetweenRows(t *testing.T) {
	ctx := &cancelRawRowsAfterChecks{Context: context.Background()}
	_, err := rawDataRows(ctx, []byte(`{"data":[{"id":1},{"id":2},{"id":3}]}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation between rows", err)
	}
}

func TestRawDataRowsRejectsNonArray(t *testing.T) {
	_, err := rawDataRows(context.Background(), []byte(`{"data":{}}`))
	if err == nil {
		t.Fatal("non-array data was accepted")
	}
}

func BenchmarkRawDataRows(b *testing.B) {
	row := `{"vector":[` + strings.TrimSuffix(strings.Repeat("0.125,", 128), ",") + `]}`
	body := []byte(`{"data":[` + strings.TrimSuffix(strings.Repeat(row+",", 8192), ",") + `]}`)
	b.Run("gjson-full-body", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if len(gjson.GetBytes(body, "data").Array()) != 8192 {
				b.Fatal("wrong row count")
			}
		}
	})
	b.Run("sonic-ast-row-view", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := rawDataRows(context.Background(), body)
			if err != nil || len(rows) != 8192 {
				b.Fatalf("rows=%d err=%v", len(rows), err)
			}
		}
	})
}
