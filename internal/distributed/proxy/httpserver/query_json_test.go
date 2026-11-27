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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/errors"
)

func TestQueryJSONParity(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 257} {
		rows := make([]map[string]interface{}, size)
		for i := range rows {
			rows[i] = map[string]interface{}{
				"id": int64(9223372036854775807), "vector": []float32{0.1, -0.5, 1},
				"text": "<script>&\u2028\u2029\xff\n\"", "null": nil,
				"json": json.RawMessage(`{"x":"<>&","n":1.25}`),
			}
		}
		for _, data := range [][]map[string]interface{}{rows, nil} {
			result := map[string]interface{}{"code": 0, "data": data, "cost": 1, "storage": 0.5}
			var want, got bytes.Buffer
			if err := sonic.ConfigStd.NewEncoder(&want).Encode(result); err != nil {
				t.Fatal(err)
			}
			if err := writeDQLJSON(context.Background(), &got, result, 2); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want.Bytes(), got.Bytes()) {
				t.Fatalf("size=%d: different JSON\nwant %s\ngot %s", size, want.Bytes(), got.Bytes())
			}
		}
	}
}

type queryFailWriter struct{ err error }

func (w queryFailWriter) Write(p []byte) (int, error) { return 0, w.err }

func TestQueryJSONFailures(t *testing.T) {
	rows := []map[string]interface{}{{"x": 1}, {"x": math.NaN()}}
	result := map[string]interface{}{"code": 0, "data": rows}
	var output bytes.Buffer
	if err := writeDQLJSON(context.Background(), &output, result, 1); err == nil {
		t.Fatal("expected encoding failure")
	}
	if json.Valid(output.Bytes()) {
		t.Fatal("failed response must not be completed")
	}
	output.Reset()
	if err := writeDQLJSON(context.Background(), &output, result, 2); err == nil || output.Len() != 0 {
		t.Fatal("first batch failure must not commit output")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeDQLJSON(ctx, &output, result, 1); !errors.Is(err, context.Canceled) || output.Len() != 0 {
		t.Fatalf("cancel: %v", err)
	}
	sentinel := io.ErrClosedPipe
	if err := writeDQLJSON(context.Background(), queryFailWriter{sentinel}, result, 1); !errors.Is(err, sentinel) {
		t.Fatalf("write: %v", err)
	}
	if err := writeDQLJSON(context.Background(), queryFailWriter{}, result, 1); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
	// Even envelope fields sorted after data must fail before the first write.
	result = map[string]interface{}{"data": []map[string]interface{}{{"id": 1}}, "z": math.NaN()}
	if err := writeDQLJSON(context.Background(), &output, result, 1); err == nil || output.Len() != 0 {
		t.Fatal("envelope failure must not commit output")
	}
}

type queryReuseWriter struct {
	pointers []*byte
	cancel   context.CancelFunc
}

func (w *queryReuseWriter) Write(p []byte) (int, error) {
	if len(p) > 0 && p[0] == '{' && bytes.Contains(p, []byte(`"vector"`)) {
		w.pointers = append(w.pointers, &p[0])
		if w.cancel != nil {
			w.cancel()
		}
	}
	return len(p), nil
}

func TestQueryJSONReusesBatchBuffer(t *testing.T) {
	row := map[string]interface{}{"vector": make([]float32, 768), "text": "<>&"}
	rows := []map[string]interface{}{row, row, row, row}
	result := map[string]interface{}{"data": rows}
	w := &queryReuseWriter{}
	if err := writeDQLJSON(context.Background(), w, result, 1); err != nil {
		t.Fatal(err)
	}
	if len(w.pointers) != 4 {
		t.Fatalf("batches: %d", len(w.pointers))
	}
	for _, p := range w.pointers {
		if p != w.pointers[0] {
			t.Fatal("batch destination was not reused")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w = &queryReuseWriter{cancel: cancel}
	if err := writeDQLJSON(ctx, w, result, 1); !errors.Is(err, context.Canceled) || len(w.pointers) != 1 {
		t.Fatalf("cancel between batches: %v", err)
	}
}

func BenchmarkQueryJSON(b *testing.B) {
	rows := make([]map[string]interface{}, 6000)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": i, "text": "<>&"}
		for _, name := range []string{"v1", "v2", "v3", "v4", "v5"} {
			v := make([]float32, 768)
			for j := range v {
				v[j] = float32(j+1) / 769
			}
			rows[i][name] = v
		}
	}
	result := map[string]interface{}{"code": 0, "data": rows, "cost": 1}
	for _, batch := range []int{0, 64, 256, 1000} {
		name := map[int]string{0: "whole", 64: "batch64", 256: "batch256", 1000: "batch1000"}[batch]
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				if batch == 0 {
					err = sonic.ConfigStd.NewEncoder(io.Discard).Encode(result)
				} else {
					err = writeDQLJSON(context.Background(), io.Discard, result, batch)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
