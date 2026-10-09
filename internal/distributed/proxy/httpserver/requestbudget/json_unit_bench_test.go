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
	stdjson "encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/sonic/ast"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus/internal/json"
)

func BenchmarkDecodeDataRows(b *testing.B) {
	vector := make([]float64, 128)
	for i := range vector {
		vector[i] = float64(i) / 128
	}
	rows := make([]map[string]any, 8192)
	for i := range rows {
		rows[i] = map[string]any{"id": i, "vector": vector}
	}
	body, err := json.Marshal(map[string]any{"collectionName": "bench", "data": rows})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("whole-Sonic", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var req struct {
				CollectionName string           `json:"collectionName"`
				Data           []map[string]any `json:"data"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("rowwise-Sonic", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := DecodeDataRows(context.Background(), body, MaxJSONUnitBytes); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("gjson-scan-only", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := gjson.GetBytes(body, "data")
			result.ForEach(func(_, _ gjson.Result) bool { return true })
		}
	})
	b.Run("gjson-get-only", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = gjson.GetBytes(body, "data")
		}
	})
	b.Run("std-json-valid", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !stdjson.Valid(body) {
				b.Fatal("invalid benchmark body")
			}
		}
	})
	b.Run("sonic-ast-scan-only", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			parser := ast.NewParser(string(body))
			root, parseErr := parser.Parse()
			if parseErr != 0 {
				b.Fatal(parseErr)
			}
			data := root.Get("data")
			if err := data.ForEach(func(_ ast.Sequence, value *ast.Node) bool {
				_, err := value.Raw()
				return err == nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkJSONUnitSonic measures a single accepted value, the work that
// cannot be interrupted without changing Sonic itself. These measurements
// guide a separate per-unit capacity decision; they are not timeout tests.
func BenchmarkJSONUnitSonic(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 4 << 20, 8 << 20} {
		row := map[string]string{"text": strings.Repeat("x", size)}
		encoded, err := json.Marshal(row)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("encode-%dB", size), func(b *testing.B) {
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := json.Marshal(row); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("decode-%dB", size), func(b *testing.B) {
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var got map[string]string
				if err := json.Unmarshal(encoded, &got); err != nil {
					b.Fatal(err)
				}
			}
		})
		vector := make([]float64, size/8)
		for i := range vector {
			vector[i] = float64(i%1000) / 1000
		}
		vectorJSON, err := json.Marshal(vector)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("encode-vector-%dB", size), func(b *testing.B) {
			b.SetBytes(int64(len(vectorJSON)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := json.Marshal(vector); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("decode-vector-%dB", size), func(b *testing.B) {
			b.SetBytes(int64(len(vectorJSON)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var got []float64
				if err := json.Unmarshal(vectorJSON, &got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
