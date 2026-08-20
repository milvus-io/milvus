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

package json

import (
	"bytes"
	"testing"
)

type benchObj struct {
	A string    `json:"a"`
	B int       `json:"b"`
	C float64   `json:"c"`
	D []int     `json:"d"`
	E *benchObj `json:"e,omitempty"`
}

var benchPayload = []byte(`{"a":"hello","b":42,"c":3.14,"d":[1,2,3,4,5]}`)

// baseline marshal bypasses the gate by calling sonic directly.
func baselineMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// baseline unmarshal bypasses the gate by calling sonic directly.
func baselineUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func BenchmarkMarshal_Small_Gated(b *testing.B) {
	o := benchObj{A: "hello", B: 42, C: 3.14, D: []int{1, 2, 3, 4, 5}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Marshal(&o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Small_Baseline(b *testing.B) {
	o := benchObj{A: "hello", B: 42, C: 3.14, D: []int{1, 2, 3, 4, 5}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := baselineMarshal(&o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshal_Small_Gated(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var o benchObj
		if err := Unmarshal(benchPayload, &o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshal_Small_Baseline(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var o benchObj
		if err := baselineUnmarshal(benchPayload, &o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDecoderDecode_Gated(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := NewDecoder(bytes.NewReader(benchPayload))
		var o benchObj
		if err := d.Decode(&o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewDecoderDecode_Baseline(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := json.NewDecoder(bytes.NewReader(benchPayload))
		var o benchObj
		if err := d.Decode(&o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Small_Parallel_Gated(b *testing.B) {
	o := benchObj{A: "hello", B: 42, C: 3.14, D: []int{1, 2, 3, 4, 5}}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Marshal(&o); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMarshal_Small_Parallel_Baseline(b *testing.B) {
	o := benchObj{A: "hello", B: 42, C: 3.14, D: []int{1, 2, 3, 4, 5}}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := baselineMarshal(&o); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMarshal_Small_Parallel_GateDisabled is the steady state after all
// plugin loading finished: the gate costs nothing beyond one atomic load.
func BenchmarkMarshal_Small_Parallel_GateDisabled(b *testing.B) {
	DisableGate()
	defer func() { gateDisabled.Store(false) }()
	o := benchObj{A: "hello", B: 42, C: 3.14, D: []int{1, 2, 3, 4, 5}}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Marshal(&o); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMarshal_Large_Gated(b *testing.B) {
	big := benchBig(2048)
	b.SetBytes(int64(len(big)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Marshal(&big); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Large_Baseline(b *testing.B) {
	big := benchBig(2048)
	b.SetBytes(int64(len(big)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := baselineMarshal(&big); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Large_Parallel_Gated(b *testing.B) {
	big := benchBig(2048)
	b.SetBytes(int64(len(big)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Marshal(&big); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMarshal_Large_Parallel_Baseline(b *testing.B) {
	big := benchBig(2048)
	b.SetBytes(int64(len(big)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := baselineMarshal(&big); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchBig(n int) map[string]any {
	m := make(map[string]any, n)
	for i := 0; i < n; i++ {
		m[benchKey(i)] = benchVal(i)
	}
	return m
}

func benchKey(i int) string {
	return "key_" + string(rune('a'+i%26)) + string(rune('0'+i%10))
}

func benchVal(i int) any {
	return map[string]any{"id": i, "name": "item", "score": float64(i) / 100}
}
