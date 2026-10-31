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
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/require"
)

// Repeated vectors must not allocate a fresh raw/Base64 buffer after warm-up.
func TestBase64FloatVectorReusesBuffer(t *testing.T) {
	values := make([]float32, 768)
	var scratch []byte
	vector := outputVectorEncoding("base64").floatVector(values, &scratch).(encoding.TextMarshaler)
	allocs := testing.AllocsPerRun(100, func() {
		text, err := vector.MarshalText()
		if err != nil || len(text) != 4096 {
			t.Fatal("invalid encoded vector", err)
		}
	})
	require.Zero(t, allocs, "warmed vector encoding must reuse its temporary storage")
}

func TestBase64FloatVectorSharedScratch(t *testing.T) {
	// Each parallel case owns its scratch. Different fields and batches share it
	// only within that result, including when a larger vector grows the buffer.
	for _, batchRows := range []int{1, 2, 256} {
		t.Run(fmt.Sprint(batchRows), func(t *testing.T) {
			t.Parallel()
			var scratch []byte
			vectors := [][]float32{nil, {}, {1, -2}, make([]float32, 768), {math.Float32frombits(0x80000000)}, {3}}
			for i := range vectors[3] {
				vectors[3][i] = float32(i) / 7919
			}
			rows := make([]map[string]interface{}, len(vectors))
			want := make([]map[string]interface{}, len(vectors))
			for i, values := range vectors {
				rows[i] = map[string]interface{}{"vector": outputVectorEncoding("base64").floatVector(values, &scratch), "text": "<>&"}
				var text any
				if values != nil {
					raw := make([]byte, len(values)*4)
					for j, value := range values {
						binary.LittleEndian.PutUint32(raw[j*4:], math.Float32bits(value))
					}
					text = base64.StdEncoding.EncodeToString(raw)
				}
				want[i] = map[string]interface{}{"vector": text, "text": "<>&"}
			}
			result := map[string]interface{}{"code": 0, "data": rows}
			expected, err := json.Marshal(map[string]interface{}{"code": 0, "data": want})
			require.NoError(t, err)
			for _, marshal := range []func(any) ([]byte, error){json.Marshal, sonic.ConfigStd.Marshal} {
				actual, err := marshal(result)
				require.NoError(t, err)
				require.Equal(t, string(expected), string(actual))
			}
			var actual bytes.Buffer
			require.NoError(t, writeDQLJSON(context.Background(), &actual, result, batchRows))
			require.Equal(t, string(expected)+"\n", actual.String())
		})
	}
}

// Includes the production batch encoder and its request-owned Sonic buffers.
func BenchmarkBase64QueryBufferReuse(b *testing.B) {
	for _, encoding := range []outputVectorEncoding{"array", "base64"} {
		b.Run(string(encoding), func(b *testing.B) {
			var scratch []byte
			rows := make([]map[string]interface{}, 6000)
			values := make([]float32, 768)
			for i := range values {
				values[i] = float32(i+1) / 7919
			}
			for i := range rows {
				rows[i] = map[string]interface{}{"id": i, "text": "<>&"}
				for j := 0; j < 5; j++ {
					rows[i][fmt.Sprintf("v%d", j)] = encoding.floatVector(values, &scratch)
				}
			}
			result := map[string]interface{}{"code": 0, "data": rows}
			if err := writeDQLJSON(context.Background(), io.Discard, result, 256); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scratch = nil // Reuse within a request, never across requests.
				if err := writeDQLJSON(context.Background(), io.Discard, result, 256); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestBase64FloatVectorWireFormat(t *testing.T) {
	for _, marshal := range []func(any) ([]byte, error){json.Marshal, sonic.ConfigStd.Marshal} {
		for _, tc := range []struct {
			input []float32
			want  string
		}{
			{nil, `null`},
			{[]float32{}, `""`},
			{[]float32{1}, `"AACAPw=="`},
			{[]float32{1, -2}, `"AACAPwAAAMA="`},
			{[]float32{1, -2, 0}, `"AACAPwAAAMAAAAAA"`},
		} {
			var scratch []byte
			encoded, err := marshal(outputVectorEncoding("base64").floatVector(tc.input, &scratch))
			require.NoError(t, err)
			require.Equal(t, tc.want, string(encoded))
		}
	}
}

func TestBase64FloatVectorPreservesFiniteBits(t *testing.T) {
	values := []float32{0, math.Float32frombits(0x80000000), math.SmallestNonzeroFloat32,
		-math.SmallestNonzeroFloat32, math.MaxFloat32, -math.MaxFloat32, 0.1}
	var scratch []byte
	encoded, err := sonic.ConfigStd.Marshal(outputVectorEncoding("base64").floatVector(values, &scratch))
	require.NoError(t, err)
	var text string
	require.NoError(t, json.Unmarshal(encoded, &text))
	decoded, err := base64.StdEncoding.DecodeString(text)
	require.NoError(t, err)
	require.Len(t, decoded, 4*len(values))
	for i, value := range values {
		require.Equal(t, math.Float32bits(value), binary.LittleEndian.Uint32(decoded[i*4:]))
	}
}

func TestBase64FloatVectorRejectsNonfinite(t *testing.T) {
	for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
		var scratch []byte
		_, err := sonic.ConfigStd.Marshal(outputVectorEncoding("base64").floatVector([]float32{1, value}, &scratch))
		require.Error(t, err, "Base64 must not silently make an invalid vector result encodable")
	}
}

func TestOutputVectorEncodingValues(t *testing.T) {
	for _, input := range []string{`""`, `"array"`, `"base64"`, `null`} {
		var encoding outputVectorEncoding
		require.NoError(t, sonic.ConfigStd.UnmarshalFromString(input, &encoding))
	}
	for _, input := range []string{`"BASE64"`, `"gzip"`, `true`, `123`, `{}`, `[]`} {
		var encoding outputVectorEncoding
		require.Error(t, sonic.ConfigStd.UnmarshalFromString(input, &encoding), input)
	}
}

// This isolates the actual TextMarshaler used by the response path. It includes
// Sonic's string quoting and HTML escaping overhead.
func BenchmarkBase64FloatVectorEncoding(b *testing.B) {
	for _, count := range []int{1, 256} {
		for _, encoding := range []outputVectorEncoding{"array", "base64"} {
			b.Run(fmt.Sprintf("rows%d/%s", count, encoding), func(b *testing.B) {
				var scratch []byte
				rng := rand.New(rand.NewSource(42))
				rows := make([]map[string]interface{}, count)
				for i := range rows {
					rows[i] = map[string]interface{}{"id": i, "text": "<>&"}
					for field := 0; field < 5; field++ {
						values := make([]float32, 768)
						for j := range values {
							values[j] = rng.Float32()*2 - 1
						}
						rows[i][fmt.Sprintf("v%d", field)] = encoding.floatVector(values, &scratch)
					}
				}
				result := map[string]interface{}{"code": 0, "data": rows}
				// Warm up JIT/type caches outside the measured loop.
				if err := sonic.ConfigStd.NewEncoder(io.Discard).Encode(result); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scratch = nil
					if err := sonic.ConfigStd.NewEncoder(io.Discard).Encode(result); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
