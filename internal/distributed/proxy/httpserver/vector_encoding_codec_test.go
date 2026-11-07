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
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/require"
)

func TestBase64FloatVectorAllocationBudget(t *testing.T) {
	values := make([]float32, 768)
	var scratch []byte
	vector := floatVectorEncoding("base64").floatVector(values, &scratch).(encoding.TextMarshaler)
	allocs := testing.AllocsPerRun(100, func() {
		text, err := vector.MarshalText()
		if err != nil || len(text) != 4096 {
			t.Fatal("invalid encoded vector", err)
		}
	})
	require.Zero(t, allocs, "encoding another vector must not allocate another temporary byte buffer")
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
			encoded, err := marshal(floatVectorEncoding("base64").floatVector(tc.input, &scratch))
			require.NoError(t, err)
			require.Equal(t, tc.want, string(encoded))
		}
	}
}

func TestBase64FloatVectorPreservesFiniteBits(t *testing.T) {
	values := []float32{
		0, math.Float32frombits(0x80000000), math.SmallestNonzeroFloat32,
		-math.SmallestNonzeroFloat32, math.MaxFloat32, -math.MaxFloat32, 0.1,
	}
	var scratch []byte
	encoded, err := sonic.ConfigStd.Marshal(floatVectorEncoding("base64").floatVector(values, &scratch))
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
		_, err := sonic.ConfigStd.Marshal(floatVectorEncoding("base64").floatVector([]float32{1, value}, &scratch))
		require.Error(t, err, "Base64 must not silently make an invalid vector result encodable")
	}
}

func TestFloatVectorEncodingValues(t *testing.T) {
	encoding, err := parseFloatVectorEncoding(nil)
	require.NoError(t, err)
	require.Empty(t, encoding)
	for _, tc := range []struct{ input, want string }{
		{"", ""}, {"array", "array"}, {"base64", "base64"}, {" \tbase64\t ", "base64"}, {"\t ", ""},
	} {
		header := http.Header{}
		header.Set(HTTPHeaderFloatVectorEncoding, tc.input)
		encoding, err := parseFloatVectorEncoding(header)
		require.NoError(t, err)
		require.Equal(t, floatVectorEncoding(tc.want), encoding)
	}
}

// This isolates the actual TextMarshaler used by the response path. It includes
// Sonic's string quoting and HTML escaping overhead.
func BenchmarkBase64FloatVectorEncoding(b *testing.B) {
	for _, count := range []int{1, 256} {
		for _, encoding := range []floatVectorEncoding{"array", "base64"} {
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
					scratch = nil // Reuse within one response, never across responses.
					if err := sonic.ConfigStd.NewEncoder(io.Discard).Encode(result); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
