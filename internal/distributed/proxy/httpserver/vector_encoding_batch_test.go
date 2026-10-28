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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/require"
)

func TestBase64FloatVectorMixedDimensions(t *testing.T) {
	for _, batchRows := range []int{1, 2, 256} {
		t.Run(fmt.Sprint(batchRows), func(t *testing.T) {
			t.Parallel()
			var scratch []byte
			vectors := [][]float32{
				nil,
				{},
				{1, -2},
				make([]float32, 768),
				{math.Float32frombits(0x80000000)},
				{3},
			}
			for i := range vectors[3] {
				vectors[3][i] = float32(i+batchRows) / 7919
			}
			rows := make([]map[string]interface{}, len(vectors))
			want := make([]map[string]interface{}, len(vectors))
			for i, values := range vectors {
				rows[i] = map[string]interface{}{
					"vector": floatVectorEncoding("base64").floatVector(values, &scratch),
					"nested": []map[string]interface{}{{"vector": floatVectorEncoding("base64").floatVector([]float32{float32(i)}, &scratch)}},
					"text":   "<>&",
				}
				var encoded any
				if values != nil {
					raw := make([]byte, len(values)*4)
					for j, value := range values {
						binary.LittleEndian.PutUint32(raw[j*4:], math.Float32bits(value))
					}
					encoded = base64.StdEncoding.EncodeToString(raw)
				}
				var raw [4]byte
				binary.LittleEndian.PutUint32(raw[:], math.Float32bits(float32(i)))
				want[i] = map[string]interface{}{
					"vector": encoded,
					"nested": []map[string]interface{}{{"vector": base64.StdEncoding.EncodeToString(raw[:])}},
					"text":   "<>&",
				}
			}
			result := map[string]interface{}{"code": 0, "data": rows}
			expected, err := json.Marshal(map[string]interface{}{"code": 0, "data": want})
			require.NoError(t, err)
			for repeat := 0; repeat < 3; repeat++ {
				for _, marshal := range []func(any) ([]byte, error){json.Marshal, sonic.ConfigStd.Marshal} {
					actual, err := marshal(result)
					require.NoError(t, err)
					require.Equal(t, string(expected), string(actual))
				}
				var actual bytes.Buffer
				require.NoError(t, writeDQLJSON(context.Background(), &actual, result, batchRows))
				require.Equal(t, string(expected)+"\n", actual.String())
			}
		})
	}
}

// Measures the production batch encoder after projection. RPCs, projection and
// networking are excluded. Temporary bytes must be allocated anew per response.
func BenchmarkBase64QueryEncoding(b *testing.B) {
	for _, encoding := range []floatVectorEncoding{"array", "base64"} {
		b.Run(string(encoding), func(b *testing.B) {
			var scratch []byte
			values := make([]float32, 768)
			for i := range values {
				values[i] = float32(i+1) / 7919
			}
			rows := make([]map[string]interface{}, 6000)
			for i := range rows {
				rows[i] = map[string]interface{}{"id": i}
				for field := 0; field < 5; field++ {
					rows[i][fmt.Sprintf("v%d", field)] = encoding.floatVector(values, &scratch)
				}
				for field := 0; field < 19; field++ {
					rows[i][fmt.Sprintf("s%d", field)] = field
				}
			}
			result := map[string]interface{}{"code": 0, "data": rows}
			if err := writeDQLJSON(context.Background(), io.Discard, result, dqlJSONBatchRows); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scratch = nil
				if err := writeDQLJSON(context.Background(), io.Discard, result, dqlJSONBatchRows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
