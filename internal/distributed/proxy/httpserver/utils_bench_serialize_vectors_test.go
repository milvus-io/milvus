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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

/*
 * Benchmarkings for different serialization implementations
 * See results: https://github.com/milvus-io/milvus/pull/37556#issuecomment-2491668743
 */

// serializeFloatVectorsBaseline uses []gjson.Result as input and calls json.Unmarshal in multiple times,
// which downgrades the performance
func serializeFloatVectorsBaseline(vectors []gjson.Result, dataType schemapb.DataType, dimension, bytesLen int64) ([][]byte, error) {
	values := make([][]byte, 0)
	for _, vector := range vectors {
		var vectorArray []float32
		err := json.Unmarshal([]byte(vector.String()), &vectorArray)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), err.Error())
		}
		if int64(len(vectorArray)) != dimension {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(),
				fmt.Sprintf("dimension: %d, but length of []float: %d", dimension, len(vectorArray)))
		}
		vectorBytes := typeutil.Float32ArrayToBytes(vectorArray)
		values = append(values, vectorBytes)
	}
	return values, nil
}

// serializeFloatOrByteVectorsBaseline calls json.Unmarshal in multiple times, which downgrades the performance
func serializeFloatOrByteVectorsBaseline(jsonResult gjson.Result, dataType schemapb.DataType, dimension int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	values := make([][]byte, 0)
	for _, vector := range jsonResult.Array() {
		if vector.IsArray() {
			var vectorArray []float32
			err := json.Unmarshal([]byte(vector.Raw), &vectorArray)
			if err != nil {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), err.Error())
			}
			if int64(len(vectorArray)) != dimension {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(),
					fmt.Sprintf("dimension: %d, but length of []float: %d", dimension, len(vectorArray)))
			}
			vectorBytes := fpArrayToBytesFunc(vectorArray)
			values = append(values, vectorBytes)
		} else if vector.Type == gjson.String {
			var vectorArray []byte
			err := json.Unmarshal([]byte(vector.Raw), &vectorArray)
			if err != nil {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), err.Error())
			}
			if int64(len(vectorArray)) != dimension*2 {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], string(vectorArray),
					fmt.Sprintf("dimension: %d, but length of []byte: %d", dimension, len(vectorArray)))
			}
			values = append(values, vectorArray)
		} else {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), "invalid type")
		}
	}
	return values, nil
}

// serializeFloatOrByteVectorsUnmarshalTwice calls Unmarshal twice, which downgrades the performance
// See: https://github.com/milvus-io/milvus/pull/37556#discussion_r1849672721
func serializeFloatOrByteVectorsUnmarshalTwice(vectorStr string, dataType schemapb.DataType, dimension int64, serializeFunc func([]float32) []byte) ([][]byte, error) {
	// try to unmarshal as [][]float32 first to make sure `[[3, 3]]` is [][]float32 instead of [][]byte
	fp32Values := make([][]float32, 0)
	err := json.Unmarshal([]byte(vectorStr), &fp32Values)
	if err == nil {
		values := make([][]byte, 0)
		for _, vectorArray := range fp32Values {
			if int64(len(vectorArray)) != dimension {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], fmt.Sprintf("%v", vectorArray),
					fmt.Sprintf("dimension: %d, but length of []float: %d", dimension, len(vectorArray)))
			}
			vectorBytes := serializeFunc(vectorArray)
			values = append(values, vectorBytes)
		}
		return values, nil
	}
	return serializeByteVectors(vectorStr, dataType, dimension, dimension*2)
}

func generateVectorsStr() string {
	vectors := make([][]float32, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		vector := make([]float32, 0, 128)
		for j := 0; j < 128; j++ {
			vector = append(vector, rand.Float32())
		}
		vectors = append(vectors, vector)
	}
	vectorJSON, _ := json.Marshal(vectors)
	return string(vectorJSON)
}

func generateVectorsJSON() gjson.Result {
	vectorJSON := generateVectorsStr()
	return gjson.Parse(vectorJSON)
}

func generateByteVectorsStr() string {
	vectors := make([][]byte, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		vector := make([]byte, 0, 128*4)
		for j := 0; j < 128*4; j++ {
			vector = append(vector, byte(rand.Intn(256)))
		}
		vectors = append(vectors, vector)
	}
	vectorJSON, _ := json.Marshal(vectors)
	return string(vectorJSON)
}

func generateByteVectorsJSON() gjson.Result {
	vectorJSON := generateByteVectorsStr()
	return gjson.Parse(vectorJSON)
}

func BenchmarkSerialize_FloatVectors_Baseline(b *testing.B) {
	vectorsJSON := generateVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatVectorsBaseline(vectorsJSON.Array(), schemapb.DataType_FloatVector, 128, -1)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatVectors(b *testing.B) {
	vectorsJSON := generateVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatVectors(vectorsJSON.Raw, schemapb.DataType_FloatVector, 128, -1, typeutil.Float32ArrayToBytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatVectors_Float16(b *testing.B) {
	vectorsJSON := generateVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatVectors(vectorsJSON.Raw, schemapb.DataType_Float16Vector, 128, -1, typeutil.Float32ArrayToFloat16Bytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatOrByteVectors_Fp32(b *testing.B) {
	vectorsJSON := generateVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatOrByteVectors(vectorsJSON, schemapb.DataType_Float16Vector, 128, typeutil.Float32ArrayToFloat16Bytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatOrByteVectors_Byte(b *testing.B) {
	vectorsJSON := generateByteVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatOrByteVectors(vectorsJSON, schemapb.DataType_Float16Vector, 256, typeutil.Float32ArrayToFloat16Bytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatOrByteVectors_Fp32_UnmashalTwice(b *testing.B) {
	vectorsJSON := generateVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatOrByteVectorsUnmarshalTwice(vectorsJSON.Raw, schemapb.DataType_Float16Vector, 128, typeutil.Float32ArrayToFloat16Bytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_FloatOrByteVectors_Byte_UnmashalTwice(b *testing.B) {
	vectorsJSON := generateByteVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeFloatOrByteVectorsUnmarshalTwice(vectorsJSON.Raw, schemapb.DataType_Float16Vector, 256, typeutil.Float32ArrayToFloat16Bytes)
		assert.Nil(b, err)
	}
}

func BenchmarkSerialize_ByteVectors(b *testing.B) {
	vectorsJSON := generateByteVectorsJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializeByteVectors(vectorsJSON.Raw, schemapb.DataType_BinaryVector, -1, 512)
		assert.Nil(b, err)
	}
}
