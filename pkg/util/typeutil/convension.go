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

package typeutil

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/x448/float16"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

// Generic Clone for proto message
func Clone[T proto.Message](msg T) T {
	return proto.Clone(msg).(T)
}

// Float32ToBytes converts a float to byte slice.
func Float32ToBytes(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	common.Endian.PutUint32(bytes, bits)

	return bytes
}

// BytesToFloat32 converts a byte slice to float32.
func BytesToFloat32(bytes []byte) float32 {
	bits := common.Endian.Uint32(bytes)

	return math.Float32frombits(bits)
}

// BytesToInt64 converts a byte slice to uint64.
func BytesToInt64(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("failed to convert []byte to int64: invalid data, must 8 bytes, but %d", len(b))
	}

	return int64(common.Endian.Uint64(b)), nil
}

// Int64ToBytes converts uint64 to a byte slice.
func Int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, uint64(v))
	return b
}

// BigEndianBytesToUint64 converts a byte slice (big endian) to uint64.
func BigEndianBytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("failed to convert []byte to uint64: invalid data, must 8 bytes, but %d", len(b))
	}

	// do not use little or common endian for compatibility issues(the msgid used in rocksmq is using this)
	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytesBigEndian converts uint64 to a byte slice(big endian).
func Uint64ToBytesBigEndian(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// BytesToUint64 converts a byte slice to uint64.
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("Failed to convert []byte to uint64: invalid data, must 8 bytes, but %d", len(b))
	}
	return common.Endian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	common.Endian.PutUint64(b, v)
	return b
}

// SliceRemoveDuplicate is used to dedup a Slice
func SliceRemoveDuplicate(a interface{}) (ret []interface{}) {
	if reflect.TypeOf(a).Kind() != reflect.Slice {
		log.Error("input is not slice", zap.String("inputType", fmt.Sprintf("%T", a)))
		return ret
	}

	va := reflect.ValueOf(a)
	for i := 0; i < va.Len(); i++ {
		if i > 0 && reflect.DeepEqual(va.Index(i-1).Interface(), va.Index(i).Interface()) {
			continue
		}
		ret = append(ret, va.Index(i).Interface())
	}

	return ret
}

func Float32ToFloat16Bytes(f float32) []byte {
	ret := make([]byte, 2)
	common.Endian.PutUint16(ret[:], float16.Fromfloat32(f).Bits())
	return ret
}

func Float16BytesToFloat32(b []byte) float32 {
	return float16.Frombits(common.Endian.Uint16(b)).Float32()
}

func Float16BytesToFloat32Vector(b []byte) []float32 {
	dim := len(b) / 2
	vec := make([]float32, 0, dim)
	for j := 0; j < dim; j++ {
		vec = append(vec, Float16BytesToFloat32(b[j*2:]))
	}
	return vec
}

func Float32ToBFloat16Bytes(f float32) []byte {
	ret := make([]byte, 2)
	common.Endian.PutUint16(ret[:], uint16(math.Float32bits(f)>>16))
	return ret
}

func BFloat16BytesToFloat32(b []byte) float32 {
	return math.Float32frombits(uint32(common.Endian.Uint16(b)) << 16)
}

func BFloat16BytesToFloat32Vector(b []byte) []float32 {
	dim := len(b) / 2
	vec := make([]float32, 0, dim)
	for j := 0; j < dim; j++ {
		vec = append(vec, BFloat16BytesToFloat32(b[j*2:]))
	}
	return vec
}

func SparseFloatBytesToMap(b []byte) map[uint32]float32 {
	elemCount := len(b) / 8
	values := make(map[uint32]float32)
	for j := 0; j < elemCount; j++ {
		idx := common.Endian.Uint32(b[j*8:])
		f := BytesToFloat32(b[j*8+4:])
		values[idx] = f
	}
	return values
}

// Float32ArrayToBytes serialize vector into byte slice, used in search placeholder
// LittleEndian is used for convention
func Float32ArrayToBytes(fv []float32) []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

// Float32ArrayToFloat16Bytes converts float32 vector `fv` to float16 vector
func Float32ArrayToFloat16Bytes(fv []float32) []byte {
	data := make([]byte, 0, 2*len(fv)) // float16 occupies 2 bytes
	for _, f := range fv {
		data = append(data, Float32ToFloat16Bytes(f)...)
	}
	return data
}

// Float32ArrayToBFloat16Bytes converts float32 vector `fv` to bfloat16 vector
func Float32ArrayToBFloat16Bytes(fv []float32) []byte {
	data := make([]byte, 0, 2*len(fv)) // bfloat16 occupies 2 bytes
	for _, f := range fv {
		data = append(data, Float32ToBFloat16Bytes(f)...)
	}
	return data
}

// Int8ArrayToBytes serialize vector into byte slice, used in search placeholder
// LittleEndian is used for convention
func Int8ArrayToBytes(iv []int8) []byte {
	data := make([]byte, 0, len(iv))
	for _, i := range iv {
		data = append(data, byte(i))
	}
	return data
}
