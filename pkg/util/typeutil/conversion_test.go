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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

func TestConversion(t *testing.T) {
	t.Run("TestConvertFloat32", func(t *testing.T) {
		comp := func(f float32) {
			fb := Float32ToBytes(f)
			f1 := BytesToFloat32(fb)
			assert.Less(t, math.Abs(float64(f)-float64(f1)), 0.00001)
		}
		comp(float32(3.14))
		comp(float32(0))
		comp(float32(-139.866))
		comp(float32(math.MaxFloat32))
		comp(float32(-math.MaxFloat32))
	})

	t.Run("TestConvertInt64", func(t *testing.T) {
		comp := func(i int64) {
			ib := Int64ToBytes(i)
			i1, err := BytesToInt64(ib)
			assert.NoError(t, err)
			assert.Equal(t, i, i1)
		}
		comp(int64(314))
		comp(int64(0))
		comp(int64(-8654273))
		comp(int64(math.MaxInt64))
		comp(int64(math.MinInt64))

		_, err := BytesToInt64([]byte("ab"))
		assert.Error(t, err)
	})

	t.Run("TestConvertUint64", func(t *testing.T) {
		comp := func(u uint64) {
			ub := Uint64ToBytes(u)
			u1, err := BytesToUint64(ub)
			assert.NoError(t, err)
			assert.Equal(t, u, u1)
		}
		comp(uint64(314))
		comp(uint64(0))
		comp(uint64(75123348654273))
		comp(uint64(math.MaxUint64))

		_, err := BytesToUint64([]byte("ab"))
		assert.Error(t, err)
	})

	t.Run("TestConvertUint64BigEndian", func(t *testing.T) {
		comp := func(u uint64) {
			ub := Uint64ToBytesBigEndian(u)
			u1, err := BigEndianBytesToUint64(ub)
			assert.NoError(t, err)
			assert.Equal(t, u, u1)
		}
		comp(uint64(314))
		comp(uint64(0))
		comp(uint64(75123348654273))
		comp(uint64(math.MaxUint64))

		_, err := BytesToUint64([]byte("ab"))
		assert.Error(t, err)
	})

	t.Run("TestSliceRemoveDuplicate", func(t *testing.T) {
		ret := SliceRemoveDuplicate(1)
		assert.Equal(t, 0, len(ret))

		arr := []int64{1, 1, 1, 2, 2, 3}
		ret1 := SliceRemoveDuplicate(arr)
		assert.Equal(t, 3, len(ret1))
	})

	t.Run("TestFloat16", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			v := (rand.Float32() - 0.5) * 100
			b := Float32ToFloat16Bytes(v)
			v2 := Float16BytesToFloat32(b)
			log.Info("float16", zap.Float32("v", v), zap.Float32("v2", v2))
			assert.Less(t, math.Abs(float64(v2/v-1)), 0.001)
		}
	})

	t.Run("TestBFloat16", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			v := (rand.Float32() - 0.5) * 100
			b := Float32ToBFloat16Bytes(v)
			v2 := BFloat16BytesToFloat32(b)
			log.Info("bfloat16", zap.Float32("v", v), zap.Float32("v2", v2))
			assert.Less(t, math.Abs(float64(v2/v-1)), 0.01)
		}
	})

	t.Run("TestFloatArrays", func(t *testing.T) {
		parameters := []float32{0.11111, 0.22222}
		assert.Equal(t, "\xa4\x8d\xe3=\xa4\x8dc>", string(Float32ArrayToBytes(parameters)))

		f16vec := Float32ArrayToFloat16Bytes(parameters)
		assert.Equal(t, 4, len(f16vec))
		// \x1c/ is 0.1111, \x1c3 is 0.2222
		assert.Equal(t, "\x1c/\x1c3", string(f16vec))
		assert.Equal(t, "\x1c/", string(Float32ToFloat16Bytes(0.11111)))
		assert.Equal(t, "\x1c3", string(Float32ToFloat16Bytes(0.22222)))

		bf16vec := Float32ArrayToBFloat16Bytes(parameters)
		assert.Equal(t, 4, len(bf16vec))
		assert.Equal(t, "\xe3=c>", string(bf16vec))
		assert.Equal(t, "\xe3=", string(Float32ToBFloat16Bytes(0.11111)))
		assert.Equal(t, "c>", string(Float32ToBFloat16Bytes(0.22222)))
	})
}
