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

package entity

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectors(t *testing.T) {
	dim := rand.Intn(127) + 1

	t.Run("test float vector", func(t *testing.T) {
		raw := make([]float32, dim)
		for i := 0; i < dim; i++ {
			raw[i] = rand.Float32()
		}

		fv := FloatVector(raw)
		assert.Equal(t, dim, fv.Dim())
		assert.Equal(t, dim*4, len(fv.Serialize()))

		var fvConverted FloatVector

		fp16v := fv.ToFloat16Vector()
		assert.Equal(t, dim, fp16v.Dim())
		assert.Equal(t, dim*2, len(fp16v.Serialize()))
		fvConverted = fp16v.ToFloat32Vector()
		assert.Equal(t, dim, fvConverted.Dim())
		assert.Equal(t, dim*4, len(fvConverted.Serialize()))

		bf16v := fv.ToBFloat16Vector()
		assert.Equal(t, dim, bf16v.Dim())
		assert.Equal(t, dim*2, len(bf16v.Serialize()))
		fvConverted = bf16v.ToFloat32Vector()
		assert.Equal(t, dim, fvConverted.Dim())
		assert.Equal(t, dim*4, len(fvConverted.Serialize()))
	})

	t.Run("test fp32 <-> fp16/bf16 vector conversion", func(t *testing.T) {
		raw := make([]float32, dim)
		for i := 0; i < dim; i++ {
			raw[i] = rand.Float32() // rand result [0.1, 1.0)
		}

		fv := FloatVector(raw)
		fp16v := fv.ToFloat16Vector()
		bf16v := fv.ToBFloat16Vector()

		assert.Equal(t, dim, fp16v.Dim())
		assert.Equal(t, dim*2, len(fp16v.Serialize()))
		assert.Equal(t, dim, bf16v.Dim())
		assert.Equal(t, dim*2, len(bf16v.Serialize()))

		// TODO calculate max precision loss
		maxDelta := float64(0.4)

		fp32vFromfp16v := fp16v.ToFloat32Vector()
		for i := 0; i < dim; i++ {
			assert.InDelta(t, fv[i], fp32vFromfp16v[i], maxDelta)
		}

		fp32vFrombf16v := bf16v.ToFloat32Vector()
		for i := 0; i < dim; i++ {
			assert.InDelta(t, fp32vFromfp16v[i], fp32vFrombf16v[i], maxDelta)
		}
	})

	t.Run("test binary vector", func(t *testing.T) {
		raw := make([]byte, dim)
		_, err := rand.Read(raw)
		assert.Nil(t, err)

		bv := BinaryVector(raw)

		assert.Equal(t, dim*8, bv.Dim())
		assert.ElementsMatch(t, raw, bv.Serialize())
	})

	t.Run("test int8 vector", func(t *testing.T) {
		raw := make([]int8, dim)
		for i := 0; i < dim; i++ {
			raw[i] = int8(rand.Intn(256) - 128)
		}

		iv := Int8Vector(raw)
		assert.Equal(t, dim, iv.Dim())
		assert.Equal(t, dim, len(iv.Serialize()))
	})
}
