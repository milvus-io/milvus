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

func TestVectorArrays(t *testing.T) {
	const dim = 4
	const rows = 3

	t.Run("float vector array", func(t *testing.T) {
		fa := FloatVectorArray{
			FloatVector{1, 2, 3, 4},
			FloatVector{5, 6, 7, 8},
			FloatVector{9, 10, 11, 12},
		}
		assert.Equal(t, FieldTypeFloatVector, fa.FieldType())
		assert.Equal(t, dim, fa.Dim())
		// 3 vectors * dim floats * 4 bytes/float
		assert.Equal(t, rows*dim*4, len(fa.Serialize()))

		var empty FloatVectorArray
		assert.Equal(t, 0, empty.Dim())
		assert.Nil(t, empty.Serialize())
	})

	t.Run("float16 vector array", func(t *testing.T) {
		raw := make([]byte, dim*2)
		fa := Float16VectorArray{Float16Vector(raw), Float16Vector(raw)}
		assert.Equal(t, FieldTypeFloat16Vector, fa.FieldType())
		assert.Equal(t, dim, fa.Dim())
		assert.Equal(t, 2*dim*2, len(fa.Serialize()))
	})

	t.Run("bfloat16 vector array", func(t *testing.T) {
		raw := make([]byte, dim*2)
		fa := BFloat16VectorArray{BFloat16Vector(raw), BFloat16Vector(raw)}
		assert.Equal(t, FieldTypeBFloat16Vector, fa.FieldType())
		assert.Equal(t, dim, fa.Dim())
		assert.Equal(t, 2*dim*2, len(fa.Serialize()))
	})

	t.Run("binary vector array", func(t *testing.T) {
		// binary dim is bits; use 2 bytes -> 16 bits.
		raw := make([]byte, 2)
		fa := BinaryVectorArray{BinaryVector(raw), BinaryVector(raw)}
		assert.Equal(t, FieldTypeBinaryVector, fa.FieldType())
		assert.Equal(t, 16, fa.Dim())
		assert.Equal(t, 2*2, len(fa.Serialize()))
	})

	t.Run("int8 vector array", func(t *testing.T) {
		raw := make([]int8, dim)
		fa := Int8VectorArray{Int8Vector(raw), Int8Vector(raw), Int8Vector(raw)}
		assert.Equal(t, FieldTypeInt8Vector, fa.FieldType())
		assert.Equal(t, dim, fa.Dim())
		assert.Equal(t, rows*dim, len(fa.Serialize()))
	})

	t.Run("empty arrays report zero dim and nil serialize", func(t *testing.T) {
		assert.Equal(t, 0, FloatVectorArray(nil).Dim())
		assert.Equal(t, 0, Float16VectorArray(nil).Dim())
		assert.Equal(t, 0, BFloat16VectorArray(nil).Dim())
		assert.Equal(t, 0, BinaryVectorArray(nil).Dim())
		assert.Equal(t, 0, Int8VectorArray(nil).Dim())

		assert.Nil(t, FloatVectorArray(nil).Serialize())
		assert.Nil(t, Float16VectorArray(nil).Serialize())
		assert.Nil(t, BFloat16VectorArray(nil).Serialize())
		assert.Nil(t, BinaryVectorArray(nil).Serialize())
		assert.Nil(t, Int8VectorArray(nil).Serialize())
	})
}

func TestVectorFieldType(t *testing.T) {
	// FieldType() for each Vector type is used by vector2Placeholder to pick the placeholder slot.
	assert.Equal(t, FieldTypeFloatVector, FloatVector{}.FieldType())
	assert.Equal(t, FieldTypeFloat16Vector, Float16Vector{}.FieldType())
	assert.Equal(t, FieldTypeBFloat16Vector, BFloat16Vector{}.FieldType())
	assert.Equal(t, FieldTypeBinaryVector, BinaryVector{}.FieldType())
	assert.Equal(t, FieldTypeInt8Vector, Int8Vector{}.FieldType())
}

func TestTextVector(t *testing.T) {
	txt := Text("hello")
	assert.Equal(t, 0, txt.Dim())
	assert.Equal(t, FieldTypeVarChar, txt.FieldType())
	assert.Equal(t, []byte("hello"), txt.Serialize())
}
