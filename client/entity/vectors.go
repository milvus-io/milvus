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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Vector interface vector used int search
type Vector interface {
	Dim() int
	Serialize() []byte
	FieldType() FieldType
}

// FloatVector float32 vector wrapper.
type FloatVector []float32

// Dim returns vector dimension.
func (fv FloatVector) Dim() int {
	return len(fv)
}

// entity.FieldType returns coresponding field type.
func (fv FloatVector) FieldType() FieldType {
	return FieldTypeFloatVector
}

// Serialize serializes vector into byte slice, used in search placeholder
// LittleEndian is used for convention
func (fv FloatVector) Serialize() []byte {
	return typeutil.Float32ArrayToBytes(fv)
}

func (fv FloatVector) ToFloat16Vector() Float16Vector {
	return typeutil.Float32ArrayToFloat16Bytes(fv)
}

// SerializeToBFloat16Bytes serializes vector into bfloat16 byte slice,
// used in search placeholder
func (fv FloatVector) ToBFloat16Vector() BFloat16Vector {
	return typeutil.Float32ArrayToBFloat16Bytes(fv)
}

// Float16Vector float16 vector wrapper.
type Float16Vector []byte

// Dim returns vector dimension.
func (fv Float16Vector) Dim() int {
	return len(fv) / 2
}

// entity.FieldType returns coresponding field type.
func (fv Float16Vector) FieldType() FieldType {
	return FieldTypeFloat16Vector
}

func (fv Float16Vector) Serialize() []byte {
	return fv
}

func (fv Float16Vector) ToFloat32Vector() FloatVector {
	return typeutil.Float16BytesToFloat32Vector(fv)
}

// BFloat16Vector bfloat16 vector wrapper.
type BFloat16Vector []byte

// Dim returns vector dimension.
func (fv BFloat16Vector) Dim() int {
	return len(fv) / 2
}

// entity.FieldType returns coresponding field type.
func (fv BFloat16Vector) FieldType() FieldType {
	return FieldTypeBFloat16Vector
}

func (fv BFloat16Vector) Serialize() []byte {
	return fv
}

func (fv BFloat16Vector) ToFloat32Vector() FloatVector {
	return typeutil.BFloat16BytesToFloat32Vector(fv)
}

// BinaryVector []byte vector wrapper
type BinaryVector []byte

// Dim return vector dimension, note that binary vector is bits count
func (bv BinaryVector) Dim() int {
	return 8 * len(bv)
}

// Serialize just return bytes
func (bv BinaryVector) Serialize() []byte {
	return bv
}

// entity.FieldType returns coresponding field type.
func (bv BinaryVector) FieldType() FieldType {
	return FieldTypeBinaryVector
}

type Text string

// Dim returns vector dimension.
func (t Text) Dim() int {
	return 0
}

// entity.FieldType returns coresponding field type.
func (t Text) FieldType() FieldType {
	return FieldTypeVarChar
}

func (t Text) Serialize() []byte {
	return []byte(t)
}

// Int8Vector []int8 vector wrapper
type Int8Vector []int8

// Dim return vector dimension
func (iv Int8Vector) Dim() int {
	return len(iv)
}

// Serialize just return bytes
func (iv Int8Vector) Serialize() []byte {
	return typeutil.Int8ArrayToBytes(iv)
}

// entity.FieldType returns coresponding field type.
func (iv Int8Vector) FieldType() FieldType {
	return FieldTypeInt8Vector
}

// serializeVectorArray concatenates inner vector bytes for EmbList placeholder values.
func serializeVectorArray[V Vector](vs []V) []byte {
	if len(vs) == 0 {
		return nil
	}
	out := make([]byte, 0, len(vs)*len(vs[0].Serialize()))
	for _, v := range vs {
		out = append(out, v.Serialize()...)
	}
	return out
}

// FloatVectorArray groups multiple float vectors into one search query, used for MAX_SIM-style
// search against ArrayOfVector sub-fields of struct array (e.g. anns_field="clips[clip_emb]").
// Each call to Search receives one or more `Vector` items; pass FloatVectorArray instead of
// FloatVector when you want each query slot to carry a list of vectors (the EmbeddingList
// equivalent in pymilvus).
type FloatVectorArray []FloatVector

// Dim returns the dim of the inner vectors (assumed uniform).
func (fa FloatVectorArray) Dim() int {
	if len(fa) == 0 {
		return 0
	}
	return fa[0].Dim()
}

// FieldType returns the underlying float vector field type so the read path can pick the right
// EmbList placeholder type.
func (fa FloatVectorArray) FieldType() FieldType {
	return FieldTypeFloatVector
}

// Serialize concatenates the bytes of each inner vector, matching the format the server expects
// for EmbListFloatVector placeholder values.
func (fa FloatVectorArray) Serialize() []byte {
	return serializeVectorArray(fa)
}

// Float16VectorArray groups multiple float16 vectors for EmbListFloat16Vector search.
type Float16VectorArray []Float16Vector

func (fa Float16VectorArray) Dim() int {
	if len(fa) == 0 {
		return 0
	}
	return fa[0].Dim()
}

func (fa Float16VectorArray) FieldType() FieldType {
	return FieldTypeFloat16Vector
}

func (fa Float16VectorArray) Serialize() []byte {
	return serializeVectorArray(fa)
}

// BFloat16VectorArray groups multiple bfloat16 vectors for EmbListBFloat16Vector search.
type BFloat16VectorArray []BFloat16Vector

func (fa BFloat16VectorArray) Dim() int {
	if len(fa) == 0 {
		return 0
	}
	return fa[0].Dim()
}

func (fa BFloat16VectorArray) FieldType() FieldType {
	return FieldTypeBFloat16Vector
}

func (fa BFloat16VectorArray) Serialize() []byte {
	return serializeVectorArray(fa)
}

// BinaryVectorArray groups multiple binary vectors for EmbListBinaryVector search.
type BinaryVectorArray []BinaryVector

func (fa BinaryVectorArray) Dim() int {
	if len(fa) == 0 {
		return 0
	}
	return fa[0].Dim()
}

func (fa BinaryVectorArray) FieldType() FieldType {
	return FieldTypeBinaryVector
}

func (fa BinaryVectorArray) Serialize() []byte {
	return serializeVectorArray(fa)
}

// Int8VectorArray groups multiple int8 vectors for EmbListInt8Vector search.
type Int8VectorArray []Int8Vector

func (fa Int8VectorArray) Dim() int {
	if len(fa) == 0 {
		return 0
	}
	return fa[0].Dim()
}

func (fa Int8VectorArray) FieldType() FieldType {
	return FieldTypeInt8Vector
}

func (fa Int8VectorArray) Serialize() []byte {
	return serializeVectorArray(fa)
}
