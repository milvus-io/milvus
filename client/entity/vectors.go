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
	"encoding/binary"
	"math"
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
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

// FloatVector float32 vector wrapper.
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

// FloatVector float32 vector wrapper.
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
