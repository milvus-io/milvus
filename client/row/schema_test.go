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

package row

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/client/v2/entity"
)

// ArrayRow test case type
type ArrayRow [16]float32

func (ar *ArrayRow) Collection() string  { return "" }
func (ar *ArrayRow) Partition() string   { return "" }
func (ar *ArrayRow) Description() string { return "" }

type Uint8Struct struct {
	Attr uint8
}

type StringArrayStruct struct {
	Vector [8]string
}

type StringSliceStruct struct {
	Vector []string `milvus:"dim:8"`
}

type SliceNoDimStruct struct {
	Vector []float32 `milvus:""`
}

type SliceBadDimStruct struct {
	Vector []float32 `milvus:"dim:str"`
}

type SliceBadDimStruct2 struct {
	Vector []float32 `milvus:"dim:0"`
}

func TestParseSchema(t *testing.T) {
	t.Run("invalid cases", func(t *testing.T) {
		// anonymous struct with default collection name ("") will cause error
		anonymusStruct := struct{}{}
		sch, err := ParseSchema(anonymusStruct)
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// non struct
		arrayRow := ArrayRow([16]float32{})
		sch, err = ParseSchema(&arrayRow)
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// uint8 not supported
		sch, err = ParseSchema(&Uint8Struct{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// string array not supported
		sch, err = ParseSchema(&StringArrayStruct{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// string slice not supported
		sch, err = ParseSchema(&StringSliceStruct{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// slice vector with no dim
		sch, err = ParseSchema(&SliceNoDimStruct{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// slice vector with bad format dim
		sch, err = ParseSchema(&SliceBadDimStruct{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)

		// slice vector with bad format dim 2
		sch, err = ParseSchema(&SliceBadDimStruct2{})
		assert.Nil(t, sch)
		assert.NotNil(t, err)
	})

	t.Run("valid cases", func(t *testing.T) {
		getVectorField := func(schema *entity.Schema) *entity.Field {
			for _, field := range schema.Fields {
				if field.DataType == entity.FieldTypeFloatVector ||
					field.DataType == entity.FieldTypeBinaryVector ||
					field.DataType == entity.FieldTypeBFloat16Vector ||
					field.DataType == entity.FieldTypeFloat16Vector {
					return field
				}
			}
			return nil
		}

		type ValidStruct struct {
			ID     int64 `milvus:"primary_key"`
			Attr1  int8
			Attr2  int16
			Attr3  int32
			Attr4  float32
			Attr5  float64
			Attr6  string
			Vector []float32 `milvus:"dim:128"`
		}
		vs := &ValidStruct{}
		sch, err := ParseSchema(vs)
		assert.Nil(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, "ValidStruct", sch.CollectionName)

		type ValidFp16Struct struct {
			ID     int64 `milvus:"primary_key"`
			Attr1  int8
			Attr2  int16
			Attr3  int32
			Attr4  float32
			Attr5  float64
			Attr6  string
			Vector []byte `milvus:"dim:128;vector_type:fp16"`
		}
		fp16Vs := &ValidFp16Struct{}
		sch, err = ParseSchema(fp16Vs)
		assert.Nil(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, "ValidFp16Struct", sch.CollectionName)
		vectorField := getVectorField(sch)
		assert.Equal(t, entity.FieldTypeFloat16Vector, vectorField.DataType)

		type ValidBf16Struct struct {
			ID     int64 `milvus:"primary_key"`
			Attr1  int8
			Attr2  int16
			Attr3  int32
			Attr4  float32
			Attr5  float64
			Attr6  string
			Vector []byte `milvus:"dim:128;vector_type:bf16"`
		}
		bf16Vs := &ValidBf16Struct{}
		sch, err = ParseSchema(bf16Vs)
		assert.Nil(t, err)
		assert.NotNil(t, sch)
		assert.Equal(t, "ValidBf16Struct", sch.CollectionName)
		vectorField = getVectorField(sch)
		assert.Equal(t, entity.FieldTypeBFloat16Vector, vectorField.DataType)

		type ValidByteStruct struct {
			ID     int64  `milvus:"primary_key"`
			Vector []byte `milvus:"dim:128"`
		}
		vs2 := &ValidByteStruct{}
		sch, err = ParseSchema(vs2)
		assert.Nil(t, err)
		assert.NotNil(t, sch)

		type ValidArrayStruct struct {
			ID     int64 `milvus:"primary_key"`
			Vector [64]float32
		}
		vs3 := &ValidArrayStruct{}
		sch, err = ParseSchema(vs3)
		assert.Nil(t, err)
		assert.NotNil(t, sch)

		type ValidArrayStructByte struct {
			ID     int64   `milvus:"primary_key;auto_id"`
			Data   *string `milvus:"extra:test\\;false"`
			Vector [64]byte
		}
		vs4 := &ValidArrayStructByte{}
		sch, err = ParseSchema(vs4)
		assert.Nil(t, err)
		assert.NotNil(t, sch)

		vs5 := &ValidStructWithNamedTag{}
		sch, err = ParseSchema(vs5)
		assert.Nil(t, err)
		assert.NotNil(t, sch)
		i64f, vecf := false, false
		for _, field := range sch.Fields {
			if field.Name == "id" {
				i64f = true
			}
			if field.Name == "vector" {
				vecf = true
			}
		}

		assert.True(t, i64f)
		assert.True(t, vecf)
	})
}
