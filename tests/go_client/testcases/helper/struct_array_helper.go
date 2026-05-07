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

package helper

import (
	"fmt"
	"math/rand"

	"github.com/milvus-io/milvus/client/v2/entity"
)

// Mirrored constants from tests/python_client/milvus_client/test_milvus_client_struct_array.py
const (
	StructArrayPrefix          = "struct_array"
	StructArrayDefaultDim      = 128
	StructArrayDefaultCapacity = 100
	// default_nb in python_client/common/common_type.py is 3000; we stick to a smaller number
	// for Go SDK tests so a single test stays under ~5s while still exercising the code paths.
	StructArrayDefaultNb = 500
)

// StructArraySchemaOption lets tests customize the canonical struct-array schema.
type StructArraySchemaOption struct {
	Dim             int
	Capacity        int
	IncludeClipStr  bool // add clip_str sub-field (default true)
	IncludeEmb1     bool // add clip_embedding1 sub-field (default true)
	IncludeEmb2     bool // add clip_embedding2 sub-field (default true)
	CollectionName  string
	NormalMaxLength int64 // max_length for clip_str; defaults to 65535
}

// DefaultStructArraySchemaOption returns the canonical struct array schema option matching the
// Python tests: id (Int64 PK), normal_vector (FloatVector), clips (struct array with
// clip_str + clip_embedding1 + clip_embedding2).
func DefaultStructArraySchemaOption(name string) StructArraySchemaOption {
	return StructArraySchemaOption{
		Dim:             StructArrayDefaultDim,
		Capacity:        StructArrayDefaultCapacity,
		IncludeClipStr:  true,
		IncludeEmb1:     true,
		IncludeEmb2:     true,
		CollectionName:  name,
		NormalMaxLength: 65535,
	}
}

// CreateStructArraySchema builds the canonical struct-array schema used across the Python tests.
// The returned schema and the returned StructSchema are paired - the StructSchema must be passed
// to WithStructArrayColumn when inserting struct array data.
func CreateStructArraySchema(opt StructArraySchemaOption) (*entity.Schema, *entity.StructSchema) {
	if opt.Dim == 0 {
		opt.Dim = StructArrayDefaultDim
	}
	if opt.Capacity == 0 {
		opt.Capacity = StructArrayDefaultCapacity
	}
	if opt.NormalMaxLength == 0 {
		opt.NormalMaxLength = 65535
	}

	structSchema := entity.NewStructSchema()
	if opt.IncludeClipStr {
		structSchema.WithField(entity.NewField().WithName("clip_str").
			WithDataType(entity.FieldTypeVarChar).WithMaxLength(opt.NormalMaxLength))
	}
	if opt.IncludeEmb1 {
		structSchema.WithField(entity.NewField().WithName("clip_embedding1").
			WithDataType(entity.FieldTypeFloatVector).WithDim(int64(opt.Dim)))
	}
	if opt.IncludeEmb2 {
		structSchema.WithField(entity.NewField().WithName("clip_embedding2").
			WithDataType(entity.FieldTypeFloatVector).WithDim(int64(opt.Dim)))
	}

	schema := entity.NewSchema().WithName(opt.CollectionName).
		WithField(entity.NewField().WithName("id").
			WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("normal_vector").
			WithDataType(entity.FieldTypeFloatVector).WithDim(int64(opt.Dim))).
		WithField(entity.NewField().WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(int64(opt.Capacity)).
			WithStructSchema(structSchema))

	return schema, structSchema
}

// StructArrayTestData carries the generated columns ready to feed into
// WithInt64Column / WithFloatVectorColumn / WithStructArrayColumn.
type StructArrayTestData struct {
	IDs           []int64
	NormalVectors [][]float32
	ClipsRows     []map[string]any
	Dim           int
}

// GenerateStructArrayData generates numRows rows matching the Python generator:
//   - id: sequential int64
//   - normal_vector: random float vector of `dim`
//   - clips: array of 1..min(capacity,20) struct elements with clip_str + clip_embedding1/2
//
// Pass includeClipStr/IncludeEmb1/IncludeEmb2 = false to omit the corresponding sub-field.
func GenerateStructArrayData(numRows int, opt StructArraySchemaOption) StructArrayTestData {
	if opt.Dim == 0 {
		opt.Dim = StructArrayDefaultDim
	}
	if opt.Capacity == 0 {
		opt.Capacity = StructArrayDefaultCapacity
	}
	maxLen := opt.Capacity
	if maxLen > 20 {
		maxLen = 20
	}

	data := StructArrayTestData{
		IDs:           make([]int64, numRows),
		NormalVectors: make([][]float32, numRows),
		ClipsRows:     make([]map[string]any, numRows),
		Dim:           opt.Dim,
	}

	for i := 0; i < numRows; i++ {
		data.IDs[i] = int64(i)
		data.NormalVectors[i] = RandFloatVector(opt.Dim)

		arrLen := 1 + rand.Intn(maxLen)
		strs := make([]string, 0, arrLen)
		emb1 := make([][]float32, 0, arrLen)
		emb2 := make([][]float32, 0, arrLen)
		for j := 0; j < arrLen; j++ {
			strs = append(strs, fmtItem(i, j))
			emb1 = append(emb1, RandFloatVector(opt.Dim))
			emb2 = append(emb2, RandFloatVector(opt.Dim))
		}
		row := map[string]any{}
		if opt.IncludeClipStr {
			row["clip_str"] = strs
		}
		if opt.IncludeEmb1 {
			row["clip_embedding1"] = emb1
		}
		if opt.IncludeEmb2 {
			row["clip_embedding2"] = emb2
		}
		data.ClipsRows[i] = row
	}
	return data
}

// RandFloatVector returns a random float32 vector of the given dimension.
func RandFloatVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rand.Float32()
	}
	return v
}

func fmtItem(i, j int) string {
	// "item_{i}_{j}" — match Python generator for diagnostic parity.
	return fmt.Sprintf("item_%d_%d", i, j)
}
