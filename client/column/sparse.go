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

package column

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

var _ (Column) = (*ColumnSparseFloatVector)(nil)

type ColumnSparseFloatVector struct {
	ColumnBase

	vectors []entity.SparseEmbedding
	name    string
}

// Name returns column name.
func (c *ColumnSparseFloatVector) Name() string {
	return c.name
}

// Type returns column FieldType.
func (c *ColumnSparseFloatVector) Type() entity.FieldType {
	return entity.FieldTypeSparseVector
}

// Len returns column values length.
func (c *ColumnSparseFloatVector) Len() int {
	return len(c.vectors)
}

// Get returns value at index as interface{}.
func (c *ColumnSparseFloatVector) Get(idx int) (interface{}, error) {
	if idx < 0 || idx >= c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.vectors[idx], nil
}

// ValueByIdx returns value of the provided index
// error occurs when index out of range
func (c *ColumnSparseFloatVector) ValueByIdx(idx int) (entity.SparseEmbedding, error) {
	var r entity.SparseEmbedding // use default value
	if idx < 0 || idx >= c.Len() {
		return r, errors.New("index out of range")
	}
	return c.vectors[idx], nil
}

func (c *ColumnSparseFloatVector) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: c.name,
	}

	dim := int(0)
	data := make([][]byte, 0, len(c.vectors))
	for _, vector := range c.vectors {
		row := make([]byte, 8*vector.Len())
		for idx := 0; idx < vector.Len(); idx++ {
			pos, value, _ := vector.Get(idx)
			binary.LittleEndian.PutUint32(row[idx*8:], pos)
			binary.LittleEndian.PutUint32(row[idx*8+4:], math.Float32bits(value))
		}
		data = append(data, row)
		if vector.Dim() > dim {
			dim = vector.Dim()
		}
	}

	fd.Field = &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_SparseFloatVector{
				SparseFloatVector: &schemapb.SparseFloatArray{
					Dim:      int64(dim),
					Contents: data,
				},
			},
		},
	}
	return fd
}

func (c *ColumnSparseFloatVector) AppendValue(i interface{}) error {
	v, ok := i.(entity.SparseEmbedding)
	if !ok {
		return fmt.Errorf("invalid type, expect SparseEmbedding interface, got %T", i)
	}
	c.vectors = append(c.vectors, v)

	return nil
}

func (c *ColumnSparseFloatVector) Data() []entity.SparseEmbedding {
	return c.vectors
}

func NewColumnSparseVectors(name string, values []entity.SparseEmbedding) *ColumnSparseFloatVector {
	return &ColumnSparseFloatVector{
		name:    name,
		vectors: values,
	}
}
