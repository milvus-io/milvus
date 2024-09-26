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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestColumnSparseEmbedding(t *testing.T) {
	columnName := fmt.Sprintf("column_sparse_embedding_%d", rand.Int())
	columnLen := 8 + rand.Intn(10)

	v := make([]entity.SparseEmbedding, 0, columnLen)
	for i := 0; i < columnLen; i++ {
		length := 1 + rand.Intn(5)
		positions := make([]uint32, length)
		values := make([]float32, length)
		for j := 0; j < length; j++ {
			positions[j] = uint32(j)
			values[j] = rand.Float32()
		}
		se, err := entity.NewSliceSparseEmbedding(positions, values)
		require.NoError(t, err)
		v = append(v, se)
	}
	column := NewColumnSparseVectors(columnName, v)

	t.Run("test column attribute", func(t *testing.T) {
		assert.Equal(t, columnName, column.Name())
		assert.Equal(t, entity.FieldTypeSparseVector, column.Type())
		assert.Equal(t, columnLen, column.Len())
		assert.EqualValues(t, v, column.Data())
	})

	t.Run("test column field data", func(t *testing.T) {
		fd := column.FieldData()
		assert.NotNil(t, fd)
		assert.Equal(t, fd.GetFieldName(), columnName)
	})

	t.Run("test column value by idx", func(t *testing.T) {
		_, err := column.ValueByIdx(-1)
		assert.Error(t, err)
		_, err = column.ValueByIdx(columnLen)
		assert.Error(t, err)

		_, err = column.Get(-1)
		assert.Error(t, err)
		_, err = column.Get(columnLen)
		assert.Error(t, err)

		for i := 0; i < columnLen; i++ {
			v, err := column.ValueByIdx(i)
			assert.NoError(t, err)
			assert.Equal(t, column.vectors[i], v)
			getV, err := column.Get(i)
			assert.NoError(t, err)
			assert.Equal(t, v, getV)
		}
	})
}
