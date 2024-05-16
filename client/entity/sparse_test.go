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
	"github.com/stretchr/testify/require"
)

func TestSliceSparseEmbedding(t *testing.T) {
	t.Run("normal_case", func(t *testing.T) {
		length := 1 + rand.Intn(5)
		positions := make([]uint32, length)
		values := make([]float32, length)
		for i := 0; i < length; i++ {
			positions[i] = uint32(i)
			values[i] = rand.Float32()
		}
		se, err := NewSliceSparseEmbedding(positions, values)
		require.NoError(t, err)

		assert.EqualValues(t, length, se.Dim())
		assert.EqualValues(t, length, se.Len())

		bs := se.Serialize()
		nv, err := deserializeSliceSparceEmbedding(bs)
		require.NoError(t, err)

		for i := 0; i < length; i++ {
			pos, val, ok := se.Get(i)
			require.True(t, ok)
			assert.Equal(t, positions[i], pos)
			assert.Equal(t, values[i], val)

			npos, nval, ok := nv.Get(i)
			require.True(t, ok)
			assert.Equal(t, positions[i], npos)
			assert.Equal(t, values[i], nval)
		}

		_, _, ok := se.Get(-1)
		assert.False(t, ok)
		_, _, ok = se.Get(length)
		assert.False(t, ok)
	})

	t.Run("position values not match", func(t *testing.T) {
		_, err := NewSliceSparseEmbedding([]uint32{1}, []float32{})
		assert.Error(t, err)
	})
}
