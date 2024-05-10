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
	})

	t.Run("test binary vector", func(t *testing.T) {
		raw := make([]byte, dim)
		_, err := rand.Read(raw)
		assert.Nil(t, err)

		bv := BinaryVector(raw)

		assert.Equal(t, dim*8, bv.Dim())
		assert.ElementsMatch(t, raw, bv.Serialize())
	})
}
