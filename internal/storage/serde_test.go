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

package storage

import (
	"io"
	"testing"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestBinlogDeserializeReader(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(nil, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)

		// blobs := generateTestData(t, 0)
		// reader, err = NewBinlogDeserializeReader(blobs, common.RowIDField)
		// assert.NoError(t, err)
		// err = reader.Next()
		// assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		len := 3
		blobs := generateTestData(t, len)
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= len; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()

			f102 := make([]float32, 8)
			for j := range f102 {
				f102[j] = float32(i)
			}
			assertTestData(t, i, value)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})
}
