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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/common"
)

func TestPackedSerde(t *testing.T) {
	t.Run("test binlog packed deserialize reader", func(t *testing.T) {
		size := 10

		blobs, err := generateTestData(size)
		assert.NoError(t, err)

		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs))
		assert.NoError(t, err)
		defer reader.Close()

		path := "/tmp"
		bufferSize := 10 * 1024 * 1024 // 10MB
		schema := generateTestSchema()

		writer, err := NewPackedSerializeWriter(schema, 0, 0, 7, path, bufferSize)
		assert.NoError(t, err)

		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestData(t, i, value)
			err := writer.Write(value)
			assert.NoError(t, err)
		}
		err = writer.Close()
		assert.NoError(t, err)

		reader, err = NewPackedDeserializeReader(path, schema, bufferSize, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err)
			value := reader.Value()
			assertTestData(t, i, value)
		}
	})
}
