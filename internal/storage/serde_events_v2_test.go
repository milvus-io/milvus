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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/initcore"
)

func TestPackedSerde(t *testing.T) {
	t.Run("test binlog packed serde v2", func(t *testing.T) {
		t.Skip("storage v2 cgo not ready yet")
		initcore.InitLocalArrowFileSystem("/tmp")
		size := 10

		paths := [][]string{{"/tmp/0"}, {"/tmp/1"}}
		bufferSize := int64(10 * 1024 * 1024) // 10MB
		schema := generateTestSchema()

		prepareChunkData := func(chunkPaths []string, size int) {
			blobs, err := generateTestData(size)
			assert.NoError(t, err)

			reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs))
			assert.NoError(t, err)

			group := storagecommon.ColumnGroup{}
			for i := 0; i < len(schema.Fields); i++ {
				group.Columns = append(group.Columns, i)
			}
			multiPartUploadSize := int64(0)
			batchSize := 7
			writer, err := NewPackedSerializeWriter(chunkPaths, generateTestSchema(), bufferSize, multiPartUploadSize, []storagecommon.ColumnGroup{group}, batchSize)
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
			err = reader.Close()
			assert.NoError(t, err)
		}

		for _, chunkPaths := range paths {
			prepareChunkData(chunkPaths, size)
		}

		reader, err := NewPackedDeserializeReader(paths, schema, bufferSize)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 0; i < size*len(paths); i++ {
			err = reader.Next()
			assert.NoError(t, err)
			value := reader.Value()
			assertTestData(t, i%10+1, value)
		}
		err = reader.Next()
		assert.Equal(t, err, io.EOF)
	})
}
