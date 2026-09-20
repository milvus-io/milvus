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
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPackedSerde(t *testing.T) {
	t.Run("test binlog packed serde v2", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
		initcore.InitLocalArrowFileSystem("/tmp")
		size := 10
		bucketName := ""
		paths := [][]string{{"/tmp/0"}, {"/tmp/1"}}
		bufferSize := int64(10 * 1024 * 1024) // 10MB
		schema := generateTestSchema()

		prepareChunkData := func(chunkPaths []string, size int) {
			blobs, err := generateTestData(size)
			assert.NoError(t, err)

			reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
			assert.NoError(t, err)

			group := storagecommon.ColumnGroup{GroupID: storagecommon.DefaultShortColumnGroupID}
			for i := 0; i < len(schema.Fields); i++ {
				group.Columns = append(group.Columns, i)
			}
			multiPartUploadSize := int64(0)
			batchSize := 7
			writer, err := NewPackedSerializeWriter(bucketName, chunkPaths, generateTestSchema(), bufferSize, multiPartUploadSize, []storagecommon.ColumnGroup{group}, batchSize)
			assert.NoError(t, err)

			for i := 1; i <= size; i++ {
				value, err := reader.NextValue()
				assert.NoError(t, err)

				assertTestData(t, i, *value)
				err = writer.WriteValue(*value)
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

		reader := newIterativePackedRecordReader(paths, schema, bufferSize, nil, nil, packed.ExternalReaderContext{})
		defer reader.Close()

		nRows := 0
		for {
			rec, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
			nRows += rec.Len()
		}
		assert.Equal(t, size*len(paths), nRows)
	})
}

// TestPackedChunksParallelRead drives the parallel chunk reader through real
// packed files, which the fake-based reader tests cannot: it covers the eager
// range mode of the packed reader (several ranges per file), the lazy mode, and
// a read buffer small enough to take a chunk in more than one round.
func TestPackedChunksParallelRead(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	initcore.InitLocalArrowFileSystem("/tmp")

	const (
		numChunks    = 5
		rowsPerChunk = 2000
	)
	schema := generateTestSchema()
	group := storagecommon.ColumnGroup{GroupID: storagecommon.DefaultShortColumnGroupID}
	for i := range schema.Fields {
		group.Columns = append(group.Columns, i)
	}

	paths := make([][]string, 0, numChunks)
	for chunk := 0; chunk < numChunks; chunk++ {
		chunkPaths := []string{fmt.Sprintf("/tmp/parallel_chunk_read_%d", chunk)}
		paths = append(paths, chunkPaths)

		// Field 13 runs 1..numChunks*rowsPerChunk across the chunks in order, so
		// a reordered, dropped or repeated record shows up as a broken sequence.
		blobs, err := generateTestDataWithSeed(chunk*rowsPerChunk+1, rowsPerChunk)
		require.NoError(t, err)
		source, err := NewBinlogDeserializeReader(schema, MakeBlobsReader(blobs), false)
		require.NoError(t, err)
		writer, err := NewPackedSerializeWriter("", chunkPaths, schema, int64(10*1024*1024), 0, []storagecommon.ColumnGroup{group}, 7)
		require.NoError(t, err)
		for i := 0; i < rowsPerChunk; i++ {
			value, err := source.NextValue()
			require.NoError(t, err)
			require.NoError(t, writer.WriteValue(*value))
		}
		require.NoError(t, writer.Close())
		require.NoError(t, source.Close())
	}

	for _, tc := range []struct {
		name       string
		rangeSize  int64
		bufferSize int64
	}{
		{name: "eager ranges", rangeSize: 16 * 1024, bufferSize: 10 * 1024 * 1024},
		{name: "lazy ranges", rangeSize: 0, bufferSize: 10 * 1024 * 1024},
		{name: "eager ranges with a small read buffer", rangeSize: 16 * 1024, bufferSize: 64 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{
				bufferSize:           tc.bufferSize,
				chunkReadConcurrency: 3,
				chunkReadRangeSize:   tc.rangeSize,
			}, nil)
			_, parallel := reader.(*parallelChunkRecordReader)
			require.True(t, parallel, "concurrency > 1 must select the parallel reader")
			defer reader.Close()

			next := int64(1)
			for {
				rec, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				values := rec.Column(13).(*array.Int64).Int64Values()
				for _, v := range values {
					require.Equal(t, next, v)
					next++
				}
			}
			assert.EqualValues(t, numChunks*rowsPerChunk, next-1)
		})
	}
}
