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
	"sort"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
		bufferSize int64
	}{
		{name: "one round per chunk", bufferSize: 10 * 1024 * 1024},
		{name: "several rounds per chunk", bufferSize: 64 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{
				parallelChunkRead: ParallelChunkRead{Concurrency: 3, BufferSize: tc.bufferSize},
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

// TestPackedChunksRecordReaderSelection pins which reader a caller gets and the
// read buffer each chunk is opened with. It does not touch object storage: the
// packed reader constructor is replaced by one that records its arguments.
func TestPackedChunksRecordReaderSelection(t *testing.T) {
	paths := [][]string{{"chunk0/cg0", "chunk0/cg1"}, {"chunk1/cg0", "chunk1/cg1"}, {"chunk2/cg0", "chunk2/cg1"}}
	schema := generateTestSchema()

	type openCall struct {
		paths      []string
		bufferSize int64
		numOpts    int
	}
	var mu sync.Mutex
	var calls []openCall
	mock := mockey.Mock(newPackedRecordReader).To(
		func(chunkPaths []string,
			_ *schemapb.CollectionSchema,
			bufferSize int64,
			_ *indexpb.StorageConfig,
			_ *indexcgopb.StoragePluginContext,
			_ packed.ExternalReaderContext,
			opts ...packed.ReaderOption,
		) (*packedRecordReader, error) {
			mu.Lock()
			calls = append(calls, openCall{paths: chunkPaths, bufferSize: bufferSize, numOpts: len(opts)})
			mu.Unlock()
			// A PackedReader without a native handle reports EOF and closes cleanly.
			return &packedRecordReader{reader: &packed.PackedReader{}}, nil
		}).Build()
	defer mock.UnPatch()

	drainAll := func(t *testing.T, reader RecordReader) []openCall {
		mu.Lock()
		calls = nil
		mu.Unlock()
		_, err := reader.Next()
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, reader.Close())
		mu.Lock()
		defer mu.Unlock()
		got := append([]openCall(nil), calls...)
		sort.Slice(got, func(i, j int) bool { return got[i].paths[0] < got[j].paths[0] })
		return got
	}

	t.Run("no option keeps the serial reader and its buffer size", func(t *testing.T) {
		reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{bufferSize: 7}, nil)
		require.IsType(t, &IterativeRecordReader{}, reader)
		for _, call := range drainAll(t, reader) {
			assert.EqualValues(t, 7, call.bufferSize)
			assert.Zero(t, call.numOpts, "the serial reader must not switch on eager prebuffering")
		}
	})

	t.Run("concurrency 1 keeps the serial reader and ignores the parallel buffer size", func(t *testing.T) {
		reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{
			bufferSize:        7,
			parallelChunkRead: ParallelChunkRead{Concurrency: 1, BufferSize: 99},
		}, nil)
		require.IsType(t, &IterativeRecordReader{}, reader)
		for _, call := range drainAll(t, reader) {
			assert.EqualValues(t, 7, call.bufferSize, "switching the feature off must restore the old read buffer")
			assert.Zero(t, call.numOpts)
		}
	})

	t.Run("parallel reader opens every chunk with the parallel buffer size", func(t *testing.T) {
		reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{
			bufferSize:        7,
			parallelChunkRead: ParallelChunkRead{Concurrency: 2, BufferSize: 99},
		}, nil)
		require.IsType(t, &parallelChunkRecordReader{}, reader)
		got := drainAll(t, reader)
		require.Len(t, got, len(paths))
		for i, call := range got {
			assert.Equal(t, paths[i], call.paths)
			assert.EqualValues(t, 99, call.bufferSize)
			assert.Equal(t, 1, call.numOpts, "the eager prebuffer option must reach the packed reader")
		}
	})

	t.Run("a non-positive parallel buffer size does not become unlimited", func(t *testing.T) {
		for _, bufferSize := range []int64{0, -1} {
			reader := newPackedChunksRecordReader(context.Background(), paths, schema, &rwOptions{
				parallelChunkRead: ParallelChunkRead{Concurrency: 2, BufferSize: bufferSize},
			}, nil)
			got := drainAll(t, reader)
			require.Len(t, got, len(paths))
			for _, call := range got {
				assert.EqualValues(t, packed.DefaultReadBufferSize, call.bufferSize)
			}
		}
	})
}
