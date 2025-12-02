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
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLOBWriter(t *testing.T) {
	t.Run("success with default options", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
		require.NoError(t, err)
		require.NotNil(t, writer)

		assert.Equal(t, int64(1), writer.segmentID)
		assert.Equal(t, int64(2), writer.partitionID)
		assert.Equal(t, int64(3), writer.fieldID)
		assert.Equal(t, int64(4), writer.collectionID)
		assert.Equal(t, int64(65536), writer.sizeThreshold)          // default 64KB
		assert.Equal(t, int64(256*1024*1024), writer.maxLOBFileSize) // default 256MB
		assert.Equal(t, int64(16*1024*1024), writer.maxBatchSize)    // default 16MB
	})

	t.Run("nil allocator", func(t *testing.T) {
		writer, err := NewLOBWriter(1, 2, 3, 4, nil, nil)
		require.Error(t, err)
		require.Nil(t, writer)
		assert.Contains(t, err.Error(), "allocator cannot be nil")
	})

	t.Run("with custom options", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		customMem := memory.NewGoAllocator()

		writer, err := NewLOBWriter(
			1, 2, 3, 4, mockAlloc, nil,
			WithLOBSizeThreshold(1024),
			WithMaxLOBFileSize(100*1024*1024),
			WithMaxBatchSize(8*1024*1024),
			WithLOBMemAllocator(customMem),
		)
		require.NoError(t, err)
		require.NotNil(t, writer)

		assert.Equal(t, int64(1024), writer.sizeThreshold)
		assert.Equal(t, int64(100*1024*1024), writer.maxLOBFileSize)
		assert.Equal(t, int64(8*1024*1024), writer.maxBatchSize)
		assert.Equal(t, customMem, writer.memAllocator)
	})
}

func TestLOBWriter_GetLOBSizeThreshold(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithLOBSizeThreshold(2048))
	require.NoError(t, err)

	assert.Equal(t, int64(2048), writer.GetLOBSizeThreshold())
}

func TestLOBWriter_GetStatistics(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
	require.NoError(t, err)

	stats := writer.GetStatistics()
	require.NotNil(t, stats)

	assert.Equal(t, 0, stats["total_lob_files"])
	assert.Equal(t, int64(0), stats["total_lob_records"])
	assert.Equal(t, int64(0), stats["total_lob_bytes"])
	assert.Equal(t, int64(0), stats["current_lob_file"])
	assert.Equal(t, uint32(0), stats["current_offset"])
}

func TestLOBWriter_GetLOBFileInfos(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
	require.NoError(t, err)

	// initially empty
	infos := writer.GetLOBFileInfos()
	assert.Empty(t, infos)
}

func TestLOBWriter_GetCurrentFilePath(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
	require.NoError(t, err)

	// no active writer
	path, active := writer.GetCurrentFilePath()
	assert.Empty(t, path)
	assert.False(t, active)
}

func TestLOBWriter_WriteTextBelowThreshold(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithLOBSizeThreshold(100))
	require.NoError(t, err)

	// text below threshold should return error
	ctx := context.Background()
	ref, err := writer.WriteText(ctx, "small text")
	require.Error(t, err)
	require.Nil(t, ref)
	assert.Contains(t, err.Error(), "text size is below threshold")
}

func TestLOBWriter_FlushEmpty(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
	require.NoError(t, err)

	// flush with no data should succeed
	ctx := context.Background()
	err = writer.Flush(ctx)
	require.NoError(t, err)
}

func TestLOBWriter_CloseEmpty(t *testing.T) {
	mockAlloc := newMockAllocator()
	writer, err := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil)
	require.NoError(t, err)

	// close with no data should succeed
	ctx := context.Background()
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestLOBWriterOptions(t *testing.T) {
	t.Run("WithLOBSizeThreshold", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		writer, _ := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithLOBSizeThreshold(4096))
		assert.Equal(t, int64(4096), writer.sizeThreshold)
	})

	t.Run("WithMaxLOBFileSize", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		writer, _ := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithMaxLOBFileSize(512*1024*1024))
		assert.Equal(t, int64(512*1024*1024), writer.maxLOBFileSize)
	})

	t.Run("WithMaxBatchSize", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		writer, _ := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithMaxBatchSize(32*1024*1024))
		assert.Equal(t, int64(32*1024*1024), writer.maxBatchSize)
	})

	t.Run("WithLOBMemAllocator", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		customMem := memory.NewGoAllocator()
		writer, _ := NewLOBWriter(1, 2, 3, 4, mockAlloc, nil, WithLOBMemAllocator(customMem))
		assert.Equal(t, customMem, writer.memAllocator)
	})
}
