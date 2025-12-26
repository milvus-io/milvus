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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func TestNewLOBReader(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}

	reader := NewLOBReader(storageConfig)

	assert.NotNil(t, reader)
	assert.NotNil(t, reader.storageConfig)
	assert.NotNil(t, reader.arrowSchema)
	assert.NotNil(t, reader.cache)
	assert.False(t, reader.loaded)
	assert.NotNil(t, reader.logger)
}

func TestNewLOBReader_NilConfig(t *testing.T) {
	reader := NewLOBReader(nil)

	assert.NotNil(t, reader)
	assert.Nil(t, reader.storageConfig)
	assert.NotNil(t, reader.arrowSchema)
	assert.NotNil(t, reader.cache)
}

func TestLOBReader_ReadText_NilRef(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	_, err := reader.ReadText(context.Background(), nil, "test/path")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOB reference is nil")
}

func TestLOBReader_ReadText_InvalidRef(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Create invalid reference (wrong magic)
	invalidRef := &LOBReference{
		Magic:     0x12345678, // wrong magic
		LobFileID: 100,
		RowOffset: 0,
	}

	_, err := reader.ReadText(context.Background(), invalidRef, "test/path")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid LOB reference")
}

func TestLOBReader_ReadText_CacheHit(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Manually set cache to simulate loaded state
	reader.cache[0] = "cached text 0"
	reader.cache[1] = "cached text 1"
	reader.cache[2] = "cached text 2"
	reader.loaded = true

	ref := NewLOBReference(12345, 1)

	text, err := reader.ReadText(context.Background(), ref, "test/path")

	require.NoError(t, err)
	assert.Equal(t, "cached text 1", text)
}

func TestLOBReader_ReadText_CacheHit_MultipleReads(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Manually set cache
	reader.cache[0] = "text 0"
	reader.cache[5] = "text 5"
	reader.cache[10] = "text 10"
	reader.loaded = true

	tests := []struct {
		rowOffset uint32
		expected  string
	}{
		{0, "text 0"},
		{5, "text 5"},
		{10, "text 10"},
		{0, "text 0"}, // read again
		{5, "text 5"}, // read again
	}

	for _, tt := range tests {
		ref := NewLOBReference(12345, tt.rowOffset)
		text, err := reader.ReadText(context.Background(), ref, "test/path")

		require.NoError(t, err)
		assert.Equal(t, tt.expected, text)
	}
}

func TestLOBReader_ReadText_RowOffsetNotFound(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Manually set cache (but without row offset 999)
	reader.cache[0] = "cached text 0"
	reader.cache[1] = "cached text 1"
	reader.loaded = true

	ref := NewLOBReference(12345, 999) // offset 999 doesn't exist

	_, err := reader.ReadText(context.Background(), ref, "test/path")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "row offset 999 not found")
}

func TestLOBReader_ReadText_ConcurrentAccess(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Manually set cache
	for i := uint32(0); i < 100; i++ {
		reader.cache[i] = "text"
	}
	reader.loaded = true

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(offset uint32) {
			defer wg.Done()
			ref := NewLOBReference(12345, offset%100)
			_, err := reader.ReadText(context.Background(), ref, "test/path")
			assert.NoError(t, err)
		}(uint32(i))
	}
	wg.Wait()
}

func TestLOBReader_LoadedState(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp/milvus",
	}
	reader := NewLOBReader(storageConfig)

	// Initially not loaded
	assert.False(t, reader.loaded)
	assert.Len(t, reader.cache, 0)

	// Simulate loading
	reader.cache[0] = "text"
	reader.loaded = true

	assert.True(t, reader.loaded)
	assert.Len(t, reader.cache, 1)
}

func TestLOBReader_ArrowSchema(t *testing.T) {
	reader := NewLOBReader(nil)

	// Verify arrow schema has correct structure
	assert.NotNil(t, reader.arrowSchema)
	assert.Equal(t, 1, reader.arrowSchema.NumFields())
	assert.Equal(t, LOBFieldName, reader.arrowSchema.Field(0).Name)
}
