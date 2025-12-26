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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
)

// mockAllocator is a simple mock implementation of allocator.Interface
type mockAllocator struct {
	mu      sync.Mutex
	counter int64
}

func newMockAllocator() allocator.Interface {
	return &mockAllocator{counter: 1000}
}

func (m *mockAllocator) Alloc(count uint32) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := m.counter
	m.counter += int64(count)
	return start, m.counter - 1, nil
}

func (m *mockAllocator) AllocOne() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.counter
	m.counter++
	return id, nil
}

func TestNewLOBManager(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc)
		require.NoError(t, err)
		require.NotNil(t, manager)
		assert.Equal(t, int64(1), manager.collectionID)
		assert.Equal(t, int64(65536), manager.sizeThreshold)
		assert.Equal(t, int64(10485760), manager.lazyWriteMaxSize)
	})

	t.Run("with custom options", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(
			1,
			mockAlloc,
			WithLOBManagerSizeThreshold(1024),
			WithLOBManagerLazyWriteMaxSize(5*1024*1024),
		)
		require.NoError(t, err)
		require.NotNil(t, manager)
		assert.Equal(t, int64(1024), manager.sizeThreshold)
		assert.Equal(t, int64(5*1024*1024), manager.lazyWriteMaxSize)
	})

	t.Run("nil allocator", func(t *testing.T) {
		manager, err := NewLOBManager(1, nil)
		require.Error(t, err)
		require.Nil(t, manager)
		assert.Contains(t, err.Error(), "allocator cannot be nil")
	})
}

func TestLOBManager_GetSizeThreshold(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(2048))
	require.NoError(t, err)
	assert.Equal(t, int64(2048), manager.GetSizeThreshold())
}

func TestLOBManager_ProcessInsertData_SmallText(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(100))
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text_field",
				DataType: schemapb.DataType_Text,
			},
		},
	}

	// Small text that should stay inline
	smallText := "small text"
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{smallText},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB, "small text should not be converted to LOB")

	// Verify data is unchanged
	stringField := insertData.Data[101].(*StringFieldData)
	assert.Equal(t, smallText, stringField.Data[0])
}

func TestLOBManager_ProcessInsertData_MediumTextLazyMode(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text_field",
				DataType: schemapb.DataType_Text,
			},
		},
	}

	// Medium text (lazy mode)
	mediumText := strings.Repeat("a", 100)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{mediumText},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.True(t, hasLOB, "medium text should be marked for LOB processing")

	// Verify pending LOBs
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 1)
	assert.Equal(t, mediumText, pendingList[0].data)
	assert.Equal(t, int64(101), pendingList[0].fieldID)
}

func TestLOBManager_ProcessInsertData_NonTextFields(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "int_field",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "varchar_field",
				DataType: schemapb.DataType_VarChar,
			},
		},
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &Int64FieldData{
				Data: []int64{1, 2, 3},
			},
			102: &StringFieldData{
				Data: []string{"varchar1", "varchar2"},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB, "non-TEXT fields should not trigger LOB processing")
}

func TestLOBManager_ProcessInsertData_MultipleRows(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text_field",
				DataType: schemapb.DataType_Text,
			},
		},
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{
					strings.Repeat("a", 50), // medium
					strings.Repeat("b", 50), // medium
					strings.Repeat("c", 50), // medium
				},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.True(t, hasLOB)

	// Verify pending LOBs count
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 3)
}

func TestLOBManager_PendingLOBs(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	// Add pending LOBs manually
	mediumText := strings.Repeat("test", 25)
	refPlaceholder := "lob:1001:0"
	manager.pendingMu.Lock()
	manager.pendingLOBs[1000] = []*PendingLOB{
		{
			segmentID:   1000,
			partitionID: 100,
			fieldID:     101,
			rowIndex:    0,
			data:        mediumText,
			originalRef: &refPlaceholder,
		},
	}
	manager.pendingMu.Unlock()

	// Verify the pending LOBs exist
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 1)
	assert.Equal(t, mediumText, pendingList[0].data)
	assert.Equal(t, int64(1000), pendingList[0].segmentID)
	assert.Equal(t, int64(100), pendingList[0].partitionID)
	assert.Equal(t, int64(101), pendingList[0].fieldID)
}

func TestLOBManager_GetWriterKey(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	key := manager.getWriterKey(1000, 101)
	assert.Equal(t, "1000:101", key)

	key2 := manager.getWriterKey(2000, 102)
	assert.Equal(t, "2000:102", key2)
}

func TestLOBManager_GetFieldSchema(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "field1", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "field2", DataType: schemapb.DataType_Int64},
		},
	}

	// Found
	fieldSchema := manager.getFieldSchema(schema, 101)
	require.NotNil(t, fieldSchema)
	assert.Equal(t, int64(101), fieldSchema.FieldID)
	assert.Equal(t, "field1", fieldSchema.Name)

	// Not found
	fieldSchema = manager.getFieldSchema(schema, 999)
	assert.Nil(t, fieldSchema)
}

func TestLOBManager_MixedTextSizes(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(50),
		WithLOBManagerLazyWriteMaxSize(200),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{
					strings.Repeat("a", 10),  // small - inline
					strings.Repeat("b", 100), // medium - lazy
					strings.Repeat("c", 20),  // small - inline
					strings.Repeat("d", 150), // medium - lazy
				},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.True(t, hasLOB)

	// Verify: 2 pending LOBs (medium texts)
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 2)

	// Verify data
	stringField := insertData.Data[101].(*StringFieldData)
	// Row 0 and 2 should be unchanged (small)
	assert.Equal(t, strings.Repeat("a", 10), stringField.Data[0])
	assert.Equal(t, strings.Repeat("c", 20), stringField.Data[2])
}

func BenchmarkLOBManager_ProcessInsertData_SmallText(b *testing.B) {
	mockAlloc := newMockAllocator()
	manager, _ := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(100))

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{"small text"},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	}
}

func TestLOBManager_GetStatistics(t *testing.T) {
	t.Run("empty manager", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc,
			WithLOBManagerSizeThreshold(100),
			WithLOBManagerLazyWriteMaxSize(1000),
		)
		require.NoError(t, err)

		stats := manager.GetStatistics()
		assert.Equal(t, int64(100), stats["size_threshold"])
		assert.Equal(t, int64(1000), stats["lazy_write_max_size"])
		assert.Equal(t, 0, stats["total_writers"])
		assert.Equal(t, 0, stats["total_lob_files"])
		assert.Equal(t, int64(0), stats["total_lob_records"])
		assert.Equal(t, int64(0), stats["total_lob_bytes"])
		assert.Equal(t, 0, stats["pending_lobs"])
		assert.Equal(t, int64(0), stats["pending_lobs_bytes"])
	})

	t.Run("with pending LOBs", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc)
		require.NoError(t, err)

		// Add pending LOBs
		manager.pendingMu.Lock()
		manager.pendingLOBs[1000] = []*PendingLOB{
			{segmentID: 1000, data: strings.Repeat("a", 100)},
			{segmentID: 1000, data: strings.Repeat("b", 200)},
		}
		manager.pendingLOBs[2000] = []*PendingLOB{
			{segmentID: 2000, data: strings.Repeat("c", 150)},
		}
		manager.pendingMu.Unlock()

		stats := manager.GetStatistics()
		assert.Equal(t, 3, stats["pending_lobs"])
		assert.Equal(t, int64(450), stats["pending_lobs_bytes"])
	})
}

func TestLOBManager_Close(t *testing.T) {
	t.Run("with pending LOBs should fail", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc)
		require.NoError(t, err)

		// Add pending LOBs
		placeholder := "placeholder"
		manager.pendingMu.Lock()
		manager.pendingLOBs[1000] = []*PendingLOB{
			{segmentID: 1000, data: "test", originalRef: &placeholder},
		}
		manager.pendingMu.Unlock()

		err = manager.Close(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pending LOBs not flushed")
	})

	t.Run("empty manager should succeed", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc)
		require.NoError(t, err)

		err = manager.Close(context.Background())
		require.NoError(t, err)
	})
}

func TestLOBManager_GetSegmentMetadata(t *testing.T) {
	t.Run("no writers should return nil", func(t *testing.T) {
		mockAlloc := newMockAllocator()
		manager, err := NewLOBManager(1, mockAlloc)
		require.NoError(t, err)

		metadata := manager.GetSegmentMetadata(1000)
		assert.Nil(t, metadata)
	})
}

func TestLOBManager_ProcessInsertData_NullableField(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text, Nullable: true},
		},
	}

	// Medium text with nullable field - null value should be skipped
	mediumText := strings.Repeat("a", 100)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data:      []string{mediumText, "", mediumText},
				ValidData: []bool{true, false, true}, // second row is null
				Nullable:  true,
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.True(t, hasLOB)

	// Only 2 pending LOBs (not 3, because the null one is skipped)
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 2)
	assert.Equal(t, 0, pendingList[0].rowIndex)
	assert.Equal(t, 2, pendingList[1].rowIndex)
}

func TestLOBManager_FlushSegment_NoPendingLOBs(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	// Flush a segment with no pending LOBs - should succeed
	metadata, err := manager.FlushSegment(context.Background(), 1000)
	require.NoError(t, err)
	assert.Nil(t, metadata) // No LOB data, metadata should be nil
}

func TestLOBManager_ConcurrentAddPendingLOB(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	// Concurrent inserts to different segments
	var wg sync.WaitGroup
	numGoroutines := 10
	rowsPerGoroutine := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(segmentID int64) {
			defer wg.Done()

			mediumText := strings.Repeat("x", 50)
			data := make([]string, rowsPerGoroutine)
			for j := 0; j < rowsPerGoroutine; j++ {
				data[j] = mediumText
			}

			insertData := &InsertData{
				Data: map[int64]FieldData{
					101: &StringFieldData{Data: data},
				},
			}

			_, err := manager.ProcessInsertData(context.Background(), segmentID, 100, schema, insertData)
			assert.NoError(t, err)
		}(int64(1000 + i))
	}

	wg.Wait()

	// Verify total pending LOBs
	manager.pendingMu.RLock()
	totalPending := 0
	for _, list := range manager.pendingLOBs {
		totalPending += len(list)
	}
	manager.pendingMu.RUnlock()

	assert.Equal(t, numGoroutines*rowsPerGoroutine, totalPending)
}

func TestLOBManager_MultipleSegments(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	mediumText := strings.Repeat("a", 50)

	// Insert to segment 1000
	insertData1 := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{Data: []string{mediumText, mediumText}},
		},
	}
	_, err = manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData1)
	require.NoError(t, err)

	// Insert to segment 2000
	insertData2 := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{Data: []string{mediumText}},
		},
	}
	_, err = manager.ProcessInsertData(context.Background(), 2000, 100, schema, insertData2)
	require.NoError(t, err)

	// Verify pending LOBs per segment
	manager.pendingMu.RLock()
	assert.Len(t, manager.pendingLOBs[1000], 2)
	assert.Len(t, manager.pendingLOBs[2000], 1)
	manager.pendingMu.RUnlock()
}

func TestLOBManager_MultipleFields(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
			{FieldID: 102, DataType: schemapb.DataType_Text},
		},
	}

	mediumText := strings.Repeat("a", 50)

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{Data: []string{mediumText}},
			102: &StringFieldData{Data: []string{mediumText, mediumText}},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.True(t, hasLOB)

	// Verify pending LOBs
	manager.pendingMu.RLock()
	pendingList := manager.pendingLOBs[1000]
	manager.pendingMu.RUnlock()

	assert.Len(t, pendingList, 3)

	// Count by field
	fieldCounts := make(map[int64]int)
	for _, p := range pendingList {
		fieldCounts[p.fieldID]++
	}
	assert.Equal(t, 1, fieldCounts[101])
	assert.Equal(t, 2, fieldCounts[102])
}

func TestLOBManager_ProcessInsertData_EmptyData(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{Data: []string{}},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB)
}

func TestLOBManager_ProcessInsertData_FieldNotInSchema(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(10))
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	// Insert data with field 102 which is not in schema
	insertData := &InsertData{
		Data: map[int64]FieldData{
			102: &StringFieldData{Data: []string{strings.Repeat("a", 100)}},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB, "field not in schema should not trigger LOB")
}

func TestLOBManager_ProcessInsertData_WrongFieldDataType(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(10))
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	// Insert int64 data for a TEXT field (type mismatch in FieldData)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &Int64FieldData{Data: []int64{1, 2, 3}},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB, "wrong FieldData type should not trigger LOB")
}

func TestLOBManager_AddPendingLOB(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc)
	require.NoError(t, err)

	placeholder1 := "placeholder1"
	placeholder2 := "placeholder2"

	manager.addPendingLOB(&PendingLOB{
		segmentID:   1000,
		partitionID: 100,
		fieldID:     101,
		rowIndex:    0,
		data:        "test1",
		originalRef: &placeholder1,
	})

	manager.addPendingLOB(&PendingLOB{
		segmentID:   1000,
		partitionID: 100,
		fieldID:     101,
		rowIndex:    1,
		data:        "test2",
		originalRef: &placeholder2,
	})

	manager.pendingMu.RLock()
	defer manager.pendingMu.RUnlock()

	assert.Len(t, manager.pendingLOBs[1000], 2)
	assert.Equal(t, "test1", manager.pendingLOBs[1000][0].data)
	assert.Equal(t, "test2", manager.pendingLOBs[1000][1].data)
}

func TestLOBManager_ProcessInsertData_AllSmallTexts(t *testing.T) {
	mockAlloc := newMockAllocator()
	manager, err := NewLOBManager(1, mockAlloc, WithLOBManagerSizeThreshold(1000))
	require.NoError(t, err)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	// All texts are small (< threshold)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{"small1", "small2", "small3"},
			},
		},
	}

	hasLOB, err := manager.ProcessInsertData(context.Background(), 1000, 100, schema, insertData)
	require.NoError(t, err)
	assert.False(t, hasLOB)

	// Verify no pending LOBs
	manager.pendingMu.RLock()
	assert.Empty(t, manager.pendingLOBs)
	manager.pendingMu.RUnlock()

	// Verify data unchanged
	stringField := insertData.Data[101].(*StringFieldData)
	assert.Equal(t, []string{"small1", "small2", "small3"}, stringField.Data)
}

func BenchmarkLOBManager_ProcessInsertData_MediumText(b *testing.B) {
	mockAlloc := newMockAllocator()
	manager, _ := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	mediumText := strings.Repeat("test", 50)
	insertData := &InsertData{
		Data: map[int64]FieldData{
			101: &StringFieldData{
				Data: []string{mediumText},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear pending LOBs between iterations
		manager.pendingMu.Lock()
		manager.pendingLOBs = make(map[int64][]*PendingLOB)
		manager.pendingMu.Unlock()

		_, _ = manager.ProcessInsertData(context.Background(), int64(1000+i), 100, schema, insertData)
	}
}

func BenchmarkLOBManager_ConcurrentInserts(b *testing.B) {
	mockAlloc := newMockAllocator()
	manager, _ := NewLOBManager(
		1,
		mockAlloc,
		WithLOBManagerSizeThreshold(10),
		WithLOBManagerLazyWriteMaxSize(1000),
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_Text},
		},
	}

	mediumText := strings.Repeat("test", 50)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		segmentID := int64(1000)
		for pb.Next() {
			insertData := &InsertData{
				Data: map[int64]FieldData{
					101: &StringFieldData{Data: []string{mediumText}},
				},
			}
			_, _ = manager.ProcessInsertData(context.Background(), segmentID, 100, schema, insertData)
			segmentID++
		}
	})
}
