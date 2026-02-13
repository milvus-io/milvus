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

package segments

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/state"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newTestLocalSegment creates a LocalSegment with a mock CSegment for unit testing.
// This allows testing LocalSegment methods (DropIndex, HasFieldData, etc.)
// without requiring a real C++ segment.
func newTestLocalSegment(
	t *testing.T,
	collectionID int64,
	segmentID int64,
	schema *schemapb.CollectionSchema,
) (*LocalSegment, *mock_segcore.MockCSegment) {
	t.Helper()

	collection := NewCollectionWithoutSegcoreForTest(collectionID, schema)
	mockCSeg := mock_segcore.NewMockCSegment(t)

	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID:  collectionID,
		SegmentID:     segmentID,
		PartitionID:   1,
		InsertChannel: "test-channel",
	}

	seg := &LocalSegment{
		baseSegment: baseSegment{
			collection:         collection,
			version:            atomic.NewInt64(0),
			segmentType:        SegmentTypeSealed,
			bloomFilterSet:     pkoracle.NewBloomFilterSet(segmentID, 1, SegmentTypeSealed),
			loadInfo:           atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo),
			indexStatus:        atomic.NewInt32(int32(IndexStatusUnindexed)),
			needUpdatedVersion: atomic.NewInt64(0),
			resourceUsageCache: atomic.NewPointer[ResourceUsage](nil),
			bm25Stats:          make(map[int64]*storage.BM25Stats),
		},
		ptrLock:            state.NewLoadStateLock(state.LoadStateOnlyMeta),
		csegment:           mockCSeg,
		memSize:            atomic.NewInt64(-1),
		binlogSize:         atomic.NewInt64(0),
		rowNum:             atomic.NewInt64(-1),
		insertCount:        atomic.NewInt64(0),
		lastDeltaTimestamp: atomic.NewUint64(0),
		fields:             typeutil.NewConcurrentMap[int64, *FieldInfo](),
		fieldIndexes:       typeutil.NewConcurrentMap[int64, *IndexedFieldInfo](),
		fieldJSONStats:     make(map[int64]*querypb.JsonStatsInfo),
	}

	return seg, mockCSeg
}

// =============================================================================
// IndexStatus type tests
// =============================================================================

func TestIndexStatus_String(t *testing.T) {
	assert.Equal(t, "unindexed", IndexStatusUnindexed.String())
	assert.Equal(t, "indexed", IndexStatusIndexed.String())
	assert.Equal(t, "unknown", IndexStatus(99).String())
}

func TestBaseSegment_IndexStatusMethods(t *testing.T) {
	paramtable.Init()

	seg, _ := newTestLocalSegment(t, 100, 1, &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
		},
	})

	// Default is unindexed
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())

	// SetIndexStatus
	seg.SetIndexStatus(IndexStatusIndexed)
	assert.Equal(t, IndexStatusIndexed, seg.GetIndexStatus())

	// CompareAndSetIndexStatus: success
	ok := seg.CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusUnindexed)
	assert.True(t, ok)
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())

	// CompareAndSetIndexStatus: failure (wrong old value)
	ok = seg.CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusUnindexed)
	assert.False(t, ok)
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())
}

// =============================================================================
// DropIndex tests using mock CSegment
// =============================================================================

func TestLocalSegment_DropIndex_NonExistentIndex(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	seg, _ := newTestLocalSegment(t, 100, 1, schema)

	// Drop a non-existent index should return nil
	err := seg.DropIndex(context.Background(), 999)
	assert.NoError(t, err)
}

func TestLocalSegment_DropIndex_VectorField(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	// Pre-populate field index for vector field
	indexID := int64(1001)
	seg.fieldIndexes.Insert(indexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{
			FieldID: 101,
			IndexID: indexID,
		},
		IsLoaded: true,
	})

	// Set segment as indexed
	seg.SetIndexStatus(IndexStatusIndexed)

	// Mock CSegment.DropIndex
	mockCSeg.EXPECT().DropIndex(ctx, int64(101)).Return(nil).Once()

	err := seg.DropIndex(ctx, indexID)
	assert.NoError(t, err)

	// Verify index was removed
	_, ok := seg.fieldIndexes.Get(indexID)
	assert.False(t, ok)

	// Since the only vector field lost its index, status should be unindexed
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())
}

func TestLocalSegment_DropIndex_MultipleVectorFields_PartialDrop(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	// Pre-populate indexes for both vector fields
	indexID1 := int64(1001)
	indexID2 := int64(1002)
	seg.fieldIndexes.Insert(indexID1, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: indexID1},
		IsLoaded:  true,
	})
	seg.fieldIndexes.Insert(indexID2, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 102, IndexID: indexID2},
		IsLoaded:  true,
	})

	seg.SetIndexStatus(IndexStatusIndexed)

	// Drop only one vector field index
	mockCSeg.EXPECT().DropIndex(ctx, int64(101)).Return(nil).Once()

	err := seg.DropIndex(ctx, indexID1)
	assert.NoError(t, err)

	// After dropping one of two vector indexes, segment should become unindexed
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())
}

func TestLocalSegment_DropIndex_ScalarField_NoStatusChange(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, DataType: schemapb.DataType_VarChar},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	// Add indexes for both vector and scalar fields
	vecIndexID := int64(1001)
	scalarIndexID := int64(1002)
	seg.fieldIndexes.Insert(vecIndexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: vecIndexID},
		IsLoaded:  true,
	})
	seg.fieldIndexes.Insert(scalarIndexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 102, IndexID: scalarIndexID},
		IsLoaded:  true,
	})

	seg.SetIndexStatus(IndexStatusIndexed)

	// Drop the scalar field index
	mockCSeg.EXPECT().DropIndex(ctx, int64(102)).Return(nil).Once()

	err := seg.DropIndex(ctx, scalarIndexID)
	assert.NoError(t, err)

	// Scalar index drop should NOT change index status (vector index still exists)
	assert.Equal(t, IndexStatusIndexed, seg.GetIndexStatus())
}

func TestLocalSegment_DropIndex_JSONField(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 103, DataType: schemapb.DataType_JSON},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	jsonIndexID := int64(2001)
	seg.fieldIndexes.Insert(jsonIndexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{
			FieldID: 103,
			IndexID: jsonIndexID,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.JSONPathKey, Value: "$.name"},
			},
		},
		IsLoaded: true,
	})

	mockCSeg.EXPECT().DropJSONIndex(ctx, int64(103), "$.name").Return(nil).Once()

	err := seg.DropIndex(ctx, jsonIndexID)
	assert.NoError(t, err)

	// Verify index was removed
	_, ok := seg.fieldIndexes.Get(jsonIndexID)
	assert.False(t, ok)
}

func TestLocalSegment_DropIndex_JSONField_MissingPath(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 103, DataType: schemapb.DataType_JSON},
		},
	}
	seg, _ := newTestLocalSegment(t, 100, 1, schema)

	jsonIndexID := int64(2001)
	seg.fieldIndexes.Insert(jsonIndexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{
			FieldID:     103,
			IndexID:     jsonIndexID,
			IndexParams: []*commonpb.KeyValuePair{}, // no JSONPathKey
		},
		IsLoaded: true,
	})

	err := seg.DropIndex(ctx, jsonIndexID)
	assert.Error(t, err)
}

func TestLocalSegment_DropIndex_JSONField_DropError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 103, DataType: schemapb.DataType_JSON},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	jsonIndexID := int64(2001)
	seg.fieldIndexes.Insert(jsonIndexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{
			FieldID: 103,
			IndexID: jsonIndexID,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.JSONPathKey, Value: "$.name"},
			},
		},
		IsLoaded: true,
	})

	mockCSeg.EXPECT().DropJSONIndex(ctx, int64(103), "$.name").Return(errors.New("drop json index error")).Once()

	err := seg.DropIndex(ctx, jsonIndexID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "drop json index error")

	// Index should NOT be removed on error
	_, ok := seg.fieldIndexes.Get(jsonIndexID)
	assert.True(t, ok)
}

func TestLocalSegment_DropIndex_CSegmentError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	indexID := int64(1001)
	seg.fieldIndexes.Insert(indexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: indexID},
		IsLoaded:  true,
	})

	mockCSeg.EXPECT().DropIndex(ctx, int64(101)).Return(errors.New("csegment drop error")).Once()

	err := seg.DropIndex(ctx, indexID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "csegment drop error")

	// Index should NOT be removed on error
	_, ok := seg.fieldIndexes.Get(indexID)
	assert.True(t, ok)
}

func TestLocalSegment_DropIndex_SegmentReleased(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
		},
	}
	seg, _ := newTestLocalSegment(t, 100, 1, schema)

	// Release the segment
	guard := seg.ptrLock.StartReleaseAll()
	guard.Done(nil)

	err := seg.DropIndex(ctx, 1001)
	assert.Error(t, err)
}

func TestLocalSegment_DropIndex_AlreadyUnindexed(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	indexID := int64(1001)
	seg.fieldIndexes.Insert(indexID, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: indexID},
		IsLoaded:  true,
	})

	// Segment is already unindexed (default)
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())

	mockCSeg.EXPECT().DropIndex(ctx, int64(101)).Return(nil).Once()

	err := seg.DropIndex(ctx, indexID)
	assert.NoError(t, err)

	// Status should remain unindexed (no metric transition needed)
	assert.Equal(t, IndexStatusUnindexed, seg.GetIndexStatus())
}

// =============================================================================
// HasFieldData tests using mock CSegment
// =============================================================================

func TestLocalSegment_HasFieldData(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
		},
	}
	seg, mockCSeg := newTestLocalSegment(t, 100, 1, schema)

	mockCSeg.EXPECT().HasFieldData(int64(100)).Return(true).Once()
	assert.True(t, seg.HasFieldData(100))

	mockCSeg.EXPECT().HasFieldData(int64(200)).Return(false).Once()
	assert.False(t, seg.HasFieldData(200))
}

func TestLocalSegment_HasFieldData_Released(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
		},
	}
	seg, _ := newTestLocalSegment(t, 100, 1, schema)

	// Release the segment
	guard := seg.ptrLock.StartReleaseAll()
	guard.Done(nil)

	assert.False(t, seg.HasFieldData(100))
}

// =============================================================================
// checkAllVectorFieldsIndexed tests
// =============================================================================

func Test_checkAllVectorFieldsIndexed(t *testing.T) {
	tests := []struct {
		name           string
		schema         *schemapb.CollectionSchema
		indexedFields  []int64
		expectedResult bool
	}{
		{
			name:           "nil schema returns false",
			schema:         nil,
			indexedFields:  nil,
			expectedResult: false,
		},
		{
			name: "empty schema returns true",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{},
			},
			expectedResult: true,
		},
		{
			name: "only scalar fields returns true",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64},
					{FieldID: 101, DataType: schemapb.DataType_VarChar},
				},
			},
			expectedResult: true,
		},
		{
			name: "all vector fields indexed",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64},
					{FieldID: 101, DataType: schemapb.DataType_FloatVector},
					{FieldID: 102, DataType: schemapb.DataType_BinaryVector},
				},
			},
			indexedFields:  []int64{101, 102},
			expectedResult: true,
		},
		{
			name: "partial vector fields indexed",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64},
					{FieldID: 101, DataType: schemapb.DataType_FloatVector},
					{FieldID: 102, DataType: schemapb.DataType_BinaryVector},
				},
			},
			indexedFields:  []int64{101},
			expectedResult: false,
		},
		{
			name: "no vector fields indexed",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, DataType: schemapb.DataType_FloatVector},
				},
			},
			indexedFields:  nil,
			expectedResult: false,
		},
		{
			name: "all five vector types indexed",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64},
					{FieldID: 101, DataType: schemapb.DataType_FloatVector},
					{FieldID: 102, DataType: schemapb.DataType_BinaryVector},
					{FieldID: 103, DataType: schemapb.DataType_Float16Vector},
					{FieldID: 104, DataType: schemapb.DataType_BFloat16Vector},
					{FieldID: 105, DataType: schemapb.DataType_SparseFloatVector},
				},
			},
			indexedFields:  []int64{101, 102, 103, 104, 105},
			expectedResult: true,
		},
		{
			name: "sparse vector not indexed",
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, DataType: schemapb.DataType_FloatVector},
					{FieldID: 105, DataType: schemapb.DataType_SparseFloatVector},
				},
			},
			indexedFields:  []int64{101},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSeg := NewMockSegment(t)

			indexedSet := make(map[int64]bool)
			for _, id := range tt.indexedFields {
				indexedSet[id] = true
			}

			if tt.schema != nil {
				for _, field := range tt.schema.GetFields() {
					if indexedSet[field.GetFieldID()] {
						mockSeg.EXPECT().ExistIndex(field.GetFieldID()).Return(true).Maybe()
					} else {
						mockSeg.EXPECT().ExistIndex(field.GetFieldID()).Return(false).Maybe()
					}
				}
			}

			result := checkAllVectorFieldsIndexed(mockSeg, tt.schema)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// =============================================================================
// updateSegmentIndexMetric tests
// =============================================================================

func Test_updateSegmentIndexMetric(t *testing.T) {
	paramtable.Init()

	t.Run("same status returns false without any calls", func(t *testing.T) {
		mockSeg := NewMockSegment(t)
		// No EXPECT calls - should return early without touching the mock
		result := updateSegmentIndexMetric(mockSeg, IndexStatusIndexed, IndexStatusIndexed)
		assert.False(t, result)
	})

	t.Run("CAS success returns true and updates metrics", func(t *testing.T) {
		mockSeg := NewMockSegment(t)
		mockSeg.EXPECT().CompareAndSetIndexStatus(IndexStatusUnindexed, IndexStatusIndexed).Return(true).Once()
		mockSeg.EXPECT().Collection().Return(int64(1)).Maybe()
		mockSeg.EXPECT().Type().Return(SegmentTypeSealed).Maybe()

		result := updateSegmentIndexMetric(mockSeg, IndexStatusUnindexed, IndexStatusIndexed)
		assert.True(t, result)
	})

	t.Run("CAS failure returns false", func(t *testing.T) {
		mockSeg := NewMockSegment(t)
		mockSeg.EXPECT().CompareAndSetIndexStatus(IndexStatusUnindexed, IndexStatusIndexed).Return(false).Once()
		mockSeg.EXPECT().ID().Return(int64(1)).Maybe()
		mockSeg.EXPECT().GetIndexStatus().Return(IndexStatusIndexed).Maybe()

		result := updateSegmentIndexMetric(mockSeg, IndexStatusUnindexed, IndexStatusIndexed)
		assert.False(t, result)
	})

	t.Run("indexed to unindexed transition", func(t *testing.T) {
		mockSeg := NewMockSegment(t)
		mockSeg.EXPECT().CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusUnindexed).Return(true).Once()
		mockSeg.EXPECT().Collection().Return(int64(200)).Maybe()
		mockSeg.EXPECT().Type().Return(SegmentTypeGrowing).Maybe()

		result := updateSegmentIndexMetric(mockSeg, IndexStatusIndexed, IndexStatusUnindexed)
		assert.True(t, result)
	})
}

// =============================================================================
// LoadedBinlogSize tests
// =============================================================================

func TestSegmentManager_LoadedBinlogSize(t *testing.T) {
	paramtable.Init()
	mgr := NewSegmentManager()

	t.Run("add and get", func(t *testing.T) {
		mgr.AddLoadedBinlogSize(1000)
		assert.Equal(t, int64(1000), mgr.GetLoadedBinlogSize())

		mgr.AddLoadedBinlogSize(500)
		assert.Equal(t, int64(1500), mgr.GetLoadedBinlogSize())
	})

	t.Run("sub normal", func(t *testing.T) {
		mgr.SubLoadedBinlogSize(500)
		assert.Equal(t, int64(1000), mgr.GetLoadedBinlogSize())
	})

	t.Run("sub underflow clamped to zero", func(t *testing.T) {
		mgr.SubLoadedBinlogSize(9999)
		assert.Equal(t, int64(0), mgr.GetLoadedBinlogSize())
	})

	t.Run("negative value protection in get", func(t *testing.T) {
		// Directly set to negative via atomic to test the guard
		mgr.loadedBinlogSize.Store(-100)
		assert.Equal(t, int64(0), mgr.GetLoadedBinlogSize())

		// Reset
		mgr.loadedBinlogSize.Store(0)
	})
}
