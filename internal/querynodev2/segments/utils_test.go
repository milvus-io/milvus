package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestFilterZeroValuesFromSlice(t *testing.T) {
	var ints []int64
	ints = append(ints, 10)
	ints = append(ints, 0)
	ints = append(ints, 5)
	ints = append(ints, 13)
	ints = append(ints, 0)

	filteredInts := FilterZeroValuesFromSlice(ints)
	assert.Equal(t, 3, len(filteredInts))
	assert.EqualValues(t, []int64{10, 5, 13}, filteredInts)
}

func TestGetSegmentRelatedDataSize(t *testing.T) {
	t.Run("seal segment", func(t *testing.T) {
		segment := NewMockSegment(t)
		segment.EXPECT().Type().Return(SegmentTypeSealed)
		segment.EXPECT().LoadInfo().Return(&querypb.SegmentLoadInfo{
			BinlogPaths: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 10,
						},
						{
							LogSize: 20,
						},
					},
				},
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 30,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 30,
						},
					},
				},
			},
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogSize: 10,
						},
					},
				},
			},
		})
		assert.EqualValues(t, 100, GetSegmentRelatedDataSize(segment))
	})

	t.Run("growing segment", func(t *testing.T) {
		segment := NewMockSegment(t)
		segment.EXPECT().Type().Return(SegmentTypeGrowing)
		segment.EXPECT().MemSize().Return(int64(100))
		assert.EqualValues(t, 100, GetSegmentRelatedDataSize(segment))
	})
}

func TestGetFieldSchema(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		filedSchema, err := getFieldSchema(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 1,
				},
			},
		}, 1)
		assert.NotNil(t, filedSchema)
		assert.NoError(t, err)
	})

	t.Run("error", func(t *testing.T) {
		filedSchema, err := getFieldSchema(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 2,
				},
			},
		}, 1)
		assert.Nil(t, filedSchema)
		assert.Error(t, err)
	})
}

func TestIsIndexMmapEnable(t *testing.T) {
	paramtable.Init()

	t.Run("mmap index param exist", func(t *testing.T) {
		enable := isIndexMmapEnable(&schemapb.FieldSchema{}, &querypb.FieldIndexInfo{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MmapEnabledKey,
					Value: "false",
				},
			},
		})
		assert.False(t, enable)
	})

	t.Run("mmap vector index param not exist", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapVectorIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapVectorIndex.Key)
		enable := isIndexMmapEnable(&schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		}, &querypb.FieldIndexInfo{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "IVF_FLAT",
				},
			},
		})
		assert.True(t, enable)
	})

	t.Run("mmap scalar index param not exist", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key)
		enable := isIndexMmapEnable(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		}, &querypb.FieldIndexInfo{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "INVERTED",
				},
			},
		})
		assert.True(t, enable)
	})

	t.Run("mmap scalar index param not supported", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key)
		enable := isIndexMmapEnable(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		}, &querypb.FieldIndexInfo{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "STL_SORT",
				},
				{Key: common.MmapEnabledKey, Value: "true"},
			},
		})
		assert.False(t, enable)
	})
}

func TestIsDataMmmapEnable(t *testing.T) {
	paramtable.Init()

	t.Run("mmap data param exist", func(t *testing.T) {
		enable := isDataMmapEnable(&schemapb.FieldSchema{
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MmapEnabledKey,
					Value: "true",
				},
			},
		})
		assert.True(t, enable)
	})

	t.Run("mmap scalar data param not exist", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)
		enable := isDataMmapEnable(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.True(t, enable)
	})

	t.Run("mmap vector data param not exist", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapVectorField.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapVectorField.Key)
		enable := isDataMmapEnable(&schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		})
		assert.True(t, enable)
	})
}

func TestGetFieldWarmupPolicy(t *testing.T) {
	paramtable.Init()

	t.Run("field TypeParams has warmup", func(t *testing.T) {
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupSync},
			},
		})
		assert.Equal(t, common.WarmupSync, policy)
	})

	t.Run("field TypeParams warmup propagated from collection level", func(t *testing.T) {
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupDisable},
			},
		})
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("async policy in TypeParams is returned", func(t *testing.T) {
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupAsync},
			},
		})
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global config for scalar field", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupSync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.Equal(t, common.WarmupSync, policy)
	})

	t.Run("fallback to global config for scalar field async", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupAsync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global config for vector field", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key, common.WarmupDisable)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key)
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		})
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("fallback to global config for vector field async", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key, common.WarmupAsync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key)
		policy := getFieldWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		})
		assert.Equal(t, common.WarmupAsync, policy)
	})
}

func TestGetIndexWarmupPolicy(t *testing.T) {
	paramtable.Init()

	t.Run("index params has warmup", func(t *testing.T) {
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.FieldIndexInfo{
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.WarmupKey, Value: common.WarmupSync},
				},
			},
		)
		assert.Equal(t, common.WarmupSync, policy)
	})

	t.Run("index params warmup propagated from collection level", func(t *testing.T) {
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.FieldIndexInfo{
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.WarmupKey, Value: common.WarmupDisable},
				},
			},
		)
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("async policy in index params is returned", func(t *testing.T) {
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.FieldIndexInfo{
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.WarmupKey, Value: common.WarmupAsync},
				},
			},
		)
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global config for scalar index", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key, common.WarmupSync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key)
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.FieldIndexInfo{},
		)
		assert.Equal(t, common.WarmupSync, policy)
	})

	t.Run("fallback to global config for scalar index async", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key, common.WarmupAsync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key)
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.FieldIndexInfo{},
		)
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global config for vector index", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.Key, common.WarmupDisable)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.Key)
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
			&querypb.FieldIndexInfo{},
		)
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("fallback to global config for vector index async", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.Key, common.WarmupAsync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.Key)
		policy := getIndexWarmupPolicy(
			&schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
			&querypb.FieldIndexInfo{},
		)
		assert.Equal(t, common.WarmupAsync, policy)
	})
}

func TestGetScalarDataWarmupPolicy(t *testing.T) {
	paramtable.Init()

	t.Run("TypeParams warmup key takes priority over global config", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupSync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupDisable},
			},
		})
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("async in TypeParams is returned", func(t *testing.T) {
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupAsync},
			},
		})
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global scalar field config async", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupAsync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.Equal(t, common.WarmupAsync, policy)
	})

	t.Run("fallback to global scalar field config disable", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupDisable)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.Equal(t, common.WarmupDisable, policy)
	})

	t.Run("field TypeParams has warmup sync", func(t *testing.T) {
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.WarmupKey, Value: common.WarmupSync},
			},
		})
		assert.Equal(t, common.WarmupSync, policy)
	})

	t.Run("fallback to global config sync", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupSync)
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
		policy := getScalarDataWarmupPolicy(&schemapb.FieldSchema{
			DataType: schemapb.DataType_String,
		})
		assert.Equal(t, common.WarmupSync, policy)
	})
}

// Tests for external collection utilities

func TestIsExternalField(t *testing.T) {
	t.Run("ExternalField", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:       100,
			Name:          "vector",
			ExternalField: "external_vector_col",
		}
		assert.True(t, IsExternalField(field))
	})

	t.Run("RegularField", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:       100,
			Name:          "vector",
			ExternalField: "",
		}
		assert.False(t, IsExternalField(field))
	})

	t.Run("NilExternalField", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID: 100,
			Name:    "vector",
		}
		assert.False(t, IsExternalField(field))
	})
}

func TestGetVirtualPK(t *testing.T) {
	t.Run("BasicGeneration", func(t *testing.T) {
		segmentID := int64(12345)
		offset := int64(100)
		virtualPK := GetVirtualPK(segmentID, offset)

		expected := (segmentID << 32) | offset
		assert.Equal(t, expected, virtualPK)
	})

	t.Run("ZeroOffset", func(t *testing.T) {
		segmentID := int64(1)
		offset := int64(0)
		virtualPK := GetVirtualPK(segmentID, offset)

		assert.Equal(t, int64(1)<<32, virtualPK)
	})

	t.Run("LargeOffset", func(t *testing.T) {
		segmentID := int64(1)
		offset := int64(0xFFFFFFFF) // Max 32-bit value
		virtualPK := GetVirtualPK(segmentID, offset)

		extractedSegment := ExtractSegmentIDFromVirtualPK(virtualPK)
		extractedOffset := ExtractOffsetFromVirtualPK(virtualPK)

		assert.Equal(t, int64(1), extractedSegment)
		assert.Equal(t, int64(0xFFFFFFFF), extractedOffset)
	})
}

func TestExtractSegmentIDFromVirtualPK(t *testing.T) {
	t.Run("BasicExtraction", func(t *testing.T) {
		segmentID := int64(999)
		offset := int64(500)
		virtualPK := GetVirtualPK(segmentID, offset)

		extracted := ExtractSegmentIDFromVirtualPK(virtualPK)
		assert.Equal(t, segmentID, extracted)
	})

	t.Run("ZeroSegmentID", func(t *testing.T) {
		virtualPK := GetVirtualPK(0, 100)
		extracted := ExtractSegmentIDFromVirtualPK(virtualPK)
		assert.Equal(t, int64(0), extracted)
	})

	t.Run("MaxValidSegmentID", func(t *testing.T) {
		segmentID := int64(0xFFFFFFFF)
		virtualPK := GetVirtualPK(segmentID, 0)
		extracted := ExtractSegmentIDFromVirtualPK(virtualPK)
		assert.Equal(t, segmentID, extracted)
	})

	t.Run("LargeSegmentIDTruncated", func(t *testing.T) {
		// Milvus segment IDs are TSO-allocated 64-bit values.
		// GetVirtualPK truncates to lower 32 bits.
		segmentID := int64(464224901019732378) // Real Milvus segment ID
		virtualPK := GetVirtualPK(segmentID, 0)
		extracted := ExtractSegmentIDFromVirtualPK(virtualPK)
		assert.Equal(t, segmentID&0xFFFFFFFF, extracted)
		assert.True(t, IsVirtualPKFromSegment(virtualPK, segmentID))
	})
}

func TestExtractOffsetFromVirtualPK(t *testing.T) {
	t.Run("BasicExtraction", func(t *testing.T) {
		segmentID := int64(999)
		offset := int64(500)
		virtualPK := GetVirtualPK(segmentID, offset)

		extracted := ExtractOffsetFromVirtualPK(virtualPK)
		assert.Equal(t, offset, extracted)
	})

	t.Run("ZeroOffset", func(t *testing.T) {
		virtualPK := GetVirtualPK(100, 0)
		extracted := ExtractOffsetFromVirtualPK(virtualPK)
		assert.Equal(t, int64(0), extracted)
	})

	t.Run("MaxOffset", func(t *testing.T) {
		maxOffset := int64(0xFFFFFFFF)
		virtualPK := GetVirtualPK(100, maxOffset)
		extracted := ExtractOffsetFromVirtualPK(virtualPK)
		assert.Equal(t, maxOffset, extracted)
	})
}

func TestIsVirtualPKFromSegment(t *testing.T) {
	t.Run("Matching", func(t *testing.T) {
		segmentID := int64(12345)
		virtualPK := GetVirtualPK(segmentID, 100)

		assert.True(t, IsVirtualPKFromSegment(virtualPK, segmentID))
	})

	t.Run("NotMatching", func(t *testing.T) {
		segmentID := int64(12345)
		virtualPK := GetVirtualPK(segmentID, 100)

		assert.False(t, IsVirtualPKFromSegment(virtualPK, segmentID+1))
		assert.False(t, IsVirtualPKFromSegment(virtualPK, 0))
	})

	t.Run("TruncatedSegmentID", func(t *testing.T) {
		// Segment ID with upper bits set - truncation is expected
		segmentID := int64(0x100000001) // 33-bit value, lower 32 bits = 1
		virtualPK := GetVirtualPK(segmentID, 100)

		// Should match both the original and truncated segment ID
		assert.True(t, IsVirtualPKFromSegment(virtualPK, segmentID))
		assert.True(t, IsVirtualPKFromSegment(virtualPK, 1)) // Truncated
	})
}

func TestVirtualPKRoundTrip(t *testing.T) {
	testCases := []struct {
		segmentID int64
		offset    int64
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{100, 100},
		{12345, 67890},
		{0xFFFFFFFF, 0xFFFFFFFF},
		{1, 0xFFFFFFFF},
		{0xFFFFFFFF, 1},
	}

	for _, tc := range testCases {
		virtualPK := GetVirtualPK(tc.segmentID, tc.offset)
		extractedSegment := ExtractSegmentIDFromVirtualPK(virtualPK)
		extractedOffset := ExtractOffsetFromVirtualPK(virtualPK)

		// Note: segment ID is truncated to lower 32 bits
		expectedSegment := tc.segmentID & 0xFFFFFFFF
		expectedOffset := tc.offset & 0xFFFFFFFF

		assert.Equal(t, expectedSegment, extractedSegment,
			"Segment ID mismatch for input segmentID=%d, offset=%d", tc.segmentID, tc.offset)
		assert.Equal(t, expectedOffset, extractedOffset,
			"Offset mismatch for input segmentID=%d, offset=%d", tc.segmentID, tc.offset)
		assert.True(t, IsVirtualPKFromSegment(virtualPK, tc.segmentID),
			"IsVirtualPKFromSegment should return true for input segmentID=%d", tc.segmentID)
	}
}
