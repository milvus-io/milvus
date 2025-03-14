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
