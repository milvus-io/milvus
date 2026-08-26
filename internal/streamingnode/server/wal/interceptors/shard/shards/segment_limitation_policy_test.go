package shards

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestJitterSegmentLimitationPolicyL1WholeRow(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricWholeRow)
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
	}}
	limitation := getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1, schema)
	assert.Equal(t, "jitter_segment_limitation", limitation.PolicyName)
	assert.Equal(t, uint64(math.MaxUint64), limitation.SegmentRows)
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
	assert.Less(t, limitation.SegmentSize, uint64(float64(maxSegmentSize)*proportion)+1)
}

func TestJitterSegmentLimitationPolicyL1MainIndex(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricMainIndex)
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key, "-1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
		{FieldID: 11, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}}},
	}}
	limitation := getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1, schema)

	// SegmentRows should be the row-cap formula bound (not unbounded).
	assert.NotEqual(t, uint64(math.MaxUint64), limitation.SegmentRows)
	// 8×4 bytes per row of main column; budget = jitter×maxSize×proportion / 32.
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
	assert.LessOrEqual(t, limitation.SegmentRows, uint64(float64(maxSegmentSize)*proportion/32))
}

func TestJitterSegmentLimitationPolicyL1MainIndexCeiling(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricMainIndex)
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key, "64")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key)

	// 1KB scalar + dim=8 float vector: main-index budget allows millions of
	// rows, but a 64MB whole-row ceiling caps rows to ~200k (whole-row estimate
	// is capped at the VarChar estimate length).
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
		{FieldID: 11, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}}},
	}}
	limitation := getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1, schema)

	// Ceiling rows (64MB / whole-row-per-record) is far below the main-index
	// budget rows, so the ceiling dominates the row cap.
	ceilingRows := uint64(64*1024*1024) / 296
	assert.LessOrEqual(t, limitation.SegmentRows, ceilingRows)
	assert.Less(t, limitation.SegmentRows, uint64(500000))
}

func TestJitterSegmentLimitationPolicyL1MainIndexSparseOnly(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricMainIndex)
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	// Sparse-only has no dense vector field → whole-row fallback (unbounded rows).
	limitation := getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L1, schema)
	assert.Equal(t, uint64(math.MaxUint64), limitation.SegmentRows)
}

func TestJitterSegmentLimitationPolicyL0(t *testing.T) {
	paramtable.Init()
	limitation := getSegmentLimitationPolicy().GenerateLimitation(datapb.SegmentLevel_L0, nil)
	assert.Equal(t, "jitter_segment_limitation", limitation.PolicyName)
	assert.NotZero(t, limitation.SegmentSize)
}
