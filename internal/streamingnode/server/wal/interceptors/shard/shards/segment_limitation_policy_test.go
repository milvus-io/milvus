package shards

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// An L1 segment is sealed at the seal proportion of the segment size. A row
// capped collection is additionally sealed at dataCoord.segment.maxRows rows,
// without the seal proportion; the byte limit does not change.
func TestL1LimitationRowCap(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxSize.Key, "1024"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "4000000"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentSealProportion.Key, "0.5"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentSealProportionJitter.Key, "0"))
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.SegmentMaxSize.Key)
		params.Reset(params.DataCoordCfg.SegmentMaxRows.Key)
		params.Reset(params.DataCoordCfg.SegmentSealProportion.Key)
		params.Reset(params.DataCoordCfg.SegmentSealProportionJitter.Key)
	})
	policy := getSegmentLimitationPolicy()

	uncapped := policy.GenerateLimitation(datapb.SegmentLevel_L1, false)
	require.Equal(t, uint64(512*1024*1024), uncapped.SegmentSize)
	require.Equal(t, uint64(math.MaxUint64), uncapped.SegmentRows)

	capped := policy.GenerateLimitation(datapb.SegmentLevel_L1, true)
	require.Equal(t, uint64(512*1024*1024), capped.SegmentSize)
	require.Equal(t, uint64(4000000), capped.SegmentRows)

	// maxRows 0 disables the cap
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
	require.Equal(t, uint64(math.MaxUint64), policy.GenerateLimitation(datapb.SegmentLevel_L1, true).SegmentRows)
}

func TestCollectionInfoRowCapped(t *testing.T) {
	info := &CollectionInfo{PartitionIDs: map[int64]struct{}{}}
	require.False(t, info.RowCapped())
	require.False(t, (*CollectionInfo)(nil).RowCapped())

	pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	info.setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		pk, {FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32},
	}}})
	require.True(t, info.RowCapped())

	info.setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		pk, {
			FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
		},
	}}})
	require.False(t, info.RowCapped())

	info.setSchema(nil)
	require.False(t, info.RowCapped())
}
