package shards

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The L1 seal size follows the per-collection target DataCoord uses: maxSize
// capped at maxRows rows of the collection's row width, then the seal
// proportion and the jitter.
func TestL1LimitationFollowsRowWidth(t *testing.T) {
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

	// unknown row width: maxSize x proportion
	require.Equal(t, uint64(512*1024*1024), policy.GenerateLimitation(datapb.SegmentLevel_L1, 0).SegmentSize)
	// 28 B rows: 112 MB target x proportion
	require.Equal(t, uint64(28*4000000/2), policy.GenerateLimitation(datapb.SegmentLevel_L1, 28).SegmentSize)
	// wide rows keep maxSize
	require.Equal(t, uint64(512*1024*1024), policy.GenerateLimitation(datapb.SegmentLevel_L1, 1000).SegmentSize)
	// maxRows 0 disables the cap
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
	require.Equal(t, uint64(512*1024*1024), policy.GenerateLimitation(datapb.SegmentLevel_L1, 28).SegmentSize)
}

func TestCollectionInfoRowWidth(t *testing.T) {
	info := &CollectionInfo{PartitionIDs: map[int64]struct{}{}}
	require.Equal(t, int64(0), info.RowWidth())
	require.Equal(t, int64(0), (*CollectionInfo)(nil).RowWidth())
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32},
	}}
	info.setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: schema})
	require.Equal(t, typeutil.EstimateRowWidth(schema), info.RowWidth())
	require.Equal(t, int64(28), info.RowWidth())
	info.setSchema(nil)
	require.Equal(t, int64(0), info.RowWidth())
}
