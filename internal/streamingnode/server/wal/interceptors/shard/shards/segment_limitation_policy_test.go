package shards

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// An L1 segment is sealed at the seal proportion of the segment size and at
// dataCoord.segment.maxRows rows, without the seal proportion on the rows. The
// row limit applies to every collection.
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

	capped := policy.GenerateLimitation(datapb.SegmentLevel_L1)
	require.Equal(t, uint64(512*1024*1024), capped.SegmentSize)
	require.Equal(t, uint64(4000000), capped.SegmentRows)

	// the row limit does not follow maxSize
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxSize.Key, "4096"))
	require.Equal(t, uint64(4000000), policy.GenerateLimitation(datapb.SegmentLevel_L1).SegmentRows)
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxSize.Key, "1024"))

	// maxRows 0 disables the cap
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
	off := policy.GenerateLimitation(datapb.SegmentLevel_L1)
	require.Equal(t, uint64(512*1024*1024), off.SegmentSize)
	require.Equal(t, uint64(math.MaxUint64), off.SegmentRows)
}
