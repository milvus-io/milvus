package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommonParam(t *testing.T) {
	var CParams ComponentParam
	CParams.Init()

	t.Run("test commonConfig", func(t *testing.T) {
		Params := CParams.CommonCfg

		assert.NotEqual(t, Params.DefaultPartitionName, "")
		t.Logf("default partition name = %s", Params.DefaultPartitionName)

		assert.NotEqual(t, Params.DefaultIndexName, "")
		t.Logf("default index name = %s", Params.DefaultIndexName)

		assert.Equal(t, Params.RetentionDuration, int64(DefaultRetentionDuration))
		t.Logf("default retention duration = %d", Params.RetentionDuration)

		assert.Equal(t, int64(Params.EntityExpirationTTL), int64(-1))
		t.Logf("default entity expiration = %d", Params.EntityExpirationTTL)

		// test the case coommo
		Params.Base.Save("common.entityExpiration", "50")
		Params.initEntityExpiration()
		assert.Equal(t, int64(Params.EntityExpirationTTL.Seconds()), int64(DefaultRetentionDuration))

		assert.NotEqual(t, Params.SimdType, "")
		t.Logf("knowhere simd type = %s", Params.SimdType)

		assert.Equal(t, Params.IndexSliceSize, int64(DefaultIndexSliceSize))
		t.Logf("knowhere index slice size = %d", Params.IndexSliceSize)

		assert.Equal(t, Params.GracefulTime, int64(DefaultGracefulTime))
		t.Logf("default grafeful time = %d", Params.GracefulTime)

		// -- proxy --
		assert.Equal(t, Params.ProxySubName, "by-dev-proxy")
		t.Logf("ProxySubName: %s", Params.ProxySubName)

		// -- rootcoord --
		assert.Equal(t, Params.RootCoordTimeTick, "by-dev-rootcoord-timetick")
		t.Logf("rootcoord timetick channel = %s", Params.RootCoordTimeTick)

		assert.Equal(t, Params.RootCoordStatistics, "by-dev-rootcoord-statistics")
		t.Logf("rootcoord statistics channel = %s", Params.RootCoordStatistics)

		assert.Equal(t, Params.RootCoordDml, "by-dev-rootcoord-dml")
		t.Logf("rootcoord dml channel = %s", Params.RootCoordDml)

		assert.Equal(t, Params.RootCoordDelta, "by-dev-rootcoord-delta")
		t.Logf("rootcoord delta channel = %s", Params.RootCoordDelta)

		assert.Equal(t, Params.RootCoordSubName, "by-dev-rootCoord")
		t.Logf("rootcoord subname = %s", Params.RootCoordSubName)

		// -- querycoord --
		assert.Equal(t, Params.QueryCoordTimeTick, "by-dev-queryTimeTick")
		t.Logf("querycoord timetick channel = %s", Params.QueryCoordTimeTick)

		// -- querynode --
		assert.Equal(t, Params.QueryNodeSubName, "by-dev-queryNode")
		t.Logf("querynode subname = %s", Params.QueryNodeSubName)

		// -- datacoord --
		assert.Equal(t, Params.DataCoordTimeTick, "by-dev-datacoord-timetick-channel")
		t.Logf("datacoord timetick channel = %s", Params.DataCoordTimeTick)

		assert.Equal(t, Params.DataCoordSegmentInfo, "by-dev-segment-info-channel")
		t.Logf("datacoord segment info channel = %s", Params.DataCoordSegmentInfo)

		assert.Equal(t, Params.DataCoordSubName, "by-dev-dataCoord")
		t.Logf("datacoord subname = %s", Params.DataCoordSubName)

		assert.Equal(t, Params.DataNodeSubName, "by-dev-dataNode")
		t.Logf("datanode subname = %s", Params.DataNodeSubName)
	})

}
