package qnview

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

type distributionTestSegment struct{ segments.Segment }

func (*distributionTestSegment) ID() int64                             { panic("unpatched") }
func (*distributionTestSegment) Collection() int64                     { panic("unpatched") }
func (*distributionTestSegment) Partition() int64                      { panic("unpatched") }
func (*distributionTestSegment) Shard() metautil.Channel               { panic("unpatched") }
func (*distributionTestSegment) Version() int64                        { panic("unpatched") }
func (*distributionTestSegment) Level() datapb.SegmentLevel            { panic("unpatched") }
func (*distributionTestSegment) IsSorted() bool                        { panic("unpatched") }
func (*distributionTestSegment) LastDeltaTimestamp() uint64            { panic("unpatched") }
func (*distributionTestSegment) Indexes() []*segments.IndexedFieldInfo { panic("unpatched") }
func (*distributionTestSegment) LoadInfo() *querypb.SegmentLoadInfo    { panic("unpatched") }
func (*distributionTestSegment) GetFieldJSONIndexStats() map[int64]*querypb.JsonStatsInfo {
	panic("unpatched")
}

func TestSegmentDistributionRetainsPhysicalBases(t *testing.T) {
	mockey.PatchConvey("distribution pins physical resources and returns owned metadata", t, func() {
		mockey.Mock((*distributionTestSegment).ID).Return(int64(10)).Build()
		mockey.Mock((*distributionTestSegment).Collection).Return(int64(1)).Build()
		mockey.Mock((*distributionTestSegment).Partition).Return(int64(2)).Build()
		mockey.Mock((*distributionTestSegment).Shard).Return(metautil.NewChannel("p", 1, 0, metautil.NewDynChannelMapper())).Build()
		mockey.Mock((*distributionTestSegment).Level).Return(datapb.SegmentLevel_L1).Build()
		mockey.Mock((*distributionTestSegment).IsSorted).Return(true).Build()
		mockey.Mock((*distributionTestSegment).LastDeltaTimestamp).Return(uint64(90)).Build()
		index := &querypb.FieldIndexInfo{FieldID: 101, IndexID: 20, BuildID: 30}
		mockey.Mock((*distributionTestSegment).Indexes).Return([]*segments.IndexedFieldInfo{{IndexInfo: index, IsLoaded: true}}).Build()
		stats := &querypb.JsonStatsInfo{FieldID: 102, BuildID: 40, VersionID: 3}
		mockey.Mock((*distributionTestSegment).GetFieldJSONIndexStats).Return(map[int64]*querypb.JsonStatsInfo{102: stats}).Build()
		info := &querypb.SegmentLoadInfo{ManifestPath: "manifest-7", DataVersion: 3}
		mockey.Mock((*distributionTestSegment).LoadInfo).Return(info).Build()
		entered, proceed := make(chan struct{}), make(chan struct{})
		mockey.Mock((*distributionTestSegment).Version).To(func(*distributionTestSegment) int64 {
			close(entered)
			<-proceed
			return 0 // QueryView physical bases do not use legacy task versions.
		}).Build()
		m := &SegmentResourceManager{resources: make(map[int64]*segmentResource)}
		r := &segmentResource{manager: m, id: 10, collectionID: 1, scheduled: true}
		base := &segmentBase{segment: &metricsTransformSegment{physical: &distributionTestSegment{}}}
		r.bases = []*segmentBase{base}
		m.resources[10] = r
		m.resources[11] = &segmentResource{collectionID: 1} // No physical load yet.
		m.resources[12] = &segmentResource{collectionID: 1, closing: true, bases: []*segmentBase{base}}
		result := make(chan []*querypb.SegmentVersionInfo, 1)
		go func() { result <- m.SegmentDistribution() }()
		<-entered
		m.mu.Lock()
		require.Equal(t, 1, base.operationRefs)
		require.Empty(t, r.pruneBasesLocked())
		m.mu.Unlock()
		close(proceed)
		got := <-result
		require.Len(t, got, 1)
		require.EqualValues(t, 10, got[0].GetID())
		require.EqualValues(t, 0, got[0].GetVersion())
		require.Equal(t, "manifest-7", got[0].GetManifestPath())
		require.EqualValues(t, 3, got[0].GetDataVersion())
		index.BuildID, stats.BuildID = 99, 99
		info.ManifestPath = "later"
		require.EqualValues(t, 30, got[0].GetIndexInfo()[20].GetBuildID())
		require.EqualValues(t, 40, got[0].GetJsonStatsInfo()[102].GetBuildID())
		require.Equal(t, "manifest-7", got[0].GetManifestPath())
		m.mu.Lock()
		require.Zero(t, base.operationRefs)
		require.Len(t, r.pruneBasesLocked(), 1)
		m.mu.Unlock()
	})
}
