package querynodev2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryViewSegmentMetricsRegistration(t *testing.T) {
	node := &QueryNode{serverID: 5, manager: &segments.Manager{Segment: segments.NewSegmentManager()}}
	// An unavailable factory must return a nil interface, not a typed nil.
	require.Nil(t, node.NewQueryViewSegmentManager(t.Context(), nil, nil, nil))
	require.Nil(t, node.queryViewSegments.Load())

	mockey.PatchConvey("metrics reads the same resource manager used by QueryViews", t, func() {
		resources := &qnview.SegmentResourceManager{}
		mockey.Mock(qvresource.NewQueryViewSegmentManager).Return(resources).Build()
		require.Same(t, resources, node.NewQueryViewSegmentManager(t.Context(), nil, nil, nil))
		require.Same(t, resources, node.queryViewSegments.Load())

		mockey.Mock((*qnview.SegmentResourceManager).SegmentMetrics).To(func(m *qnview.SegmentResourceManager, collectionID, nodeID int64) []*metricsinfo.Segment {
			require.Same(t, resources, m)
			require.EqualValues(t, 1, collectionID)
			require.EqualValues(t, 5, nodeID)
			return []*metricsinfo.Segment{{SegmentID: 10, CollectionID: collectionID, NodeID: nodeID, MemSize: 50}}
		}).Build()

		var result []*metricsinfo.Segment
		require.NoError(t, json.Unmarshal([]byte(getSegmentJSON(node, 1)), &result))
		require.Len(t, result, 1, "QV resources must be visible even when the legacy segment manager is empty")
		require.EqualValues(t, 10, result[0].SegmentID)
		require.EqualValues(t, 50, result[0].MemSize)
	})

	// The published manager remains readable after all physical resources retire.
	var result []*metricsinfo.Segment
	require.NoError(t, json.Unmarshal([]byte(getSegmentJSON(node, 1)), &result))
	require.Empty(t, result)
	var absent *QueryNode
	require.Nil(t, absent.NewQueryViewSegmentManager(context.Background(), nil, nil, nil))
}

func TestQueryViewDataDistributionUsesPhysicalResources(t *testing.T) {
	mockey.PatchConvey("legacy delta requests cannot hide QueryView bases", t, func() {
		resources := &qnview.SegmentResourceManager{}
		node := queryViewDistributionTestNode()
		unchanged, err := node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{LastUpdateTs: 100, SupportDelta: true})
		require.NoError(t, merr.CheckRPCCall(unchanged, err))
		require.Empty(t, unchanged.GetSegments())
		node.queryViewSegments.Store(resources)
		version := mockey.Mock((*qnview.SegmentResourceManager).DistributionModifyTS).Return(int64(101)).Build()
		infos := []*querypb.SegmentVersionInfo{{ID: 10, Collection: 1, ManifestPath: "segment/_manifest/7.manifest"}}
		report := mockey.Mock((*qnview.SegmentResourceManager).SegmentDistribution).Return(infos).Build()
		response, err := node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{LastUpdateTs: 100, SupportDelta: true})
		require.NoError(t, merr.CheckRPCCall(response, err))
		require.False(t, response.GetIsDelta())
		require.Equal(t, infos, response.GetSegments())
		require.EqualValues(t, 1, response.GetTotalSegmentCount())
		require.EqualValues(t, 5, response.GetNodeID())
		require.Empty(t, response.GetLeaderViews(), "serving topology belongs to QueryViews")
		unchanged, err = node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{LastUpdateTs: 101, SupportDelta: true})
		require.NoError(t, merr.CheckRPCCall(unchanged, err))
		require.Empty(t, unchanged.GetSegments())
		require.Equal(t, 1, report.Times(), "unchanged requests must not sample or clone QV resources")
		version.Return(int64(102))
		response, err = node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{LastUpdateTs: 101, SupportDelta: true})
		require.NoError(t, merr.CheckRPCCall(response, err))
		require.False(t, response.GetIsDelta())
		require.Equal(t, infos, response.GetSegments())
		require.Equal(t, 2, report.Times())
	})
}

func queryViewDistributionTestNode() *QueryNode {
	return &QueryNode{
		serverID: 5, lifetime: lifetime.NewLifetime(commonpb.StateCode_Healthy),
		manager:               &segments.Manager{Segment: segments.NewSegmentManager()},
		delegators:            typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		distDeltaTracker:      newDataDistributionDeltaTracker(),
		subscribingChannels:   typeutil.NewConcurrentSet[string](),
		unsubscribingChannels: typeutil.NewConcurrentSet[string](),
	}
}

type distributionLegacyManager struct{ segments.SegmentManager }

func (*distributionLegacyManager) CountBy(...segments.SegmentFilter) int { panic("unpatched") }
func (*distributionLegacyManager) RangeBy(segments.SegmentVisitor, ...segments.SegmentFilter) {
	panic("unpatched")
}
func (*distributionLegacyManager) GetSealed(int64) segments.Segment { panic("unpatched") }
func (*distributionLegacyManager) GetWithType(int64, segments.SegmentType) segments.Segment {
	panic("unpatched")
}

type distributionLegacySegment struct{ segments.Segment }

func (*distributionLegacySegment) ID() int64                         { panic("unpatched") }
func (*distributionLegacySegment) InsertCount() int64                { panic("unpatched") }
func (*distributionLegacySegment) StartPosition() *msgpb.MsgPosition { panic("unpatched") }

type distributionLegacyDelegator struct{ delegator.ShardDelegator }

func (*distributionLegacyDelegator) Serviceable() bool { panic("unpatched") }
func (*distributionLegacyDelegator) Collection() int64 { panic("unpatched") }
func (*distributionLegacyDelegator) Version() int64    { panic("unpatched") }
func (*distributionLegacyDelegator) GetSegmentInfo(bool) ([]delegator.SnapshotItem, []delegator.SegmentEntry) {
	panic("unpatched")
}

func (*distributionLegacyDelegator) GetPartitionStatsVersions(context.Context) map[int64]int64 {
	panic("unpatched")
}
func (*distributionLegacyDelegator) CatchingUpStreamingData() bool { panic("unpatched") }

func TestQueryViewDataDistributionPreservesLegacyResources(t *testing.T) {
	mockey.PatchConvey("a QV manager must not hide legacy resources on the same node", t, func() {
		node := queryViewDistributionTestNode()
		node.queryViewSegments.Store(&qnview.SegmentResourceManager{})
		mockey.Mock((*qnview.SegmentResourceManager).DistributionModifyTS).Return(int64(101)).Build()
		mockey.Mock((*qnview.SegmentResourceManager).SegmentDistribution).Return([]*querypb.SegmentVersionInfo{{ID: 10}}).Build()
		segment := &distributionLegacySegment{}
		mockey.Mock((*distributionLegacySegment).ID).Return(int64(20)).Build()
		mockey.Mock((*distributionLegacyManager).CountBy).Return(1).Build()
		mockey.Mock((*distributionLegacyManager).RangeBy).To(func(_ *distributionLegacyManager, visit segments.SegmentVisitor, _ ...segments.SegmentFilter) {
			visit(segment)
		}).Build()
		mockey.Mock((*distributionLegacyManager).GetSealed).Return(segment).Build()
		mockey.Mock(buildSegmentVersionInfo).Return(&querypb.SegmentVersionInfo{ID: 20}).Build()
		node.manager.Segment = &distributionLegacyManager{}
		mockey.Mock((*distributionLegacyManager).GetWithType).To(func(_ *distributionLegacyManager, id int64, kind segments.SegmentType) segments.Segment {
			require.Equal(t, segments.SegmentTypeGrowing, kind)
			if id == 30 {
				return segment
			}
			return nil
		}).Build()
		mockey.Mock((*distributionLegacySegment).InsertCount).Return(int64(7)).Build()
		mockey.Mock((*distributionLegacySegment).StartPosition).Return(&msgpb.MsgPosition{Timestamp: 40}).Build()
		legacy := &distributionLegacyDelegator{}
		node.delegators.Insert("legacy-channel", legacy)
		node.delegators.Insert("retired-channel", &distributionLegacyDelegator{})
		mockey.Mock((*distributionLegacyDelegator).Serviceable).To(func(d *distributionLegacyDelegator) bool { return d == legacy }).Build()
		mockey.Mock((*distributionLegacyDelegator).Collection).Return(int64(1)).Build()
		mockey.Mock((*distributionLegacyDelegator).Version).Return(int64(7)).Build()
		mockey.Mock((*distributionLegacyDelegator).GetSegmentInfo).Return(
			[]delegator.SnapshotItem{{NodeID: 5, Segments: []delegator.SegmentEntry{{SegmentID: 20, Version: 3}}}},
			[]delegator.SegmentEntry{{SegmentID: 30}, {SegmentID: 31}},
		).Build()
		mockey.Mock((*distributionLegacyDelegator).GetChannelQueryView).Return(delegator.NewChannelQueryView([]int64{30, 31}, map[int64]int64{20: 8}, nil, 7)).Build()
		mockey.Mock((*distributionLegacyDelegator).GetPartitionStatsVersions).Return(map[int64]int64{2: 9}).Build()
		mockey.Mock((*distributionLegacyDelegator).CatchingUpStreamingData).Return(false).Build()
		response, err := node.GetDataDistribution(t.Context(), &querypb.GetDataDistributionRequest{LastUpdateTs: 100, SupportDelta: true})
		require.NoError(t, merr.CheckRPCCall(response, err))
		require.False(t, response.GetIsDelta())
		require.EqualValues(t, 2, response.GetTotalSegmentCount())
		require.EqualValues(t, 20, response.GetSegments()[0].GetID())
		require.EqualValues(t, 10, response.GetSegments()[1].GetID())
		require.EqualValues(t, 1, response.GetTotalChannelCount())
		require.Len(t, response.GetChannels(), 1)
		require.Equal(t, "legacy-channel", response.GetChannels()[0].GetChannel())
		require.Len(t, response.GetLeaderViews(), 1)
		leader := response.GetLeaderViews()[0]
		require.Equal(t, "legacy-channel", leader.GetChannel())
		require.EqualValues(t, 7, leader.GetNumOfGrowingRows())
		require.EqualValues(t, 40, leader.GetGrowingSegments()[30].GetTimestamp())
		require.NotNil(t, leader.GetGrowingSegments()[31], "a missing legacy Growing still retains its reported identity")
		require.Zero(t, leader.GetGrowingSegments()[31].GetTimestamp())
		require.EqualValues(t, 5, leader.GetSegmentDist()[20].GetNodeID())
		require.EqualValues(t, 7, leader.GetTargetVersion())
		require.Equal(t, map[int64]int64{2: 9}, leader.GetPartitionStatsVersions())
	})
}
