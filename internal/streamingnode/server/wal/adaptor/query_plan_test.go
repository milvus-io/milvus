//go:build test && dynamic

package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const queryPlanTestVChannel = "by-dev-rootcoord-dml_0_100v0"

type queryPlanTestCatalog struct {
	metastore.StreamingNodeCataLog
}

func (queryPlanTestCatalog) SaveQueryViews(context.Context, string, []*viewpb.QueryViewOfShard) error {
	return nil
}

type queryPlanTestResourceManager struct {
	acquired []snview.AcquireResource
}

func (m *queryPlanTestResourceManager) Acquire(req snview.AcquireResource) {
	m.acquired = append(m.acquired, req)
}

func (m *queryPlanTestResourceManager) Release(req snview.ReleaseResource) {
	if req.OnDropped != nil {
		req.OnDropped()
	}
}

func newQueryPlanTestMeta(state viewpb.QueryViewState) *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: 10,
		ReplicaId:    1,
		Vchannel:     queryPlanTestVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		},
		State: state,
	}
}

func newQueryPlanTestView(state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	return qviews.NewFullQueryViewAtStreamingNode(
		newQueryPlanTestMeta(state),
		&viewpb.QueryViewOfStreamingNode{},
		[]*viewpb.QueryViewOfQueryNode{
			{
				NodeId: 1,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{1001}},
				},
			},
			{
				NodeId: 2,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 20, SegmentIds: []int64{2001}},
				},
			},
		},
	)
}

func newQueryPlanTestWALAdaptor(t *testing.T) *walAdaptorImpl {
	t.Helper()

	resource.InitForTest(t)

	resMgr := &queryPlanTestResourceManager{}
	queryViewHandler := snview.RecoverPChannelSNQueryViewHandler(
		"by-dev-rootcoord-dml_0",
		queryPlanTestCatalog{},
		resMgr,
		nil,
	)
	queryViewHandler.ApplyViews([]handler.ApplyView{{View: newQueryPlanTestView(viewpb.QueryViewState_QueryViewStatePreparing)}})
	require.Len(t, resMgr.acquired, 1)
	resMgr.acquired[0].OnReady()
	queryViewHandler.ApplyViews([]handler.ApplyView{{View: newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp)}})

	walImpls := newRecoveryBarrierWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) {
		panic("unexpected append")
	})
	return &walAdaptorImpl{
		roWALAdaptorImpl: &roWALAdaptorImpl{
			lifetime:   typeutil.NewLifetime(),
			roWALImpls: walImpls,
		},
		rwWALImpls: walImpls,
		param: &interceptors.InterceptorBuildParam{
			MVCCManager: mvcc.NewMVCCManager(0),
		},
		queryViewHandler: queryViewHandler,
	}
}

func TestWALAdaptorGetQueryPlanBuildsPlanFromLatestUpView(t *testing.T) {
	walAdaptor := newQueryPlanTestWALAdaptor(t)
	searchReq := &internalpb.SearchRequest{CollectionID: 10}
	req := &viewpb.GetQueryPlanRequest{
		CollectionId: 10,
		ShardId:      &viewpb.ShardID{ReplicaId: 1, Vchannel: queryPlanTestVChannel},
		Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{
			GrowingTimetick:      123,
			TransformingTimetick: 122,
		}},
		PartitionIds: []int64{20},
		Request:      &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: searchReq},
	}

	plan, err := walAdaptor.GetQueryPlan(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, uint64(123), plan.GetMvcc().GetGrowingTimetick())
	assert.Equal(t, uint64(122), plan.GetMvcc().GetTransformingTimetick())
	assert.Equal(t, req.GetShardId(), plan.GetShardId())
	assert.Equal(t, newQueryPlanTestMeta(viewpb.QueryViewState_QueryViewStateUp).GetVersion(), plan.GetVersion())
	assert.NotSame(t, searchReq, plan.GetLegacySearchRequest())
	require.Len(t, plan.GetWorkNodes(), 2)
	assert.Equal(t, "by-dev-rootcoord-dml_0", plan.GetWorkNodes()[0].GetStreamingNode().GetPchannel())
	assert.Equal(t, int64(2), plan.GetWorkNodes()[1].GetQueryNode().GetNodeId())
}

func TestWALAdaptorGetMVCCTimestampReturnsQueryPlanMVCC(t *testing.T) {
	walAdaptor := newQueryPlanTestWALAdaptor(t)
	walAdaptor.param.MVCCManager.ApplyRecoveryBarrier(queryPlanTestVChannel, 100)
	walAdaptor.param.MVCCManager.UpdateMVCC(createQueryPlanTestMessage(t, 130, queryPlanTestVChannel, message.MessageTypeInsert))
	walAdaptor.param.MVCCManager.UpdateMVCC(createQueryPlanTestMessage(t, 140, queryPlanTestVChannel, message.MessageTypeDelete))

	resp, err := walAdaptor.GetMVCCTimestamp(context.Background(), &viewpb.GetMVCCTimestampRequest{
		Vchannel: queryPlanTestVChannel,
	})

	require.NoError(t, err)
	require.NotNil(t, resp.GetMvcc())
	assert.Equal(t, uint64(140), resp.GetMvcc().GetGrowingTimetick())
	assert.Equal(t, uint64(140), resp.GetMvcc().GetTransformingTimetick())
}

func TestBuildQueryPlanWorkNodesPrunesByPartitionAndIgnoreGrowing(t *testing.T) {
	view := &viewpb.QueryViewOfShard{
		Meta:          &viewpb.QueryViewMeta{Vchannel: queryPlanTestVChannel},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{
				NodeId: 1,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{1001}},
				},
			},
			{
				NodeId: 2,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 20, SegmentIds: []int64{2001}},
				},
			},
		},
	}
	req := &viewpb.GetQueryPlanRequest{
		PartitionIds: []int64{20},
		Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{
			LegacySearchRequest: &internalpb.SearchRequest{IgnoreGrowing: true},
		},
	}

	nodes := buildQueryPlanWorkNodes(view, req)

	assert.Len(t, nodes, 1)
	assert.Equal(t, int64(2), nodes[0].GetQueryNode().GetNodeId())
}

func TestBuildQueryPlanWorkNodesIncludesStreamingAndNonEmptyQueryNodes(t *testing.T) {
	view := &viewpb.QueryViewOfShard{
		Meta:          &viewpb.QueryViewMeta{Vchannel: queryPlanTestVChannel},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{
				NodeId: 1,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 10},
				},
			},
			{
				NodeId: 2,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 20, SegmentIds: []int64{2001}},
				},
			},
		},
	}

	nodes := buildQueryPlanWorkNodes(view, &viewpb.GetQueryPlanRequest{})

	assert.Len(t, nodes, 2)
	assert.NotNil(t, nodes[0].GetStreamingNode())
	assert.Equal(t, int64(2), nodes[1].GetQueryNode().GetNodeId())
}

func createQueryPlanTestMessage(t *testing.T, timetick uint64, vchannel string, msgType message.MessageType) message.MutableMessage {
	t.Helper()

	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().IsPersisted().Return(true)
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().VChannel().Return(vchannel).Maybe()
	msg.EXPECT().MessageType().Return(msgType).Maybe()
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	return msg
}
