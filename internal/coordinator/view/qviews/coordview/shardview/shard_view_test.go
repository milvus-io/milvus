package shardview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_recovery"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var eventsIter *events.RecordAllEventsNotifierIter

func initTestRecoveryStorage(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().List(mock.Anything).Return(nil, nil).Maybe()
	rs.EXPECT().Save(mock.Anything, mock.Anything).Return(nil).Maybe()
	rs.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	recoveryStorage = rs
	syncer := mock_syncer.NewMockCoordSyncer(t)
	syncer.EXPECT().Sync(mock.Anything).Return().Maybe()
	coordSyncer = syncer
	eventsIter = events.InitRecordAllEventsNotifier().Iter()
}

func TestShardViewNormalPath(t *testing.T) {
	initTestRecoveryStorage(t)

	ctx := context.Background()
	sv := NewShardView(qviews.ShardID{
		ReplicaID: 1,
		VChannel:  "v1",
	})

	// continuously apply view 1, 2, 3, 4, 5...
	qv1 := createQV(1, "v1", 1)
	v1 := applyNewView(t, sv, qv1)

	qv2 := createQV(1, "v1", 1)
	v2 := applyNewView(t, sv, qv2)
	assert.True(t, v2.GT(v1))

	qv3 := createQV(1, "v1", 2)
	v3 := applyNewView(t, sv, qv3)
	assert.True(t, v3.GT(v2))

	qv4 := createQV(1, "v1", 2)
	v4 := applyNewView(t, sv, qv4)
	assert.True(t, v4.GT(v2))

	// applying a rollback data version is not allowed.
	qv5 := createQV(1, "v1", 1)
	v5, err := sv.ApplyNewQueryView(ctx, qv5)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDataVersionTooOld)
	assert.Nil(t, v5)

	// test recovery path.
	protos := make([]*viewpb.QueryViewOfShard, 0, len(sv.queryViews))
	for _, v := range sv.queryViews {
		protos = append(protos, proto.Clone(v.inner).(*viewpb.QueryViewOfShard))
	}
	sv2 := RecoverShardView(protos)
	assert.Equal(t, len(sv.queryViews), len(sv2.queryViews))

	sv.RequestRelease(ctx)
	// apply after release should be error.
	v5, err = sv.ApplyNewQueryView(ctx, qv3)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrShardReleased)
	assert.Nil(t, v5)
}

func TestRecoveryPath(t *testing.T) {
	initTestRecoveryStorage(t)

	ctx := context.Background()
	shardID := qviews.ShardID{
		ReplicaID: 1,
		VChannel:  "v1",
	}
	sv := NewShardView(shardID)

	// continuously apply view 1, 2, 3, 4, 5...
	qv1 := createQV(1, "v1", 1)
	_ = applyNewView(t, sv, qv1)

	// Test a overwite view when there's a on-preparing view.
	qv2 := createQV(1, "v1", 1)
	v2, err := sv.ApplyNewQueryView(ctx, qv2)
	assert.NoError(t, err)
	assert.NotNil(t, v2)

	// test recovery path.
	protos := make([]*viewpb.QueryViewOfShard, 0, len(sv.queryViews))
	for _, v := range sv.queryViews {
		protos = append(protos, proto.Clone(v.inner).(*viewpb.QueryViewOfShard))
	}
	sv2 := RecoverShardView(protos)
	assert.Equal(t, len(sv.queryViews), len(sv2.queryViews))
	assert.Equal(t, *sv.onPreparingVersion, *sv2.onPreparingVersion)
	assert.Equal(t, *sv.upVersion, *sv2.upVersion)

	sv.queryViews[*sv.upVersion].EnterDown()
	protos = make([]*viewpb.QueryViewOfShard, 0, len(sv.queryViews))
	for _, v := range sv.queryViews {
		protos = append(protos, proto.Clone(v.inner).(*viewpb.QueryViewOfShard))
	}
	sv2 = RecoverShardView(protos)
	assert.Equal(t, len(sv.queryViews), len(sv2.queryViews))
	assert.Equal(t, *sv.onPreparingVersion, *sv2.onPreparingVersion)
	assert.Nil(t, sv2.upVersion)

	sv.RequestRelease(ctx)
}

func TestShardViewAtNodeUnrecoverable(t *testing.T) {
	initTestRecoveryStorage(t)

	ctx := context.Background()
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}
	sv := NewShardView(shardID)
	vup := applyNewView(t, sv, createQV(1, "v1", 1))

	qv := createQV(1, "v1", 1)
	v1, err := sv.ApplyNewQueryView(ctx, qv)
	assert.NoError(t, err)
	assert.NotNil(t, v1)

	// Test a overwite view when it's a ready view.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateReady))
	assertNoStateTransition(t)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewQueryNode(1), qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStateUnrecoverable)
	assertStateTransition(t, qviews.QueryViewStatePreparing, qviews.QueryViewStateUnrecoverable)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewQueryNode(2), qviews.QueryViewStateReady))
	assertNoStateTransition(t)
	assert.Nil(t, sv.onPreparingVersion)
	assert.NotNil(t, *sv.upVersion)
	assert.Equal(t, *sv.upVersion, vup)

	sv.RequestRelease(ctx)
}

func TestShardViewAtOverwitePath(t *testing.T) {
	initTestRecoveryStorage(t)

	ctx := context.Background()
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}
	sv := NewShardView(shardID)

	applyNewView(t, sv, createQV(1, "v1", 1))

	qv := createQV(1, "v1", 1)
	v1, err := sv.ApplyNewQueryView(ctx, qv)
	assert.NoError(t, err)
	assert.NotNil(t, v1)
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v1)
	assert.Len(t, sv.queryViews, 2)

	// Test a overwite view when there's a on-preparing view.
	qv = createQV(1, "v1", 1)
	v2, err := sv.ApplyNewQueryView(ctx, qv)
	assert.NoError(t, err)
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v2)
	assert.Len(t, sv.queryViews, 3)
	// The previous on-preparing view should be dropping.
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStateDropping)
	assertStateTransition(t, qviews.QueryViewStatePreparing, qviews.QueryViewStateUnrecoverable)
	assertStateTransition(t, qviews.QueryViewStateUnrecoverable, qviews.QueryViewStateDropping)

	// Test a overwite view when it's a ready view.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v2, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateReady))
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v2, qviews.NewQueryNode(1), qviews.QueryViewStateReady))
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v2, qviews.NewQueryNode(2), qviews.QueryViewStateReady))
	assertStateTransition(t, qviews.QueryViewStatePreparing, qviews.QueryViewStateReady)

	qv = createQV(1, "v1", 1)
	v3, err := sv.ApplyNewQueryView(ctx, qv)
	assert.NoError(t, err)
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v3)
	assert.Len(t, sv.queryViews, 4)
	// The previous on-preparing view should be down.
	assert.Equal(t, sv.queryViews[*v2].State(), qviews.QueryViewStateDown)
	assertStateTransition(t, qviews.QueryViewStateReady, qviews.QueryViewStateUnrecoverable)
	assertStateTransition(t, qviews.QueryViewStateUnrecoverable, qviews.QueryViewStateDown)

	sv.RequestRelease(ctx)
}

func applyNewView(t *testing.T, sv *shardViewImpl, qv *QueryViewAtCoordBuilder) qviews.QueryViewVersion {
	ctx := context.Background()
	qvLen := len(sv.queryViews) + 1
	v1, err := sv.ApplyNewQueryView(ctx, qv)
	assert.NoError(t, err)
	assert.NotNil(t, v1)

	// Apply a preparing state view, no event happens.
	shardID := qviews.ShardID{
		ReplicaID: 1,
		VChannel:  "v1",
	}
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStatePreparing))
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStatePreparing))
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v1)
	assert.Len(t, sv.queryViews, qvLen)
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStatePreparing)
	assertNoStateTransition(t)

	// Apply a ready state view, should trigger a state transition.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.QueryNode{ID: 1}, qviews.QueryViewStateReady))
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v1)
	assert.Len(t, sv.queryViews, qvLen)
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStatePreparing)
	assertNoStateTransition(t)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateReady))
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v1)
	assert.Len(t, sv.queryViews, qvLen)
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStatePreparing)
	assertNoStateTransition(t)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.QueryNode{ID: 2}, qviews.QueryViewStateReady))
	assert.NotNil(t, sv.onPreparingVersion)
	assert.Equal(t, *sv.onPreparingVersion, *v1)
	assert.Len(t, sv.queryViews, qvLen)
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStateReady)
	assertStateTransition(t, qviews.QueryViewStatePreparing, qviews.QueryViewStateReady)

	// Apply a up state view, should trigger a state transition, and the view is up.
	// Get previous up state view.
	oldUpVersion := sv.upVersion
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *v1, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateUp))
	assert.Nil(t, sv.onPreparingVersion)
	assert.NotNil(t, sv.upVersion)
	assert.Equal(t, *sv.upVersion, *v1)
	assert.Len(t, sv.queryViews, qvLen)
	assert.Equal(t, sv.queryViews[*v1].State(), qviews.QueryViewStateUp)
	assertStateTransition(t, qviews.QueryViewStateReady, qviews.QueryViewStateUp)

	if oldUpVersion == nil {
		return *v1
	}
	// make assertion to check the old up version is down.
	assert.NotNil(t, sv.queryViews[*oldUpVersion])
	assert.Equal(t, sv.queryViews[*oldUpVersion].State(), qviews.QueryViewStateDown)
	assertStateTransition(t, qviews.QueryViewStateUp, qviews.QueryViewStateDown)

	// Start the normal down path for previous up view.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *oldUpVersion, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateDown))
	assert.Equal(t, sv.queryViews[*oldUpVersion].State(), qviews.QueryViewStateDropping)
	assertStateTransition(t, qviews.QueryViewStateDown, qviews.QueryViewStateDropping)

	// Apply a new view with the same data version, should return error.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *oldUpVersion, qviews.NewStreamingNodeFromVChannel("v1"), qviews.QueryViewStateDropped))
	assert.Equal(t, sv.queryViews[*oldUpVersion].State(), qviews.QueryViewStateDropping)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *oldUpVersion, qviews.NewQueryNode(1), qviews.QueryViewStateDropped))
	assert.Equal(t, sv.queryViews[*oldUpVersion].State(), qviews.QueryViewStateDropping)
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *oldUpVersion, qviews.NewQueryNode(2), qviews.QueryViewStateDropped))
	// repeated apply should be ok.
	sv.ApplyViewFromWorkNode(ctx, newMockQueryViewAtWorkNode(shardID, *oldUpVersion, qviews.NewQueryNode(2), qviews.QueryViewStateDropped))
	assert.Len(t, sv.queryViews, qvLen-1)
	assert.Nil(t, sv.queryViews[*oldUpVersion])
	assertStateTransition(t, qviews.QueryViewStateDropping, qviews.QueryViewStateDropped)
	return *v1
}

func newMockQueryViewAtWorkNode(
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	workNode qviews.WorkNode,
	state qviews.QueryViewState,
) qviews.QueryViewAtWorkNode {
	qvMeta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    shardID.ReplicaID,
		Vchannel:     shardID.VChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  version.DataVersion,
			QueryVersion: version.QueryVersion,
		},
		State: viewpb.QueryViewState(state),
	}

	switch v := workNode.(type) {
	case qviews.QueryNode:
		return qviews.NewQueryViewAtQueryNode(qvMeta, &viewpb.QueryViewOfQueryNode{
			NodeId: v.ID,
		})
	case qviews.StreamingNode:
		return qviews.NewQueryViewAtStreamingNode(qvMeta, &viewpb.QueryViewOfStreamingNode{})
	}
	panic("unreachable")
}

func createQV(replicaID int64, vchannel string, dataVersion int64) *QueryViewAtCoordBuilder {
	inner := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    replicaID,
			Vchannel:     vchannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion: dataVersion,
			},
		},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{NodeId: 1},
			{NodeId: 2},
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	return &QueryViewAtCoordBuilder{inner: inner}
}

func assertNoStateTransition(t *testing.T) {
	assert.False(t, eventsIter.Next())
}

func assertStateTransition(t *testing.T, from qviews.QueryViewState, to qviews.QueryViewState) {
	assert.True(t, eventsIter.Next())
	ev := eventsIter.Value()
	assert.Equal(t, ev.EventType(), events.EventTypeStateTransition)
	transition := ev.(events.EventStateTransition)
	assert.Equal(t, transition.Transition.From, from)
	assert.Equal(t, transition.Transition.To, to)
}
