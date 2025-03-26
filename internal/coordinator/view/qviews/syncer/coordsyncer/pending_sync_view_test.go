package coordsyncer

import (
	"testing"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPendingAckView(t *testing.T) {
	views := newPendingAckViews()
	v1 := newMockView(t, qviews.NewStreamingNodeFromVChannel("v1"),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 2},
		qviews.ShardID{ReplicaID: 1, VChannel: "v1"})
	v2 := newMockView(t, qviews.NewStreamingNodeFromVChannel("v1"),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 2},
		qviews.ShardID{ReplicaID: 1, VChannel: "v1"})
	g := syncer.SyncGroup{}
	g.AddView(v1)
	g.AddView(v2)
	evs := views.Add(g)
	assert.Equal(t, 2, len(evs))
	assert.Equal(t, events.EventTypeSyncSent, evs[0].EventType())
	assert.Equal(t, events.EventTypeSyncOverwrite, evs[1].EventType())
	assert.Equal(t, 1, len(views.CollectResync(qviews.StreamingNode{PChannel: "v1"})))
	views.Observe(events.SyncerEventAck{
		AcknowledgedView: v1,
	})
	assert.Equal(t, 0, len(views.CollectResync(qviews.StreamingNode{PChannel: "v1"})))

	v3 := newMockView(t, qviews.NewStreamingNodeFromVChannel("v2"),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 3},
		qviews.ShardID{ReplicaID: 1, VChannel: "v2"})
	g = syncer.SyncGroup{}
	g.AddView(v3)
	evs = views.Add(g)
	assert.Equal(t, 1, len(evs))
	assert.Equal(t, events.EventTypeSyncSent, evs[0].EventType())
	assert.Equal(t, 1, len(views.CollectResync(qviews.StreamingNode{PChannel: "v2"})))

	v4 := newMockView(t, qviews.NewQueryNode(1),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 3},
		qviews.ShardID{ReplicaID: 1, VChannel: "v2"})
	g = syncer.SyncGroup{}
	g.AddView(v4)
	evs = views.Add(g)
	assert.Equal(t, 1, len(evs))
	assert.Equal(t, events.EventTypeSyncSent, evs[0].EventType())
	assert.Equal(t, 1, len(views.CollectResync(qviews.QueryNode{ID: 1})))

	v5 := newMockView(t, qviews.NewQueryNode(1),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 4},
		qviews.ShardID{ReplicaID: 1, VChannel: "v2"})
	g = syncer.SyncGroup{}
	g.AddView(v5)
	evs = views.Add(g)
	assert.Equal(t, 1, len(evs))
	assert.Equal(t, events.EventTypeSyncSent, evs[0].EventType())
	assert.Equal(t, 2, len(views.CollectResync(qviews.QueryNode{ID: 1})))

	v6 := newMockView(t, qviews.NewQueryNode(1),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 3},
		qviews.ShardID{ReplicaID: 1, VChannel: "v1"})
	g = syncer.SyncGroup{}
	g.AddView(v6)
	evs = views.Add(g)
	assert.Equal(t, 1, len(evs))
	assert.Equal(t, events.EventTypeSyncSent, evs[0].EventType())
	assert.Equal(t, 3, len(views.CollectResync(qviews.QueryNode{ID: 1})))
}

func newMockView(t *testing.T, node qviews.WorkNode, version qviews.QueryViewVersion, shardID qviews.ShardID) *mock_syncer.MockQueryViewAtWorkNodeWithAck {
	view := mock_syncer.NewMockQueryViewAtWorkNodeWithAck(t)
	view.EXPECT().WorkNode().Return(node).Maybe()
	view.EXPECT().Version().Return(version).Maybe()
	view.EXPECT().ShardID().Return(shardID).Maybe()
	view.EXPECT().IntoProto().Return(&viewpb.QueryViewOfShard{}).Maybe()
	view.EXPECT().ObserveSyncerEvent(mock.Anything).RunAndReturn(func(sea events.SyncerEventAck) bool {
		return sea.AcknowledgedView.ShardID() == shardID
	}).Maybe()
	return view
}
