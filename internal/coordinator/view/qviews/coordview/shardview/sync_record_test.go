package shardview

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/mock_qviews"
)

func TestSyncRecord(t *testing.T) {
	qvb := createQV(1, "v1", 1)
	assert.Equal(t, qvb.ShardID(), qviews.ShardID{
		ReplicaID: 1,
		VChannel:  "v1",
	})
	qv := qvb.withQueryVersion(1).Build()

	sr := newAllWorkNodeSyncRecord(qv.inner, []qviews.QueryViewState{qviews.QueryViewStateReady}, qviews.QueryViewStateUnrecoverable)
	assert.False(t, sr.IsAllReady())
	views := sr.ConsumePendingAckViews()
	assert.Len(t, views, 3)
	assert.Len(t, sr.ConsumePendingAckViews(), 0)

	for _, v := range views {
		v2 := mock_qviews.NewMockQueryViewAtWorkNode(t)
		v2.EXPECT().State().Return(qviews.QueryViewStatePreparing)
		assert.False(t, v.ObserveSyncerEvent(events.SyncerEventAck{AcknowledgedView: v2}))
		v2.EXPECT().State().Unset()
		v2.EXPECT().State().Return(qviews.QueryViewStateReady)
		assert.Equal(t, v.GenerateViewWhenNodeDown().State(), qviews.QueryViewStateUnrecoverable)
		assert.False(t, sr.IsAllReady())
		sr.MarkNodeReady(v.WorkNode())
	}
	assert.True(t, sr.IsAllReady())

	sr = newStreamingNodeSyncRecord(qv.inner, []qviews.QueryViewState{qviews.QueryViewStateReady})
	assert.False(t, sr.IsAllReady())
	views = sr.ConsumePendingAckViews()
	assert.Len(t, views, 1)
	assert.Len(t, sr.ConsumePendingAckViews(), 0)
	for _, v := range views {
		_ = v.WorkNode().(qviews.StreamingNode)
		v2 := mock_qviews.NewMockQueryViewAtWorkNode(t)
		v2.EXPECT().State().Return(qviews.QueryViewStatePreparing)
		assert.False(t, v.ObserveSyncerEvent(events.SyncerEventAck{AcknowledgedView: v2}))
		v2.EXPECT().State().Unset()
		v2.EXPECT().State().Return(qviews.QueryViewStateReady)
		assert.True(t, v.ObserveSyncerEvent(events.SyncerEventAck{AcknowledgedView: v2}))
		assert.Panics(t, func() {
			v.GenerateViewWhenNodeDown()
		})
		assert.False(t, sr.IsAllReady())
		sr.MarkNodeReady(v.WorkNode())
	}
	assert.True(t, sr.IsAllReady())
}
