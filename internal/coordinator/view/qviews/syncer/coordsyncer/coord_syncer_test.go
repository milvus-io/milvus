package coordsyncer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/client"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/syncer/mock_client"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestCoordSyncer(t *testing.T) {
	qvServiceClient := mock_client.NewMockQueryViewServiceClient(t)
	recevier := typeutil.NewConcurrentMap[qviews.WorkNode, chan<- client.SyncMessage]()
	syncerCreation := atomic.NewInt64(0)
	syncerDeletetion := atomic.NewInt64(0)
	qvServiceClient.EXPECT().CreateSyncer(mock.Anything).RunAndReturn(func(so client.SyncOption) client.QueryViewServiceSyncer {
		syncerCreation.Inc()
		syncer := mock_client.NewMockQueryViewServiceSyncer(t)
		syncer.EXPECT().SyncAtBackground(mock.Anything).Return().Maybe()
		recevier.Insert(so.WorkNode, so.Receiver)
		syncer.EXPECT().Close().Run(func() {
			recevier.Remove(so.WorkNode)
			syncerDeletetion.Inc()
		})
		return syncer
	})
	cs := NewCoordSyncer(qvServiceClient)
	v1 := newMockView(t, qviews.NewQueryNode(1),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 3},
		qviews.ShardID{ReplicaID: 1, VChannel: "v1"})
	v2 := newMockView(t, qviews.NewStreamingNodeFromVChannel("v1"),
		qviews.QueryViewVersion{QueryVersion: 1, DataVersion: 2},
		qviews.ShardID{ReplicaID: 1, VChannel: "v1"})
	g := syncer.SyncGroup{}
	g.AddView(v1)
	g.AddView(v2)
	cs.Sync(g)
	assert.Eventually(t, func() bool {
		return recevier.Len() == 2 && syncerCreation.Load() == 2
	}, time.Second, 1*time.Millisecond)

	// Test recv sync message.
	ch, _ := recevier.Get(v1.WorkNode())
	// illegal message should be ignored.
	ch <- client.SyncResponseMessage{
		WorkNode: v1.WorkNode(),
		Response: &viewpb.SyncQueryViewsResponse{},
	}
	ch, _ = recevier.Get(v2.WorkNode())
	// illegal message should be ignored.
	ch <- client.SyncResponseMessage{
		WorkNode: v2.WorkNode(),
		Response: &viewpb.SyncQueryViewsResponse{},
	}

	// Test recv error message.
	ch, _ = recevier.Get(v1.WorkNode())
	ch <- client.SyncErrorMessage{
		WorkNode: v1.WorkNode(),
		Error:    errors.New("123"),
	}
	ch, _ = recevier.Get(v2.WorkNode())
	ch <- client.SyncErrorMessage{
		WorkNode: v2.WorkNode(),
		Error:    errors.New("123"),
	}
	assert.Eventually(t, func() bool {
		return recevier.Len() == 2 && syncerDeletetion.Load() == 2 && syncerCreation.Load() == 4
	}, time.Second, 1*time.Millisecond)

	// Test querynode gone
	v1.EXPECT().GenerateViewWhenNodeDown().Return(v1).Maybe()
	ch, _ = recevier.Get(v1.WorkNode())
	ch <- client.SyncErrorMessage{
		WorkNode: v1.WorkNode(),
		Error:    client.ErrNodeGone,
	}
	// double sent should be ok.
	ch <- client.SyncErrorMessage{
		WorkNode: v1.WorkNode(),
		Error:    client.ErrNodeGone,
	}
	assert.Eventually(t, func() bool {
		return recevier.Len() == 1 && syncerDeletetion.Load() == 3 && syncerCreation.Load() == 4
	}, time.Second, 1*time.Millisecond)

	// Test streaming acked.
	ch, _ = recevier.Get(v2.WorkNode())
	// illegal message should be ignored.
	ch <- client.SyncResponseMessage{
		WorkNode: v2.WorkNode(),
		Response: &viewpb.SyncQueryViewsResponse{
			QueryViews: []*viewpb.QueryViewOfShard{
				{
					Meta: &viewpb.QueryViewMeta{
						Vchannel:  "v1",
						ReplicaId: 1,
						Version:   &viewpb.QueryViewVersion{QueryVersion: 1, DataVersion: 2},
					},
					StreamingNode: &viewpb.QueryViewOfStreamingNode{},
				},
			},
			BalanceAttributes: &viewpb.SyncQueryViewsResponse_StreamingNode{
				StreamingNode: &viewpb.StreamingNodeBalanceAttributes{},
			},
		},
	}
	ev := <-cs.Receiver()
	assert.Equal(t, 3, len(ev))
	assert.Equal(t, v1.WorkNode(), ev[0].(events.SyncerEventAck).AcknowledgedView.WorkNode())
	assert.Equal(t, v1.ShardID(), ev[0].(events.SyncerEventAck).AcknowledgedView.ShardID())
	assert.Equal(t, v2.ShardID(), ev[1].(events.SyncerEventAck).AcknowledgedView.ShardID())
	assert.Equal(t, v2.WorkNode(), ev[1].(events.SyncerEventAck).AcknowledgedView.WorkNode())
	assert.Equal(t, events.EventTypeBalanceAttrUpdate, ev[2].EventType())
	assert.Equal(t, v2.WorkNode(), ev[2].(events.SyncerEventBalanceAttrUpdate).BalanceAttr.WorkNode())

	cs.Close()
	assert.Equal(t, 0, recevier.Len())
	_, ok := <-cs.Receiver()
	assert.False(t, ok)
}
