package coordsyncer

import (
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/client"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"go.uber.org/zap"
)

var _ syncer.CoordSyncer = (*coordSyncerImpl)(nil)

// NewCoordSyncer creates a new CoordSyncer.
func NewCoordSyncer(qvServiceClient client.QueryViewServiceClient) syncer.CoordSyncer {
	cs := &coordSyncerImpl{
		backgroundNotifier:  syncutil.NewAsyncTaskNotifier[struct{}](),
		pendingAckViews:     newPendingAckViews(),
		pendingSentEvents:   nil,
		serviceClient:       qvServiceClient,
		syncers:             make(map[qviews.WorkNode]client.QueryViewServiceSyncer),
		syncChan:            make(chan syncer.SyncGroup),
		syncMessageReceiver: make(chan client.SyncMessage),
		eventReceiver:       make(chan []events.SyncerEvent),
	}
	cs.SetLogger(log.With(log.FieldComponent("view-coordsyncer")))
	go cs.loop()
	return cs
}

type coordSyncerImpl struct {
	log.Binder
	backgroundNotifier *syncutil.AsyncTaskNotifier[struct{}]

	pendingAckViews     *pendingAckViews              // Pending ack views, used to track the unacknowledged views, will be resent if the sync stream is down.
	pendingSentEvents   []events.SyncerEvent          // pendingSentEvents is the events that are sent to the remote worknode.
	serviceClient       client.QueryViewServiceClient // serviceClient is the underlying grpc client to create a new worknode syncer 。
	syncers             map[qviews.WorkNode]client.QueryViewServiceSyncer
	syncChan            chan syncer.SyncGroup
	syncMessageReceiver chan client.SyncMessage
	eventReceiver       chan []events.SyncerEvent
}

func (cs *coordSyncerImpl) Sync(g syncer.SyncGroup) {
	// Empty group should be ignored.
	if len(g.Views) == 0 {
		panic("empty sync operation is never allowed, at least streaming node")
	}
	select {
	case <-cs.backgroundNotifier.Context().Done():
		cs.Logger().Info("syncer is closed, ignore the sync operation after that")
	case cs.syncChan <- g:
	}
}

func (cs *coordSyncerImpl) Receiver() <-chan []events.SyncerEvent {
	return cs.eventReceiver
}

// loop is the background loop to handle the sync events.
func (cs *coordSyncerImpl) loop() {
	cs.Logger().Info("coord syncer is on running...")
	defer func() {
		cs.Logger().Info("background task of coord syncer exitting, close all syncers", zap.Int("syncerCount", len(cs.syncers)))
		for node, syncer := range cs.syncers {
			cs.Logger().Info("close syncer...", zap.Any("workNode", node))
			syncer.Close()
		}
		cs.syncers = nil
		cs.Logger().Info("background task of coord syncer exitting, close all syncers")
		cs.backgroundNotifier.Finish(struct{}{})
	}()

	for {
		var receiver chan []events.SyncerEvent
		if len(cs.pendingSentEvents) > 0 {
			receiver = cs.eventReceiver
		} else {
			receiver = nil
		}

		select {
		case <-cs.backgroundNotifier.Context().Done():
			return
		case newGroup := <-cs.syncChan:
			// When new sync group comes, the views should be dispatched right away,
			// and add it into pending view to wait for the ack.
			cs.pendingAckViews.Add(newGroup)
			cs.dispatch(newGroup.Views)
		case receiver <- cs.pendingSentEvents:
			cs.pendingSentEvents = nil
		case syncMessage := <-cs.syncMessageReceiver:
			cs.handleMessage(syncMessage)
		}
	}
}

// handleMessage handles the sync message from the syncer.
func (cs *coordSyncerImpl) handleMessage(syncMessage client.SyncMessage) {
	switch msg := syncMessage.(type) {
	case client.SyncResponseMessage:
		for _, viewProto := range msg.Response.QueryViews {
			view := qviews.NewQueryViewAtWorkNodeFromProto(viewProto)
			cs.addPendingEvent(events.SyncerEventAck{
				AcknowledgedView: view,
			})
		}
		if msg.Response.BalanceAttributes != nil {
			cs.addPendingEvent(events.SyncerEventBalanceAttrUpdate{
				BalanceAttr: qviews.NewBalanceAttrAtWorkNodeFromProto(msg.GetWorkNode(), msg.Response),
			})
		}
	case client.SyncErrorMessage:
		if errors.Is(msg.Error, client.ErrNodeGone) {
			// When the node is gone, all the views should be unrecoverable.
			cs.whenWorkNodeIsDown(msg.GetWorkNode())
		} else {
			// When the stream is broken, the syncer should be refreshed and
			// the views should be resynced.
			cs.whenSyncerBroken(msg.GetWorkNode())
		}
	}
}

// dispatch dispatches the views to the work nodes.
func (cs *coordSyncerImpl) dispatch(viewOnNodes map[qviews.WorkNode][]syncer.QueryViewAtWorkNodeWithAck) {
	for node, views := range viewOnNodes {
		// Create a new syncer if the syncer is not exist.
		if _, ok := cs.syncers[node]; !ok {
			cs.syncers[node] = cs.serviceClient.CreateSyncer(client.SyncOption{
				WorkNode: node,
				Receiver: cs.syncMessageReceiver,
			})
			cs.Logger().Info("create a new syncer for the work node", zap.Stringer("workNode", node))
		}
		cs.syncWorkNode(node, views)
	}
}

// syncWorkNode sync the views to the work node.
func (cs *coordSyncerImpl) syncWorkNode(node qviews.WorkNode, views []syncer.QueryViewAtWorkNodeWithAck) {
	syncer, ok := cs.syncers[node]
	if !ok {
		panic("syncer must always exist")
	}
	req := &viewpb.SyncQueryViewsRequest{
		QueryViews: make([]*viewpb.QueryViewOfShard, 0, len(views)),
	}
	for _, view := range views {
		req.QueryViews = append(req.QueryViews, view.IntoProto())
	}
	syncer.SyncAtBackground(req)
}

// whenWorkNodeIsDown handles the situation when the work node is down.
func (cs *coordSyncerImpl) whenWorkNodeIsDown(node qviews.WorkNode) {
	views := cs.pendingAckViews.CollectResync(node)
	// client not exist, the related node is down or gone.
	// so generate a query view when node is down.
	for _, view := range views {
		downView := view.GenerateViewWhenNodeDown()
		cs.addPendingEvent(events.SyncerEventAck{
			AcknowledgedView: downView,
		})
	}
	client, ok := cs.syncers[node]
	if !ok {
		return
	}
	client.Close()
	delete(cs.syncers, node)
}

// whenSyncerBroken refresh the syncer for the work node and resync the related views.
func (cs *coordSyncerImpl) whenSyncerBroken(node qviews.WorkNode) {
	if client, ok := cs.syncers[node]; ok {
		client.Close()
		delete(cs.syncers, node)
	}
	cs.syncers[node] = cs.serviceClient.CreateSyncer(client.SyncOption{
		WorkNode: node,
		Receiver: cs.syncMessageReceiver,
	})
	views := cs.pendingAckViews.CollectResync(node)
	cs.syncWorkNode(node, views)
	cs.Logger().Info(
		"create a new syncer for the work node when syncer stream broken and resent pending views...",
		zap.Stringer("workNode", node),
		zap.Int("pendingViewCount", len(views)),
	)
}

// addPendingEvent add the events to the pending list.
func (cs *coordSyncerImpl) addPendingEvent(ev events.SyncerEvent) {
	cs.pendingAckViews.Observe(ev)
	cs.pendingSentEvents = append(cs.pendingSentEvents, ev)
}

func (cs *coordSyncerImpl) Close() {
	cs.Logger().Info("coord syncer is on closing...")
	cs.backgroundNotifier.Cancel()
	cs.backgroundNotifier.BlockUntilFinish()
	cs.Logger().Info("coord syncer is closed")
	close(cs.eventReceiver)
}
