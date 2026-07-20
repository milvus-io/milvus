package grpcmixcoordclient

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type segmentLoadInfoWatchOpener func(context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error)

type segmentLoadInfoWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	open    segmentLoadInfoWatchOpener
	handler qnview.SegmentLoadInfoSnapshotHandler

	mu              sync.Mutex
	subscriptions   map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription
	dirty           map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription
	unsubscriptions []qnview.SegmentLoadInfoSubscription
	notify          chan struct{}
	snapshots       chan *querypb.QueryViewSegmentLoadInfoSnapshot
}

type segmentLoadInfoWatchKey struct {
	collectionID int64
	segmentID    int64
}

func (c *Client) NewSegmentLoadInfoWatcher(ctx context.Context, handler qnview.SegmentLoadInfoSnapshotHandler) qnview.SegmentLoadInfoWatcher {
	return newSegmentLoadInfoWatcher(ctx, func(ctx context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
		ret, err := c.grpcClient.ReCall(ctx, func(client MixCoordClient) (any, error) {
			if !funcutil.CheckCtxValid(ctx) {
				return nil, ctx.Err()
			}
			return client.WatchQueryViewSegmentLoadInfo(ctx)
		})
		if err != nil || ret == nil {
			return nil, err
		}
		return ret.(querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient), nil
	}, handler)
}

func newSegmentLoadInfoWatcher(ctx context.Context, open segmentLoadInfoWatchOpener, handler qnview.SegmentLoadInfoSnapshotHandler) *segmentLoadInfoWatcher {
	watchCtx, cancel := context.WithCancel(ctx)
	w := &segmentLoadInfoWatcher{
		ctx:             watchCtx,
		cancel:          cancel,
		open:            open,
		handler:         handler,
		subscriptions:   make(map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription),
		dirty:           make(map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription),
		notify:          make(chan struct{}, 1),
		snapshots:       make(chan *querypb.QueryViewSegmentLoadInfoSnapshot, 128),
		unsubscriptions: nil,
	}
	w.wg.Add(2)
	go w.loop()
	go w.applyLoop()
	return w
}

func (w *segmentLoadInfoWatcher) Subscribe(subscription qnview.SegmentLoadInfoSubscription) {
	if subscription.CollectionID == 0 || subscription.SegmentID == 0 {
		return
	}
	key := segmentLoadInfoWatchKey{collectionID: subscription.CollectionID, segmentID: subscription.SegmentID}
	w.mu.Lock()
	w.subscriptions[key] = subscription
	w.dirty[key] = subscription
	w.mu.Unlock()
	w.wake()
}

func (w *segmentLoadInfoWatcher) Unsubscribe(collectionID int64, segmentID int64) {
	if collectionID == 0 || segmentID == 0 {
		return
	}
	key := segmentLoadInfoWatchKey{collectionID: collectionID, segmentID: segmentID}
	w.mu.Lock()
	delete(w.subscriptions, key)
	delete(w.dirty, key)
	w.unsubscriptions = append(w.unsubscriptions, qnview.SegmentLoadInfoSubscription{
		CollectionID: collectionID,
		SegmentID:    segmentID,
	})
	w.mu.Unlock()
	w.wake()
}

func (w *segmentLoadInfoWatcher) Close() {
	w.cancel()
	w.wg.Wait()
}

func (w *segmentLoadInfoWatcher) loop() {
	defer w.wg.Done()
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0
	bo.Reset()

	for w.ctx.Err() == nil {
		if !w.waitSubscription() {
			return
		}
		stream, err := w.open(w.ctx)
		if err != nil {
			mlog.Warn(w.ctx, "failed to open query view segment load info watch stream", mlog.Err(err))
			if !w.waitBackoff(bo.NextBackOff()) {
				return
			}
			continue
		}
		bo.Reset()
		if err := w.sendSnapshot(stream); err != nil {
			_ = stream.CloseSend()
			continue
		}

		streamCtx, cancel := context.WithCancel(w.ctx)
		sendDone := make(chan struct{})
		go func() {
			defer close(sendDone)
			w.sendLoop(streamCtx, stream)
		}()
		w.recvLoop(stream)
		cancel()
		_ = stream.CloseSend()
		<-sendDone
	}
}

func (w *segmentLoadInfoWatcher) applyLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case snapshot := <-w.snapshots:
			w.apply(snapshot)
		}
	}
}

func (w *segmentLoadInfoWatcher) waitSubscription() bool {
	for {
		w.mu.Lock()
		hasSubscription := len(w.subscriptions) > 0
		hasUnsubscription := len(w.unsubscriptions) > 0
		w.mu.Unlock()
		if hasSubscription || hasUnsubscription {
			return true
		}
		select {
		case <-w.ctx.Done():
			return false
		case <-w.notify:
		}
	}
}

func (w *segmentLoadInfoWatcher) sendLoop(ctx context.Context, stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.notify:
			if err := w.sendDelta(stream); err != nil {
				return
			}
		}
	}
}

func (w *segmentLoadInfoWatcher) recvLoop(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			mlog.Warn(w.ctx, "query view segment load info watch stream recv failed", mlog.Err(err))
			return
		}
		if err := merr.CheckRPCCall(resp, nil); err != nil {
			mlog.Warn(w.ctx, "query view segment load info watch stream response failed", mlog.Err(err))
			return
		}
		for _, snapshot := range resp.GetSnapshots() {
			select {
			case w.snapshots <- snapshot:
			case <-w.ctx.Done():
				return
			}
		}
	}
}

func (w *segmentLoadInfoWatcher) sendSnapshot(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) error {
	w.mu.Lock()
	req := w.buildRequestLocked(w.subscriptions, nil)
	w.dirty = make(map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription)
	w.unsubscriptions = nil
	w.mu.Unlock()
	if len(req.GetSubscribe()) == 0 && len(req.GetUnsubscribe()) == 0 {
		return nil
	}
	return stream.Send(req)
}

func (w *segmentLoadInfoWatcher) sendDelta(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) error {
	w.mu.Lock()
	req := w.buildRequestLocked(w.dirty, w.unsubscriptions)
	w.dirty = make(map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription)
	w.unsubscriptions = nil
	w.mu.Unlock()
	if len(req.GetSubscribe()) == 0 && len(req.GetUnsubscribe()) == 0 {
		return nil
	}
	return stream.Send(req)
}

func (w *segmentLoadInfoWatcher) buildRequestLocked(
	subscriptions map[segmentLoadInfoWatchKey]qnview.SegmentLoadInfoSubscription,
	unsubscriptions []qnview.SegmentLoadInfoSubscription,
) *querypb.WatchQueryViewSegmentLoadInfoRequest {
	req := &querypb.WatchQueryViewSegmentLoadInfoRequest{}
	for _, subscription := range subscriptions {
		req.Subscribe = append(req.Subscribe, &querypb.WatchQueryViewSegmentLoadInfoSubscription{
			CollectionID: subscription.CollectionID,
			SegmentID:    subscription.SegmentID,
			Revision:     toQueryViewSegmentLoadInfoRevisionPB(subscription.Revision),
		})
	}
	for _, subscription := range unsubscriptions {
		req.Unsubscribe = append(req.Unsubscribe, &querypb.WatchQueryViewSegmentLoadInfoUnsubscription{
			CollectionID: subscription.CollectionID,
			SegmentID:    subscription.SegmentID,
		})
	}
	return req
}

func (w *segmentLoadInfoWatcher) apply(snapshot *querypb.QueryViewSegmentLoadInfoSnapshot) {
	if snapshot == nil || w.handler == nil {
		return
	}
	w.handler(w.ctx, qnview.SegmentLoadInfoSnapshot{
		CollectionID: snapshot.GetCollectionID(),
		SegmentID:    snapshot.GetSegmentID(),
		Revision:     fromQueryViewSegmentLoadInfoRevisionPB(snapshot.GetRevision()),
		LoadInfo:     snapshot.GetLoadInfo(),
		IndexInfos:   snapshot.GetIndexInfoList(),
	})
}

func (w *segmentLoadInfoWatcher) waitBackoff(duration time.Duration) bool {
	select {
	case <-time.After(duration):
		return true
	case <-w.ctx.Done():
		return false
	}
}

func (w *segmentLoadInfoWatcher) wake() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func toQueryViewSegmentLoadInfoRevisionPB(revision qnview.SegmentLoadInfoRevision) *querypb.QueryViewSegmentLoadInfoRevision {
	return &querypb.QueryViewSegmentLoadInfoRevision{
		LoadInfoRevision: revision.Revision,
	}
}

func fromQueryViewSegmentLoadInfoRevisionPB(revision *querypb.QueryViewSegmentLoadInfoRevision) qnview.SegmentLoadInfoRevision {
	if revision == nil {
		return qnview.SegmentLoadInfoRevision{}
	}
	return qnview.SegmentLoadInfoRevision{
		Revision: revision.GetLoadInfoRevision(),
	}
}
