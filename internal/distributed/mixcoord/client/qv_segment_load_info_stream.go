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

type segmentLoadInfoStreamOpener func(context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error)

type segmentLoadInfoStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	open segmentLoadInfoStreamOpener

	mu              sync.Mutex
	subscriptions   map[int64]*segmentLoadInfoSubscription
	dirty           map[int64]*segmentLoadInfoSubscription
	unsubscriptions []*segmentLoadInfoSubscription
	notify          chan struct{}
	closeOnce       sync.Once
}

type segmentLoadInfoSubscription struct {
	stream       *segmentLoadInfoStream
	collectionID int64
	segmentID    int64
	revision     qnview.SegmentLoadInfoRevision
	handler      *checkpointSegmentLoadInfoHandler

	handlerMu sync.Mutex
	closed    bool
	err       error
	closeOnce sync.Once
}

func (c *Client) NewSegmentLoadInfoStream(ctx context.Context) qnview.SegmentLoadInfoStream {
	return newSegmentLoadInfoStream(ctx, func(ctx context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
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
	})
}

func newSegmentLoadInfoStream(ctx context.Context, open segmentLoadInfoStreamOpener) *segmentLoadInfoStream {
	streamCtx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored in the stream and called in Close()
	s := &segmentLoadInfoStream{
		ctx:             streamCtx,
		cancel:          cancel,
		open:            open,
		subscriptions:   make(map[int64]*segmentLoadInfoSubscription),
		dirty:           make(map[int64]*segmentLoadInfoSubscription),
		notify:          make(chan struct{}, 1),
		unsubscriptions: nil,
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func (s *segmentLoadInfoStream) Subscribe(option qnview.SegmentLoadInfoSubscriptionOption) qnview.SegmentLoadInfoSubscription {
	if option.CollectionID == 0 || option.SegmentID == 0 || option.Handler == nil {
		return nil
	}
	sub := &segmentLoadInfoSubscription{
		stream:       s,
		collectionID: option.CollectionID,
		segmentID:    option.SegmentID,
	}
	sub.handler = &checkpointSegmentLoadInfoHandler{sub: sub, inner: option.Handler}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil
	}
	previous := s.subscriptions[option.SegmentID]
	s.subscriptions[option.SegmentID] = sub
	s.dirty[option.SegmentID] = sub
	sub.revision = option.Revision
	s.mu.Unlock()
	if previous != nil {
		previous.finish(nil)
	}
	s.wake()
	return sub
}

func (s *segmentLoadInfoStream) Close() {
	s.closeOnce.Do(s.cancel)
	s.wg.Wait()
}

func (s *segmentLoadInfoStream) loop() {
	defer s.wg.Done()
	defer s.finish()
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0
	bo.Reset()

	for s.ctx.Err() == nil {
		if !s.waitSubscription() {
			return
		}
		streamCtx, cancel := context.WithCancel(s.ctx)
		stream, err := s.open(streamCtx)
		if err != nil {
			cancel()
			mlog.Warn(s.ctx, "failed to open query view segment load info stream", mlog.Err(err))
			if !s.waitBackoff(bo.NextBackOff()) {
				return
			}
			continue
		}
		if err := s.sendSnapshot(stream); err != nil {
			cancel()
			_ = stream.CloseSend()
			if !s.waitBackoff(bo.NextBackOff()) {
				return
			}
			continue
		}

		sendDone := make(chan struct{})
		go func() {
			defer close(sendDone)
			s.sendLoop(streamCtx, cancel, stream)
		}()
		s.recvLoop(stream)
		cancel()
		_ = stream.CloseSend()
		<-sendDone
		if !s.waitBackoff(bo.NextBackOff()) {
			return
		}
	}
}

func (s *segmentLoadInfoStream) finish() {
	s.mu.Lock()
	subscriptions := s.subscriptions
	s.subscriptions = make(map[int64]*segmentLoadInfoSubscription)
	s.dirty = make(map[int64]*segmentLoadInfoSubscription)
	s.unsubscriptions = nil
	s.mu.Unlock()
	for _, sub := range subscriptions {
		sub.finish(nil)
	}
}

func (s *segmentLoadInfoStream) waitSubscription() bool {
	for {
		s.mu.Lock()
		hasSubscription := len(s.subscriptions) > 0
		hasUnsubscription := len(s.unsubscriptions) > 0
		s.mu.Unlock()
		if hasSubscription || hasUnsubscription {
			return true
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-s.notify:
		}
	}
}

func (s *segmentLoadInfoStream) sendLoop(ctx context.Context, cancel context.CancelFunc, stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notify:
			if err := s.sendDelta(stream); err != nil {
				cancel()
				return
			}
		}
	}
}

func (s *segmentLoadInfoStream) recvLoop(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			mlog.Warn(s.ctx, "query view segment load info stream recv failed", mlog.Err(err))
			return
		}
		if err := merr.CheckRPCCall(resp, nil); err != nil {
			mlog.Warn(s.ctx, "query view segment load info stream response failed", mlog.Err(err))
			return
		}
		for _, snapshot := range resp.GetSnapshots() {
			s.handleSnapshot(snapshot)
		}
	}
}

func (s *segmentLoadInfoStream) handleSnapshot(snapshot *querypb.QueryViewSegmentLoadInfoSnapshot) {
	if snapshot == nil {
		return
	}
	s.mu.Lock()
	sub := s.subscriptions[snapshot.GetSegmentID()]
	if sub == nil || sub.revisionLocked() == snapshot.GetRevision().GetLoadInfoRevision() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	localSnapshot := qnview.SegmentLoadInfoSnapshot{
		CollectionID: snapshot.GetCollectionID(),
		SegmentID:    snapshot.GetSegmentID(),
		Revision:     fromQueryViewSegmentLoadInfoRevisionPB(snapshot.GetRevision()),
		LoadInfo:     snapshot.GetLoadInfo(),
		IndexInfos:   snapshot.GetIndexInfoList(),
	}
	if err := sub.handle(localSnapshot); err != nil {
		s.removeSubscription(sub, err)
	}
}

func (s *segmentLoadInfoStream) sendSnapshot(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) error {
	s.mu.Lock()
	req := s.buildRequestLocked(s.subscriptions, nil)
	s.dirty = make(map[int64]*segmentLoadInfoSubscription)
	s.unsubscriptions = nil
	s.mu.Unlock()
	if len(req.GetSubscribe()) == 0 && len(req.GetUnsubscribe()) == 0 {
		return nil
	}
	return stream.Send(req)
}

func (s *segmentLoadInfoStream) sendDelta(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient) error {
	s.mu.Lock()
	req := s.buildRequestLocked(s.dirty, s.unsubscriptions)
	s.dirty = make(map[int64]*segmentLoadInfoSubscription)
	s.unsubscriptions = nil
	s.mu.Unlock()
	if len(req.GetSubscribe()) == 0 && len(req.GetUnsubscribe()) == 0 {
		return nil
	}
	return stream.Send(req)
}

func (s *segmentLoadInfoStream) buildRequestLocked(
	subscriptions map[int64]*segmentLoadInfoSubscription,
	unsubscriptions []*segmentLoadInfoSubscription,
) *querypb.WatchQueryViewSegmentLoadInfoRequest {
	req := &querypb.WatchQueryViewSegmentLoadInfoRequest{}
	for _, subscription := range subscriptions {
		req.Subscribe = append(req.Subscribe, &querypb.WatchQueryViewSegmentLoadInfoSubscription{
			CollectionID: subscription.collectionID,
			SegmentID:    subscription.segmentID,
			Revision: &querypb.QueryViewSegmentLoadInfoRevision{
				LoadInfoRevision: subscription.revisionLocked(),
			},
		})
	}
	for _, subscription := range unsubscriptions {
		req.Unsubscribe = append(req.Unsubscribe, &querypb.WatchQueryViewSegmentLoadInfoUnsubscription{
			CollectionID: subscription.collectionID,
			SegmentID:    subscription.segmentID,
		})
	}
	return req
}

func (s *segmentLoadInfoStream) removeSubscription(sub *segmentLoadInfoSubscription, err error) {
	s.mu.Lock()
	if s.subscriptions[sub.segmentID] == sub {
		delete(s.subscriptions, sub.segmentID)
		delete(s.dirty, sub.segmentID)
		s.unsubscriptions = append(s.unsubscriptions, sub)
	}
	s.mu.Unlock()
	sub.finish(err)
	s.wake()
}

func (s *segmentLoadInfoStream) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *segmentLoadInfoStream) waitBackoff(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.notify:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *segmentLoadInfoSubscription) CollectionID() int64 {
	return s.collectionID
}

func (s *segmentLoadInfoSubscription) SegmentID() int64 {
	return s.segmentID
}

func (s *segmentLoadInfoSubscription) Error() error {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	return s.err
}

func (s *segmentLoadInfoSubscription) Close() {
	s.closeOnce.Do(func() {
		s.stream.removeSubscription(s, nil)
	})
}

func (s *segmentLoadInfoSubscription) handle(snapshot qnview.SegmentLoadInfoSnapshot) error {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	if s.closed {
		return nil
	}
	return s.handler.Handle(snapshot)
}

func (s *segmentLoadInfoSubscription) finish(err error) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.err = err
	s.handler.Close()
}

func (s *segmentLoadInfoSubscription) revisionLocked() uint64 {
	return s.revision.Revision
}

func (s *segmentLoadInfoSubscription) advance(revision qnview.SegmentLoadInfoRevision) {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	if s.stream.subscriptions[s.segmentID] == s {
		s.revision = revision
	}
}

type checkpointSegmentLoadInfoHandler struct {
	sub   *segmentLoadInfoSubscription
	inner qnview.SegmentLoadInfoEventHandler
}

func (h *checkpointSegmentLoadInfoHandler) Handle(snapshot qnview.SegmentLoadInfoSnapshot) error {
	if err := h.inner.Handle(snapshot); err != nil {
		return err
	}
	h.sub.advance(snapshot.Revision)
	return nil
}

func (h *checkpointSegmentLoadInfoHandler) Close() {
	h.inner.Close()
}

func fromQueryViewSegmentLoadInfoRevisionPB(revision *querypb.QueryViewSegmentLoadInfoRevision) qnview.SegmentLoadInfoRevision {
	if revision == nil {
		return qnview.SegmentLoadInfoRevision{}
	}
	return qnview.SegmentLoadInfoRevision{Revision: revision.GetLoadInfoRevision()}
}
