package querycoordv2

import (
	"cmp"
	"context"
	"hash/fnv"
	"io"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type queryViewSegmentLoadInfoSubscription struct {
	collectionID int64
	segmentID    int64
	revision     *querypb.QueryViewSegmentLoadInfoRevision
}

type queryViewSegmentLoadInfoWatchSession struct {
	server        *Server
	stream        querypb.QueryCoord_WatchQueryViewSegmentLoadInfoServer
	subscriptions map[int64]queryViewSegmentLoadInfoSubscription
	watcher       *queryViewSegmentLoadInfoWatcher
	notifyCh      chan struct{}
	dirty         map[int64]struct{}
	mu            sync.Mutex
}

func (s *Server) WatchQueryViewSegmentLoadInfo(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoServer) error {
	if s.segmentLoadInfoWatcher == nil {
		s.segmentLoadInfoWatcher = newQueryViewSegmentLoadInfoWatcher()
	}
	session := &queryViewSegmentLoadInfoWatchSession{
		server:        s,
		stream:        stream,
		subscriptions: make(map[int64]queryViewSegmentLoadInfoSubscription),
		watcher:       s.segmentLoadInfoWatcher,
		notifyCh:      make(chan struct{}, 1),
		dirty:         make(map[int64]struct{}),
	}
	return session.run()
}

func (s *Server) NotifyQueryViewSegmentLoadInfoChanged(collectionID int64, segmentIDs ...int64) {
	if s.segmentLoadInfoWatcher == nil {
		return
	}
	s.segmentLoadInfoWatcher.notify(collectionID, segmentIDs...)
}

func (s *queryViewSegmentLoadInfoWatchSession) run() error {
	if s.watcher != nil {
		s.watcher.register(s)
		defer s.watcher.unregister(s)
	}
	defer s.clear()

	reqCh := make(chan *querypb.WatchQueryViewSegmentLoadInfoRequest, 1)
	errCh := make(chan error, 1)
	go s.recvLoop(reqCh, errCh)

	for {
		select {
		case <-s.stream.Context().Done():
			return nil
		case err := <-errCh:
			if err != nil && err != io.EOF {
				mlog.Warn(s.stream.Context(), "query view segment load info watch recv failed", mlog.Err(err))
			}
			return nil
		case req := <-reqCh:
			resp := s.handle(req)
			if !s.sendResponse(resp) {
				return nil
			}
		case <-s.notifyCh:
			resp := s.handleDirtySegments()
			if !s.sendResponse(resp) {
				return nil
			}
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) recvLoop(reqCh chan<- *querypb.WatchQueryViewSegmentLoadInfoRequest, errCh chan<- error) {
	for {
		req, err := s.stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case reqCh <- req:
		case <-s.stream.Context().Done():
			return
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) sendResponse(resp *querypb.WatchQueryViewSegmentLoadInfoResponse) bool {
	if resp == nil || len(resp.GetSnapshots()) == 0 && merr.Ok(resp.GetStatus()) {
		return true
	}
	if err := s.stream.Send(resp); err != nil {
		mlog.Warn(s.stream.Context(), "query view segment load info watch send failed", mlog.Err(err))
		return false
	}
	if !merr.Ok(resp.GetStatus()) {
		return false
	}
	s.updateSnapshotRevisions(resp.GetSnapshots())
	return true
}

func (s *queryViewSegmentLoadInfoWatchSession) handle(req *querypb.WatchQueryViewSegmentLoadInfoRequest) *querypb.WatchQueryViewSegmentLoadInfoResponse {
	resp := &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Status: merr.Success(),
	}
	ctx := s.stream.Context()
	if err := merr.CheckHealthy(s.server.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	if s.server.mixCoord == nil {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("mixcoord is not initialized"))
		return resp
	}

	for _, unsubscribe := range req.GetUnsubscribe() {
		s.unsubscribe(unsubscribe.GetSegmentID())
	}
	subscriptions := make([]queryViewSegmentLoadInfoSubscription, 0, len(req.GetSubscribe()))
	for _, subscribe := range req.GetSubscribe() {
		if subscribe.GetCollectionID() == 0 || subscribe.GetSegmentID() == 0 {
			continue
		}
		subscription := queryViewSegmentLoadInfoSubscription{
			collectionID: subscribe.GetCollectionID(),
			segmentID:    subscribe.GetSegmentID(),
			revision:     subscribe.GetRevision(),
		}
		s.subscribe(subscription)
		subscriptions = append(subscriptions, subscription)
	}

	snapshots, err := s.buildSnapshots(ctx, subscriptions)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	resp.Snapshots = snapshots
	return resp
}

func (s *queryViewSegmentLoadInfoWatchSession) handleDirtySegments() *querypb.WatchQueryViewSegmentLoadInfoResponse {
	resp := &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Status: merr.Success(),
	}
	ctx := s.stream.Context()
	if err := merr.CheckHealthy(s.server.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	if s.server.mixCoord == nil {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("mixcoord is not initialized"))
		return resp
	}

	snapshots, err := s.buildSnapshots(ctx, s.drainDirtySubscriptions())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	resp.Snapshots = snapshots
	return resp
}

func (s *queryViewSegmentLoadInfoWatchSession) buildSnapshots(ctx context.Context, subscriptions []queryViewSegmentLoadInfoSubscription) ([]*querypb.QueryViewSegmentLoadInfoSnapshot, error) {
	byCollection := make(map[int64][]queryViewSegmentLoadInfoSubscription)
	for _, subscription := range subscriptions {
		if subscription.collectionID == 0 || subscription.segmentID == 0 {
			continue
		}
		byCollection[subscription.collectionID] = append(byCollection[subscription.collectionID], subscription)
	}
	snapshots := make([]*querypb.QueryViewSegmentLoadInfoSnapshot, 0, len(subscriptions))
	for collectionID, collectionSubscriptions := range byCollection {
		segmentIDs := make([]int64, 0, len(collectionSubscriptions))
		expected := make(map[int64]*querypb.QueryViewSegmentLoadInfoRevision, len(collectionSubscriptions))
		for _, subscription := range collectionSubscriptions {
			segmentIDs = append(segmentIDs, subscription.segmentID)
			expected[subscription.segmentID] = subscription.revision
		}
		infos, indexInfos, err := s.server.mixCoord.GetQueryViewSegmentLoadInfos(ctx, collectionID, segmentIDs)
		if err != nil {
			return nil, err
		}
		for _, loadInfo := range infos {
			revision := calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexInfos)
			if sameQueryViewSegmentLoadInfoRevision(expected[loadInfo.GetSegmentID()], revision) {
				continue
			}
			snapshots = append(snapshots, &querypb.QueryViewSegmentLoadInfoSnapshot{
				CollectionID:  collectionID,
				SegmentID:     loadInfo.GetSegmentID(),
				Revision:      revision,
				LoadInfo:      loadInfo,
				IndexInfoList: indexInfos,
			})
		}
	}
	return snapshots, nil
}

func (s *queryViewSegmentLoadInfoWatchSession) subscribe(subscription queryViewSegmentLoadInfoSubscription) {
	s.mu.Lock()
	s.subscriptions[subscription.segmentID] = subscription
	delete(s.dirty, subscription.segmentID)
	s.mu.Unlock()
	if s.watcher != nil {
		s.watcher.subscribe(s, subscription.segmentID)
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) unsubscribe(segmentID int64) {
	s.mu.Lock()
	delete(s.subscriptions, segmentID)
	delete(s.dirty, segmentID)
	s.mu.Unlock()
	if s.watcher != nil {
		s.watcher.unsubscribe(s, segmentID)
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) markDirty(collectionID int64, segmentIDs []int64) {
	s.mu.Lock()
	dirty := false
	for _, segmentID := range segmentIDs {
		subscription, ok := s.subscriptions[segmentID]
		if !ok || subscription.collectionID != collectionID {
			continue
		}
		s.dirty[segmentID] = struct{}{}
		dirty = true
	}
	s.mu.Unlock()
	if dirty {
		select {
		case s.notifyCh <- struct{}{}:
		default:
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) drainDirtySubscriptions() []queryViewSegmentLoadInfoSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscriptions := make([]queryViewSegmentLoadInfoSubscription, 0, len(s.dirty))
	for segmentID := range s.dirty {
		subscription, ok := s.subscriptions[segmentID]
		if ok {
			subscriptions = append(subscriptions, subscription)
		}
		delete(s.dirty, segmentID)
	}
	return subscriptions
}

func (s *queryViewSegmentLoadInfoWatchSession) updateSnapshotRevisions(snapshots []*querypb.QueryViewSegmentLoadInfoSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snapshot := range snapshots {
		subscription, ok := s.subscriptions[snapshot.GetSegmentID()]
		if !ok {
			continue
		}
		subscription.revision = snapshot.GetRevision()
		s.subscriptions[snapshot.GetSegmentID()] = subscription
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.subscriptions)
	clear(s.dirty)
}

type queryViewSegmentLoadInfoWatcher struct {
	mu        sync.RWMutex
	sessions  map[*queryViewSegmentLoadInfoWatchSession]struct{}
	bySegment map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}
}

func newQueryViewSegmentLoadInfoWatcher() *queryViewSegmentLoadInfoWatcher {
	return &queryViewSegmentLoadInfoWatcher{
		sessions:  make(map[*queryViewSegmentLoadInfoWatchSession]struct{}),
		bySegment: make(map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}),
	}
}

func (w *queryViewSegmentLoadInfoWatcher) register(session *queryViewSegmentLoadInfoWatchSession) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sessions[session] = struct{}{}
	for segmentID := range session.subscriptions {
		w.addSubscriptionLocked(session, segmentID)
	}
}

func (w *queryViewSegmentLoadInfoWatcher) unregister(session *queryViewSegmentLoadInfoWatchSession) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.sessions, session)
	for segmentID, sessions := range w.bySegment {
		delete(sessions, session)
		if len(sessions) == 0 {
			delete(w.bySegment, segmentID)
		}
	}
}

func (w *queryViewSegmentLoadInfoWatcher) subscribe(session *queryViewSegmentLoadInfoWatchSession, segmentID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.sessions[session]; !ok {
		return
	}
	w.addSubscriptionLocked(session, segmentID)
}

func (w *queryViewSegmentLoadInfoWatcher) addSubscriptionLocked(session *queryViewSegmentLoadInfoWatchSession, segmentID int64) {
	sessions, ok := w.bySegment[segmentID]
	if !ok {
		sessions = make(map[*queryViewSegmentLoadInfoWatchSession]struct{})
		w.bySegment[segmentID] = sessions
	}
	sessions[session] = struct{}{}
}

func (w *queryViewSegmentLoadInfoWatcher) unsubscribe(session *queryViewSegmentLoadInfoWatchSession, segmentID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	sessions, ok := w.bySegment[segmentID]
	if !ok {
		return
	}
	delete(sessions, session)
	if len(sessions) == 0 {
		delete(w.bySegment, segmentID)
	}
}

func (w *queryViewSegmentLoadInfoWatcher) notify(collectionID int64, segmentIDs ...int64) {
	if collectionID == 0 || len(segmentIDs) == 0 {
		return
	}
	targets := make(map[*queryViewSegmentLoadInfoWatchSession][]int64)
	w.mu.RLock()
	for _, segmentID := range segmentIDs {
		for session := range w.bySegment[segmentID] {
			targets[session] = append(targets[session], segmentID)
		}
	}
	w.mu.RUnlock()
	for session, sessionSegmentIDs := range targets {
		session.markDirty(collectionID, sessionSegmentIDs)
	}
}

func calculateQueryViewSegmentLoadInfoRevision(loadInfo *querypb.SegmentLoadInfo, indexInfos []*indexpb.IndexInfo) *querypb.QueryViewSegmentLoadInfoRevision {
	snapshot := &querypb.QueryViewSegmentLoadInfoSnapshot{
		LoadInfo:      loadInfo,
		IndexInfoList: indexInfos,
	}
	canonicalizeQueryViewSegmentLoadInfoSnapshot(snapshot)
	return &querypb.QueryViewSegmentLoadInfoRevision{
		LoadInfoRevision: hashProto(snapshot),
	}
}

func canonicalizeQueryViewSegmentLoadInfoSnapshot(snapshot *querypb.QueryViewSegmentLoadInfoSnapshot) {
	for _, index := range snapshot.GetIndexInfoList() {
		sortKeyValuePairs(index.TypeParams)
		sortKeyValuePairs(index.IndexParams)
		sortKeyValuePairs(index.UserIndexParams)
	}
	slices.SortFunc(snapshot.IndexInfoList, compareIndexInfo)

	loadInfo := snapshot.GetLoadInfo()
	if loadInfo == nil {
		return
	}
	for _, index := range loadInfo.GetIndexInfos() {
		sortKeyValuePairs(index.IndexParams)
		slices.Sort(index.IndexFilePaths)
	}
	slices.SortFunc(loadInfo.IndexInfos, compareFieldIndexInfo)
}

func sortKeyValuePairs(pairs []*commonpb.KeyValuePair) {
	slices.SortFunc(pairs, func(left, right *commonpb.KeyValuePair) int {
		if result := cmp.Compare(left.GetKey(), right.GetKey()); result != 0 {
			return result
		}
		return cmp.Compare(left.GetValue(), right.GetValue())
	})
}

func compareIndexInfo(left, right *indexpb.IndexInfo) int {
	if result := cmp.Compare(left.GetCollectionID(), right.GetCollectionID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetFieldID(), right.GetFieldID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexID(), right.GetIndexID()); result != 0 {
		return result
	}
	return cmp.Compare(left.GetIndexName(), right.GetIndexName())
}

func compareFieldIndexInfo(left, right *querypb.FieldIndexInfo) int {
	if result := cmp.Compare(left.GetFieldID(), right.GetFieldID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexID(), right.GetIndexID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetBuildID(), right.GetBuildID()); result != 0 {
		return result
	}
	return cmp.Compare(left.GetIndexName(), right.GetIndexName())
}

func hashProto(message proto.Message) uint64 {
	if message == nil {
		return 0
	}
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return 0
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write(bytes)
	return hasher.Sum64()
}

func sameQueryViewSegmentLoadInfoRevision(left, right *querypb.QueryViewSegmentLoadInfoRevision) bool {
	if left == nil || right == nil {
		return false
	}
	return left.GetLoadInfoRevision() == right.GetLoadInfoRevision()
}
