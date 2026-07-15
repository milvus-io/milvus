package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ViewScopedPhysicalSegmentManager struct {
	scheduler SegmentLoadScheduler

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*viewRef
	segments map[int64]*physicalSegmentState
	cancels  map[qviews.QueryViewKey]context.CancelFunc
}

type viewRef struct {
	segments        map[int64]int64
	loadWG          *sync.WaitGroup
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(segmentID int64, err error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	segment    TransformSegment
	loading    bool
	pending    bool
	loadCancel context.CancelFunc
	refs       map[qviews.QueryViewKey]struct{}
	requests   map[qviews.QueryViewKey]segmentLoadRequest
}

type segmentLoadSubmission struct {
	segmentID int64
	ctx       context.Context
	request   segmentLoadRequest
	done      func()
}

type segmentLoadRequest struct {
	meta                        *viewpb.QueryViewMeta
	collection                  CollectionRuntime
	transformStartAfterTimeTick uint64
}

func NewViewScopedPhysicalSegmentManager(meta QueryViewLoadMetadataProvider, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithScheduler(NewQueryViewSegmentLoadScheduler(meta, loader, estimators...))
}

func NewViewScopedPhysicalSegmentManagerWithScheduler(scheduler SegmentLoadScheduler) *ViewScopedPhysicalSegmentManager {
	return &ViewScopedPhysicalSegmentManager{
		scheduler: scheduler,
		views:     make(map[qviews.QueryViewKey]*viewRef),
		segments:  make(map[int64]*physicalSegmentState),
		cancels:   make(map[qviews.QueryViewKey]context.CancelFunc),
	}
}

func (m *ViewScopedPhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	toLoad, loadWG, ok := m.recordView(req, cancel)
	if !ok {
		cancel()
		return
	}
	go m.load(ctx, req, toLoad, loadWG)
}

func (m *ViewScopedPhysicalSegmentManager) Release(req ReleaseSegments) {
	toCancel, loadWG := m.removeView(req.Key)
	go func() {
		for _, segmentID := range toCancel {
			m.scheduler.Cancel(segmentID)
		}
		if loadWG != nil {
			loadWG.Wait()
		}
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
}

func (m *ViewScopedPhysicalSegmentManager) recordView(req AcquirePhysicalSegments, cancel context.CancelFunc) ([]segmentLoadSubmission, *sync.WaitGroup, bool) {
	segmentPartitions := segmentPartitionMap(req.View)
	toLoad := make([]segmentLoadSubmission, 0, len(segmentPartitions))
	loadWG := &sync.WaitGroup{}

	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		return nil, nil, false
	}
	m.cancels[req.Key] = cancel
	m.views[req.Key] = &viewRef{
		segments:        segmentPartitions,
		loadWG:          loadWG,
		onLoaded:        req.OnLoaded,
		onSegmentFailed: req.OnSegmentUnrecoverable,
		onUnrecoverable: req.OnUnrecoverable,
	}
	for segmentID := range segmentPartitions {
		state, ok := m.segments[segmentID]
		if !ok {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			state = &physicalSegmentState{
				loading:    true,
				loadCancel: loadCancel,
				refs:       make(map[qviews.QueryViewKey]struct{}),
				requests:   make(map[qviews.QueryViewKey]segmentLoadRequest),
			}
			m.segments[segmentID] = state
			toLoad = append(toLoad, segmentLoadSubmission{segmentID: segmentID, ctx: loadCtx, request: newSegmentLoadRequest(req), done: loadCancel})
		} else if state.segment == nil && !state.loading && !state.pending {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			state.loading = true
			state.loadCancel = loadCancel
			toLoad = append(toLoad, segmentLoadSubmission{segmentID: segmentID, ctx: loadCtx, request: newSegmentLoadRequest(req), done: loadCancel})
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = newSegmentLoadRequest(req)
	}
	loadWG.Add(len(toLoad))
	m.mu.Unlock()

	return toLoad, loadWG, true
}

func (m *ViewScopedPhysicalSegmentManager) load(ctx context.Context, req AcquirePhysicalSegments, toLoad []segmentLoadSubmission, loadWG *sync.WaitGroup) {
	if len(toLoad) > 0 {
		for _, submission := range toLoad {
			if ctx.Err() != nil {
				loadWG.Done()
				continue
			}
			m.submitSegmentLoad(submission, loadWG.Done)
		}
		return
	}

	loaded, complete := m.collectLoaded(req.View)
	if ctx.Err() != nil {
		return
	}
	if complete && req.OnLoaded != nil {
		req.OnLoaded(loaded)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoad(submission segmentLoadSubmission, done func()) {
	done = chainLoadDone(done, submission.done)
	if done == nil {
		done = func() {}
	}
	task := SegmentLoadTask{
		Context:                     submission.ctx,
		Meta:                        proto.Clone(submission.request.meta).(*viewpb.QueryViewMeta),
		SegmentID:                   submission.segmentID,
		Collection:                  submission.request.collection,
		TransformStartAfterTimeTick: submission.request.transformStartAfterTimeTick,
		OnLoaded: func(segment TransformSegment) {
			defer done()
			if segment == nil {
				notifications, retries := m.failPhysicalSegmentLoad(submission.segmentID, nil)
				m.submitSegmentLoadSubmissions(retries)
				for _, notify := range notifications {
					notify()
				}
				return
			}
			notifications, retries, kept := m.completePhysicalSegmentLoad(segment)
			m.submitSegmentLoadSubmissions(retries)
			if !kept {
				_ = segment.Release(context.Background())
				return
			}
			for _, notify := range notifications {
				notify()
			}
		},
		OnUnrecoverable: func(err error) {
			defer done()
			notifications, retries := m.failPhysicalSegmentLoad(submission.segmentID, err)
			m.submitSegmentLoadSubmissions(retries)
			for _, notify := range notifications {
				notify()
			}
		},
	}
	m.scheduler.Submit(task)
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(segment TransformSegment) ([]func(), []segmentLoadSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil {
		return nil, nil, false
	}
	state.segment = segment
	state.loading = false
	state.pending = false
	state.loadCancel = nil
	if len(state.refs) == 0 {
		delete(m.segments, segment.ID())
		return nil, nil, false
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil || ref.onLoaded == nil {
			continue
		}
		loaded := []TransformSegment{segment}
		cb := ref.onLoaded
		notifications = append(notifications, func() {
			cb(loaded)
		})
	}
	return notifications, m.collectPendingLoadSubmissionsLocked(), true
}

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(segmentID int64, err error) ([]func(), []segmentLoadSubmission) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segmentID]
	if state == nil {
		return nil, nil
	}
	state.loading = false
	state.loadCancel = nil
	if state.segment == nil && len(state.refs) == 0 {
		delete(m.segments, segmentID)
		return nil, nil
	}
	if isSegmentResourceInsufficient(err) && m.hasOtherLoadingSegmentLocked(segmentID) {
		state.pending = true
		return nil, nil
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		if ref.onSegmentFailed != nil {
			cb := ref.onSegmentFailed
			notifications = append(notifications, func() {
				cb(segmentID, err)
			})
			continue
		}
		if ref.onUnrecoverable != nil {
			cb := ref.onUnrecoverable
			notifications = append(notifications, func() {
				cb()
			})
		}
	}
	state.refs = make(map[qviews.QueryViewKey]struct{})
	state.requests = make(map[qviews.QueryViewKey]segmentLoadRequest)
	state.loading = false
	state.pending = false
	state.loadCancel = nil
	if state.segment == nil {
		delete(m.segments, segmentID)
	}
	return notifications, m.collectPendingLoadSubmissionsLocked()
}

func (m *ViewScopedPhysicalSegmentManager) ResetSegment(segmentID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.segments[segmentID]; state != nil {
		if state.loadCancel != nil {
			state.loadCancel()
		}
		delete(m.segments, segmentID)
	}
	for _, ref := range m.views {
		delete(ref.segments, segmentID)
	}
}

func (m *ViewScopedPhysicalSegmentManager) collectLoaded(view *viewpb.QueryViewOfQueryNode) ([]TransformSegment, bool) {
	segments := make([]TransformSegment, 0, len(segmentPartitionMap(view)))

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partition := range view.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			state := m.segments[segmentID]
			if state == nil || state.segment == nil {
				return nil, false
			}
			segments = append(segments, state.segment)
		}
	}
	return segments, true
}

func (m *ViewScopedPhysicalSegmentManager) removeView(key qviews.QueryViewKey) ([]int64, *sync.WaitGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ref := m.views[key]
	if ref == nil {
		if cancel, ok := m.cancels[key]; ok {
			cancel()
			delete(m.cancels, key)
		}
		return nil, nil
	}
	delete(m.views, key)

	toCancel := make([]int64, 0, len(ref.segments))
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
		if len(state.refs) == 0 {
			if state.segment == nil && state.loading {
				toCancel = append(toCancel, segmentID)
				if state.loadCancel != nil {
					state.loadCancel()
					state.loadCancel = nil
				}
			}
			delete(m.segments, segmentID)
		}
	}
	if cancel, ok := m.cancels[key]; ok {
		cancel()
		delete(m.cancels, key)
	}
	return toCancel, ref.loadWG
}

func newSegmentLoadRequest(req AcquirePhysicalSegments) segmentLoadRequest {
	return segmentLoadRequest{
		meta:                        req.Meta,
		collection:                  req.Collection,
		transformStartAfterTimeTick: req.Meta.GetTransformStartAfterTimetick(),
	}
}

func isSegmentResourceInsufficient(err error) bool {
	return errors.Is(err, merr.ErrSegmentRequestResourceFailed)
}

func (m *ViewScopedPhysicalSegmentManager) hasOtherLoadingSegmentLocked(segmentID int64) bool {
	for id, state := range m.segments {
		if id != segmentID && state.loading {
			return true
		}
	}
	return false
}

func (m *ViewScopedPhysicalSegmentManager) collectPendingLoadSubmissionsLocked() []segmentLoadSubmission {
	submissions := make([]segmentLoadSubmission, 0)
	for segmentID, state := range m.segments {
		if !state.pending || state.loading || state.segment != nil || len(state.refs) == 0 {
			continue
		}
		request, ok := state.loadRequest()
		if !ok {
			continue
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		state.pending = false
		state.loading = true
		state.loadCancel = loadCancel
		submissions = append(submissions, segmentLoadSubmission{
			segmentID: segmentID,
			ctx:       loadCtx,
			request:   request,
			done:      chainLoadDone(loadCancel, m.trackPendingLoadAttemptLocked(state)),
		})
	}
	return submissions
}

func (m *ViewScopedPhysicalSegmentManager) trackPendingLoadAttemptLocked(state *physicalSegmentState) func() {
	waitGroups := make([]*sync.WaitGroup, 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil || ref.loadWG == nil {
			continue
		}
		ref.loadWG.Add(1)
		waitGroups = append(waitGroups, ref.loadWG)
	}
	return func() {
		for _, wg := range waitGroups {
			wg.Done()
		}
	}
}

func (s *physicalSegmentState) loadRequest() (segmentLoadRequest, bool) {
	for key := range s.refs {
		request, ok := s.requests[key]
		if ok {
			return request, true
		}
	}
	return segmentLoadRequest{}, false
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoadSubmissions(submissions []segmentLoadSubmission) {
	for _, submission := range submissions {
		m.submitSegmentLoad(submission, nil)
	}
}

func chainLoadDone(first func(), second func()) func() {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func() {
		defer first()
		second()
	}
}
