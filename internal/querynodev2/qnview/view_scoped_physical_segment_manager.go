package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"google.golang.org/protobuf/proto"
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
	loadCancel context.CancelFunc
	refs       map[qviews.QueryViewKey]struct{}
}

type segmentLoadSubmission struct {
	segmentID int64
	ctx       context.Context
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
	toLoad, loadWG := m.recordView(req, cancel)
	go m.load(ctx, req, toLoad, loadWG)
}

func (m *ViewScopedPhysicalSegmentManager) Release(req ReleaseSegments) {
	go func() {
		toCancel, loadWG := m.removeView(req.Key)
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

func (m *ViewScopedPhysicalSegmentManager) recordView(req AcquirePhysicalSegments, cancel context.CancelFunc) ([]segmentLoadSubmission, *sync.WaitGroup) {
	segmentPartitions := segmentPartitionMap(req.View)
	toLoad := make([]segmentLoadSubmission, 0, len(segmentPartitions))
	loadWG := &sync.WaitGroup{}

	m.mu.Lock()
	if oldCancel, ok := m.cancels[req.Key]; ok {
		oldCancel()
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
			}
			m.segments[segmentID] = state
			toLoad = append(toLoad, segmentLoadSubmission{segmentID: segmentID, ctx: loadCtx})
		} else if state.segment == nil && !state.loading {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			state.loading = true
			state.loadCancel = loadCancel
			toLoad = append(toLoad, segmentLoadSubmission{segmentID: segmentID, ctx: loadCtx})
		}
		state.refs[req.Key] = struct{}{}
	}
	loadWG.Add(len(toLoad))
	m.mu.Unlock()

	return toLoad, loadWG
}

func (m *ViewScopedPhysicalSegmentManager) load(ctx context.Context, req AcquirePhysicalSegments, toLoad []segmentLoadSubmission, loadWG *sync.WaitGroup) {
	if len(toLoad) > 0 {
		for _, submission := range toLoad {
			if ctx.Err() != nil {
				loadWG.Done()
				continue
			}
			m.submitSegmentLoad(submission.ctx, req, submission.segmentID, loadWG)
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

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoad(ctx context.Context, req AcquirePhysicalSegments, segmentID int64, loadWG *sync.WaitGroup) {
	task := SegmentLoadTask{
		Context:                     ctx,
		Meta:                        proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		SegmentID:                   segmentID,
		Collection:                  req.Collection,
		TransformStartAfterTimeTick: req.Meta.GetDeleteApplyStartAfterTimetick(),
		OnLoaded: func(segment TransformSegment) {
			defer loadWG.Done()
			if segment == nil {
				for _, notify := range m.failPhysicalSegmentLoad(segmentID, nil) {
					notify()
				}
				return
			}
			notifications, kept := m.completePhysicalSegmentLoad(segment)
			if !kept {
				_ = segment.Release(context.Background())
				return
			}
			for _, notify := range notifications {
				notify()
			}
		},
		OnUnrecoverable: func(err error) {
			defer loadWG.Done()
			for _, notify := range m.failPhysicalSegmentLoad(segmentID, err) {
				notify()
			}
		},
	}
	m.scheduler.Submit(task)
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(segment TransformSegment) ([]func(), bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil {
		return nil, false
	}
	state.segment = segment
	state.loading = false
	state.loadCancel = nil
	if len(state.refs) == 0 {
		delete(m.segments, segment.ID())
		return nil, false
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
	return notifications, true
}

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(segmentID int64, err error) []func() {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segmentID]
	if state == nil {
		return nil
	}
	state.loading = false
	state.loadCancel = nil
	if state.segment == nil && len(state.refs) == 0 {
		delete(m.segments, segmentID)
		return nil
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
	state.loading = false
	state.loadCancel = nil
	if state.segment == nil {
		delete(m.segments, segmentID)
	}
	return notifications
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
		if len(toCancel) == len(ref.segments) {
			cancel()
		}
		delete(m.cancels, key)
	}
	return toCancel, ref.loadWG
}
