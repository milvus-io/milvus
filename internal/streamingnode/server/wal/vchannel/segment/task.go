package segment

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type segmentTask interface {
	nodescheduler.Task
	Done() bool
}

type segmentTaskBase struct {
	segment      *SegmentView
	predecessors []segmentTask
	done         atomic.Bool
}

func (t *segmentTaskBase) Done() bool {
	return t.done.Load()
}

func (t *segmentTaskBase) execute(ctx context.Context, fn func(context.Context) error) error {
	for _, predecessor := range t.predecessors {
		if predecessor != nil && !predecessor.Done() {
			return nodescheduler.ErrDelay
		}
	}
	defer t.done.Store(true)
	return fn(ctx)
}

type ensureGrowingSegmentTask struct {
	segmentTaskBase
	timetick uint64
}

func (t *ensureGrowingSegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		segment := t.segment
		meta := segment.AssignmentMeta()
		if err := segment.lifecycle.EnsureGrowingSegment(ctx, meta); err != nil {
			return err
		}

		segment.mu.Lock()
		segment.MarkPendingDataDurable(t.timetick)
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		return nil
	})
}

type flushL1BufferTask struct {
	segmentTaskBase
	timetick uint64
}

func (t *flushL1BufferTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		return t.segment.FlushInsertChunk(ctx, t.timetick)
	})
}

type commitL1SegmentTask struct {
	segmentTaskBase
	timetick      uint64
	flushTimeTick uint64
}

func (t *commitL1SegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		segment := t.segment
		if limiter := segment.commitL1Limiter; limiter != nil {
			release, err := limiter.Acquire(ctx)
			if err != nil {
				return err
			}
			defer release()
		}
		if err := segment.FlushInsertChunk(ctx, t.flushTimeTick); err != nil {
			return err
		}
		meta := segment.AssignmentMeta()
		sealedAt, err := segment.lifecycle.CommitL1Segment(ctx, meta)
		if err != nil {
			return err
		}

		segment.mu.Lock()
		segment.MarkPendingDataDurable(t.timetick)
		sealedEvent, sealed := segment.markSealedAtDataVersionLocked(sealedAt)
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		if sealed {
			segment.NotifySegmentSealed(sealedEvent)
		}
		return nil
	})
}

func (s *SegmentView) newEnsureGrowingSegmentTaskLocked(timetick uint64) segmentTask {
	task := &ensureGrowingSegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked(),
		timetick:        timetick,
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newFlushL1BufferTaskLocked() segmentTask {
	task := &flushL1BufferTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked(),
		timetick:        s.enqueuePendingFlushChunkLocked(),
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newCommitL1SegmentTaskLocked(timetick uint64) segmentTask {
	task := &commitL1SegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked(),
		timetick:        timetick,
		flushTimeTick:   s.enqueuePendingFlushChunkLocked(),
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newSegmentTaskBaseLocked() segmentTaskBase {
	return segmentTaskBase{
		segment:      s,
		predecessors: s.segmentTaskPredecessorsLocked(),
	}
}

func (s *SegmentView) segmentTaskPredecessorsLocked() []segmentTask {
	pending := s.pendingTasks[:0]
	for _, task := range s.pendingTasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	s.pendingTasks = pending
	return append([]segmentTask(nil), pending...)
}
