package segment

import (
	"context"

	"go.uber.org/atomic"

	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type segmentTaskBase struct {
	segment      *segmentView
	name         string
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *segmentTaskBase) Name() string {
	return "growing-" + t.name
}

func (t *segmentTaskBase) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *segmentTaskBase) Done() bool {
	return t.done.Load()
}

func (t *segmentTaskBase) run(ctx context.Context, fn func(context.Context) error) error {
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
	}
	return err
}

type ensureGrowingSegmentTask struct {
	segmentTaskBase
	timetick uint64
}

func (t *ensureGrowingSegmentTask) Run(ctx context.Context) error {
	return t.run(ctx, func(ctx context.Context) error {
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

func (t *flushL1BufferTask) Run(ctx context.Context) error {
	return t.run(ctx, func(ctx context.Context) error {
		return t.segment.FlushInsertChunk(ctx, t.timetick)
	})
}

type commitL1SegmentTask struct {
	segmentTaskBase
	timetick      uint64
	flushTimeTick uint64
}

func (t *commitL1SegmentTask) Run(ctx context.Context) error {
	return t.run(ctx, func(ctx context.Context) error {
		segment := t.segment
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

func (s *segmentView) newEnsureGrowingSegmentTaskLocked(timetick uint64) scheduler.Task {
	task := &ensureGrowingSegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("ensure-growing-segment"),
		timetick:        timetick,
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *segmentView) newFlushL1BufferTaskLocked() scheduler.Task {
	task := &flushL1BufferTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("flush-l1-buffer"),
		timetick:        s.enqueuePendingFlushChunkLocked(),
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *segmentView) newCommitL1SegmentTaskLocked(timetick uint64) scheduler.Task {
	task := &commitL1SegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("commit-l1-segment"),
		timetick:        timetick,
		flushTimeTick:   s.enqueuePendingFlushChunkLocked(),
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *segmentView) newSegmentTaskBaseLocked(name string) segmentTaskBase {
	return segmentTaskBase{
		segment:      s,
		name:         name,
		precondition: s.segmentTaskPreconditionLocked(),
	}
}

func (s *segmentView) segmentTaskPreconditionLocked() scheduler.Precondition {
	pending := s.pendingTasks[:0]
	preconditions := make([]scheduler.Precondition, 0, len(s.pendingTasks))
	for _, task := range s.pendingTasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
		preconditions = append(preconditions, scheduler.After(task))
	}
	s.pendingTasks = pending
	return scheduler.All(preconditions...)
}
