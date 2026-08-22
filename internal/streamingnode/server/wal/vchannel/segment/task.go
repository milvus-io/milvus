package segment

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
		return nil
	}
	return errors.Mark(err, nodescheduler.ErrDelay)
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
		handles := segment.markPendingDataDurableLocked(t.timetick)
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		releaseMessages(handles)
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
		segment.mu.Lock()
		finalCommitDone := segment.finalCommitDone.Load()
		segment.mu.Unlock()
		if finalCommitDone {
			return nil
		}
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
		if err := segment.lifecycle.CommitL1Segment(ctx, meta); err != nil {
			return err
		}

		segment.mu.Lock()
		handles := segment.markPendingDataDurableLocked(t.timetick)
		segment.finalCommitDone.Store(true)
		segment.meta.L1CommitDone = true
		segment.durableMeta.State = segment.meta.State
		segment.durableMeta.L1CommitDone = true
		if segment.meta.GetStat() != nil {
			segment.durableMeta.Stat.LastModifiedTimestamp = segment.meta.GetStat().GetLastModifiedTimestamp()
		}
		segment.dirty = true
		// After the final commit no flush task can ever cover the buffer again.
		// A non-empty buffer here means out-of-timetick-order inserts slipped
		// past the replay guards: their handles were released without their
		// rows being persisted. Warn loudly so the anomaly is observable.
		if len(segment.pending.entries) > 0 {
			mlog.Warn(ctx, "final commit leaves pending insert data unpersisted",
				mlog.Int64("segmentID", segment.meta.GetSegmentId()),
				mlog.String("vchannel", segment.meta.GetVchannel()),
				mlog.Int("pendingEntries", len(segment.pending.entries)),
				mlog.Uint64("pendingFromTimeTick", segment.pending.fromTimeTick),
				mlog.Uint64("commitTimeTick", t.timetick))
		}
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		releaseMessages(handles)
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
	if !s.canScheduleFinalCommitLocked() {
		return nil
	}
	return s.newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, s.enqueuePendingFlushChunkLocked())
}

func (s *SegmentView) newRecoveredCommitL1SegmentTaskLocked(timetick uint64) segmentTask {
	if !s.canScheduleFinalCommitLocked() {
		return nil
	}
	return s.newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, 0)
}

func (s *SegmentView) canScheduleFinalCommitLocked() bool {
	return !s.finalCommitDone.Load() && (s.pendingFinalCommit == nil || s.pendingFinalCommit.Done())
}

func (s *SegmentView) newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, flushTimeTick uint64) segmentTask {
	task := &commitL1SegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked(),
		timetick:        timetick,
		flushTimeTick:   flushTimeTick,
	}
	s.pendingFinalCommit = task
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
