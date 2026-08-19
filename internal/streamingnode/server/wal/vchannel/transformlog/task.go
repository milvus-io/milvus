package transformlog

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type transformTask interface {
	nodescheduler.Task
	Done() bool
}

type transformTaskBase struct {
	log          *TransformLog
	timetick     uint64
	predecessors []transformTask
	done         atomic.Bool
}

func (t *transformTaskBase) Done() bool {
	return t.done.Load()
}

func (t *transformTaskBase) predecessorsDone() bool {
	for _, predecessor := range t.predecessors {
		if predecessor != nil && !predecessor.Done() {
			return false
		}
	}
	return true
}

func (t *transformTaskBase) execute(ctx context.Context, ready bool, fn func(context.Context) error) error {
	if !ready {
		return nodescheduler.ErrDelay
	}
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
		return nil
	}
	return errors.Mark(err, nodescheduler.ErrDelay)
}

type transformFlushTask struct {
	transformTaskBase
}

func (t *transformFlushTask) Execute(ctx context.Context) error {
	return t.execute(ctx, t.predecessorsDone(), func(ctx context.Context) error {
		result, err := t.log.flush(ctx, flushOption{TargetTimeTick: t.timetick})
		if err != nil {
			return err
		}
		if result.NextTargetTimeTick > 0 {
			t.log.submitFlushTask(result.NextTargetTimeTick)
		}
		if t.log.shouldMaterialize(ctx) && !t.log.HasPendingMaterializeTask() {
			t.log.submitMaterializeTask(t.log.checkpointTimeTick())
		}
		t.log.notifyUpdated()
		releaseMessages(result.CompletedMessages)
		return nil
	})
}

type transformMaterializeTask struct {
	transformTaskBase
}

func (t *transformMaterializeTask) Execute(ctx context.Context) error {
	ready := t.predecessorsDone() && t.log.LatestTimeTick() >= t.timetick
	return t.execute(ctx, ready, func(ctx context.Context) error {
		if _, err := t.log.materialize(ctx, materializeOption{TargetTimeTick: t.timetick}); err != nil {
			return err
		}
		t.log.notifyUpdated()
		return nil
	})
}

func (t *TransformLog) submitFlushTask(timetick uint64) {
	scheduler := t.runtime.Scheduler
	if scheduler == nil {
		return
	}
	t.mu.Lock()
	task := t.newFlushTaskLocked(timetick)
	t.mu.Unlock()
	scheduler.Submit(task)
}

func (t *TransformLog) newFlushTaskLocked(timetick uint64) *transformFlushTask {
	task := &transformFlushTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     timetick,
			predecessors: t.taskPredecessorsLocked(),
		},
	}
	t.flushTasks = append(t.flushTasks, task)
	return task
}

func (t *TransformLog) submitMaterializeTask(timetick uint64) {
	scheduler := t.runtime.Scheduler
	if scheduler == nil {
		return
	}
	t.mu.Lock()
	task := &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     timetick,
			predecessors: t.taskPredecessorsLocked(),
		},
	}
	t.materializeTasks = append(t.materializeTasks, task)
	t.mu.Unlock()
	scheduler.Submit(task)
}

func (t *TransformLog) taskPredecessorsLocked() []transformTask {
	t.flushTasks = compactTransformFlushTasks(t.flushTasks)
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	predecessors := make([]transformTask, 0, len(t.flushTasks)+len(t.materializeTasks))
	for _, task := range t.flushTasks {
		predecessors = append(predecessors, task)
	}
	for _, task := range t.materializeTasks {
		predecessors = append(predecessors, task)
	}
	return predecessors
}

func (t *TransformLog) HasPendingFlushTask() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hasPendingFlushTaskLocked()
}

func (t *TransformLog) hasPendingFlushTaskLocked() bool {
	t.flushTasks = compactTransformFlushTasks(t.flushTasks)
	return len(t.flushTasks) > 0
}

func (t *TransformLog) pendingFlushTargetLocked() uint64 {
	t.flushTasks = compactTransformFlushTasks(t.flushTasks)
	var target uint64
	for _, task := range t.flushTasks {
		if task.timetick > target {
			target = task.timetick
		}
	}
	return target
}

func (t *TransformLog) HasPendingMaterializeTask() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	return len(t.materializeTasks) > 0
}

func (t *TransformLog) notifyUpdated() {
	if t.runtime.Notifier == nil {
		return
	}
	t.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
}

func compactTransformFlushTasks(tasks []*transformFlushTask) []*transformFlushTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	return pending
}

func compactTransformMaterializeTasks(tasks []*transformMaterializeTask) []*transformMaterializeTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	return pending
}

var (
	_ nodescheduler.Task = (*transformFlushTask)(nil)
	_ nodescheduler.Task = (*transformMaterializeTask)(nil)
)
