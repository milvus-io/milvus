package transformlog

import (
	"context"

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

type transformFlushTask struct {
	transformTaskBase
}

func (t *transformFlushTask) Execute(ctx context.Context) error {
	if !t.predecessorsDone() {
		return nodescheduler.ErrDelay
	}
	defer t.done.Store(true)
	result, err := t.log.flush(ctx, flushOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	if result.NextTargetTimeTick > 0 {
		t.log.submitFlushTask(result.NextTargetTimeTick)
	}
	if t.log.shouldMaterialize() && !t.log.HasPendingMaterializeTask() {
		t.log.submitMaterializeTask(t.log.dataCheckpointTimeTick())
	}
	t.log.notifyUpdated()
	return nil
}

type transformMaterializeTask struct {
	transformTaskBase
}

func (t *transformMaterializeTask) Execute(ctx context.Context) error {
	if !t.predecessorsDone() || t.log.LatestTimeTick() < t.timetick {
		return nodescheduler.ErrDelay
	}
	defer t.done.Store(true)
	_, err := t.log.materialize(ctx, materializeOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.log.notifyUpdated()
	return nil
}

func (t *TransformLog) submitFlushTask(timetick uint64) {
	if t.runtime.Scheduler == nil {
		return
	}
	task := &transformFlushTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     timetick,
			predecessors: t.taskPredecessors(),
		},
	}
	t.flushTasks = append(t.flushTasks, task)
	t.runtime.Scheduler.Submit(task)
}

func (t *TransformLog) submitMaterializeTask(timetick uint64) {
	if t.runtime.Scheduler == nil {
		return
	}
	task := &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     timetick,
			predecessors: t.taskPredecessors(),
		},
	}
	t.materializeTasks = append(t.materializeTasks, task)
	t.runtime.Scheduler.Submit(task)
}

func (t *TransformLog) taskPredecessors() []transformTask {
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
	t.flushTasks = compactTransformFlushTasks(t.flushTasks)
	return len(t.flushTasks) > 0
}

func (t *TransformLog) HasPendingMaterializeTask() bool {
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	return len(t.materializeTasks) > 0
}

func (t *TransformLog) notifyUpdated() {
	if t.runtime.Notifier == nil {
		return
	}
	t.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
	t.runtime.Notifier.NotifyBarrierUpdated()
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
