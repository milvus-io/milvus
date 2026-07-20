package transformlog

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type transformTaskBase struct {
	log          *TransformLog
	name         string
	timetick     uint64
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *transformTaskBase) Name() string {
	return "vchan-transformlog-" + t.name
}

func (t *transformTaskBase) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *transformTaskBase) Done() bool {
	return t.done.Load()
}

type transformFlushTask struct {
	transformTaskBase
}

func (t *transformFlushTask) Run(ctx context.Context) error {
	result, err := t.log.flush(ctx, flushOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.done.Store(true)
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

func (t *transformMaterializeTask) Run(ctx context.Context) error {
	_, err := t.log.materialize(ctx, materializeOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.done.Store(true)
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
			name:         "flush",
			timetick:     timetick,
			precondition: t.taskPrecondition(),
		},
	}
	handle := t.runtime.Scheduler.Submit(task)
	t.flushTasks = append(t.flushTasks, handle)
}

func (t *TransformLog) submitMaterializeTask(timetick uint64) {
	if t.runtime.Scheduler == nil {
		return
	}
	task := &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			log:      t,
			name:     "materialize",
			timetick: timetick,
			precondition: scheduler.All(t.taskPrecondition(), scheduler.PreconditionFunc(func() bool {
				return t.LatestTimeTick() >= timetick
			})),
		},
	}
	handle := t.runtime.Scheduler.Submit(task)
	t.materializeTasks = append(t.materializeTasks, handle)
}

func (t *TransformLog) taskPrecondition() scheduler.Precondition {
	t.flushTasks = compactPendingTasks(t.flushTasks)
	t.materializeTasks = compactPendingTasks(t.materializeTasks)
	preconditions := make([]scheduler.Precondition, 0, len(t.flushTasks)+len(t.materializeTasks))
	for _, task := range t.flushTasks {
		if task == nil || task.Done() {
			continue
		}
		preconditions = append(preconditions, scheduler.After(task))
	}
	for _, task := range t.materializeTasks {
		if task == nil || task.Done() {
			continue
		}
		preconditions = append(preconditions, scheduler.After(task))
	}
	return scheduler.All(preconditions...)
}

func (t *TransformLog) HasPendingFlushTask() bool {
	t.flushTasks = compactPendingTasks(t.flushTasks)
	return len(t.flushTasks) > 0
}

func (t *TransformLog) HasPendingMaterializeTask() bool {
	t.materializeTasks = compactPendingTasks(t.materializeTasks)
	return len(t.materializeTasks) > 0
}

func (t *TransformLog) notifyUpdated() {
	if t.runtime.Notifier == nil {
		return
	}
	t.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
	t.runtime.Notifier.NotifyBarrierUpdated()
}

func compactPendingTasks(tasks []scheduler.TaskHandle) []scheduler.TaskHandle {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	return pending
}

var _ scheduler.Task = (*transformFlushTask)(nil)
var _ scheduler.Task = (*transformMaterializeTask)(nil)
