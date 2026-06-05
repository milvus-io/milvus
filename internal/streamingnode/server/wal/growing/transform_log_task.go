package growing

import (
	"context"

	"go.uber.org/atomic"

	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type flushTransformLogBufferTask struct {
	vchannel     *vChannelView
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *flushTransformLogBufferTask) Name() string {
	return "growing-flush-transform-log-buffer"
}

func (t *flushTransformLogBufferTask) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *flushTransformLogBufferTask) Done() bool {
	return t.done.Load()
}

func (t *flushTransformLogBufferTask) Run(ctx context.Context) error {
	err := t.run(ctx)
	if err == nil {
		t.done.Store(true)
	}
	return err
}

func (t *flushTransformLogBufferTask) run(ctx context.Context) error {
	vchannel := t.vchannel
	var nextTask scheduler.Task
	runtime := vchannel.runtime
	for {
		var targetTimeTick uint64
		var pack *deleteFlushPack
		var nextTargetTimeTick uint64
		vchannel.mu.Lock()
		targetTimeTick = vchannel.transformLogBuffer.FlushTargetTimeTick()
		if targetTimeTick > vchannel.meta.GetDataCheckpointTimeTick() {
			_, schema := vchannel.GetSchemaLocked(targetTimeTick)
			pack = vchannel.transformLogBuffer.FlushPack(vchannel.meta, schema, targetTimeTick)
		}
		vchannel.mu.Unlock()

		if pack != nil {
			result, err := vchannel.packWriter.FlushDeleteBuffer(ctx, pack)
			if err != nil {
				return err
			}
			if err := vchannel.lifecycle.CommitL0Segment(ctx, result.Batch); err != nil {
				return err
			}
		}

		vchannel.mu.Lock()
		if pack != nil {
			vchannel.transformLogBuffer.DiscardThrough(pack.ToTimeTick)
			durableTimeTick := pack.ToTimeTick
			if !vchannel.transformLogBuffer.HasFlushWorkThrough(targetTimeTick) {
				durableTimeTick = targetTimeTick
			}
			vchannel.MarkDeleteDataDurable(durableTimeTick)
		} else if targetTimeTick > vchannel.meta.GetDataCheckpointTimeTick() {
			vchannel.MarkDeleteDataDurable(targetTimeTick)
		}
		currentFlushTarget := vchannel.transformLogBuffer.FlushTargetTimeTick()
		vchannel.transformLogBuffer.FinishFlush()
		switch {
		case currentFlushTarget > vchannel.meta.GetDataCheckpointTimeTick():
			nextTargetTimeTick = currentFlushTarget
		case vchannel.transformLogBuffer.HasFlushWorkThrough(currentFlushTarget):
			nextTargetTimeTick = currentFlushTarget
		case vchannel.transformLogBuffer.ShouldFlush():
			nextTargetTimeTick = vchannel.transformLogBuffer.DataTimeTick()
		}
		if nextTargetTimeTick > 0 {
			nextTask = vchannel.StartFlushTransformLogBufferTaskLocked(nextTargetTimeTick)
		}
		vchannel.mu.Unlock()
		vchannel.NotifyDataUpdated()
		break
	}
	if nextTask != nil {
		runtime.Scheduler.Submit(nextTask)
	}
	return nil
}
