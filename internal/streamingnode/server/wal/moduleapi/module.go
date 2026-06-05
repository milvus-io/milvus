package moduleapi

import (
	"context"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type Module interface {
	Name() string
	ObserveMessage(ctx context.Context, msg message.ImmutableMessage) ObserveResult
	SwitchIntoMetaAndData() Snapshot
	RequirePersist()
}

type ObserveResult struct {
	Meta walcheckpoint.Barrier
	Data walcheckpoint.Barrier
}

type Snapshot interface{}

type CheckpointPersistedObserver interface {
	NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64)
}

type DurableFrontierView interface {
	PartitionDurableFrontier(collectionID int64, partitionID int64) walcheckpoint.Barrier
	VChannelDurableFrontier(vchannel string) walcheckpoint.Barrier
	AllDurableFrontier() walcheckpoint.Barrier
}

type DataCheckpointView interface {
	DataCheckpointTimeTick() uint64
}

type Runtime struct {
	Scheduler AsyncTaskScheduler
	Notifier  BarrierUpdatedNotifier
}

type AsyncTaskScheduler interface {
	Submit(task scheduler.Task) scheduler.TaskHandle
	Notify()
}

type BarrierUpdatedNotifier interface {
	NotifyBarrierUpdated()
}

func ComposeBarriers(results []ObserveResult) ObserveResult {
	metaBarriers := make([]walcheckpoint.Barrier, 0, len(results))
	dataBarriers := make([]walcheckpoint.Barrier, 0, len(results))
	for _, result := range results {
		if result.Meta != nil {
			metaBarriers = append(metaBarriers, result.Meta)
		}
		if result.Data != nil {
			dataBarriers = append(dataBarriers, result.Data)
		}
	}
	return ObserveResult{
		Meta: walcheckpoint.NewCompositeBarrier(metaBarriers...),
		Data: walcheckpoint.NewCompositeBarrier(dataBarriers...),
	}
}
