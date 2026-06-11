package moduleapi

import (
	"context"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
	"google.golang.org/protobuf/proto"
)

type Module interface {
	Name() ModuleName
	ObserveMessage(ctx context.Context, msg message.ImmutableMessage) ObserveResult
	SwitchIntoMetaAndData() ModuleSnapshot
	// ConsumeDirtySnapshots captures module-local dirty views as stable
	// snapshots for RecoveryStorage-owned catalog persistence. It does not
	// return an error because it only snapshots in-memory state.
	ConsumeDirtySnapshots() []DirtySnapshot
}

type ModuleName string

const (
	ModuleNameVChannel     ModuleName = "vchannel"
	ModuleNameSegment      ModuleName = "segment"
	ModuleNameTransformLog ModuleName = "transformlog"
	ModuleNameAck          ModuleName = "ack"
)

type ObserveResult struct {
	Meta walcheckpoint.Barrier
	Data walcheckpoint.Barrier
}

type ModuleSnapshot interface {
	ModuleName() ModuleName
}

type CompositeModuleSnapshot []ModuleSnapshot

func (CompositeModuleSnapshot) ModuleName() ModuleName {
	return ""
}

func FlattenModuleSnapshot(snapshot ModuleSnapshot) []ModuleSnapshot {
	if snapshot == nil {
		return nil
	}
	if composite, ok := snapshot.(CompositeModuleSnapshot); ok {
		return composite
	}
	return []ModuleSnapshot{snapshot}
}

type VChannelModuleSnapshot struct {
	VChannels map[string]*streamingpb.VChannelMeta
}

func (*VChannelModuleSnapshot) ModuleName() ModuleName {
	return ModuleNameVChannel
}

type SegmentModuleSnapshot struct {
	Segments map[int64]*streamingpb.SegmentAssignmentMeta
}

func (*SegmentModuleSnapshot) ModuleName() ModuleName {
	return ModuleNameSegment
}

type TransformLogModuleSnapshot struct {
	TransformLogs map[string]*streamingpb.VChannelTransformLogMeta
}

func (*TransformLogModuleSnapshot) ModuleName() ModuleName {
	return ModuleNameTransformLog
}

type SnapshotKey struct {
	PChannel  string
	VChannel  string
	SegmentID int64
}

type SnapshotOp int

const (
	SnapshotOpUpsert SnapshotOp = iota
	SnapshotOpDelete
)

type DirtySnapshot interface {
	ModuleName() ModuleName
	Key() SnapshotKey
	Op() SnapshotOp
	Payload() proto.Message
	MetaTimeTick() uint64
	DataTimeTick() uint64
	MarkPersisted()
}

type CheckpointPersistedObserver interface {
	NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64)
}

type ScopeType int

const (
	ScopeAll ScopeType = iota
	ScopeVChannel
	ScopePartition
)

type Scope struct {
	Type ScopeType

	VChannel     string
	CollectionID int64
	PartitionID  int64
}

type DataFrontierView interface {
	DataFrontier(scope Scope) walcheckpoint.Barrier
}

type DataFrontierProvider interface {
	DataFrontier(scope Scope) walcheckpoint.Barrier
}

type Runtime struct {
	Scheduler AsyncTaskScheduler
	Notifier  ModuleNotifier
}

type AsyncTaskScheduler interface {
	Submit(task scheduler.Task) scheduler.TaskHandle
	Notify()
}

type ModuleNotifier interface {
	NotifyModuleUpdated(module ModuleName)
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
