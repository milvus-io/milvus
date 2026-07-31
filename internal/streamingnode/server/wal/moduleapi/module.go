package moduleapi

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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

type CleanupContext struct {
	MetaPhysicalTimeTick uint64
	DataPhysicalTimeTick uint64
}

type CleanupModule interface {
	ConsumeCleanupSnapshots(CleanupContext) []DirtySnapshot
}

// PendingCleanupModule exposes whether a cleanup module still has work that
// RecoveryStorage must drain before closing.
type PendingCleanupModule interface {
	CleanupModule
	HasPendingCleanup() bool
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
	Segments             map[int64]*streamingpb.SegmentAssignmentMeta
	DataVersionSummaries map[string]*streamingpb.SegmentDataVersionSummary
}

func (*SegmentModuleSnapshot) ModuleName() ModuleName {
	return ModuleNameSegment
}

type TransformLogModuleSnapshot struct {
	TransformLogs map[string]*streamingpb.VChannelTransformLogMeta
}

// WritePathRecoveryModuleSnapshot contains only the state needed to resume the WAL
// write path. It intentionally excludes persisted binlogs and historical
// schemas retained by QueryView recovery.
type WritePathRecoveryModuleSnapshot struct {
	VChannels       map[string]VChannelWritePathRecoveryState
	GrowingSegments map[int64]SegmentWritePathRecoveryState
}

func (*WritePathRecoveryModuleSnapshot) ModuleName() ModuleName {
	return ModuleNameVChannel
}

type VChannelWritePathRecoveryState struct {
	VChannel     string
	CollectionID int64
	PartitionIDs []int64
	Schema       *schemapb.CollectionSchema
}

type SegmentWritePathRecoveryState struct {
	VChannel     string
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	Stat         *streamingpb.SegmentAssignmentStat
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

type ScopeType int

const (
	ScopeAll ScopeType = iota
	ScopeVChannel
	ScopePartition
)

type DataProgressKind int

const (
	DataProgressDurable DataProgressKind = iota
	DataProgressMaterialized
)

type Scope struct {
	Type ScopeType
	Kind DataProgressKind

	VChannel     string
	CollectionID int64
	PartitionID  int64
}

type DataFrontierProvider interface {
	DataFrontier(scope Scope) walcheckpoint.Barrier
}

type Runtime struct {
	Scheduler AsyncTaskScheduler
	Notifier  ModuleNotifier
}

type AsyncTaskScheduler interface {
	Submit(task nodescheduler.Task) nodescheduler.TaskHandle
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
