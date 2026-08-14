package moduleapi

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type CleanupContext struct {
	MetaPhysicalTimeTick uint64
	DataPhysicalTimeTick uint64
}

type ModuleName string

const (
	ModuleNameVChannel     ModuleName = "vchannel"
	ModuleNameSegment      ModuleName = "segment"
	ModuleNameTransformLog ModuleName = "transformlog"
)

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
	SnapshotOpUpsertBase
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

type Runtime struct {
	Scheduler AsyncTaskScheduler
	Notifier  ModuleNotifier
}

type AsyncTaskScheduler interface {
	Submit(task nodescheduler.Task) nodescheduler.TaskHandle
}

type ModuleNotifier interface {
	NotifyModuleUpdated(module ModuleName)
}
