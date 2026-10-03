package moduleapi

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type CleanupContext struct {
	PhysicalTimeTick uint64
	// SummaryRetired proves the persisted manifest no longer needs this
	// VChannel tombstone to recover its transform GC frontier.
	SummaryRetired func(vchannel string, through uint64) bool
}

type ModuleName string

const (
	ModuleNameVChannel     ModuleName = "vchannel"
	ModuleNameSegment      ModuleName = "segment"
	ModuleNameTransformLog ModuleName = "transformlog"
)

// WritePathRecoveryModuleSnapshot contains only the state needed to resume the WAL
// write path. It intentionally excludes persisted binlogs and historical schemas.
type WritePathRecoveryModuleSnapshot struct {
	VChannels       map[string]VChannelWritePathRecoveryState
	GrowingSegments map[int64]SegmentWritePathRecoveryState
}

type VChannelWritePathRecoveryState struct {
	VChannel     string
	CollectionID int64
	PartitionIDs []int64
	Schema       *schemapb.CollectionSchema
	// SplitFenceTimeTick is T_switch when this vchannel is a shard split source
	// that has been fenced, and zero otherwise. A fenced source stays NORMAL --
	// it keeps observing DDL and draining its own data until adoption drops it
	// -- so this field, not the vchannel state, is what rebuilds the write
	// path's DoAppend gate after a restart.
	SplitFenceTimeTick uint64
	// SplitFenceTaskID is the split task that placed the fence, carried beside
	// it so a re-sent fence can be told from a concurrent task's.
	SplitFenceTaskID int64
}

type SegmentWritePathRecoveryState struct {
	VChannel     string
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	Stat         *streamingpb.SegmentAssignmentStat
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
	// SnapshotOpDeleteSchemas removes persisted schema tombstones only.
	SnapshotOpDeleteSchemas
)

type DirtySnapshot interface {
	ModuleName() ModuleName
	Key() SnapshotKey
	Op() SnapshotOp
	Payload() proto.Message
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
