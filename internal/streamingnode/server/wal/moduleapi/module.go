package moduleapi

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type CleanupContext struct {
	PhysicalTimeTick uint64
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
