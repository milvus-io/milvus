package recovery

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

type WALCheckpoint = utility.WALCheckpoint

// RecoverySnapshot is the snapshot of the recovery info.
type RecoverySnapshot struct {
	VChannels          map[string]*streamingpb.VChannelMeta
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	WritePathRecovery  *moduleapi.WritePathRecoveryModuleSnapshot
	Checkpoint         *WALCheckpoint
	CheckpointDirty    bool
	TxnBuffer          *utility.TxnBuffer
	// Used during WAL alteration process
	AlterWALInfo *AlterWALInfo
	// SalvageCheckpoint captures the replicate checkpoint at force-promote time.
	// It must be persisted before the consume checkpoint so that the ordering guarantee holds.
	SalvageCheckpoint *utility.ReplicateCheckpoint
}

type dirtyPersistSnapshot struct {
	Checkpoint         *WALCheckpoint
	CheckpointDirty    bool
	ModuleDirtySnaps   []moduleapi.DirtySnapshot
	ModuleSnapshotsAck bool
	SalvageCheckpoint  *utility.ReplicateCheckpoint
}

// AlterWALInfo contains information about WAL alteration process.
type AlterWALInfo struct {
	FoundAlterWALMsg bool
	TargetWALName    commonpb.WALName
	AlterWALConfig   map[string]string
	AlterWALTs       uint64
}

type BuildRecoveryStreamParam struct {
	StartCheckpoint message.MessageID
	EndTimeTick     uint64
	// UseWriteAheadBuffer lets unbounded live scanners switch to WAB tailing after
	// catching up durable WAL. Bounded startup recovery keeps this disabled.
	UseWriteAheadBuffer bool
}

// RecoveryMetrics is the metrics of the recovery info.
type RecoveryMetrics struct {
	RecoveryTimeTick uint64
}

// RecoveryStreamBuilder is an interface that is used to build a recovery stream from the WAL.
type RecoveryStreamBuilder interface {
	// WALName returns the name of the WAL.
	WALName() message.WALName

	// Channel returns the channel info of wal.
	Channel() types.PChannelInfo

	// Build builds a recovery stream from the given channel info.
	// The recovery stream will return the messages from the start checkpoint to the end time tick.
	Build(param BuildRecoveryStreamParam) RecoveryStream

	// Return the underlying walimpls.WALImpls.
	RWWALImpls() walimpls.WALImpls
}

// RecoveryStream is an interface that is used to recover the recovery storage from the WAL.
type RecoveryStream interface {
	// Chan returns the channel of the recovery stream.
	// The channel is closed when the recovery stream is done.
	Chan() <-chan message.ImmutableMessage

	// Error should be called after the stream `Chan()` is consumed.
	// It returns the error if the stream is not done.
	// If the stream is full consumed, it returns nil.
	Error() error

	// TxnBuffer returns the uncommitted txn buffer after recovery stream is done.
	// Can be only called the stream is drained and Error() return nil.
	TxnBuffer() *utility.TxnBuffer

	// Close closes the recovery stream.
	Close() error
}

// RecoveryStorage owns WAL recovery state for one pchannel.
type RecoveryStorage interface {
	// Metrics gets the metrics of the recovery storage.
	Metrics() RecoveryMetrics

	// GetDataCheckpoint returns the recovery-owned data checkpoint.
	GetDataCheckpoint(ctx context.Context) *WALCheckpoint

	// VChannelManager returns the PChannel-local vchannel recovery manager.
	VChannelManager() *vchannel.PChannelRecoveryManager

	// Close closes the recovery storage.
	Close()
}
