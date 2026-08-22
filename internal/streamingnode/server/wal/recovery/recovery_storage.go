package recovery

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

type WALCheckpoint = utility.WALCheckpoint

// RecoverySnapshot is the snapshot of the recovery info.
type RecoverySnapshot struct {
	WritePathRecovery *moduleapi.WritePathRecoveryModuleSnapshot
	// Checkpoint is the in-memory completed frontier after bounded startup
	// replay. It fences write-path recovery, but is not catalog-published until
	// the background persistence transaction stores all required snapshots.
	Checkpoint      *WALCheckpoint
	PChannelControl *streamingpb.PChannelRecoveryControlMeta
	TxnBuffer       *utility.TxnBuffer
}

type dirtyPersistSnapshot struct {
	Checkpoint        *WALCheckpoint
	LogicalEndOffset  uint64
	CheckpointDirty   bool
	PChannelControl   *streamingpb.PChannelRecoveryControlMeta
	ControlDirty      bool
	ModuleDirtySnaps  []moduleapi.DirtySnapshot
	SalvageCheckpoint *utility.ReplicateCheckpoint
}

func clonePChannelControl(control *streamingpb.PChannelRecoveryControlMeta) *streamingpb.PChannelRecoveryControlMeta {
	if control == nil {
		return &streamingpb.PChannelRecoveryControlMeta{}
	}
	return proto.Clone(control).(*streamingpb.PChannelRecoveryControlMeta)
}

type BuildRecoveryStreamParam struct {
	StartCheckpoint message.MessageID
	StartAfter      bool
	EndTimeTick     uint64
	// UseWriteAheadBuffer lets unbounded live scanners switch to WAB tailing after
	// catching up durable WAL. Bounded startup recovery keeps this disabled.
	UseWriteAheadBuffer bool
}

// RecoveryMetrics is the metrics of the recovery info.
type RecoveryMetrics struct {
	RecoveryTimeTick  uint64
	RecoveryTailBytes uint64
	BlockingBytes     uint64
	PublishLagBytes   uint64
}

// RecoveryTailRateLimiter is the WAL append-pressure surface used by
// RecoveryStorage. AdaptiveRateLimitController satisfies this interface.
type RecoveryTailRateLimiter interface {
	EnterSlowdownMode(ratelimit.SlowdownChecker)
	EnterRejectMode()
	EnterRecoveryMode()
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

	// GetCheckpoint returns the latest global checkpoint published to the
	// catalog. Every component snapshot required by this point is already
	// visible when it is returned.
	GetCheckpoint(ctx context.Context) *WALCheckpoint

	// VChannelManager returns the PChannel-local vchannel recovery manager.
	VChannelManager() *vchannel.PChannelRecoveryManager

	// Close closes the recovery storage.
	Close()
}
