package shards

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type ShardManager interface {
	mlog.WithLogger

	Channel() types.PChannelInfo

	CheckIfCollectionCanBeCreated(collectionID int64) error

	CheckIfCollectionExists(collectionID int64) error

	// CheckIfVChannelCanBeWritten checks if the named vchannel of the collection
	// still accepts new DML. It returns ErrVChannelFenced if the vchannel has
	// been fenced by shard split, and ErrCollectionNotFound if this pchannel
	// does not hold that vchannel.
	CheckIfVChannelCanBeWritten(collectionID int64, vchannel string) error

	// CheckIfVChannelCanBeCreated checks if the named vchannel can be
	// registered on this pchannel. It returns ErrCollectionExists for an
	// idempotent replay of the same vchannel, and ErrVChannelConflict when
	// another vchannel of the same collection already holds the entry.
	CheckIfVChannelCanBeCreated(collectionID int64, vchannel string) error

	// CheckIfVChannelCanBeDropped checks if the named vchannel may be retired.
	// It returns ErrVChannelNotFenced when this pchannel holds the vchannel and
	// no shard split has fenced it, so a live shard is never torn down.
	CheckIfVChannelCanBeDropped(collectionID int64, vchannel string) error

	// GetSplitFence returns the fence recorded for the named vchannel: T_switch
	// and the split task that placed it. Zero values when the vchannel is
	// unknown or not fenced. The task id is what lets a caller tell its own
	// retry from a concurrent task's fence.
	GetSplitFence(collectionID int64, vchannel string) SplitFence

	// SplitShard marks the vchannel of the collection as splitted (fenced)
	// when a SplitShard message is written into the wal. After it is called,
	// any new DML on the vchannel is rejected forever.
	SplitShard(msg message.ImmutableSplitShardMessageV2)

	CreateCollection(msg message.ImmutableCreateCollectionMessageV1)

	// CreateVChannel registers a shard split target vchannel (the genesis
	// message of the new vchannel) for DML and segment assignment.
	CreateVChannel(msg message.ImmutableCreateVChannelMessageV2)

	// DropVChannel retires one vchannel of a collection on this pchannel, the
	// inverse of CreateVChannel. Guarded by the vchannel name: after the
	// coordinator reclaims a retired source's slot, another vchannel of the same
	// collection may hold this pchannel's entry, and it must not be torn down.
	DropVChannel(msg message.ImmutableDropVChannelMessageV2)

	DropCollection(msg message.ImmutableDropCollectionMessageV1)

	CheckIfPartitionCanBeCreated(uniquePartitionKey PartitionUniqueKey) error

	CheckIfPartitionExists(uniquePartitionKey PartitionUniqueKey) error

	CreatePartition(msg message.ImmutableCreatePartitionMessageV1)

	DropPartition(msg message.ImmutableDropPartitionMessageV1)

	CheckIfSegmentCanBeCreated(uniquePartitionKey PartitionUniqueKey, segmentID int64) error

	CheckIfSegmentCanBeFlushed(uniquePartitionKey PartitionUniqueKey, segmentID int64) error

	CreateSegment(msg message.ImmutableCreateSegmentMessageV2)

	FlushSegment(msg message.ImmutableFlushMessageV2)

	AssignSegment(req *AssignSegmentRequest) (*AssignSegmentResult, error)

	ApplyDelete(msg message.MutableDeleteMessageV1) error

	WaitUntilGrowingSegmentReady(uniquePartitionKey PartitionUniqueKey) (<-chan struct{}, error)

	FlushAndFenceSegmentAllocUntil(collectionID int64, timetick uint64) ([]int64, error)

	FlushAllAndFenceSegmentAllocUntil(timetick uint64) ([]int64, error)

	AsyncFlushSegment(signal utils.SealSegmentSignal)

	// AlterCollection updates collection state and, for schema changes, flushes and
	// fences segment allocation atomically within one critical region.
	// Returns the IDs of flushed segments (non-empty only for schema changes).
	AlterCollection(msg message.MutableAlterCollectionMessageV2) ([]int64, error)

	// CheckWritableAndSchemaVersion answers both questions the insert path asks
	// -- may this vchannel be written, and does the header's schema version
	// match -- under ONE read lock instead of two.
	//
	// The two are asked together on every insert, and taking the lock twice
	// buys nothing: the vchannel-exclusive lock upstream keeps the fence from
	// flipping between them, so the pair was already consistent. This just
	// stops paying for a second acquisition on the hot path.
	CheckWritableAndSchemaVersion(vchannel string, header *message.InsertMessageHeader) (int32, error)

	Close()
}
