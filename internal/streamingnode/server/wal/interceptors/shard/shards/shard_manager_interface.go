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

	// CheckIfVChannelCanBeWritten checks if the vchannel of the collection
	// still accepts new DML. It returns ErrVChannelFenced if the vchannel
	// has been fenced by shard split.
	CheckIfVChannelCanBeWritten(collectionID int64) error

	// SplitShard marks the vchannel of the collection as splitted (fenced)
	// when a SplitShard message is written into the wal. After it is called,
	// any new DML on the vchannel is rejected forever.
	SplitShard(msg message.ImmutableSplitShardMessageV2)

	CreateCollection(msg message.ImmutableCreateCollectionMessageV1)

	// CreateVChannel registers a shard split target vchannel (the genesis
	// message of the new vchannel) for DML and segment assignment.
	CreateVChannel(msg message.ImmutableCreateVChannelMessageV2)

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

	// CheckIfCollectionSchemaVersionMatch validates insert header schema version against in-memory collection state.
	CheckIfCollectionSchemaVersionMatch(header *message.InsertMessageHeader) (int32, error)

	Close()
}
