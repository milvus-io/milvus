package shards

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

	// CollectionVChannel returns the vchannel the collection is registered with
	// on this pchannel.
	CollectionVChannel(collectionID int64) (string, bool)

	// FenceForSplit makes vchannel the source of the shard split splitTaskID:
	// it seals every growing segment created before timetick and refuses DML
	// and new segments from then on. It returns the sealed segments. The fence
	// is never removed.
	//
	// A vchannel that the same split already fenced is left as it is, and
	// alreadyFenced is true. It returns ErrCollectionNotFound if the collection
	// is not registered with that vchannel on this pchannel, and ErrShardFenced
	// if another split fenced the vchannel.
	FenceForSplit(collectionID int64, vchannel string, splitTaskID int64, timetick uint64) (segmentIDs []int64, alreadyFenced bool, err error)

	// GetCollectionSchemaSnapshot returns the current immutable schema, or nil
	// when the collection or its schema is absent. Callers may retain the pointer
	// across schema updates and collection removal, but must not mutate it.
	GetCollectionSchemaSnapshot(collectionID int64) *schemapb.CollectionSchema

	CreateCollection(msg message.ImmutableCreateCollectionMessageV1)

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

	// ApplyDelete returns ErrShardFenced, and applies nothing, if the vchannel
	// of the collection is the source of a shard split.
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
	// It returns ErrShardFenced if the vchannel of the collection is the source of a shard split.
	CheckIfCollectionSchemaVersionMatch(header *message.InsertMessageHeader) (int32, error)

	Close()
}
