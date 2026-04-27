package shards

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type ShardManager interface {
	log.WithLogger

	Channel() types.PChannelInfo

	CheckIfCollectionCanBeCreated(collectionID int64) error

	CheckIfCollectionExists(collectionID int64) error

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

	ApplyDelete(msg message.MutableDeleteMessageV1) error

	WaitUntilGrowingSegmentReady(uniquePartitionKey PartitionUniqueKey) (<-chan struct{}, error)

	FlushAndFenceSegmentAllocUntil(collectionID int64, timetick uint64) ([]int64, error)

	FlushAllAndFenceSegmentAllocUntil(timetick uint64) ([]int64, error)

	AsyncFlushSegment(signal utils.SealSegmentSignal)

	// AlterCollection updates collection state and, for schema changes, flushes and
	// fences segment allocation atomically within one critical region.
	// Returns the IDs of flushed segments (non-empty only for schema changes).
	AlterCollection(msg message.MutableAlterCollectionMessageV2) ([]int64, error)

	CheckIfCollectionSchemaVersionMatch(collectionID int64, schemaVersion int32) (int32, error)

	Close()
}
