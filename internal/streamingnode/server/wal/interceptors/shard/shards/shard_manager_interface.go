package shards

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type ShardManager interface {
	log.WithLogger

	Channel() types.PChannelInfo

	CheckIfCollectionCanBeCreated(collectionID int64) error

	CheckIfCollectionExists(collectionID int64) error

	CreateCollection(msg message.ImmutableCreateCollectionMessageV1)

	DropCollection(msg message.ImmutableDropCollectionMessageV1)

	CheckIfPartitionCanBeCreated(collectionID int64, partitionID int64) error

	CheckIfPartitionExists(collectionID int64, partitionID int64) error

	CreatePartition(msg message.ImmutableCreatePartitionMessageV1)

	DropPartition(msg message.ImmutableDropPartitionMessageV1)

	CheckIfSegmentCanBeCreated(collectionID int64, partitionID int64, segmentID int64) error

	CheckIfSegmentCanBeFlushed(collecionID int64, partitionID int64, segmentID int64) error

	CreateSegment(msg message.ImmutableCreateSegmentMessageV2)

	FlushSegment(msg message.ImmutableFlushMessageV2)

	AssignSegment(req *AssignSegmentRequest) (*AssignSegmentResult, error)

	WaitUntilGrowingSegmentReady(collectionID int64, partitonID int64) (<-chan struct{}, error)

	FlushAndFenceSegmentAllocUntil(collectionID int64, timetick uint64) ([]int64, error)

	AsyncFlushSegment(signal utils.SealSegmentSignal)

	Close()
}
