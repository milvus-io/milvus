package syncmgr

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncManagerOption struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface
	parallelTask int
}

type SyncMeta struct {
	collectionID int64
	partitionID  int64
	segmentID    int64
	channelName  string
	schema       *schemapb.CollectionSchema
	checkpoint   *msgpb.MsgPosition
	tsFrom       typeutil.Timestamp
	tsTo         typeutil.Timestamp

	metacache metacache.MetaCache
}

type SyncManager interface {
	SyncData(ctx context.Context, task *SyncTask) error
	Block(segmentID int64)
	Unblock(segmentID int64)
}

type syncManager struct {
	*keyLockDispatcher[int64]
	chunkManager storage.ChunkManager
	allocator    allocator.Interface
}

func NewSyncManager(parallelTask int, chunkManager storage.ChunkManager, allocator allocator.Interface) (SyncManager, error) {
	if parallelTask < 1 {
		return nil, merr.WrapErrParameterInvalid("positive parallel task number", strconv.FormatInt(int64(parallelTask), 10))
	}
	return &syncManager{
		keyLockDispatcher: newKeyLockDispatcher[int64](parallelTask),
		chunkManager:      chunkManager,
		allocator:         allocator,
	}, nil
}

func (mgr syncManager) SyncData(ctx context.Context, task *SyncTask) error {
	task.WithAllocator(mgr.allocator).WithChunkManager(mgr.chunkManager)

	// make sync for same segment execute in sequence
	// if previous sync task is not finished, block here
	mgr.Submit(task.segmentID, task)

	return nil
}

func (mgr syncManager) Block(segmentID int64) {
	mgr.keyLock.Lock(segmentID)
}

func (mgr syncManager) Unblock(segmentID int64) {
	mgr.keyLock.Unlock(segmentID)
}
