package syncmgr

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
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

// SyncMangger is the interface for sync manager.
// it processes the sync tasks inside and changes the meta.
type SyncManager interface {
	// SyncData is the method to submit sync task.
	SyncData(ctx context.Context, task *SyncTask) *conc.Future[error]
	// GetEarliestPosition returns the earliest position (normally start position) of the processing sync task of provided channel.
	GetEarliestPosition(channel string) *msgpb.MsgPosition
	// Block allows caller to block tasks of provided segment id.
	// normally used by compaction task.
	// if levelzero delta policy is enabled, this shall be an empty operation.
	Block(segmentID int64)
	// Unblock is the reverse method for `Block`.
	Unblock(segmentID int64)
}

type syncManager struct {
	*keyLockDispatcher[int64]
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	tasks *typeutil.ConcurrentMap[string, *SyncTask]
}

func NewSyncManager(parallelTask int, chunkManager storage.ChunkManager, allocator allocator.Interface) (SyncManager, error) {
	if parallelTask < 1 {
		return nil, merr.WrapErrParameterInvalid("positive parallel task number", strconv.FormatInt(int64(parallelTask), 10))
	}
	return &syncManager{
		keyLockDispatcher: newKeyLockDispatcher[int64](parallelTask),
		chunkManager:      chunkManager,
		allocator:         allocator,
		tasks:             typeutil.NewConcurrentMap[string, *SyncTask](),
	}, nil
}

func (mgr syncManager) SyncData(ctx context.Context, task *SyncTask) *conc.Future[error] {
	task.WithAllocator(mgr.allocator).WithChunkManager(mgr.chunkManager)

	taskKey := fmt.Sprintf("%d-%d", task.segmentID, task.checkpoint.GetTimestamp())
	mgr.tasks.Insert(taskKey, task)

	// make sync for same segment execute in sequence
	// if previous sync task is not finished, block here
	return mgr.Submit(task.segmentID, task, func(err error) {
		// remove task from records
		mgr.tasks.Remove(taskKey)
	})
}

func (mgr syncManager) GetEarliestPosition(channel string) *msgpb.MsgPosition {
	var cp *msgpb.MsgPosition
	mgr.tasks.Range(func(_ string, task *SyncTask) bool {
		if task.startPosition == nil {
			return true
		}
		if task.channelName == channel {
			if cp == nil || task.startPosition.GetTimestamp() < cp.GetTimestamp() {
				cp = task.startPosition
			}
		}
		return true
	})
	return cp
}

func (mgr syncManager) Block(segmentID int64) {
	mgr.keyLock.Lock(segmentID)
}

func (mgr syncManager) Unblock(segmentID int64) {
	mgr.keyLock.Unlock(segmentID)
}
