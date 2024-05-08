package syncmgr

import (
	"context"
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	SyncData(ctx context.Context, task Task) *conc.Future[struct{}]
	// GetEarliestPosition returns the earliest position (normally start position) of the processing sync task of provided channel.
	GetEarliestPosition(channel string) (int64, *msgpb.MsgPosition)
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

	tasks *typeutil.ConcurrentMap[string, Task]
}

func NewSyncManager(chunkManager storage.ChunkManager, allocator allocator.Interface) (SyncManager, error) {
	params := paramtable.Get()
	initPoolSize := params.DataNodeCfg.MaxParallelSyncMgrTasks.GetAsInt()
	if initPoolSize < 1 {
		return nil, merr.WrapErrParameterInvalid("positive parallel task number", strconv.FormatInt(int64(initPoolSize), 10))
	}
	dispatcher := newKeyLockDispatcher[int64](initPoolSize)
	log.Info("sync manager initialized", zap.Int("initPoolSize", initPoolSize))

	syncMgr := &syncManager{
		keyLockDispatcher: dispatcher,
		chunkManager:      chunkManager,
		allocator:         allocator,
		tasks:             typeutil.NewConcurrentMap[string, Task](),
	}
	// setup config update watcher
	params.Watch(params.DataNodeCfg.MaxParallelSyncMgrTasks.Key, config.NewHandler("datanode.syncmgr.poolsize", syncMgr.resizeHandler))

	return syncMgr, nil
}

func (mgr *syncManager) resizeHandler(evt *config.Event) {
	if evt.HasUpdated {
		log := log.Ctx(context.Background()).With(
			zap.String("key", evt.Key),
			zap.String("value", evt.Value),
		)
		size, err := strconv.ParseInt(evt.Value, 10, 64)
		if err != nil {
			log.Warn("failed to parse new datanode syncmgr pool size", zap.Error(err))
			return
		}
		err = mgr.keyLockDispatcher.workerPool.Resize(int(size))
		if err != nil {
			log.Warn("failed to resize datanode syncmgr pool size", zap.String("key", evt.Key), zap.String("value", evt.Value), zap.Error(err))
			return
		}
		log.Info("sync mgr pool size updated", zap.Int64("newSize", size))
	}
}

func (mgr *syncManager) SyncData(ctx context.Context, task Task) *conc.Future[struct{}] {
	switch t := task.(type) {
	case *SyncTask:
		t.WithAllocator(mgr.allocator).WithChunkManager(mgr.chunkManager)
	case *SyncTaskV2:
		t.WithAllocator(mgr.allocator)
	}

	return mgr.safeSubmitTask(task)
}

// safeSubmitTask handles submitting task logic with optimistic target check logic
// when task returns errTargetSegmentNotMatch error
// perform refetch then retry logic
func (mgr *syncManager) safeSubmitTask(task Task) *conc.Future[struct{}] {
	taskKey := fmt.Sprintf("%d-%d", task.SegmentID(), task.Checkpoint().GetTimestamp())
	mgr.tasks.Insert(taskKey, task)
	defer mgr.tasks.Remove(taskKey)

	key, err := task.CalcTargetSegment()
	if err != nil {
		return conc.Go(func() (struct{}, error) { return struct{}{}, err })
	}

	return mgr.submit(key, task)
}

func (mgr *syncManager) submit(key int64, task Task) *conc.Future[struct{}] {
	handler := func(err error) error {
		// unexpected error
		if !errors.Is(err, errTargetSegmentNotMatch) {
			task.HandleError(err)
			return err
		}

		targetID, err := task.CalcTargetSegment()
		// shall not reach, segment meta lost during sync
		if err != nil {
			task.HandleError(err)
			return err
		}
		if targetID == key {
			task.HandleError(merr.WrapErrServiceInternal("recaluated with same key", fmt.Sprint(targetID)))
			return err
		}
		log.Info("task calculated target segment id",
			zap.Int64("targetID", targetID),
			zap.Int64("segmentID", task.SegmentID()),
		)
		return mgr.submit(targetID, task).Err()
	}
	log.Info("sync mgr sumbit task with key", zap.Int64("key", key))
	return mgr.Submit(key, task, handler)
}

func (mgr *syncManager) GetEarliestPosition(channel string) (int64, *msgpb.MsgPosition) {
	var cp *msgpb.MsgPosition
	var segmentID int64
	mgr.tasks.Range(func(_ string, task Task) bool {
		if task.StartPosition() == nil {
			return true
		}
		if task.ChannelName() == channel {
			if cp == nil || task.StartPosition().GetTimestamp() < cp.GetTimestamp() {
				cp = task.StartPosition()
				segmentID = task.SegmentID()
			}
		}
		return true
	})
	return segmentID, cp
}

func (mgr *syncManager) Block(segmentID int64) {
	mgr.keyLock.Lock(segmentID)
}

func (mgr *syncManager) Unblock(segmentID int64) {
	mgr.keyLock.Unlock(segmentID)
}
