package syncmgr

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

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

// SyncManager is the interface for sync manager.
// it processes the sync tasks inside and changes the meta.
//
//go:generate mockery --name=SyncManager --structname=MockSyncManager --output=./  --filename=mock_sync_manager.go --with-expecter --inpackage
type SyncManager interface {
	// SyncData is the method to submit sync task.
	SyncData(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error)

	// Close waits for the task to finish and then shuts down the sync manager.
	Close() error
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

func (mgr *syncManager) SyncData(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if mgr.workerPool.IsClosed() {
		return nil, fmt.Errorf("sync manager is closed")
	}

	switch t := task.(type) {
	case *SyncTask:
		t.WithAllocator(mgr.allocator).WithChunkManager(mgr.chunkManager)
	case *SyncTaskV2:
		t.WithAllocator(mgr.allocator)
	}

	return mgr.safeSubmitTask(task, callbacks...), nil
}

// safeSubmitTask submits task to SyncManager
func (mgr *syncManager) safeSubmitTask(task Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	taskKey := fmt.Sprintf("%d-%d", task.SegmentID(), task.Checkpoint().GetTimestamp())
	mgr.tasks.Insert(taskKey, task)

	key := task.SegmentID()
	return mgr.submit(key, task, callbacks...)
}

func (mgr *syncManager) submit(key int64, task Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	handler := func(err error) error {
		taskKey := fmt.Sprintf("%d-%d", task.SegmentID(), task.Checkpoint().GetTimestamp())
		defer func() {
			mgr.tasks.Remove(taskKey)
		}()
		if err == nil {
			return nil
		}
		task.HandleError(err)
		return err
	}
	callbacks = append([]func(error) error{handler}, callbacks...)
	log.Info("sync mgr sumbit task with key", zap.Int64("key", key))
	return mgr.Submit(key, task, callbacks...)
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

func (mgr *syncManager) Close() error {
	timeout := paramtable.Get().CommonCfg.SyncTaskPoolReleaseTimeoutSeconds.GetAsDuration(time.Second)
	return mgr.workerPool.ReleaseTimeout(timeout)
}
