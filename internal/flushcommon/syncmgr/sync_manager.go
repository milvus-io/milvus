package syncmgr

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	SyncDataWithChunkManager(ctx context.Context, task Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error)

	// Close waits for the task to finish and then shuts down the sync manager.
	Close() error
	TaskStatsJSON() string
}

type syncManager struct {
	*keyLockDispatcher[int64]
	chunkManager storage.ChunkManager

	tasks     *typeutil.ConcurrentMap[string, Task]
	taskStats *expirable.LRU[string, Task]
	handler   config.EventHandler
}

func NewSyncManager(chunkManager storage.ChunkManager) SyncManager {
	params := paramtable.Get()
	cpuNum := hardware.GetCPUNum()
	initPoolSize := cpuNum * params.DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.GetAsInt()
	dispatcher := newKeyLockDispatcher[int64](initPoolSize)
	mlog.Info(context.TODO(), "sync manager initialized", mlog.Int("initPoolSize", initPoolSize), mlog.Int("cpuNum", cpuNum))

	syncMgr := &syncManager{
		keyLockDispatcher: dispatcher,
		chunkManager:      chunkManager,
		tasks:             typeutil.NewConcurrentMap[string, Task](),
		taskStats:         expirable.NewLRU[string, Task](64, nil, time.Minute*15),
	}
	// setup config update watcher
	handler := config.NewHandler("datanode.syncmgr.poolsize", syncMgr.resizeHandler)
	syncMgr.handler = handler
	params.Watch(params.DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.Key, handler)
	return syncMgr
}

func (mgr *syncManager) resizeHandler(evt *config.Event) {
	if evt.HasUpdated {
		log := mlog.With(
			mlog.String("key", evt.Key),
			mlog.String("value", evt.Value),
		)
		cpuNum := hardware.GetCPUNum()
		size, err := strconv.ParseInt(evt.Value, 10, 64)
		if err != nil {
			log.Warn(context.TODO(), "failed to parse new datanode syncmgr pool size", mlog.Err(err))
			return
		}
		newPoolSize := cpuNum * int(size)
		err = mgr.workerPool.Resize(newPoolSize)
		if err != nil {
			log.Warn(context.TODO(), "failed to resize datanode syncmgr pool size", mlog.String("key", evt.Key), mlog.String("value", evt.Value), mlog.Err(err))
			return
		}
		semCap := newPoolSize * 2
		if semCap < 4 {
			semCap = 4
		}
		mgr.SetSemaphoreCapacity(semCap)
		log.Info(context.TODO(), "sync mgr pool size updated", mlog.Int64("newSize", size), mlog.Int("semaphoreCapacity", semCap))
	}
}

func (mgr *syncManager) SyncData(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if mgr.workerPool.IsClosed() {
		return nil, merr.WrapErrServiceInternalMsg("sync manager is closed")
	}

	switch t := task.(type) {
	case *SyncTask:
		t.WithChunkManager(mgr.chunkManager)
	case *GrowingSourceSyncTask:
		t.WithChunkManager(mgr.chunkManager)
	}

	return mgr.safeSubmitTask(ctx, task, callbacks...), nil
}

func (mgr *syncManager) SyncDataWithChunkManager(ctx context.Context, task Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if mgr.workerPool.IsClosed() {
		return nil, merr.WrapErrServiceInternalMsg("sync manager is closed")
	}

	switch t := task.(type) {
	case *SyncTask:
		t.WithChunkManager(chunkManager)
	case *GrowingSourceSyncTask:
		t.WithChunkManager(chunkManager)
	}

	return mgr.safeSubmitTask(ctx, task, callbacks...), nil
}

// safeSubmitTask submits task to SyncManager
func (mgr *syncManager) safeSubmitTask(ctx context.Context, task Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	taskKey := fmt.Sprintf("%d-%d", task.SegmentID(), task.Checkpoint().GetTimestamp())
	mgr.tasks.Insert(taskKey, task)
	mgr.taskStats.Add(taskKey, task)

	key := task.SegmentID()
	return mgr.submit(ctx, key, task, callbacks...)
}

func (mgr *syncManager) submit(ctx context.Context, key int64, task Task, callbacks ...func(error) error) *conc.Future[struct{}] {
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
	callbacks = append(callbacks, handler)
	mlog.Info(ctx, "sync mgr sumbit task with key", mlog.Int64("key", key))

	return mgr.Submit(ctx, key, task, callbacks...)
}

func (mgr *syncManager) TaskStatsJSON() string {
	tasks := mgr.taskStats.Values()
	if len(tasks) == 0 {
		return ""
	}

	ret, err := json.Marshal(tasks)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to marshal sync task stats", mlog.Err(err))
		return ""
	}
	return string(ret)
}

func (mgr *syncManager) Close() error {
	paramtable.Get().Unwatch(paramtable.Get().DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.Key, mgr.handler)
	timeout := paramtable.Get().CommonCfg.SyncTaskPoolReleaseTimeoutSeconds.GetAsDuration(time.Second)
	err := mgr.workerPool.ReleaseTimeout(timeout)
	// Drain all remaining queued tasks that were never dispatched.
	mgr.keyLockDispatcher.Close()
	return err
}
