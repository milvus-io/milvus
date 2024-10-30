package syncmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
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
	SyncData(ctx context.Context, task Task, callbacks ...func(error) error) *conc.Future[struct{}]

	TaskStatsJSON() string
}

type syncManager struct {
	*keyLockDispatcher[int64]
	chunkManager storage.ChunkManager

	tasks     *typeutil.ConcurrentMap[string, Task]
	taskStats *expirable.LRU[string, Task]
}

func NewSyncManager(chunkManager storage.ChunkManager) SyncManager {
	params := paramtable.Get()
	initPoolSize := params.DataNodeCfg.MaxParallelSyncMgrTasks.GetAsInt()
	dispatcher := newKeyLockDispatcher[int64](initPoolSize)
	log.Info("sync manager initialized", zap.Int("initPoolSize", initPoolSize))

	syncMgr := &syncManager{
		keyLockDispatcher: dispatcher,
		chunkManager:      chunkManager,
		tasks:             typeutil.NewConcurrentMap[string, Task](),
		taskStats:         expirable.NewLRU[string, Task](512, nil, time.Minute*15),
	}
	// setup config update watcher
	params.Watch(params.DataNodeCfg.MaxParallelSyncMgrTasks.Key, config.NewHandler("datanode.syncmgr.poolsize", syncMgr.resizeHandler))
	return syncMgr
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

func (mgr *syncManager) SyncData(ctx context.Context, task Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	switch t := task.(type) {
	case *SyncTask:
		t.WithChunkManager(mgr.chunkManager)
	}

	return mgr.safeSubmitTask(ctx, task, callbacks...)
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
		if err == nil {
			return nil
		}
		task.HandleError(err)
		return err
	}
	callbacks = append([]func(error) error{handler}, callbacks...)
	log.Info("sync mgr sumbit task with key", zap.Int64("key", key))
	return mgr.Submit(ctx, key, task, callbacks...)
}

func (mgr *syncManager) TaskStatsJSON() string {
	tasks := mgr.taskStats.Values()
	if len(tasks) == 0 {
		return ""
	}

	ret, err := json.Marshal(tasks)
	if err != nil {
		log.Warn("failed to marshal sync task stats", zap.Error(err))
		return ""
	}
	return string(ret)
}
