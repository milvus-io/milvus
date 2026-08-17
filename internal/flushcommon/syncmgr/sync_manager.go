package syncmgr

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

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

// SyncManager is the interface for sync manager.
// it processes the sync tasks inside and changes the meta.
//
//go:generate mockery --name=SyncManager --structname=MockSyncManager --output=./  --filename=mock_sync_manager.go --with-expecter --inpackage
type SyncManager interface {
	// SyncData is the method to submit a sync task. Completion callbacks run
	// before the Future is published and before the task releases its dispatcher
	// admission. A callback must not synchronously submit follow-up work, wait on
	// a same-key Future, or call Close; detach follow-up submission instead.
	SyncData(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error)
	SyncDataWithChunkManager(ctx context.Context, task Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error)

	// Close fences new submissions and shuts down the sync manager. A nil return
	// means every accepted task has run its callbacks, had its result published
	// to its Future, and been accounted for — i.e. no dispatcher work remains.
	// It does NOT mean Future.Done() is already true: the Future is a relay
	// goroutine over that result, so a caller that needs completion must Await.
	// On timeout, running tasks may complete asynchronously.
	Close() error
	TaskStatsJSON() string
}

// SyncTaskAdmission is one node-wide payload slot reserved before a caller
// materializes a task. The caller retains it across whole-task retries: Submit
// may be called repeatedly, and Release ends the payload lifetime. Release is
// idempotent; it must only be called after no accepted attempt can still use the
// task payload.
type SyncTaskAdmission interface {
	Submit(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error)
	Release()
}

// SyncTaskAdmissionReservable is an optional SyncManager capability. Callers
// that build expensive task payloads may reserve capacity first; callers and
// mocks that only implement SyncManager keep the Submit-time admission path.
type SyncTaskAdmissionReservable interface {
	ReserveSyncTask(ctx context.Context) (SyncTaskAdmission, error)
}

type syncManager struct {
	*reorderDispatcher[int64]
	chunkManager storage.ChunkManager

	tasks     *typeutil.ConcurrentMap[string, Task]
	taskStats *expirable.LRU[string, Task]
	handler   config.EventHandler
}

type syncTaskAdmission struct {
	manager *syncManager
	mu      sync.Mutex

	inFlight         bool
	releaseRequested bool
	released         bool
}

func NewSyncManager(chunkManager storage.ChunkManager) SyncManager {
	params := paramtable.Get()
	cpuNum := hardware.GetCPUNum()
	initPoolSize := cpuNum * params.DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.GetAsInt()
	dispatcher := newReorderDispatcher[int64](initPoolSize)
	mlog.Info(context.TODO(), "sync manager initialized",
		mlog.Int("initPoolSize", initPoolSize),
		mlog.Int("cpuNum", cpuNum))

	syncMgr := &syncManager{
		reorderDispatcher: dispatcher,
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
		if err := mgr.resize(newPoolSize); err != nil {
			log.Warn(context.TODO(), "failed to resize datanode syncmgr pool size", mlog.Err(err))
			return
		}
		log.Info(context.TODO(), "sync mgr pool size updated", mlog.Int64("newSize", size))
	}
}

func (mgr *syncManager) SyncData(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if mgr.closeCtx.Err() != nil || mgr.preparePool.IsClosed() {
		return nil, context.Canceled
	}

	task.SetChunkManager(mgr.chunkManager)

	return mgr.safeSubmitTask(ctx, task, callbacks...)
}

func (mgr *syncManager) SyncDataWithChunkManager(ctx context.Context, task Task, chunkManager storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if mgr.closeCtx.Err() != nil || mgr.preparePool.IsClosed() {
		return nil, context.Canceled
	}

	task.SetChunkManager(chunkManager)

	return mgr.safeSubmitTask(ctx, task, callbacks...)
}

func (mgr *syncManager) ReserveSyncTask(ctx context.Context) (SyncTaskAdmission, error) {
	if err := mgr.acquireAdmission(ctx); err != nil {
		return nil, err
	}
	return &syncTaskAdmission{manager: mgr}, nil
}

func (a *syncTaskAdmission) Submit(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	a.mu.Lock()
	if a.released || a.releaseRequested {
		a.mu.Unlock()
		return nil, merr.WrapErrServiceInternalMsg("sync task admission is released")
	}
	if a.inFlight {
		a.mu.Unlock()
		return nil, merr.WrapErrServiceInternalMsg("sync task admission already has an in-flight attempt")
	}
	a.inFlight = true
	a.mu.Unlock()

	// Last in the callback chain: runCallbacks continues after earlier callback
	// panics, so the lease always becomes reusable (or fulfills a pending
	// Release) after the attempt's complete lifecycle has run.
	callbacks = append(callbacks, func(err error) error {
		a.finishAttempt()
		return err
	})
	future, err := a.manager.syncDataReserved(ctx, task, callbacks...)
	if err != nil {
		// Pre-accept rejection has no callback to close the attempt.
		a.finishAttempt()
	}
	return future, err
}

func (a *syncTaskAdmission) Release() {
	a.mu.Lock()
	if a.released || a.releaseRequested {
		a.mu.Unlock()
		return
	}
	if a.inFlight {
		a.releaseRequested = true
		a.mu.Unlock()
		return
	}
	a.released = true
	a.mu.Unlock()
	a.manager.admission.Release()
}

func (a *syncTaskAdmission) finishAttempt() {
	a.mu.Lock()
	if !a.inFlight {
		a.mu.Unlock()
		return
	}
	a.inFlight = false
	shouldRelease := a.releaseRequested && !a.released
	if shouldRelease {
		a.released = true
	}
	a.mu.Unlock()
	if shouldRelease {
		a.manager.admission.Release()
	}
}

// syncDataReserved submits an attempt against a payload admission retained by
// the caller. Rejection does not release it: the owner decides whether the task
// will be retried or has reached a terminal state.
func (mgr *syncManager) syncDataReserved(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mgr.closeCtx.Err() != nil || mgr.preparePool.IsClosed() {
		return nil, context.Canceled
	}

	task.SetChunkManager(mgr.chunkManager)
	return mgr.submitTask(ctx, task, true, callbacks...)
}

// safeSubmitTask registers the task for stats and submits it to the dispatcher,
// which serializes completion per segment.
func (mgr *syncManager) safeSubmitTask(ctx context.Context, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	return mgr.submitTask(ctx, task, false, callbacks...)
}

func (mgr *syncManager) submitTask(
	ctx context.Context,
	task Task,
	reservedAdmission bool,
	callbacks ...func(error) error,
) (*conc.Future[struct{}], error) {
	// The pointer keeps the key unique: one segment can have several admitted
	// tasks sharing a checkpoint timestamp.
	taskKey := fmt.Sprintf("%d-%d-%p", task.SegmentID(), task.Checkpoint().GetTimestamp(), task)

	handler := func(err error) error {
		defer mgr.tasks.Remove(taskKey)
		if err != nil && ClassifySyncError(ctx, err) != SyncCanceled {
			task.HandleError(err)
		}
		return err
	}
	callbacks = append([]func(error) error{handler}, callbacks...)

	onAccepted := func() {
		mgr.tasks.Insert(taskKey, task)
		mgr.taskStats.Add(taskKey, task)
	}
	var future *conc.Future[struct{}]
	var err error
	if reservedAdmission {
		future, err = mgr.submitReserved(ctx, task.SegmentID(), task, onAccepted, callbacks...)
	} else {
		future, err = mgr.submit(ctx, task.SegmentID(), task, onAccepted, callbacks...)
	}
	if err != nil {
		return nil, err
	}
	mlog.Info(ctx, "sync mgr submit task", mlog.FieldSegmentID(task.SegmentID()))
	return future, nil
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
	deadline := time.Now().Add(timeout)

	// Fence submissions and abort queued work first, then shut the pools down,
	// then wait for the accounting of everything that was still running.
	mgr.beginClose()
	err := mgr.releasePools(timeout)
	if err == nil {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		err = mgr.waitClosed(ctx)
		cancel()
	}
	return err
}
