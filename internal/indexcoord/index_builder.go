package indexcoord

import (
	"context"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"go.uber.org/zap"
)

type indexBuilder struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg               sync.WaitGroup
	taskMutex        sync.RWMutex
	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks  map[int64]indexTaskState
	notify chan bool

	ic *IndexCoord

	meta *metaTable
}

func newIndexBuilder(ctx context.Context, ic *IndexCoord, metaTable *metaTable, aliveNodes []UniqueID) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:              ctx,
		cancel:           cancel,
		meta:             metaTable,
		ic:               ic,
		tasks:            make(map[int64]indexTaskState, 1024),
		notify:           make(chan bool, 1024),
		scheduleDuration: time.Second * 10,
	}
	ib.reloadFromKV(aliveNodes)
	return ib
}

func (ib *indexBuilder) Start() {
	ib.wg.Add(1)
	go ib.schedule()
}

func (ib *indexBuilder) Stop() {
	ib.cancel()
	close(ib.notify)
	ib.wg.Wait()
}

func (ib *indexBuilder) reloadFromKV(aliveNodes []UniqueID) {
	metas := ib.meta.GetAllIndexMeta()
	for build, indexMeta := range metas {
		// deleted, need to release lock and clean meta
		if indexMeta.MarkDeleted {
			if indexMeta.NodeID != 0 {
				ib.tasks[build] = indexTaskDeleted
			}
		} else if indexMeta.State == commonpb.IndexState_Unissued && indexMeta.NodeID == 0 {
			// unissued, need to acquire lock and assign task
			ib.tasks[build] = indexTaskInit
		} else if indexMeta.State == commonpb.IndexState_Unissued && indexMeta.NodeID != 0 {
			// retry, need to release lock and reassign task
			// need to release reference lock
			ib.tasks[build] = indexTaskRetry
		} else if indexMeta.State == commonpb.IndexState_InProgress {
			// need to check IndexNode is still alive.
			alive := false
			for _, nodeID := range aliveNodes {
				if nodeID == indexMeta.NodeID {
					alive = true
					break
				}
			}
			if !alive {
				// IndexNode is down, need to retry
				ib.tasks[build] = indexTaskRetry
			} else {
				// in_progress, nothing to do
				ib.tasks[build] = indexTaskInProgress
			}
		} else if indexMeta.State == commonpb.IndexState_Finished || indexMeta.State == commonpb.IndexState_Failed {
			if indexMeta.NodeID != 0 {
				// task is done, but the lock has not been released, need to release.
				ib.tasks[build] = indexTaskDone
			}
			// else: task is done, and lock has been released, no need to add to index builder.
		}
	}
}

func (ib *indexBuilder) enqueue(buildID UniqueID) {
	// notify
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()
	ib.tasks[buildID] = indexTaskInit
	// why use false?
	ib.notify <- false
}

func (ib *indexBuilder) schedule() {
	// receive notify
	// time ticker
	defer ib.wg.Done()
	ticker := time.NewTicker(ib.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ib.ctx.Done():
			log.Warn("index builder ctx done")
			return
		case _, ok := <-ib.notify:
			if ok {
				ib.taskMutex.Lock()
				log.Info("index builder task schedule", zap.Int("task num", len(ib.tasks)))
				for buildID := range ib.tasks {
					ib.process(buildID)
				}
				ib.taskMutex.Unlock()
			}
		// !ok means indexBuild is closed.
		case <-ticker.C:
			ib.taskMutex.Lock()
			for buildID := range ib.tasks {
				ib.process(buildID)
			}
			ib.taskMutex.Unlock()
		}
	}
}

func (ib *indexBuilder) process(buildID UniqueID) {
	state := ib.tasks[buildID]
	log.Info("index task is processing", zap.Int64("buildID", buildID), zap.String("task state", state.String()))
	meta, exist := ib.meta.GetMeta(buildID)

	switch state {
	case indexTaskInit:
		// peek client
		// if all IndexNodes are executing task, wait for one of them to finish the task.
		nodeID, client := ib.ic.nodeManager.PeekClient(meta)
		if client == nil {
			log.Error("index builder peek client error, there is no available")
			return
		}
		// update version and set nodeID
		if err := ib.meta.UpdateVersion(buildID, nodeID); err != nil {
			log.Error("index builder update index version failed", zap.Int64("build", buildID), zap.Error(err))
			return
		}

		// acquire lock
		if err := ib.ic.tryAcquireSegmentReferLock(ib.ctx, buildID, nodeID, []UniqueID{meta.indexMeta.Req.SegmentID}); err != nil {
			log.Error("index builder acquire segment reference lock failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[buildID] = indexTaskRetry
			return
		}
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: buildID,
			IndexName:    meta.indexMeta.Req.IndexName,
			IndexID:      meta.indexMeta.Req.IndexID,
			Version:      meta.indexMeta.IndexVersion + 1,
			MetaPath:     path.Join(indexFilePrefix, strconv.FormatInt(buildID, 10)),
			DataPaths:    meta.indexMeta.Req.DataPaths,
			TypeParams:   meta.indexMeta.Req.TypeParams,
			IndexParams:  meta.indexMeta.Req.IndexParams,
		}
		if err := ib.ic.assignTask(client, req); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Error("index builder assign task to IndexNode failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[buildID] = indexTaskRetry
			return
		}
		// update index meta state to InProgress
		if err := ib.meta.BuildIndex(buildID); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Error("index builder update index meta to InProgress failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[buildID] = indexTaskRetry
			return
		}
		ib.tasks[buildID] = indexTaskInProgress

	case indexTaskDone:
		if err := ib.releaseLockAndResetNode(buildID, meta.indexMeta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return
		}
		delete(ib.tasks, buildID)
	case indexTaskRetry:
		if err := ib.releaseLockAndResetTask(buildID, meta.indexMeta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return
		}
		ib.tasks[buildID] = indexTaskInit
		ib.notify <- false

	case indexTaskDeleted:
		if exist && meta.indexMeta.NodeID != 0 {
			if err := ib.releaseLockAndResetNode(buildID, meta.indexMeta.NodeID); err != nil {
				// release lock failed, no need to modify state, wait to retry
				log.Error("index builder try to release reference lock failed", zap.Error(err))
				return
			}
		}
		// reset nodeID success, remove task.
		delete(ib.tasks, buildID)
	}
}

func (ib *indexBuilder) releaseLockAndResetNode(buildID UniqueID, nodeID UniqueID) error {
	log.Info("release segment reference lock and reset nodeID", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
		// release lock failed, no need to modify state, wait to retry
		log.Error("index builder try to release reference lock failed", zap.Error(err))
		return err
	}
	if err := ib.meta.ResetNodeID(buildID); err != nil {
		log.Error("index builder try to reset nodeID failed", zap.Error(err))
		return err
	}
	log.Info("release segment reference lock and reset nodeID success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) releaseLockAndResetTask(buildID UniqueID, nodeID UniqueID) error {
	log.Info("release segment reference lock and reset task", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	if nodeID != 0 {
		if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return err
		}
	}
	if err := ib.meta.ResetMeta(buildID); err != nil {
		log.Error("index builder try to reset task failed", zap.Error(err))
		return err
	}
	log.Info("release segment reference lock and reset task success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) updateStateByMeta(meta *indexpb.IndexMeta) {
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()

	state, ok := ib.tasks[meta.IndexBuildID]
	if !ok {
		log.Warn("index task has been processed", zap.Int64("buildId", meta.IndexBuildID), zap.Any("meta", meta))
		// no need to return error, this task must have been deleted.
		return
	}

	if meta.State == commonpb.IndexState_Finished || meta.State == commonpb.IndexState_Failed {
		ib.tasks[meta.IndexBuildID] = indexTaskDone
		ib.notify <- false
		log.Info("this task has been finished", zap.Int64("buildID", meta.IndexBuildID),
			zap.String("original state", state.String()), zap.String("finish or failed", meta.State.String()))
		return
	}

	// index state must be Unissued and NodeID is not zero
	ib.tasks[meta.IndexBuildID] = indexTaskRetry
	log.Info("this task need to retry", zap.Int64("buildID", meta.IndexBuildID),
		zap.String("original state", state.String()), zap.String("index state", meta.State.String()),
		zap.Int64("original nodeID", meta.NodeID))
	ib.notify <- false
}

func (ib *indexBuilder) markTaskAsDeleted(buildID UniqueID) {
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()

	if _, ok := ib.tasks[buildID]; ok {
		ib.tasks[buildID] = indexTaskDeleted
	}
	ib.notify <- false
}

func (ib *indexBuilder) nodeDown(nodeID UniqueID) {
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()
	metas := ib.meta.GetMetasByNodeID(nodeID)

	for _, meta := range metas {
		if ib.tasks[meta.indexMeta.IndexBuildID] != indexTaskDone {
			ib.tasks[meta.indexMeta.IndexBuildID] = indexTaskRetry
		}
	}
	ib.notify <- false
}

func (ib *indexBuilder) hasTask(buildID UniqueID) bool {
	ib.taskMutex.RLock()
	defer ib.taskMutex.RUnlock()

	_, ok := ib.tasks[buildID]
	return ok
}
