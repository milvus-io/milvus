package indexnode

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

func (i *IndexNode) loadOrStoreTask(ClusterID string, buildID UniqueID, info *taskInfo) *taskInfo {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	oldInfo, ok := i.tasks[key]
	if ok {
		return oldInfo
	}
	i.tasks[key] = info
	return nil
}

func (i *IndexNode) loadTaskState(ClusterID string, buildID UniqueID) commonpb.IndexState {
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	task, ok := i.tasks[key]
	if !ok {
		return commonpb.IndexState_IndexStateNone
	}
	return task.state
}

func (i *IndexNode) storeTaskState(ClusterID string, buildID UniqueID, state commonpb.IndexState, failReason string) {
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	if task, ok := i.tasks[key]; ok {
		log.Debug("IndexNode store task state", zap.String("clusterID", ClusterID), zap.Int64("buildID", buildID),
			zap.String("state", state.String()), zap.String("fail reason", failReason))
		task.state = state
		task.failReason = failReason
	}
}

func (i *IndexNode) foreachTaskInfo(fn func(ClusterID string, buildID UniqueID, info *taskInfo)) {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	for key, info := range i.tasks {
		fn(key.ClusterID, key.BuildID, info)
	}
}

func (i *IndexNode) storeIndexFilesAndStatistic(ClusterID string, buildID UniqueID, fileKeys []string, serializedSize uint64, statistic *indexpb.JobInfo) {
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	if info, ok := i.tasks[key]; ok {
		info.fileKeys = common.CloneStringList(fileKeys)
		info.serializedSize = serializedSize
		info.statistic = proto.Clone(statistic).(*indexpb.JobInfo)
		return
	}
}

func (i *IndexNode) deleteTaskInfos(keys []taskKey) []*taskInfo {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	deleted := make([]*taskInfo, 0, len(keys))
	for _, key := range keys {
		info, ok := i.tasks[key]
		if ok {
			deleted = append(deleted, info)
			delete(i.tasks, key)
		}
	}
	return deleted
}

func (i *IndexNode) deleteAllTasks() []*taskInfo {
	i.stateLock.Lock()
	deletedTasks := i.tasks
	i.tasks = make(map[taskKey]*taskInfo)
	i.stateLock.Unlock()

	deleted := make([]*taskInfo, 0, len(deletedTasks))
	for _, info := range deletedTasks {
		deleted = append(deleted, info)
	}
	return deleted
}

func (i *IndexNode) hasInProgressTask() bool {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	for _, info := range i.tasks {
		if info.state == commonpb.IndexState_InProgress {
			return true
		}
	}
	return false
}

func (i *IndexNode) waitTaskFinish() {
	if !i.hasInProgressTask() {
		return
	}

	gracefulTimeout := Params.IndexNodeCfg.GracefulStopTimeout
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(i.loopCtx, gracefulTimeout.GetAsDuration(time.Second))
	defer cancel()
	for {
		select {
		case <-ticker.C:
			if !i.hasInProgressTask() {
				return
			}
		case <-timeoutCtx.Done():
			log.Warn("timeout, the index node has some progress task")
			for _, info := range i.tasks {
				if info.state == commonpb.IndexState_InProgress {
					log.Warn("progress task", zap.Any("info", info))
				}
			}
			return
		}
	}
}
