package indexnode

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
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

func (i *IndexNode) loadTaskState(ClusterID string, buildID UniqueID) (commonpb.IndexState, bool) {
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	task, ok := i.tasks[key]
	return task.state, ok
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

func (i *IndexNode) storeIndexFilesAndStatistic(ClusterID string, buildID UniqueID, files []string, serializedSize uint64, statistic *indexpb.JobInfo) {
	key := taskKey{ClusterID: ClusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	if info, ok := i.tasks[key]; ok {
		info.indexFiles = files[:]
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
