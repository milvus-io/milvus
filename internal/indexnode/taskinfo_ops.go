package indexnode

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

func (i *IndexNode) loadOrStoreTask(clusterID, buildID UniqueID, info *taskInfo) *taskInfo {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	key := taskKey{ClusterID: clusterID, BuildID: buildID}
	oldInfo, ok := i.tasks[key]
	if ok {
		return oldInfo
	}
	i.tasks[key] = info
	return nil
}

func (i *IndexNode) loadTaskState(clusterID, buildID UniqueID) (commonpb.IndexState, bool) {
	key := taskKey{ClusterID: clusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	task, ok := i.tasks[key]
	return task.state, ok
}

func (i *IndexNode) storeTaskState(clusterID, buildID UniqueID, state commonpb.IndexState) {
	key := taskKey{ClusterID: clusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	if task, ok := i.tasks[key]; ok {
		task.state = state
	}
}

func (i *IndexNode) foreachTaskInfo(fn func(clusterID, buildID UniqueID, info *taskInfo)) {
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	for key, info := range i.tasks {
		fn(key.ClusterID, key.BuildID, info)
	}
}

func (i *IndexNode) storeIndexFilesAndStatistic(clusterID, buildID UniqueID, files []string, statistic *indexpb.JobInfo) {
	key := taskKey{ClusterID: clusterID, BuildID: buildID}
	i.stateLock.Lock()
	defer i.stateLock.Unlock()
	if info, ok := i.tasks[key]; !ok {
		return
	} else {
		info.indexfiles = files[:]
		info.statistic = proto.Clone(statistic).(*indexpb.JobInfo)
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
