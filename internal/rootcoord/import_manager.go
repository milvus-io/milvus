// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	Bucket          = "bucket"
	FailedReason    = "failed_reason"
	MaxPendingCount = 32
)

// import task state
type importTaskState struct {
	stateCode    commonpb.ImportState // state code
	segments     []int64              // id list of generated segments
	rowIDs       []int64              // id list of auto-generated is for auto-id primary key
	rowCount     int64                // how many rows imported
	failedReason string               // failed reason
}

// import task
type importTask struct {
	id         int64           // task id
	request    int64           // request id
	datanode   int64           // datanode id which execute this task
	collection string          // target collection
	partition  string          // target partition
	bucket     string          // target bucket of storage
	rowbased   bool            // row-based or column-based
	files      []string        // import files
	timestamp  int64           // the timestamp of thie task come in
	state      importTaskState // task state
}

// importManager manager for import tasks
type importManager struct {
	ctx     context.Context    // reserved
	cancel  context.CancelFunc // reserved
	etcdCli *clientv3.Client   // etcd to record import tasks

	pendingTasks []*importTask         // pending tasks
	workingTasks map[int64]*importTask // in-progress tasks
	pendingLock  sync.Mutex            // lock pending task list
	workingLock  sync.Mutex            // lock working task map
	nextTaskID   int64                 // for generating next import task id
	lastReqID    int64                 // for generateing a unique id for import request

	callImportService func(ctx context.Context, req *datapb.ImportTask) *datapb.ImportTaskResponse
}

// newImportManager helper function to create a importManager
func newImportManager(ctx context.Context, client *clientv3.Client, importService func(ctx context.Context, req *datapb.ImportTask) *datapb.ImportTaskResponse) *importManager {
	ctx, cancel := context.WithCancel(ctx)
	mgr := &importManager{
		ctx:               ctx,
		cancel:            cancel,
		etcdCli:           client,
		pendingTasks:      make([]*importTask, 0, MaxPendingCount), // currently task queue max size is 32
		workingTasks:      make(map[int64]*importTask),
		pendingLock:       sync.Mutex{},
		workingLock:       sync.Mutex{},
		nextTaskID:        0,
		lastReqID:         0,
		callImportService: importService,
	}

	return mgr
}

func (m *importManager) init() error {
	// TODO: read task list from etcd

	// trigger Import() action to DataCoord
	m.pushTasks()

	return nil
}

func (m *importManager) pushTasks() error {
	m.pendingLock.Lock()
	defer m.pendingLock.Unlock()

	// trigger Import() action to DataCoord
	for {
		if len(m.pendingTasks) == 0 {
			log.Debug("import manger pending task list is empty")
			break
		}

		task := m.pendingTasks[0]
		log.Debug("import manager send import task", zap.Int64("taskID", task.id))

		dbTask := &datapb.ImportTask{
			CollectionName: task.collection,
			PartitionName:  task.partition,
			RowBased:       task.rowbased,
			TaskId:         task.id,
			Files:          task.files,
			Infos: []*commonpb.KeyValuePair{
				{
					Key:   Bucket,
					Value: task.bucket,
				},
			},
		}

		// call DataCoord.Import()
		resp := m.callImportService(m.ctx, dbTask)
		if resp.Status.ErrorCode == commonpb.ErrorCode_UnexpectedError {
			log.Debug("import task is rejected", zap.Int64("task id", dbTask.TaskId))
			break
		}
		task.datanode = resp.DatanodeId
		log.Debug("import task is assigned", zap.Int64("task id", dbTask.TaskId), zap.Int64("datanode id", task.datanode))

		// erase this task from head of pending list if the callImportService succeed
		m.pendingTasks = m.pendingTasks[1:]

		func() {
			m.workingLock.Lock()
			defer m.workingLock.Unlock()

			log.Debug("import task was taken to execute", zap.Int64("task id", dbTask.TaskId))

			task.state.stateCode = commonpb.ImportState_ImportPending
			m.workingTasks[task.id] = task

			// TODO: write this task to etcd
		}()
	}

	return nil
}

// generate an unique id for import request, this method has no lock, should only be called by importJob()
func (m *importManager) genReqID() int64 {
	if m.lastReqID == 0 {
		m.lastReqID = time.Now().Unix()

	} else {
		id := time.Now().Unix()
		if id == m.lastReqID {
			id++
		}
		m.lastReqID = id
	}

	return m.lastReqID
}

func (m *importManager) importJob(req *milvuspb.ImportRequest) *milvuspb.ImportResponse {
	if req == nil || len(req.Files) == 0 {
		return &milvuspb.ImportResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "import request is empty",
			},
		}
	}

	if m.callImportService == nil {
		return &milvuspb.ImportResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "import service is not available",
			},
		}
	}

	resp := &milvuspb.ImportResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	log.Debug("import manager receive request", zap.String("collection", req.GetCollectionName()))
	func() {
		m.pendingLock.Lock()
		defer m.pendingLock.Unlock()

		capacity := cap(m.pendingTasks)
		length := len(m.pendingTasks)

		taskCount := 1
		if req.RowBased {
			taskCount = len(req.Files)
		}

		// task queue size has a limit, return error if import request contains too many data files
		if capacity-length < taskCount {
			resp.Status = &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IllegalArgument,
				Reason:    "Import task queue max size is " + strconv.Itoa(capacity) + ", currently there are " + strconv.Itoa(length) + " tasks is pending. Not able to execute this request with " + strconv.Itoa(taskCount) + " tasks.",
			}
			return
		}

		bucket := ""
		for _, kv := range req.Options {
			if kv.Key == Bucket {
				bucket = kv.Value
				break
			}
		}

		reqID := m.genReqID()
		// convert import request to import tasks
		if req.RowBased {
			// for row-based, each file is a task
			taskList := make([]int64, len(req.Files))
			for i := 0; i < len(req.Files); i++ {
				newTask := &importTask{
					id:         m.nextTaskID,
					request:    reqID,
					collection: req.CollectionName,
					partition:  req.PartitionName,
					bucket:     bucket,
					rowbased:   req.RowBased,
					files:      []string{req.Files[i]},
					timestamp:  time.Now().Unix(),
				}

				taskList[i] = newTask.id
				m.nextTaskID++
				m.pendingTasks = append(m.pendingTasks, newTask)
			}
			log.Debug("process row-based import request", zap.Int64("reqID", reqID), zap.Any("taskIDs", taskList))
		} else {
			// for column-based, all files is a task
			newTask := &importTask{
				id:         m.nextTaskID,
				request:    reqID,
				collection: req.CollectionName,
				partition:  req.PartitionName,
				bucket:     bucket,
				rowbased:   req.RowBased,
				files:      req.Files,
				timestamp:  time.Now().Unix(),
			}
			m.nextTaskID++
			m.pendingTasks = append(m.pendingTasks, newTask)
			log.Debug("process row-based import request", zap.Int64("reqID", reqID), zap.Int64("taskID", newTask.id))
		}
	}()

	m.pushTasks()

	return resp
}

func (m *importManager) updateTaskState(state *rootcoordpb.ImportResult) error {
	if state == nil {
		return errors.New("import task state is nil")
	}

	log.Debug("import manager update task state", zap.Int64("taskID", state.GetTaskId()))

	found := false
	func() {
		m.workingLock.Lock()
		defer m.workingLock.Unlock()

		for k, v := range m.workingTasks {
			if state.TaskId == k {
				found = true
				v.state.stateCode = state.State
				v.state.segments = state.Segments
				v.state.rowCount = state.RowCount
				for _, kv := range state.Infos {
					if kv.Key == FailedReason {
						v.state.failedReason = kv.Value
						break
					}
				}
			}
		}
	}()

	if !found {
		log.Debug("import manager update task state failed", zap.Int64("taskID", state.GetTaskId()))
		return errors.New("failed to update import task, id not found: " + strconv.FormatInt(state.TaskId, 10))
	}
	return nil
}

func (m *importManager) getTaskState(id int64) *milvuspb.GetImportStateResponse {
	resp := &milvuspb.GetImportStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "import task id doesn't exist",
		},
	}

	log.Debug("import manager get task state", zap.Int64("taskID", id))

	found := false
	func() {
		m.pendingLock.Lock()
		defer m.pendingLock.Unlock()

		for i := 0; i < len(m.pendingTasks); i++ {
			if id == m.pendingTasks[i].id {
				resp.Status = &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				}
				resp.State = commonpb.ImportState_ImportPending
				found = true
				break
			}
		}
	}()

	if found {
		return resp
	}

	func() {
		m.workingLock.Lock()
		defer m.workingLock.Unlock()

		for k, v := range m.workingTasks {
			if id == k {
				found = true
				resp.Status = &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				}
				resp.State = v.state.stateCode
				resp.RowCount = v.state.rowCount
				resp.IdList = v.state.rowIDs
				resp.Infos = append(resp.Infos, &commonpb.KeyValuePair{Key: FailedReason, Value: v.state.failedReason})

				break
			}
		}
	}()

	if !found {
		log.Debug("import manager get task state failed", zap.Int64("taskID", id))
	}

	return resp
}
