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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"go.uber.org/zap"
)

const (
	Bucket          = "bucket"
	FailedReason    = "failed_reason"
	MaxPendingCount = 32
	delimiter       = "/"
)

// import task state
type importTaskState struct {
	stateCode    commonpb.ImportState // state code
	segments     []int64              // ID list of generated segments
	rowIDs       []int64              // ID list of auto-generated is for auto-id primary key
	rowCount     int64                // how many rows imported
	failedReason string               // failed reason
}

// importManager manager for import tasks
type importManager struct {
	ctx       context.Context    // reserved
	cancel    context.CancelFunc // reserved
	taskStore kv.MetaKv          // Persistent task info storage.

	pendingTasks []*datapb.ImportTaskInfo         // pending tasks
	workingTasks map[int64]*datapb.ImportTaskInfo // in-progress tasks
	pendingLock  sync.RWMutex                     // lock pending task list
	workingLock  sync.RWMutex                     // lock working task map
	nextTaskID   int64                            // for generating next import task ID
	lastReqID    int64                            // for generating a unique ID for import request

	callImportService func(ctx context.Context, req *datapb.ImportTask) *datapb.ImportTaskResponse
}

// newImportManager helper function to create a importManager
func newImportManager(ctx context.Context, client kv.MetaKv, importService func(ctx context.Context, req *datapb.ImportTask) *datapb.ImportTaskResponse) *importManager {
	ctx, cancel := context.WithCancel(ctx)
	mgr := &importManager{
		ctx:               ctx,
		cancel:            cancel,
		taskStore:         client,
		pendingTasks:      make([]*datapb.ImportTaskInfo, 0, MaxPendingCount), // currently task queue max size is 32
		workingTasks:      make(map[int64]*datapb.ImportTaskInfo),
		pendingLock:       sync.RWMutex{},
		workingLock:       sync.RWMutex{},
		nextTaskID:        0,
		lastReqID:         0,
		callImportService: importService,
	}

	return mgr
}

func (m *importManager) init() error {
	//  Read tasks from etcd and save them as pendingTasks or workingTasks.
	m.load()

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
		log.Debug("import manager send import task", zap.Int64("taskID", task.Id))

		dbTask := &datapb.ImportTask{
			CollectionName: task.GetCollectionId(),
			PartitionName:  task.GetPartitionId(),
			RowBased:       task.GetRowBased(),
			TaskId:         task.GetId(),
			Files:          task.GetFiles(),
			Infos: []*commonpb.KeyValuePair{
				{
					Key:   Bucket,
					Value: task.GetBucket(),
				},
			},
		}

		// call DataCoord.Import()
		resp := m.callImportService(m.ctx, dbTask)
		if resp.Status.ErrorCode == commonpb.ErrorCode_UnexpectedError {
			log.Debug("import task is rejected", zap.Int64("task ID", dbTask.TaskId))
			break
		}
		task.DatanodeId = resp.GetDatanodeId()
		log.Debug("import task is assigned", zap.Int64("task ID", dbTask.TaskId), zap.Int64("datanode id", task.DatanodeId))

		// erase this task from head of pending list if the callImportService succeed
		m.pendingTasks = m.pendingTasks[1:]

		func() {
			m.workingLock.Lock()
			defer m.workingLock.Unlock()

			log.Debug("import task was taken to execute", zap.Int64("task ID", dbTask.TaskId))

			// TODO: Guard nil task state.
			task.State.StateCode = commonpb.ImportState_ImportPending
			m.workingTasks[task.Id] = task
			m.updateImportTask(task)
		}()
	}

	return nil
}

// genReqID generates a unique id for import request, this method has no lock, should only be called by importJob()
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
				newTask := &datapb.ImportTaskInfo{
					Id:           m.nextTaskID,
					RequestId:    reqID,
					CollectionId: req.GetCollectionName(),
					PartitionId:  req.GetPartitionName(),
					Bucket:       bucket,
					RowBased:     req.GetRowBased(),
					Files:        []string{req.GetFiles()[i]},
					CreateTs:     time.Now().Unix(),
					State: &datapb.ImportTaskState{
						StateCode: commonpb.ImportState_ImportPending,
					},
				}

				taskList[i] = newTask.GetId()
				m.nextTaskID++
				m.pendingTasks = append(m.pendingTasks, newTask)
				m.saveImportTask(newTask)
			}
			log.Info("process row-based import request", zap.Int64("reqID", reqID), zap.Any("taskIDs", taskList))
		} else {
			// for column-based, all files is a task
			newTask := &datapb.ImportTaskInfo{
				Id:           m.nextTaskID,
				RequestId:    reqID,
				CollectionId: req.GetCollectionName(),
				PartitionId:  req.GetPartitionName(),
				Bucket:       bucket,
				RowBased:     req.GetRowBased(),
				Files:        req.GetFiles(),
				CreateTs:     time.Now().Unix(),
				State: &datapb.ImportTaskState{
					StateCode: commonpb.ImportState_ImportPending,
				},
			}
			m.nextTaskID++
			m.pendingTasks = append(m.pendingTasks, newTask)
			m.saveImportTask(newTask)
			log.Info("process column-based import request", zap.Int64("reqID", reqID), zap.Int64("taskID", newTask.Id))
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
				v.State.StateCode = state.GetState()
				v.State.Segments = state.GetSegments()
				v.State.RowCount = state.GetRowCount()
				for _, kv := range state.GetInfos() {
					if kv.GetKey() == FailedReason {
						v.State.ErrorMessage = kv.GetValue()
						break
					}
				}
				// Update task in task store.
				m.updateImportTask(v)
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
			if id == m.pendingTasks[i].Id {
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
				resp.State = v.GetState().GetStateCode()
				resp.RowCount = v.GetState().GetRowCount()
				resp.IdList = v.GetState().GetRowIds()
				resp.Infos = append(resp.Infos, &commonpb.KeyValuePair{Key: FailedReason, Value: v.GetState().GetErrorMessage()})

				break
			}
		}
	}()

	if !found {
		log.Debug("import manager get task state failed", zap.Int64("taskID", id))
	}

	return resp
}

// load Loads task info from Etcd when RootCoord (re)starts.
func (m *importManager) load() error {
	log.Info("Import manager starts loading from Etcd")
	_, v, err := m.taskStore.LoadWithPrefix(Params.RootCoordCfg.ImportTaskSubPath)
	if err != nil {
		log.Error("RootCoord Import manager failed to load from Etcd", zap.Error(err))
		return err
	}
	m.workingLock.Lock()
	defer m.workingLock.Unlock()
	m.pendingLock.Lock()
	defer m.pendingLock.Unlock()
	for i := range v {
		ti := &datapb.ImportTaskInfo{}
		if err := proto.Unmarshal([]byte(v[i]), ti); err != nil {
			log.Error("Failed to unmarshal proto", zap.String("taskInfo", v[i]), zap.Error(err))
			// Ignore bad protos.
			continue
		}
		// Put tasks back to pending or working task list, given their import states.
		if ti.GetState().GetStateCode() == commonpb.ImportState_ImportPending {
			log.Info("Task has been reloaded as a pending task", zap.Int64("TaskID", ti.Id))
			m.pendingTasks = append(m.pendingTasks, ti)
		} else {
			log.Info("Task has been reloaded as a working tasks", zap.Int64("TaskID", ti.Id))
			m.workingTasks[ti.Id] = ti
		}
	}
	return nil
}

// saveImportTask signs a lease and saves import task info into Etcd with this lease.
func (m *importManager) saveImportTask(task *datapb.ImportTaskInfo) error {
	log.Info("Saving import task to Etcd", zap.Int64("Task ID", task.Id))
	// TODO: Change default lease time and read it into config, once we figure out a proper value.
	// Sign a lease.
	leaseID, err := m.taskStore.Grant(10800) /*3 hours*/
	if err != nil {
		log.Error("Failed to grant lease from Etcd for data import.", zap.Int64("Task ID", task.Id), zap.Error(err))
		return err
	}
	log.Info("Lease granted for task", zap.Int64("Task ID", task.Id))
	var taskInfo []byte
	if taskInfo, err = proto.Marshal(task); err != nil {
		log.Error("Failed to marshall task proto", zap.Int64("Task ID", task.Id), zap.Error(err))
		return err
	} else if err = m.taskStore.SaveWithLease(BuildImportTaskKey(task.Id), string(taskInfo), leaseID); err != nil {
		log.Error("Failed to save import task info into Etcd", zap.Int64("Task ID", task.Id), zap.Error(err))
		return err
	}
	log.Info("Task info successfully saved.", zap.Int64("Task ID", task.Id))
	return nil
}

// updateImportTask updates the task info in Etcd according to task ID. It won't change the lease on the key.
func (m *importManager) updateImportTask(task *datapb.ImportTaskInfo) error {
	log.Info("Updating import task.", zap.Int64("Task ID", task.Id))
	if taskInfo, err := proto.Marshal(task); err != nil {
		log.Error("Failed to marshall task proto.", zap.Int64("Task ID", task.Id), zap.Error(err))
		return err
	} else if err = m.taskStore.SaveWithIgnoreLease(BuildImportTaskKey(task.Id), string(taskInfo)); err != nil {
		log.Error("Failed to update import task info in Etcd.", zap.Int64("Task ID", task.Id), zap.Error(err))
		return err
	}
	log.Info("Task info successfully updated.", zap.Int64("Task ID", task.Id))
	return nil
}

// BuildImportTaskKey constructs and returns an Etcd key with given task ID.
func BuildImportTaskKey(taskID int64) string {
	return fmt.Sprintf("%s%s%d", Params.RootCoordCfg.ImportTaskSubPath, delimiter, taskID)
}
