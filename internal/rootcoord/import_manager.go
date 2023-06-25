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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	delimiter = "/"
)

// checkPendingTasksInterval is the default interval to check and send out pending tasks,
// default 60*1000 milliseconds (1 minute).
var checkPendingTasksInterval = 60 * 1000

// cleanUpLoopInterval is the default interval to (1) loop through all in memory tasks and expire old ones and (2) loop
// through all failed import tasks, and mark segments created by these tasks as `dropped`.
// default 5*60*1000 milliseconds (5 minutes)
var cleanUpLoopInterval = 5 * 60 * 1000

// flipPersistedTaskInterval is the default interval to loop through tasks and check if their states needs to be
// flipped/updated from `ImportPersisted` to `ImportCompleted`.
// default 2 * 1000 milliseconds (2 seconds)
// TODO: Make this configurable.
var flipPersistedTaskInterval = 2 * 1000

// importManager manager for import tasks
type importManager struct {
	ctx       context.Context // reserved
	taskStore kv.TxnKV        // Persistent task info storage.
	busyNodes map[int64]int64 // Set of all current working DataNode IDs and related task create timestamp.

	// TODO: Make pendingTask a map to improve look up performance.
	pendingTasks  []*datapb.ImportTaskInfo         // pending tasks
	workingTasks  map[int64]*datapb.ImportTaskInfo // in-progress tasks
	pendingLock   sync.RWMutex                     // lock pending task list
	workingLock   sync.RWMutex                     // lock working task map
	busyNodesLock sync.RWMutex                     // lock for working nodes.
	lastReqID     int64                            // for generating a unique ID for import request

	startOnce sync.Once

	idAllocator               func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error)
	callImportService         func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)
	getCollectionName         func(dbName string, collID, partitionID typeutil.UniqueID) (string, string, error)
	callGetSegmentStates      func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)
	callUnsetIsImportingState func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error)
}

// newImportManager helper function to create a importManager
func newImportManager(ctx context.Context, client kv.TxnKV,
	idAlloc func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error),
	importService func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error),
	getSegmentStates func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error),
	getCollectionName func(dbName string, collID, partitionID typeutil.UniqueID) (string, string, error),
	unsetIsImportingState func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error)) *importManager {
	mgr := &importManager{
		ctx:                       ctx,
		taskStore:                 client,
		pendingTasks:              make([]*datapb.ImportTaskInfo, 0, Params.RootCoordCfg.ImportMaxPendingTaskCount.GetAsInt()), // currently task queue max size is 32
		workingTasks:              make(map[int64]*datapb.ImportTaskInfo),
		busyNodes:                 make(map[int64]int64),
		pendingLock:               sync.RWMutex{},
		workingLock:               sync.RWMutex{},
		busyNodesLock:             sync.RWMutex{},
		lastReqID:                 0,
		idAllocator:               idAlloc,
		callImportService:         importService,
		callGetSegmentStates:      getSegmentStates,
		getCollectionName:         getCollectionName,
		callUnsetIsImportingState: unsetIsImportingState,
	}
	return mgr
}

func (m *importManager) init(ctx context.Context) {
	m.startOnce.Do(func() {
		// Read tasks from Etcd and save them as pending tasks and mark them as failed.
		if _, err := m.loadFromTaskStore(true); err != nil {
			log.Error("importManager init failed, read tasks from Etcd failed, about to panic")
			panic(err)
		}
		// Send out tasks to dataCoord.
		if err := m.sendOutTasks(ctx); err != nil {
			log.Error("importManager init failed, send out tasks to dataCoord failed")
		}
	})
}

// sendOutTasksLoop periodically calls `sendOutTasks` to process left over pending tasks.
func (m *importManager) sendOutTasksLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Duration(checkPendingTasksInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			log.Debug("import manager context done, exit check sendOutTasksLoop")
			return
		case <-ticker.C:
			if err := m.sendOutTasks(m.ctx); err != nil {
				log.Error("importManager sendOutTasksLoop fail to send out tasks")
			}
		}
	}
}

// flipTaskStateLoop periodically calls `flipTaskState` to check if states of the tasks need to be updated.
func (m *importManager) flipTaskStateLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	flipPersistedTicker := time.NewTicker(time.Duration(flipPersistedTaskInterval) * time.Millisecond)
	defer flipPersistedTicker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			log.Debug("import manager context done, exit check flipTaskStateLoop")
			return
		case <-flipPersistedTicker.C:
			// log.Debug("start trying to flip ImportPersisted task")
			if err := m.loadAndFlipPersistedTasks(m.ctx); err != nil {
				log.Error("failed to flip ImportPersisted task", zap.Error(err))
			}
		}
	}
}

// cleanupLoop starts a loop that checks and expires old tasks every `cleanUpLoopInterval` seconds.
// There are two types of tasks to clean up:
// (1) pending tasks or working tasks that existed for over `ImportTaskExpiration` seconds, these tasks will be
// removed from memory.
// (2) any import tasks that has been created over `ImportTaskRetention` seconds ago, these tasks will be removed from Etcd.
// cleanupLoop also periodically calls removeBadImportSegments to remove bad import segments.
func (m *importManager) cleanupLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Duration(cleanUpLoopInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			log.Debug("(in cleanupLoop) import manager context done, exit cleanupLoop")
			return
		case <-ticker.C:
			log.Debug("(in cleanupLoop) trying to expire old tasks from memory and Etcd")
			m.expireOldTasksFromMem()
			m.expireOldTasksFromEtcd()
			log.Debug("(in cleanupLoop) start removing bad import segments")
			m.removeBadImportSegments(m.ctx)
			log.Debug("(in cleanupLoop) start cleaning hanging busy DataNode")
			m.releaseHangingBusyDataNode()
		}
	}
}

// sendOutTasks pushes all pending tasks to DataCoord, gets DataCoord response and re-add these tasks as working tasks.
func (m *importManager) sendOutTasks(ctx context.Context) error {
	m.pendingLock.Lock()
	m.busyNodesLock.Lock()
	defer m.pendingLock.Unlock()
	defer m.busyNodesLock.Unlock()

	// Trigger Import() action to DataCoord.
	for len(m.pendingTasks) > 0 {
		log.Debug("try to send out pending tasks", zap.Int("task_number", len(m.pendingTasks)))
		task := m.pendingTasks[0]
		// TODO: Use ImportTaskInfo directly.
		it := &datapb.ImportTask{
			CollectionId: task.GetCollectionId(),
			PartitionId:  task.GetPartitionId(),
			ChannelNames: task.GetChannelNames(),
			TaskId:       task.GetId(),
			Files:        task.GetFiles(),
			Infos:        task.GetInfos(),
		}

		// Get all busy dataNodes for reference.
		var busyNodeList []int64
		for k := range m.busyNodes {
			busyNodeList = append(busyNodeList, k)
		}

		// Send import task to dataCoord, which will then distribute the import task to dataNode.
		resp, err := m.callImportService(ctx, &datapb.ImportTaskRequest{
			ImportTask:   it,
			WorkingNodes: busyNodeList,
		})
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("import task is rejected",
				zap.Int64("task ID", it.GetTaskId()),
				zap.Any("error code", resp.GetStatus().GetErrorCode()),
				zap.String("cause", resp.GetStatus().GetReason()))
			break
		}
		if err != nil {
			log.Warn("import task get error", zap.Error(err))
			break
		}

		// Successfully assigned dataNode for the import task. Add task to working task list and update task store.
		task.DatanodeId = resp.GetDatanodeId()
		log.Debug("import task successfully assigned to dataNode",
			zap.Int64("task ID", it.GetTaskId()),
			zap.Int64("dataNode ID", task.GetDatanodeId()))
		// Add new working dataNode to busyNodes.
		m.busyNodes[resp.GetDatanodeId()] = task.GetCreateTs()
		err = func() error {
			m.workingLock.Lock()
			defer m.workingLock.Unlock()
			log.Debug("import task added as working task", zap.Int64("task ID", it.TaskId))
			task.State.StateCode = commonpb.ImportState_ImportStarted
			task.StartTs = time.Now().Unix()
			// first update the import task into meta store and then put it into working tasks
			if err := m.persistTaskInfo(task); err != nil {
				log.Error("failed to update import task",
					zap.Int64("task ID", task.GetId()),
					zap.Error(err))
				return err
			}
			m.workingTasks[task.GetId()] = task
			return nil
		}()
		if err != nil {
			return err
		}
		// Remove this task from head of pending list.
		m.pendingTasks = append(m.pendingTasks[:0], m.pendingTasks[1:]...)
	}

	return nil
}

// loadAndFlipPersistedTasks checks every import task in `ImportPersisted` state and flips their import state to
// `ImportCompleted` if eligible.
func (m *importManager) loadAndFlipPersistedTasks(ctx context.Context) error {
	var importTasks []*datapb.ImportTaskInfo
	var err error
	if importTasks, err = m.loadFromTaskStore(false); err != nil {
		log.Error("failed to load from task store", zap.Error(err))
		return err
	}

	for _, task := range importTasks {
		// Checking if ImportPersisted --> ImportCompleted ready.
		if task.GetState().GetStateCode() == commonpb.ImportState_ImportPersisted {
			log.Info("<ImportPersisted> task found, checking if it is eligible to become <ImportCompleted>",
				zap.Int64("task ID", task.GetId()))
			importTask := m.getTaskState(task.GetId())

			// if this method failed, skip this task, try again in next round
			if err = m.flipTaskFlushedState(ctx, importTask, task.GetDatanodeId()); err != nil {
				log.Error("failed to flip task flushed state",
					zap.Int64("task ID", task.GetId()),
					zap.Error(err))
			}
		}
	}
	return nil
}

func (m *importManager) flipTaskFlushedState(ctx context.Context, importTask *milvuspb.GetImportStateResponse, dataNodeID int64) error {
	ok, err := m.checkFlushDone(ctx, importTask.GetSegmentIds())
	if err != nil {
		log.Error("an error occurred while checking flush state of segments",
			zap.Int64("task ID", importTask.GetId()),
			zap.Error(err))
		return err
	}
	if ok {
		// All segments are flushed. DataNode becomes available.
		func() {
			m.busyNodesLock.Lock()
			defer m.busyNodesLock.Unlock()
			delete(m.busyNodes, dataNodeID)
			log.Info("a DataNode is no longer busy after processing task",
				zap.Int64("dataNode ID", dataNodeID),
				zap.Int64("task ID", importTask.GetId()))

		}()
		// Unset isImporting flag.
		if m.callUnsetIsImportingState == nil {
			log.Error("callUnsetIsImportingState function of importManager is nil")
			return fmt.Errorf("failed to describe index: segment state method of import manager is nil")
		}
		_, err := m.callUnsetIsImportingState(ctx, &datapb.UnsetIsImportingStateRequest{
			SegmentIds: importTask.GetSegmentIds(),
		})
		if err := m.setImportTaskState(importTask.GetId(), commonpb.ImportState_ImportCompleted); err != nil {
			log.Error("failed to set import task state",
				zap.Int64("task ID", importTask.GetId()),
				zap.Any("target state", commonpb.ImportState_ImportCompleted),
				zap.Error(err))
			return err
		}
		if err != nil {
			log.Error("failed to unset importing state of all segments (could be partial failure)",
				zap.Error(err))
			return err
		}
		// Start working on new bulk insert tasks.
		if err = m.sendOutTasks(m.ctx); err != nil {
			log.Error("fail to send out import task to DataNodes",
				zap.Int64("task ID", importTask.GetId()))
		}
	}
	return nil
}

// checkFlushDone checks if flush is done on given segments.
func (m *importManager) checkFlushDone(ctx context.Context, segIDs []UniqueID) (bool, error) {
	resp, err := m.callGetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{
		SegmentIDs: segIDs,
	})
	if err != nil {
		log.Error("failed to get import task segment states",
			zap.Int64s("segment IDs", segIDs))
		return false, err
	}
	getSegmentStates := func(segment *datapb.SegmentStateInfo, _ int) string {
		return segment.GetState().String()
	}
	log.Debug("checking import segment states",
		zap.Strings("segment states", lo.Map(resp.GetStates(), getSegmentStates)))
	for _, states := range resp.GetStates() {
		// Flushed segment could get compacted, so only returns false if there are still importing segments.
		if states.GetState() == commonpb.SegmentState_Importing ||
			states.GetState() == commonpb.SegmentState_Sealed {
			return false, nil
		}
	}
	return true, nil
}

func (m *importManager) isRowbased(files []string) (bool, error) {
	isRowBased := false
	for _, filePath := range files {
		_, fileType := importutil.GetFileNameAndExt(filePath)
		if fileType == importutil.JSONFileExt {
			isRowBased = true
		} else if isRowBased {
			log.Error("row-based data file type must be JSON, mixed file types is not allowed", zap.Strings("files", files))
			return isRowBased, fmt.Errorf("row-based data file type must be JSON, file type '%s' is not allowed", fileType)
		}
	}

	// for row_based, we only allow one file so that each invocation only generate a task
	if isRowBased && len(files) > 1 {
		log.Error("row-based import, only allow one JSON file each time", zap.Strings("files", files))
		return isRowBased, fmt.Errorf("row-based import, only allow one JSON file each time")
	}

	return isRowBased, nil
}

// importJob processes the import request, generates import tasks, sends these tasks to DataCoord, and returns
// immediately.
func (m *importManager) importJob(ctx context.Context, req *milvuspb.ImportRequest, cID int64, pID int64) *milvuspb.ImportResponse {
	returnErrorFunc := func(reason string) *milvuspb.ImportResponse {
		return &milvuspb.ImportResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    reason,
			},
		}
	}

	if req == nil || len(req.Files) == 0 {
		return returnErrorFunc("import request is empty")
	}

	if m.callImportService == nil {
		return returnErrorFunc("import service is not available")
	}

	resp := &milvuspb.ImportResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Tasks: make([]int64, 0),
	}

	log.Info("receive import job",
		zap.String("database name", req.GetDbName()),
		zap.String("collection name", req.GetCollectionName()),
		zap.Int64("collection ID", cID),
		zap.Int64("partition ID", pID))
	err := func() error {
		m.pendingLock.Lock()
		defer m.pendingLock.Unlock()

		capacity := cap(m.pendingTasks)
		length := len(m.pendingTasks)

		isRowBased, err := m.isRowbased(req.GetFiles())
		if err != nil {
			return err
		}

		taskCount := 1
		if isRowBased {
			taskCount = len(req.Files)
		}

		// task queue size has a limit, return error if import request contains too many data files, and skip entire job
		if capacity-length < taskCount {
			err := fmt.Errorf("import task queue max size is %v, currently there are %v tasks is pending. Not able to execute this request with %v tasks", capacity, length, taskCount)
			log.Error(err.Error())
			return err
		}

		// convert import request to import tasks
		if isRowBased {
			// For row-based importing, each file makes a task.
			taskList := make([]int64, len(req.Files))
			for i := 0; i < len(req.Files); i++ {
				tID, _, err := m.idAllocator(1)
				if err != nil {
					log.Error("failed to allocate ID for import task", zap.Error(err))
					return err
				}
				newTask := &datapb.ImportTaskInfo{
					Id:           tID,
					CollectionId: cID,
					PartitionId:  pID,
					ChannelNames: req.ChannelNames,
					Files:        []string{req.GetFiles()[i]},
					CreateTs:     time.Now().Unix(),
					State: &datapb.ImportTaskState{
						StateCode: commonpb.ImportState_ImportPending,
					},
					Infos: req.Options,
				}

				// Here no need to check error returned by setCollectionPartitionName(),
				// since here we always return task list to client no matter something missed.
				// We make the method setCollectionPartitionName() returns error
				// because we need to make sure coverage all the code branch in unittest case.
				_ = m.setCollectionPartitionName("", cID, pID, newTask)
				resp.Tasks = append(resp.Tasks, newTask.GetId())
				taskList[i] = newTask.GetId()
				log.Info("new task created as pending task",
					zap.Int64("task ID", newTask.GetId()))
				if err := m.persistTaskInfo(newTask); err != nil {
					log.Error("failed to update import task",
						zap.Int64("task ID", newTask.GetId()),
						zap.Error(err))
					return err
				}
				m.pendingTasks = append(m.pendingTasks, newTask)
			}
			log.Info("row-based import request processed", zap.Any("task IDs", taskList))
		} else {
			// TODO: Merge duplicated code :(
			// for column-based, all files is a task
			tID, _, err := m.idAllocator(1)
			if err != nil {
				return err
			}
			newTask := &datapb.ImportTaskInfo{
				Id:           tID,
				CollectionId: cID,
				PartitionId:  pID,
				ChannelNames: req.ChannelNames,
				Files:        req.GetFiles(),
				CreateTs:     time.Now().Unix(),
				State: &datapb.ImportTaskState{
					StateCode: commonpb.ImportState_ImportPending,
				},
				Infos: req.Options,
			}
			// Here no need to check error returned by setCollectionPartitionName(),
			// since here we always return task list to client no matter something missed.
			// We make the method setCollectionPartitionName() returns error
			// because we need to make sure coverage all the code branch in unittest case.
			_ = m.setCollectionPartitionName(req.GetDbName(), cID, pID, newTask)
			resp.Tasks = append(resp.Tasks, newTask.GetId())
			log.Info("new task created as pending task",
				zap.Int64("task ID", newTask.GetId()))
			if err := m.persistTaskInfo(newTask); err != nil {
				log.Error("failed to update import task",
					zap.Int64("task ID", newTask.GetId()),
					zap.Error(err))
				return err
			}
			m.pendingTasks = append(m.pendingTasks, newTask)
			log.Info("column-based import request processed",
				zap.Int64("task ID", newTask.GetId()))
		}
		return nil
	}()
	if err != nil {
		return returnErrorFunc(err.Error())
	}
	if sendOutTasksErr := m.sendOutTasks(ctx); sendOutTasksErr != nil {
		log.Error("fail to send out tasks", zap.Error(sendOutTasksErr))
	}
	return resp
}

// updateTaskInfo updates the task's state in in-memory working tasks list and in task store, given ImportResult
// result. It returns the ImportTaskInfo of the given task.
func (m *importManager) updateTaskInfo(ir *rootcoordpb.ImportResult) (*datapb.ImportTaskInfo, error) {
	if ir == nil {
		return nil, errors.New("import result is nil")
	}
	log.Debug("import manager update task import result", zap.Int64("taskID", ir.GetTaskId()))

	updatedInfo, err := func() (*datapb.ImportTaskInfo, error) {
		found := false
		var v *datapb.ImportTaskInfo
		m.workingLock.Lock()
		defer m.workingLock.Unlock()
		ok := false
		var toPersistImportTaskInfo *datapb.ImportTaskInfo

		if v, ok = m.workingTasks[ir.GetTaskId()]; ok {
			// If the task has already been marked failed. Prevent further state updating and return an error.
			if v.GetState().GetStateCode() == commonpb.ImportState_ImportFailed ||
				v.GetState().GetStateCode() == commonpb.ImportState_ImportFailedAndCleaned {
				log.Warn("trying to update an already failed task which will end up being a no-op")
				return nil, errors.New("trying to update an already failed task " + strconv.FormatInt(ir.GetTaskId(), 10))
			}
			found = true

			// Meta persist should be done before memory objs change.
			toPersistImportTaskInfo = cloneImportTaskInfo(v)
			toPersistImportTaskInfo.State.StateCode = ir.GetState()
			// if is started state, append the new created segment id
			if v.GetState().GetStateCode() == commonpb.ImportState_ImportStarted {
				toPersistImportTaskInfo.State.Segments = append(toPersistImportTaskInfo.State.Segments, ir.GetSegments()...)
			} else {
				toPersistImportTaskInfo.State.Segments = ir.GetSegments()
			}
			toPersistImportTaskInfo.State.RowCount = ir.GetRowCount()
			toPersistImportTaskInfo.State.RowIds = ir.GetAutoIds()
			for _, kv := range ir.GetInfos() {
				if kv.GetKey() == importutil.FailedReason {
					toPersistImportTaskInfo.State.ErrorMessage = kv.GetValue()
					break
				} else if kv.GetKey() == importutil.PersistTimeCost ||
					kv.GetKey() == importutil.ProgressPercent {
					importutil.UpdateKVInfo(&toPersistImportTaskInfo.Infos, kv.GetKey(), kv.GetValue())
				}
			}
			log.Info("importManager update task info", zap.Any("toPersistImportTaskInfo", toPersistImportTaskInfo))

			// Update task in task store.
			if err := m.persistTaskInfo(toPersistImportTaskInfo); err != nil {
				log.Error("failed to update import task",
					zap.Int64("task ID", v.GetId()),
					zap.Error(err))
				return nil, err
			}
			m.workingTasks[ir.GetTaskId()] = toPersistImportTaskInfo
		}

		if !found {
			log.Debug("import manager update task import result failed", zap.Int64("task ID", ir.GetTaskId()))
			return nil, errors.New("failed to update import task, ID not found: " + strconv.FormatInt(ir.TaskId, 10))
		}

		return toPersistImportTaskInfo, nil
	}()

	if err != nil {
		return nil, err
	}
	return updatedInfo, nil
}

// setImportTaskState sets the task state of an import task. Changes to the import task state will be persisted.
func (m *importManager) setImportTaskState(taskID int64, targetState commonpb.ImportState) error {
	return m.setImportTaskStateAndReason(taskID, targetState, "")
}

// setImportTaskStateAndReason sets the task state and error message of an import task. Changes to the import task state
// will be persisted.
func (m *importManager) setImportTaskStateAndReason(taskID int64, targetState commonpb.ImportState, errReason string) error {
	log.Info("trying to set the import state of an import task",
		zap.Int64("task ID", taskID),
		zap.Any("target state", targetState))
	found := false
	m.pendingLock.Lock()
	for taskIndex, t := range m.pendingTasks {
		if taskID == t.Id {
			found = true
			// Meta persist should be done before memory objs change.
			toPersistImportTaskInfo := cloneImportTaskInfo(t)
			toPersistImportTaskInfo.State.StateCode = targetState
			if targetState == commonpb.ImportState_ImportCompleted {
				importutil.UpdateKVInfo(&toPersistImportTaskInfo.Infos, importutil.ProgressPercent, "100")
			}
			tryUpdateErrMsg(errReason, toPersistImportTaskInfo)
			// Update task in task store.
			if err := m.persistTaskInfo(toPersistImportTaskInfo); err != nil {
				return err
			}
			m.pendingTasks[taskIndex] = toPersistImportTaskInfo
			break
		}
	}
	m.pendingLock.Unlock()

	m.workingLock.Lock()
	if v, ok := m.workingTasks[taskID]; ok {
		found = true
		// Meta persist should be done before memory objs change.
		toPersistImportTaskInfo := cloneImportTaskInfo(v)
		toPersistImportTaskInfo.State.StateCode = targetState
		if targetState == commonpb.ImportState_ImportCompleted {
			importutil.UpdateKVInfo(&toPersistImportTaskInfo.Infos, importutil.ProgressPercent, "100")
		}
		tryUpdateErrMsg(errReason, toPersistImportTaskInfo)
		// Update task in task store.
		if err := m.persistTaskInfo(toPersistImportTaskInfo); err != nil {
			return err
		}
		m.workingTasks[taskID] = toPersistImportTaskInfo
	}
	m.workingLock.Unlock()

	// If task is not found in memory, try updating in Etcd.
	var v string
	var err error
	if !found {
		if v, err = m.taskStore.Load(BuildImportTaskKey(taskID)); err == nil && v != "" {
			ti := &datapb.ImportTaskInfo{}
			if err := proto.Unmarshal([]byte(v), ti); err != nil {
				log.Error("failed to unmarshal proto", zap.String("taskInfo", v), zap.Error(err))
			} else {
				toPersistImportTaskInfo := cloneImportTaskInfo(ti)
				toPersistImportTaskInfo.State.StateCode = targetState
				if targetState == commonpb.ImportState_ImportCompleted {
					importutil.UpdateKVInfo(&toPersistImportTaskInfo.Infos, importutil.ProgressPercent, "100")
				}
				tryUpdateErrMsg(errReason, toPersistImportTaskInfo)
				// Update task in task store.
				if err := m.persistTaskInfo(toPersistImportTaskInfo); err != nil {
					return err
				}
				found = true
			}
		} else {
			log.Warn("failed to load task info from Etcd",
				zap.String("value", v),
				zap.Error(err))
		}
	}

	if !found {
		return errors.New("failed to update import task state, ID not found: " + strconv.FormatInt(taskID, 10))
	}
	return nil
}

func (m *importManager) setCollectionPartitionName(dbName string, colID, partID int64, task *datapb.ImportTaskInfo) error {
	if m.getCollectionName != nil {
		colName, partName, err := m.getCollectionName(dbName, colID, partID)
		if err == nil {
			task.CollectionName = colName
			task.PartitionName = partName
			return nil
		}
		log.Error("failed to setCollectionPartitionName",
			zap.Int64("collection ID", colID),
			zap.Int64("partition ID", partID),
			zap.Error(err))
	}
	return errors.New("failed to setCollectionPartitionName for import task")
}

func (m *importManager) copyTaskInfo(input *datapb.ImportTaskInfo, output *milvuspb.GetImportStateResponse) {
	output.Status = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	output.Id = input.GetId()
	output.CollectionId = input.GetCollectionId()
	output.State = input.GetState().GetStateCode()
	output.RowCount = input.GetState().GetRowCount()
	output.IdList = input.GetState().GetRowIds()
	output.SegmentIds = input.GetState().GetSegments()
	output.CreateTs = input.GetCreateTs()
	output.Infos = append(output.Infos, &commonpb.KeyValuePair{Key: importutil.Files, Value: strings.Join(input.GetFiles(), ",")})
	output.Infos = append(output.Infos, &commonpb.KeyValuePair{Key: importutil.CollectionName, Value: input.GetCollectionName()})
	output.Infos = append(output.Infos, &commonpb.KeyValuePair{Key: importutil.PartitionName, Value: input.GetPartitionName()})
	output.Infos = append(output.Infos, &commonpb.KeyValuePair{
		Key:   importutil.FailedReason,
		Value: input.GetState().GetErrorMessage(),
	})
	output.Infos = append(output.Infos, input.Infos...)
}

// getTaskState looks for task with the given ID and returns its import state.
func (m *importManager) getTaskState(tID int64) *milvuspb.GetImportStateResponse {
	resp := &milvuspb.GetImportStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "import task id doesn't exist",
		},
		Infos: make([]*commonpb.KeyValuePair, 0),
	}
	// (1) Search in pending tasks list.
	found := false
	m.pendingLock.Lock()
	for _, t := range m.pendingTasks {
		if tID == t.Id {
			m.copyTaskInfo(t, resp)
			found = true
			break
		}
	}
	m.pendingLock.Unlock()
	if found {
		return resp
	}
	// (2) Search in working tasks map.
	m.workingLock.Lock()
	if v, ok := m.workingTasks[tID]; ok {
		found = true
		m.copyTaskInfo(v, resp)
	}
	m.workingLock.Unlock()
	if found {
		return resp
	}
	// (3) Search in Etcd.
	if v, err := m.taskStore.Load(BuildImportTaskKey(tID)); err == nil && v != "" {
		ti := &datapb.ImportTaskInfo{}
		if err := proto.Unmarshal([]byte(v), ti); err != nil {
			log.Error("failed to unmarshal proto", zap.String("taskInfo", v), zap.Error(err))
		} else {
			m.copyTaskInfo(ti, resp)
			found = true
		}
	} else {
		log.Warn("failed to load task info from Etcd",
			zap.String("value", v),
			zap.Error(err))
	}
	if found {
		log.Info("getting import task state", zap.Int64("task ID", tID), zap.Any("state", resp.State), zap.Int64s("segment", resp.SegmentIds))
		return resp
	}
	log.Debug("get import task state failed", zap.Int64("taskID", tID))
	return resp
}

// loadFromTaskStore loads task info from task store (Etcd).
// loadFromTaskStore also adds these tasks as pending import tasks, and mark
// other in-progress tasks as failed, when `load2Mem` is set to `true`.
// loadFromTaskStore instead returns a list of all import tasks if `load2Mem` is set to `false`.
func (m *importManager) loadFromTaskStore(load2Mem bool) ([]*datapb.ImportTaskInfo, error) {
	// log.Debug("import manager starts loading from Etcd")
	_, v, err := m.taskStore.LoadWithPrefix(Params.RootCoordCfg.ImportTaskSubPath.GetValue())
	if err != nil {
		log.Error("import manager failed to load from Etcd", zap.Error(err))
		return nil, err
	}
	var taskList []*datapb.ImportTaskInfo

	for i := range v {
		ti := &datapb.ImportTaskInfo{}
		if err := proto.Unmarshal([]byte(v[i]), ti); err != nil {
			log.Error("failed to unmarshal proto", zap.String("taskInfo", v[i]), zap.Error(err))
			// Ignore bad protos.
			continue
		}

		if load2Mem {
			// Put pending tasks back to pending task list.
			if ti.GetState().GetStateCode() == commonpb.ImportState_ImportPending {
				log.Info("task has been reloaded as a pending task", zap.Int64("task ID", ti.GetId()))
				m.pendingLock.Lock()
				m.pendingTasks = append(m.pendingTasks, ti)
				m.pendingLock.Unlock()
			} else {
				// other non-failed and non-completed tasks should be marked failed, so the bad s egments
				// can be cleaned up in `removeBadImportSegmentsLoop`.
				if ti.GetState().GetStateCode() != commonpb.ImportState_ImportFailed &&
					ti.GetState().GetStateCode() != commonpb.ImportState_ImportFailedAndCleaned &&
					ti.GetState().GetStateCode() != commonpb.ImportState_ImportCompleted {
					ti.State.StateCode = commonpb.ImportState_ImportFailed
					if ti.GetState().GetErrorMessage() == "" {
						ti.State.ErrorMessage = "task marked failed as service restarted"
					} else {
						ti.State.ErrorMessage = fmt.Sprintf("%s; task marked failed as service restarted",
							ti.GetState().GetErrorMessage())
					}
					if err := m.persistTaskInfo(ti); err != nil {
						log.Error("failed to mark an old task as expired",
							zap.Int64("task ID", ti.GetId()),
							zap.Error(err))
					}
					log.Info("task has been marked failed while reloading",
						zap.Int64("task ID", ti.GetId()))
				}
			}
		} else {
			taskList = append(taskList, ti)
		}
	}
	return taskList, nil
}

// persistTaskInfo stores or updates the import task info in Etcd.
func (m *importManager) persistTaskInfo(ti *datapb.ImportTaskInfo) error {
	log.Info("updating import task info in Etcd", zap.Int64("task ID", ti.GetId()))
	var taskInfo []byte
	var err error
	if taskInfo, err = proto.Marshal(ti); err != nil {
		log.Error("failed to marshall task info proto",
			zap.Int64("task ID", ti.GetId()),
			zap.Error(err))
		return err
	}
	if err = m.taskStore.Save(BuildImportTaskKey(ti.GetId()), string(taskInfo)); err != nil {
		log.Error("failed to update import task info in Etcd",
			zap.Int64("task ID", ti.GetId()),
			zap.Error(err))
		return err
	}
	return nil
}

// yieldTaskInfo removes the task info from Etcd.
func (m *importManager) yieldTaskInfo(tID int64) error {
	log.Info("removing import task info from Etcd",
		zap.Int64("task ID", tID))
	if err := m.taskStore.Remove(BuildImportTaskKey(tID)); err != nil {
		log.Error("failed to update import task info in Etcd",
			zap.Int64("task ID", tID),
			zap.Error(err))
		return err
	}
	return nil
}

// expireOldTasks removes expired tasks from memory.
func (m *importManager) expireOldTasksFromMem() {
	// no need to expire pending tasks. With old working tasks finish or turn into expired, datanodes back to idle,
	// let the sendOutTasksLoop() push pending tasks into datanodes.

	// expire old working tasks.
	func() {
		m.workingLock.Lock()
		defer m.workingLock.Unlock()
		for _, v := range m.workingTasks {
			taskExpiredAndStateUpdated := false
			if v.GetState().GetStateCode() != commonpb.ImportState_ImportCompleted && taskExpired(v) {
				log.Info("a working task has expired and will be marked as failed",
					zap.Int64("task ID", v.GetId()),
					zap.Int64("startTs", v.GetStartTs()),
					zap.Float64("ImportTaskExpiration", Params.RootCoordCfg.ImportTaskExpiration.GetAsFloat()))
				taskID := v.GetId()
				m.workingLock.Unlock()
				// Remove DataNode from busy node list, so it can serve other tasks again.
				m.busyNodesLock.Lock()
				delete(m.busyNodes, v.GetDatanodeId())
				m.busyNodesLock.Unlock()

				if err := m.setImportTaskStateAndReason(taskID, commonpb.ImportState_ImportFailed,
					"the import task has timed out"); err != nil {
					log.Error("failed to set import task state",
						zap.Int64("task ID", taskID),
						zap.Any("target state", commonpb.ImportState_ImportFailed))
				} else {
					taskExpiredAndStateUpdated = true
				}
				m.workingLock.Lock()
				if taskExpiredAndStateUpdated {
					// Remove this task from memory.
					delete(m.workingTasks, v.GetId())
				}
			}
		}
	}()
}

// expireOldTasksFromEtcd removes tasks from Etcd that are over `ImportTaskRetention` seconds old.
func (m *importManager) expireOldTasksFromEtcd() {
	var vs []string
	var err error
	// Collect all import task records.
	if _, vs, err = m.taskStore.LoadWithPrefix(Params.RootCoordCfg.ImportTaskSubPath.GetValue()); err != nil {
		log.Error("failed to load import tasks from Etcd during task cleanup")
		return
	}
	// Loop through all import tasks in Etcd and look for the ones that have passed retention period.
	for _, val := range vs {
		ti := &datapb.ImportTaskInfo{}
		if err := proto.Unmarshal([]byte(val), ti); err != nil {
			log.Error("failed to unmarshal proto", zap.String("taskInfo", val), zap.Error(err))
			// Ignore bad protos. This is just a cleanup task, so we are not panicking.
			continue
		}
		if taskPastRetention(ti) {
			log.Info("an import task has passed retention period and will be removed from Etcd",
				zap.Int64("task ID", ti.GetId()),
				zap.Int64("createTs", ti.GetCreateTs()),
				zap.Float64("ImportTaskRetention", Params.RootCoordCfg.ImportTaskRetention.GetAsFloat()))
			if err = m.yieldTaskInfo(ti.GetId()); err != nil {
				log.Error("failed to remove import task from Etcd",
					zap.Int64("task ID", ti.GetId()),
					zap.Error(err))
			}
		}
	}
}

// releaseHangingBusyDataNode checks if a busy DataNode has been 'busy' for an unexpected long time.
// We will then remove these DataNodes from `busy list`.
func (m *importManager) releaseHangingBusyDataNode() {
	m.busyNodesLock.Lock()
	for nodeID, ts := range m.busyNodes {
		log.Info("busy DataNode found",
			zap.Int64("node ID", nodeID),
			zap.Int64("busy duration (seconds)", time.Now().Unix()-ts),
		)
		if Params.RootCoordCfg.ImportTaskExpiration.GetAsFloat() <= float64(time.Now().Unix()-ts) {
			log.Warn("release a hanging busy DataNode",
				zap.Int64("node ID", nodeID))
			delete(m.busyNodes, nodeID)
		}
	}
	m.busyNodesLock.Unlock()
}

func rearrangeTasks(tasks []*milvuspb.GetImportStateResponse) {
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GetId() < tasks[j].GetId()
	})
}

func (m *importManager) listAllTasks(colID int64, limit int64) ([]*milvuspb.GetImportStateResponse, error) {
	var importTasks []*datapb.ImportTaskInfo
	var err error
	if importTasks, err = m.loadFromTaskStore(false); err != nil {
		log.Error("failed to load from task store", zap.Error(err))
		return nil, fmt.Errorf("failed to load task list from etcd, error: %w", err)
	}

	tasks := make([]*milvuspb.GetImportStateResponse, 0)
	// filter tasks by collection id
	// if colID is negative, we will return all tasks
	for _, task := range importTasks {
		if colID < 0 || colID == task.GetCollectionId() {
			currTask := &milvuspb.GetImportStateResponse{}
			m.copyTaskInfo(task, currTask)
			tasks = append(tasks, currTask)
		}
	}

	// arrange tasks by id in ascending order, actually, id is the create time of a task
	rearrangeTasks(tasks)

	// if limit is 0 or larger than length of tasks, return all tasks
	if limit <= 0 || limit >= int64(len(tasks)) {
		return tasks, nil
	}

	// return the newly tasks from the tail
	return tasks[len(tasks)-int(limit):], nil
}

// removeBadImportSegments marks segments of a failed import task as `dropped`.
func (m *importManager) removeBadImportSegments(ctx context.Context) {
	var taskList []*datapb.ImportTaskInfo
	var err error
	if taskList, err = m.loadFromTaskStore(false); err != nil {
		log.Error("failed to load from task store",
			zap.Error(err))
		return
	}
	for _, t := range taskList {
		// Only check newly failed tasks.
		if t.GetState().GetStateCode() != commonpb.ImportState_ImportFailed {
			continue
		}
		log.Info("trying to mark segments as dropped",
			zap.Int64("task ID", t.GetId()),
			zap.Int64s("segment IDs", t.GetState().GetSegments()))

		if err = m.setImportTaskState(t.GetId(), commonpb.ImportState_ImportFailedAndCleaned); err != nil {
			log.Warn("failed to set ", zap.Int64("task ID", t.GetId()), zap.Error(err))
		}
	}
}

// BuildImportTaskKey constructs and returns an Etcd key with given task ID.
func BuildImportTaskKey(taskID int64) string {
	return fmt.Sprintf("%s%s%d", Params.RootCoordCfg.ImportTaskSubPath.GetValue(), delimiter, taskID)
}

// taskExpired returns true if the in-mem task is considered expired.
func taskExpired(ti *datapb.ImportTaskInfo) bool {
	return Params.RootCoordCfg.ImportTaskExpiration.GetAsFloat() <= float64(time.Now().Unix()-ti.GetStartTs())
}

// taskPastRetention returns true if the task is considered expired in Etcd.
func taskPastRetention(ti *datapb.ImportTaskInfo) bool {
	return Params.RootCoordCfg.ImportTaskRetention.GetAsFloat() <= float64(time.Now().Unix()-ti.GetCreateTs())
}

func tryUpdateErrMsg(errReason string, toPersistImportTaskInfo *datapb.ImportTaskInfo) {
	if errReason != "" {
		if toPersistImportTaskInfo.GetState().GetErrorMessage() == "" {
			toPersistImportTaskInfo.State.ErrorMessage = errReason
		} else {
			toPersistImportTaskInfo.State.ErrorMessage =
				fmt.Sprintf("%s; %s",
					toPersistImportTaskInfo.GetState().GetErrorMessage(),
					errReason)
		}
	}
}

func cloneImportTaskInfo(taskInfo *datapb.ImportTaskInfo) *datapb.ImportTaskInfo {
	cloned := &datapb.ImportTaskInfo{
		Id:             taskInfo.GetId(),
		DatanodeId:     taskInfo.GetDatanodeId(),
		CollectionId:   taskInfo.GetCollectionId(),
		PartitionId:    taskInfo.GetPartitionId(),
		ChannelNames:   taskInfo.GetChannelNames(),
		Files:          taskInfo.GetFiles(),
		CreateTs:       taskInfo.GetCreateTs(),
		State:          taskInfo.GetState(),
		CollectionName: taskInfo.GetCollectionName(),
		PartitionName:  taskInfo.GetPartitionName(),
		Infos:          taskInfo.GetInfos(),
		StartTs:        taskInfo.GetStartTs(),
	}
	return cloned
}
