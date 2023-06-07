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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	importutil2 "github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestImportManager_NewImportManager(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskExpiration.Key, "1")  // unit: second
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskRetention.Key, "200") // unit: second
	checkPendingTasksInterval = 500                                           // unit: millisecond
	cleanUpLoopInterval = 500                                                 // unit: millisecond
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti3 := &datapb.ImportTaskInfo{
		Id: 300,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportCompleted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	taskInfo3, err := proto.Marshal(ti3)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(1), "value")
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))
	mockKv.Save(BuildImportTaskKey(300), string(taskInfo3))

	mockCallImportServiceErr := false
	callImportServiceFn := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if mockCallImportServiceErr {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, errors.New("mock err")
		}
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}
	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("working task expired", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)

		// there are 2 tasks read from store, one is pending, the other is persisted.
		// the persisted task will be marked to failed since the server restart
		// pending list: 1 task, working list: 0 task
		_, err := mgr.loadFromTaskStore(true)
		assert.NoError(t, err)
		var wgLoop sync.WaitGroup
		wgLoop.Add(2)

		// the pending task will be sent to working list
		// pending list: 0 task, working list: 1 task
		mgr.sendOutTasks(ctx)
		assert.Equal(t, 1, len(mgr.workingTasks))

		// this case wait 3 seconds, the pending task's StartTs is set when it is put into working list
		// ImportTaskExpiration is 1 second, it will be marked as expired task by the expireOldTasksFromMem()
		// pending list: 0 task, working list: 0 task
		mgr.cleanupLoop(&wgLoop)
		assert.Equal(t, 0, len(mgr.workingTasks))

		// nothing to send now
		mgr.sendOutTasksLoop(&wgLoop)
		wgLoop.Wait()
	})

	wg.Add(1)
	t.Run("context done", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)
		mgr.init(context.TODO())
		var wgLoop sync.WaitGroup
		wgLoop.Add(2)
		mgr.cleanupLoop(&wgLoop)
		mgr.sendOutTasksLoop(&wgLoop)
		wgLoop.Wait()
	})

	wg.Add(1)
	t.Run("importManager init fail because of loadFromTaskStore fail", func(t *testing.T) {
		defer wg.Done()

		mockTxnKV := &mocks.TxnKV{}
		mockTxnKV.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("mock error"))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		mgr := newImportManager(ctx, mockTxnKV, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)
		assert.Panics(t, func() {
			mgr.init(context.TODO())
		})
	})

	wg.Add(1)
	t.Run("sendOutTasks fail", func(t *testing.T) {
		defer wg.Done()

		mockTxnKV := &mocks.TxnKV{}
		mockTxnKV.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil)
		mockTxnKV.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("mock save error"))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		mgr := newImportManager(ctx, mockTxnKV, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)
		mgr.init(context.TODO())
	})

	wg.Add(1)
	t.Run("sendOutTasks fail", func(t *testing.T) {
		defer wg.Done()

		mockTxnKV := &mocks.TxnKV{}
		mockTxnKV.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		mgr := newImportManager(ctx, mockTxnKV, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)
		mgr.init(context.TODO())
		func() {
			mockTxnKV.EXPECT().Save(mock.Anything, mock.Anything).Maybe().Return(errors.New("mock save error"))
			mgr.sendOutTasks(context.TODO())
		}()

		func() {
			mockTxnKV.EXPECT().Save(mock.Anything, mock.Anything).Maybe().Return(nil)
			mockCallImportServiceErr = true
			mgr.sendOutTasks(context.TODO())
		}()
	})

	wg.Add(1)
	t.Run("check init", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		assert.NotNil(t, mgr)
		mgr.init(ctx)
		var wgLoop sync.WaitGroup
		wgLoop.Add(2)
		mgr.cleanupLoop(&wgLoop)
		mgr.sendOutTasksLoop(&wgLoop)
		time.Sleep(100 * time.Millisecond)
		wgLoop.Wait()
	})

	wg.Wait()
}

func TestImportManager_TestSetImportTaskState(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskExpiration.Key, "50")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskRetention.Key, "200")
	checkPendingTasksInterval = 100
	cleanUpLoopInterval = 100
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(1), "value")
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("working task expired", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, nil, nil, nil, nil)
		assert.NotNil(t, mgr)
		_, err := mgr.loadFromTaskStore(true)
		assert.NoError(t, err)
		// Task not exist.
		assert.Error(t, mgr.setImportTaskState(999, commonpb.ImportState_ImportStarted))
		// Normal case: update in-mem task state.
		assert.NoError(t, mgr.setImportTaskState(100, commonpb.ImportState_ImportPersisted))
		v, err := mockKv.Load(BuildImportTaskKey(100))
		assert.NoError(t, err)
		ti := &datapb.ImportTaskInfo{}
		err = proto.Unmarshal([]byte(v), ti)
		assert.NoError(t, err)
		assert.Equal(t, ti.GetState().GetStateCode(), commonpb.ImportState_ImportPersisted)
		// Normal case: update Etcd task state.
		assert.NoError(t, mgr.setImportTaskState(200, commonpb.ImportState_ImportFailedAndCleaned))
		v, err = mockKv.Load(BuildImportTaskKey(200))
		assert.NoError(t, err)
		ti = &datapb.ImportTaskInfo{}
		err = proto.Unmarshal([]byte(v), ti)
		assert.NoError(t, err)
		assert.Equal(t, ti.GetState().GetStateCode(), commonpb.ImportState_ImportFailedAndCleaned)
	})
}

func TestImportManager_TestEtcdCleanUp(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskExpiration.Key, "50")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskRetention.Key, "200")
	checkPendingTasksInterval = 100
	cleanUpLoopInterval = 100
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 500,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 500,
	}
	ti3 := &datapb.ImportTaskInfo{
		Id: 300,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo3, err := proto.Marshal(ti3)
	assert.NoError(t, err)
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))
	mockKv.Save(BuildImportTaskKey(300), string(taskInfo3))

	mockCallImportServiceErr := false
	callImportServiceFn := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if mockCallImportServiceErr {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, errors.New("mock err")
		}
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
	assert.NotNil(t, mgr)
	_, err = mgr.loadFromTaskStore(true)
	assert.NoError(t, err)
	var wgLoop sync.WaitGroup
	wgLoop.Add(2)
	keys, _, _ := mockKv.LoadWithPrefix("")
	// All 3 tasks are stored in Etcd.
	assert.Equal(t, 3, len(keys))
	mgr.busyNodes[20] = time.Now().Unix() - 20*60
	mgr.busyNodes[30] = time.Now().Unix()
	mgr.cleanupLoop(&wgLoop)
	keys, _, _ = mockKv.LoadWithPrefix("")
	// task 1 and task 2 have passed retention period.
	assert.Equal(t, 1, len(keys))
	mgr.sendOutTasksLoop(&wgLoop)
}

func TestImportManager_TestFlipTaskStateLoop(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskExpiration.Key, "50")
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskRetention.Key, "200")
	checkPendingTasksInterval = 100
	cleanUpLoopInterval = 100
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
			Segments:  []int64{201, 202, 203},
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti3 := &datapb.ImportTaskInfo{
		Id: 300,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportCompleted,
			Segments:  []int64{204, 205, 206},
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	taskInfo3, err := proto.Marshal(ti3)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))
	mockKv.Save(BuildImportTaskKey(300), string(taskInfo3))

	mockCallImportServiceErr := false
	callImportServiceFn := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if mockCallImportServiceErr {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, errors.New("mock err")
		}
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	callUnsetIsImportingState := func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	flipPersistedTaskInterval = 20
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("normal case", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn,
			callGetSegmentStates, nil, callUnsetIsImportingState)
		assert.NotNil(t, mgr)
		var wgLoop sync.WaitGroup
		wgLoop.Add(1)
		mgr.flipTaskStateLoop(&wgLoop)
		wgLoop.Wait()
		time.Sleep(200 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("describe index fail", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn,
			callGetSegmentStates, nil, callUnsetIsImportingState)
		assert.NotNil(t, mgr)
		var wgLoop sync.WaitGroup
		wgLoop.Add(1)
		mgr.flipTaskStateLoop(&wgLoop)
		wgLoop.Wait()
		time.Sleep(100 * time.Millisecond)
	})

	wg.Add(1)
	t.Run("describe index with index doesn't exist", func(t *testing.T) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr := newImportManager(ctx, mockKv, idAlloc, callImportServiceFn,
			callGetSegmentStates, nil, callUnsetIsImportingState)
		assert.NotNil(t, mgr)
		var wgLoop sync.WaitGroup
		wgLoop.Add(1)
		mgr.flipTaskStateLoop(&wgLoop)
		wgLoop.Wait()
		time.Sleep(100 * time.Millisecond)
	})
	wg.Wait()
}

func TestImportManager_ImportJob(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}

	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	paramtable.Get().Save(Params.RootCoordCfg.ImportMaxPendingTaskCount.Key, "16")
	defer paramtable.Get().Remove(Params.RootCoordCfg.ImportMaxPendingTaskCount.Key)
	colID := int64(100)
	mockKv := memkv.NewMemoryKV()
	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}
	// nil request
	mgr := newImportManager(context.TODO(), mockKv, idAlloc, nil, callGetSegmentStates, nil, nil)
	resp := mgr.importJob(context.TODO(), nil, colID, 0)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.json", "f2.json", "f3.json"},
	}

	// nil callImportService
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// row-based import not allow multiple files
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	importServiceFunc := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}

	// row-based case, task count equal to file count
	// since the importServiceFunc return error, tasks will be kept in pending list
	rowReq.Files = []string{"f1.json"}
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.Equal(t, len(rowReq.Files), len(mgr.pendingTasks))
	assert.Equal(t, 0, len(mgr.workingTasks))

	colReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.npy", "f2.npy", "f3.npy"},
	}

	// column-based case, one quest one task
	// since the importServiceFunc return error, tasks will be kept in pending list
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, 1, len(mgr.pendingTasks))
	assert.Equal(t, 0, len(mgr.workingTasks))

	importServiceFunc = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	// row-based case, since the importServiceFunc return success, tasks will be sent to working list
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, len(rowReq.Files), len(mgr.workingTasks))

	// column-based case, since the importServiceFunc return success, tasks will be sent to working list
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 1, len(mgr.workingTasks))

	count := 0
	importServiceFunc = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if count >= 1 {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
				},
			}, nil
		}
		count++
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	// row-based case, since the importServiceFunc return success for 1 task
	// the first task is sent to working list, and 1 task left in pending list
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 1, len(mgr.workingTasks))
	resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.Equal(t, 1, len(mgr.pendingTasks))
	assert.Equal(t, 1, len(mgr.workingTasks))

	// the pending list already has one task
	// once task count exceeds MaxPendingCount, return error
	for i := 0; i <= Params.RootCoordCfg.ImportMaxPendingTaskCount.GetAsInt(); i++ {
		resp = mgr.importJob(context.TODO(), rowReq, colID, 0)
		if i < Params.RootCoordCfg.ImportMaxPendingTaskCount.GetAsInt()-1 {
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		} else {
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		}
	}
}

func TestImportManager_AllDataNodesBusy(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	colID := int64(100)
	mockKv := memkv.NewMemoryKV()
	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.json"},
	}
	colReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.npy", "f2.npy"},
		Options: []*commonpb.KeyValuePair{
			{
				Key:   importutil2.Bucket,
				Value: "mybucket",
			},
		},
	}

	dnList := []int64{1, 2, 3}
	count := 0
	importServiceFunc := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if count < len(dnList) {
			count++
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				DatanodeId: dnList[count-1],
			}, nil
		}
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	// each data node owns one task
	mgr := newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	for i := 0; i < len(dnList); i++ {
		resp := mgr.importJob(context.TODO(), rowReq, colID, 0)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(mgr.pendingTasks))
		assert.Equal(t, i+1, len(mgr.workingTasks))
	}

	// all data nodes are busy, new task waiting in pending list
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp := mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, len(rowReq.Files), len(mgr.pendingTasks))
	assert.Equal(t, 0, len(mgr.workingTasks))

	// now all data nodes are free again, new task is executed instantly
	count = 0
	mgr = newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 1, len(mgr.workingTasks))

	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 2, len(mgr.workingTasks))

	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 3, len(mgr.workingTasks))

	// all data nodes are busy now, new task is pending
	resp = mgr.importJob(context.TODO(), colReq, colID, 0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 1, len(mgr.pendingTasks))
	assert.Equal(t, 3, len(mgr.workingTasks))
}

func TestImportManager_TaskState(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	colID := int64(100)
	mockKv := memkv.NewMemoryKV()
	importServiceFunc := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.json"},
	}
	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	// add 3 tasks, their ID is 10000, 10001, 10002, make sure updateTaskInfo() works correctly
	mgr := newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	mgr.importJob(context.TODO(), rowReq, colID, 0)
	rowReq.Files = []string{"f2.json"}
	mgr.importJob(context.TODO(), rowReq, colID, 0)
	rowReq.Files = []string{"f3.json"}
	mgr.importJob(context.TODO(), rowReq, colID, 0)

	info := &rootcoordpb.ImportResult{
		TaskId: 10000,
	}
	// the task id doesn't exist
	_, err := mgr.updateTaskInfo(info)
	assert.Error(t, err)

	info = &rootcoordpb.ImportResult{
		TaskId:   2,
		RowCount: 1000,
		State:    commonpb.ImportState_ImportPersisted,
		Infos: []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
			{
				Key:   importutil2.FailedReason,
				Value: "some_reason",
			},
		},
	}

	mgr.callUnsetIsImportingState = func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}
	// index doesn't exist, the persist task will be set to completed
	ti, err := mgr.updateTaskInfo(info)
	assert.NoError(t, err)

	assert.Equal(t, int64(2), ti.GetId())
	assert.Equal(t, int64(100), ti.GetCollectionId())
	assert.Equal(t, int64(0), ti.GetPartitionId())
	assert.Equal(t, []string{"f2.json"}, ti.GetFiles())
	assert.Equal(t, commonpb.ImportState_ImportPersisted, ti.GetState().GetStateCode())
	assert.Equal(t, int64(1000), ti.GetState().GetRowCount())

	resp := mgr.getTaskState(10000)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)

	resp = mgr.getTaskState(2)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, resp.State)

	resp = mgr.getTaskState(1)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, commonpb.ImportState_ImportStarted, resp.State)

	info = &rootcoordpb.ImportResult{
		TaskId:   1,
		RowCount: 1000,
		State:    commonpb.ImportState_ImportFailed,
		Infos: []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
			{
				Key:   importutil2.FailedReason,
				Value: "some_reason",
			},
		},
	}
	newTaskInfo, err := mgr.updateTaskInfo(info)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ImportState_ImportFailed, newTaskInfo.GetState().GetStateCode())

	newTaskInfo, err = mgr.updateTaskInfo(info)
	assert.Error(t, err)
	assert.Nil(t, newTaskInfo)
}

func TestImportManager_AllocFail(t *testing.T) {
	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		return 0, 0, errors.New("injected failure")
	}
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")
	colID := int64(100)
	mockKv := memkv.NewMemoryKV()
	importServiceFunc := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		Files:          []string{"f1.json"},
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}
	mgr := newImportManager(context.TODO(), mockKv, idAlloc, importServiceFunc, callGetSegmentStates, nil, nil)
	resp := mgr.importJob(context.TODO(), rowReq, colID, 0)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, 0, len(mgr.pendingTasks))
}

func TestImportManager_ListAllTasks(t *testing.T) {
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)

	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}

	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "test_import_task")

	// reject some tasks so there are 3 tasks left in pending list
	fn := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	colID1 := int64(100)
	colID2 := int64(101)
	colName1 := "c1"
	colName2 := "c2"
	partID1 := int64(200)
	partID2 := int64(201)
	partName1 := "p1"
	partName2 := "p2"
	getCollectionName := func(dbName string, collID, partitionID typeutil.UniqueID) (string, string, error) {
		collectionName := "unknow"
		if collID == colID1 {
			collectionName = colName1
		} else if collID == colID2 {
			collectionName = colName2
		}

		partitionName := "unknow"
		if partitionID == partID1 {
			partitionName = partName1
		} else if partitionID == partID2 {
			partitionName = partName2
		}

		return collectionName, partitionName, nil
	}

	mockKv := memkv.NewMemoryKV()
	mgr := newImportManager(context.TODO(), mockKv, idAlloc, fn, callGetSegmentStates, getCollectionName, nil)

	// add 10 tasks for collection1, id from 1 to 10
	file1 := "f1.json"
	rowReq1 := &milvuspb.ImportRequest{
		CollectionName: colName1,
		PartitionName:  partName1,
		Files:          []string{file1},
	}
	repeat1 := 10
	for i := 0; i < repeat1; i++ {
		mgr.importJob(context.TODO(), rowReq1, colID1, partID1)
	}

	// add 5 tasks for collection2, id from 11 to 15, totally 15 tasks
	file2 := "f2.json"
	rowReq2 := &milvuspb.ImportRequest{
		CollectionName: colName2,
		PartitionName:  partName2,
		Files:          []string{file2},
	}
	repeat2 := 5
	for i := 0; i < repeat2; i++ {
		mgr.importJob(context.TODO(), rowReq2, colID2, partID2)
	}

	verifyTaskFunc := func(task *milvuspb.GetImportStateResponse, taskID int64, colID int64, state commonpb.ImportState) {
		assert.Equal(t, commonpb.ErrorCode_Success, task.GetStatus().ErrorCode)
		assert.Equal(t, taskID, task.GetId())
		assert.Equal(t, colID, task.GetCollectionId())
		assert.Equal(t, state, task.GetState())
		compareReq := rowReq1
		if colID == colID2 {
			compareReq = rowReq2
		}
		for _, kv := range task.GetInfos() {
			if kv.GetKey() == importutil2.CollectionName {
				assert.Equal(t, compareReq.GetCollectionName(), kv.GetValue())
			} else if kv.GetKey() == importutil2.PartitionName {
				assert.Equal(t, compareReq.GetPartitionName(), kv.GetValue())
			} else if kv.GetKey() == importutil2.Files {
				assert.Equal(t, strings.Join(compareReq.GetFiles(), ","), kv.GetValue())
			}
		}
	}

	// list all tasks of collection1, id from 1 to 10
	tasks, err := mgr.listAllTasks(colID1, int64(repeat1))
	assert.NoError(t, err)
	assert.Equal(t, repeat1, len(tasks))
	for i := 0; i < repeat1; i++ {
		verifyTaskFunc(tasks[i], int64(i+1), colID1, commonpb.ImportState_ImportPending)
	}

	// list latest 3 tasks of collection1, id from 8 to 10
	limit := 3
	tasks, err = mgr.listAllTasks(colID1, int64(limit))
	assert.NoError(t, err)
	assert.Equal(t, limit, len(tasks))
	for i := 0; i < limit; i++ {
		verifyTaskFunc(tasks[i], int64(i+repeat1-limit+1), colID1, commonpb.ImportState_ImportPending)
	}

	// list all tasks of collection2, id from 11 to 15
	tasks, err = mgr.listAllTasks(colID2, int64(repeat2))
	assert.NoError(t, err)
	assert.Equal(t, repeat2, len(tasks))
	for i := 0; i < repeat2; i++ {
		verifyTaskFunc(tasks[i], int64(i+repeat1+1), colID2, commonpb.ImportState_ImportPending)
	}

	// get the first task state
	resp := mgr.getTaskState(1)
	verifyTaskFunc(resp, int64(1), colID1, commonpb.ImportState_ImportPending)

	// accept tasks to working list
	mgr.callImportService = func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	// there are 15 tasks in working list, and 1 task for collection1 in pending list, totally 16 tasks
	mgr.importJob(context.TODO(), rowReq1, colID1, partID1)
	tasks, err = mgr.listAllTasks(-1, 0)
	assert.NoError(t, err)
	assert.Equal(t, repeat1+repeat2+1, len(tasks))
	for i := 0; i < len(tasks); i++ {
		assert.Equal(t, commonpb.ImportState_ImportStarted, tasks[i].GetState())
	}

	// the id of tasks must be 1,2,3,4,5,6(sequence not guaranteed)
	ids := make(map[int64]struct{})
	for i := 0; i < len(tasks); i++ {
		ids[int64(i)+1] = struct{}{}
	}
	for i := 0; i < len(tasks); i++ {
		delete(ids, tasks[i].Id)
	}
	assert.Equal(t, 0, len(ids))

	// list the latest task, the task is for collection1
	tasks, err = mgr.listAllTasks(-1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
	verifyTaskFunc(tasks[0], int64(repeat1+repeat2+1), colID1, commonpb.ImportState_ImportStarted)

	// failed to load task from store
	mockTxnKV := &mocks.TxnKV{}
	mockTxnKV.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("mock error"))
	mgr.taskStore = mockTxnKV
	tasks, err = mgr.listAllTasks(-1, 0)
	assert.Error(t, err)
	assert.Nil(t, tasks)
}

func TestImportManager_setCollectionPartitionName(t *testing.T) {
	mgr := &importManager{
		getCollectionName: func(dbName string, collID, partitionID typeutil.UniqueID) (string, string, error) {
			if collID == 1 && partitionID == 2 {
				return "c1", "p1", nil
			}
			return "", "", errors.New("Error")
		},
	}

	info := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportStarted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	err := mgr.setCollectionPartitionName("", 1, 2, info)
	assert.NoError(t, err)
	assert.Equal(t, "c1", info.GetCollectionName())
	assert.Equal(t, "p1", info.GetPartitionName())

	err = mgr.setCollectionPartitionName("", 0, 0, info)
	assert.Error(t, err)
}

func TestImportManager_rearrangeTasks(t *testing.T) {
	tasks := make([]*milvuspb.GetImportStateResponse, 0)
	tasks = append(tasks, &milvuspb.GetImportStateResponse{
		Id: 100,
	})
	tasks = append(tasks, &milvuspb.GetImportStateResponse{
		Id: 1,
	})
	tasks = append(tasks, &milvuspb.GetImportStateResponse{
		Id: 50,
	})
	rearrangeTasks(tasks)
	assert.Equal(t, 3, len(tasks))
	assert.Equal(t, int64(1), tasks[0].GetId())
	assert.Equal(t, int64(50), tasks[1].GetId())
	assert.Equal(t, int64(100), tasks[2].GetId())
}

func TestImportManager_isRowbased(t *testing.T) {
	mgr := &importManager{}

	files := []string{"1.json"}
	rb, err := mgr.isRowbased(files)
	assert.NoError(t, err)
	assert.True(t, rb)

	files = []string{"1.json", "2.json"}
	rb, err = mgr.isRowbased(files)
	assert.Error(t, err)
	assert.True(t, rb)

	files = []string{"1.json", "2.npy"}
	rb, err = mgr.isRowbased(files)
	assert.Error(t, err)
	assert.True(t, rb)

	files = []string{"1.npy", "2.npy"}
	rb, err = mgr.isRowbased(files)
	assert.NoError(t, err)
	assert.False(t, rb)
}
