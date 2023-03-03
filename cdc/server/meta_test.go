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

package server

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/cdc/server/model/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestGetTaskInfoPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_info/"
	actual := getTaskInfoPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskInfoKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_info/1234"
	actual := getTaskInfoKey(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_position/"
	actual := getTaskCollectionPositionPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefixWithTaskID(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_position/1234/"
	actual := getTaskCollectionPositionPrefixWithTaskID(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	collectionID := int64(5678)
	expected := "/root/task_position/1234/5678"
	actual := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskInfo(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	rootPath := "/tasks"
	taskID := "123"
	key := getTaskInfoKey(rootPath, taskID)
	info := &meta.TaskInfo{
		TaskID: taskID,
	}
	value, _ := json.Marshal(info)
	t.Run("success", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call.Unset()

		got, err := getTaskInfo(mockEtcdCli, rootPath, taskID)

		assert.NoError(t, err)
		assert.Equal(t, info, got)
	})

	t.Run("etcd error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()

		got, err := getTaskInfo(mockEtcdCli, rootPath, taskID)

		assert.Nil(t, got)
		assert.Error(t, err)
	})

	t.Run("not found error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{},
		}, nil)
		defer call.Unset()

		got, err := getTaskInfo(mockEtcdCli, rootPath, taskID)

		assert.Nil(t, got)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, NotFoundErr))
	})

	t.Run("json unmarshal error", func(t *testing.T) {
		invalidValue := []byte(`"task_id": 123`) // task_id should be a string
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: invalidValue,
				},
			},
		}, nil)
		defer call.Unset()

		got, err := getTaskInfo(mockEtcdCli, rootPath, taskID)

		assert.Nil(t, got)
		assert.Error(t, err)
	})
}

func TestGetAllTaskInfo(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	key := getTaskInfoPrefix(rootPath)
	taskID1 := "123"
	info1 := &meta.TaskInfo{
		TaskID: taskID1,
	}
	value1, _ := json.Marshal(info1)

	taskID2 := "456"
	info2 := &meta.TaskInfo{
		TaskID: taskID2,
	}
	value2, _ := json.Marshal(info2)
	t.Run("success", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(getTaskInfoKey(rootPath, taskID1)),
					Value: value1,
				},
				{
					Key:   []byte(getTaskInfoKey(rootPath, taskID2)),
					Value: value2,
				},
			},
		}, nil)
		defer call.Unset()

		got, err := getAllTaskInfo(mockEtcdCli, rootPath)

		assert.NoError(t, err)
		assert.Equal(t, info1, got[0])
		assert.Equal(t, info2, got[1])
	})

	t.Run("etcd error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).Return(nil, errors.New("etcd error"))
		defer call.Unset()

		got, err := getAllTaskInfo(mockEtcdCli, rootPath)

		assert.Nil(t, got)
		assert.Error(t, err)
	})

	t.Run("not found error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{},
		}, nil)
		defer call.Unset()

		got, err := getAllTaskInfo(mockEtcdCli, rootPath)

		assert.Nil(t, got)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, NotFoundErr))
	})

	t.Run("json unmarshal error", func(t *testing.T) {
		invalidValue := []byte(`"task_id": 123`) // task_id should be a string
		call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(getTaskInfoKey(rootPath, taskID1)),
					Value: value1,
				},
				{
					Key:   []byte(getTaskInfoKey(rootPath, taskID2)),
					Value: invalidValue,
				},
			},
		}, nil)
		defer call.Unset()

		got, err := getAllTaskInfo(mockEtcdCli, rootPath)

		assert.Nil(t, got)
		assert.Error(t, err)
	})
}

func TestUpdateTaskState(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	taskID := "123"
	key := getTaskInfoKey(rootPath, taskID)
	info := &meta.TaskInfo{
		TaskID: taskID,
		State:  meta.TaskStateInitial,
	}
	value, _ := json.Marshal(info)
	t.Run("success", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(&clientv3.PutResponse{}, nil)
		defer call2.Unset()

		err := updateTaskState(mockEtcdCli, rootPath, taskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial})
		assert.NoError(t, err)

		err = updateTaskState(mockEtcdCli, rootPath, taskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStatePaused})
		assert.Error(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()
		err := updateTaskState(mockEtcdCli, rootPath, taskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial})
		assert.Error(t, err)
	})

	t.Run("etcd error", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(nil, errors.New("etcd error"))
		defer call2.Unset()

		err := updateTaskState(mockEtcdCli, rootPath, taskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial})
		assert.Error(t, err)
	})
}

func TestUpdateTaskFailedReason(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	taskID := "123"
	key := getTaskInfoKey(rootPath, taskID)
	info := &meta.TaskInfo{
		TaskID: taskID,
	}
	value, _ := json.Marshal(info)
	t.Run("success", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(&clientv3.PutResponse{}, nil)
		defer call2.Unset()

		err := updateTaskFailedReason(mockEtcdCli, rootPath, taskID, "fail reason")
		assert.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()
		err := updateTaskState(mockEtcdCli, rootPath, taskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial})
		assert.Error(t, err)
	})

	t.Run("etcd error", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(nil, errors.New("etcd error"))
		defer call2.Unset()

		err := updateTaskFailedReason(mockEtcdCli, rootPath, taskID, "fail reason")
		assert.Error(t, err)
	})
}

func TestUpdateTaskCollectionPosition(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	taskID := "123"
	collectionName := "col1"
	var collectionID int64 = 1000
	key := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	info := &meta.TaskCollectionPosition{
		TaskID:         taskID,
		CollectionName: collectionName,
	}
	value, _ := json.Marshal(info)
	t.Run("success-no-data", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(&clientv3.PutResponse{}, nil)
		defer call2.Unset()

		err := updateTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID, collectionName, "ch1", &commonpb.KeyDataPair{})
		assert.NoError(t, err)
	})

	t.Run("success-empty-data", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		pName := "ch1"
		kd := &commonpb.KeyDataPair{Key: "xxx", Data: []byte(`"task_id": 123`)}
		infoTest := &meta.TaskCollectionPosition{
			TaskID:         taskID,
			CollectionName: collectionName,
			Positions: map[string]*commonpb.KeyDataPair{
				pName: kd,
			},
		}
		infoByte, _ := json.Marshal(infoTest)
		call2 := mockEtcdCli.On("Put", mock.Anything, key, util.ToString(infoByte)).Return(&clientv3.PutResponse{}, nil).Once()
		defer call2.Unset()

		err := updateTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID, collectionName, pName, kd)
		assert.NoError(t, err)
	})

	t.Run("success-a-data", func(t *testing.T) {
		pName := "ch1"
		kd := &commonpb.KeyDataPair{Key: "xxx", Data: []byte(`"task_id": 123`)}
		info := &meta.TaskCollectionPosition{
			TaskID:         taskID,
			CollectionName: collectionName,
			Positions: map[string]*commonpb.KeyDataPair{
				pName: kd,
			},
		}
		value, _ := json.Marshal(info)
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()

		kd2 := &commonpb.KeyDataPair{Key: "xxx", Data: []byte(`"task_id": 345`)}
		infoTest := &meta.TaskCollectionPosition{
			TaskID:         taskID,
			CollectionName: collectionName,
			Positions: map[string]*commonpb.KeyDataPair{
				pName: kd2,
			},
		}
		infoByte, _ := json.Marshal(infoTest)
		call2 := mockEtcdCli.On("Put", mock.Anything, key, util.ToString(infoByte)).Return(&clientv3.PutResponse{}, nil)
		defer call2.Unset()

		err := updateTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID, collectionName, pName, kd2)
		assert.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()
		pName := "ch1"
		kd := &commonpb.KeyDataPair{Key: "xxx", Data: []byte(`"task_id": 123`)}

		err := updateTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID, collectionName, pName, kd)
		assert.Error(t, err)
	})

	t.Run("etcd error", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Put", mock.Anything, key, mock.Anything).Return(nil, errors.New("etcd error"))
		defer call2.Unset()

		err := updateTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID, collectionName, "ch1", &commonpb.KeyDataPair{})
		assert.Error(t, err)
	})
}

func TestDeleteTaskCollectionPosition(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	taskID := "123"
	var collectionID int64 = 1000
	key := getTaskCollectionPositionKey(rootPath, taskID, collectionID)

	t.Run("success", func(t *testing.T) {
		call := mockEtcdCli.On("Delete", mock.Anything, key).Return(&clientv3.DeleteResponse{}, nil)
		defer call.Unset()

		err := deleteTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID)
		assert.NoError(t, err)
	})

	t.Run("etcd error", func(t *testing.T) {
		call := mockEtcdCli.On("Delete", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()

		err := deleteTaskCollectionPosition(mockEtcdCli, rootPath, taskID, collectionID)
		assert.Error(t, err)
	})
}

type MockTxn struct {
	err error
}

func (m *MockTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return m
}

func (m *MockTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return m
}

func (m *MockTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	return m
}

func (m *MockTxn) Commit() (*clientv3.TxnResponse, error) {
	return &clientv3.TxnResponse{}, m.err
}

func TestDeleteTask(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	rootPath := "/tasks"
	taskID := "123"
	key := getTaskInfoKey(rootPath, taskID)
	t.Run("success", func(t *testing.T) {
		info := &meta.TaskInfo{
			TaskID: taskID,
		}
		value, _ := json.Marshal(info)
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte(key),
					Value: value,
				},
			},
		}, nil)
		defer call1.Unset()
		call2 := mockEtcdCli.On("Txn", mock.Anything).Return(&MockTxn{})
		defer call2.Unset()

		returnInfo, err := deleteTask(mockEtcdCli, rootPath, taskID)
		assert.Equal(t, taskID, returnInfo.TaskID)
		assert.NoError(t, err)
	})

	t.Run("get error", func(t *testing.T) {
		call := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call.Unset()

		_, err := deleteTask(mockEtcdCli, rootPath, taskID)
		assert.Error(t, err)
	})

	t.Run("etcd error", func(t *testing.T) {
		call1 := mockEtcdCli.On("Get", mock.Anything, key).Return(nil, errors.New("etcd error"))
		defer call1.Unset()
		call2 := mockEtcdCli.On("Txn", mock.Anything).Return(&MockTxn{})
		defer call2.Unset()

		_, err := deleteTask(mockEtcdCli, rootPath, taskID)
		assert.Error(t, err)
	})
}
