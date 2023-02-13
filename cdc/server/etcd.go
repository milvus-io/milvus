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
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/cdc/server/model/meta"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	taskInfoPrefix     = "task_info"
	taskPositionPrefix = "task_position"
)

func getTaskInfoPrefix(rootPath string) string {
	return path.Join(rootPath, taskInfoPrefix) + "/"
}

func getTaskInfoKey(rootPath string, taskID string) string {
	return path.Join(rootPath, taskInfoPrefix, taskID)
}

func getTaskCollectionPositionPrefix(rootPath string) string {
	return path.Join(rootPath, taskPositionPrefix) + "/"
}

func getTaskCollectionPositionPrefixWithTaskID(rootPath string, taskID string) string {
	return path.Join(rootPath, taskPositionPrefix, taskID) + "/"
}

func getTaskCollectionPositionKey(rootPath string, taskID string, collectionID int64) string {
	return path.Join(rootPath, taskPositionPrefix, taskID, strconv.FormatInt(collectionID, 10))
}

func getTaskInfo(etcdCli util.KVApi, rootPath string, taskID string) (*meta.TaskInfo, error) {
	key := getTaskInfoKey(rootPath, taskID)
	resp, err := util.EtcdGet(etcdCli, key)
	if err != nil {
		log.Warn("fail to get kv", zap.String("key", key), zap.Error(err))
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key)
	}
	info := &meta.TaskInfo{}
	err = json.Unmarshal(resp.Kvs[0].Value, info)
	if err != nil {
		log.Warn("fail to unmarshal the task info", zap.String("key", key), zap.Error(err))
		return nil, err
	}
	return info, nil
}

func getAllTaskInfo(etcdCli util.KVApi, rootPath string) ([]*meta.TaskInfo, error) {
	key := getTaskInfoPrefix(rootPath)
	resp, err := util.EtcdGet(etcdCli, key, clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get kvs with prefix", zap.String("key", key), zap.Error(err))
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, NewNotFoundError(key)
	}
	infos := make([]*meta.TaskInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &meta.TaskInfo{}
		err = json.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal the task info", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func updateTaskState(etcdCli util.KVApi, rootPath string, taskID string, newState meta.TaskState, oldStates []meta.TaskState) error {
	key := getTaskInfoKey(rootPath, taskID)
	info, err := getTaskInfo(etcdCli, rootPath, taskID)
	if err != nil {
		return err
	}
	if !lo.Contains(oldStates, info.State) {
		oldStateStrs := lo.Map[meta.TaskState, string](oldStates, func(taskState meta.TaskState, i int) string {
			return taskState.String()
		})
		return errors.Errorf("the task state can be only set to [%s] when current state is %v, but current state is %s. You can retry it.",
			newState.String(), oldStateStrs, info.State.String())
	}
	oldState := info.State
	info.State = newState
	infoByte, err := json.Marshal(info)
	if err != nil {
		log.Warn("fail to marshal the task info", zap.String("key", key), zap.Error(err))
		return err
	}
	err = util.EtcdPut(etcdCli, getTaskInfoKey(rootPath, info.TaskID), util.ToString(infoByte))
	if err != nil {
		log.Warn("fail to put the task info to etcd", zap.String("key", key), zap.Error(err))
		return err
	}
	taskNumVec.UpdateState(newState, oldState)
	return nil
}

func updateTaskFailedReason(etcdCli util.KVApi, rootPath string, taskID string, reason string) error {
	key := getTaskInfoKey(rootPath, taskID)
	info, err := getTaskInfo(etcdCli, rootPath, taskID)
	if err != nil {
		return err
	}
	info.FailedReason = reason
	infoByte, err := json.Marshal(info)
	if err != nil {
		log.Warn("fail to marshal the task info", zap.String("key", key), zap.Error(err))
		return err
	}
	err = util.EtcdPut(etcdCli, getTaskInfoKey(rootPath, info.TaskID), util.ToString(infoByte))
	if err != nil {
		log.Warn("fail to put the task info to etcd", zap.String("key", key), zap.Error(err))
		return err
	}
	return nil
}

func updateTaskCollectionPosition(etcdCli util.KVApi, rootPath string, taskID string, collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) error {
	key := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	resp, err := util.EtcdGet(etcdCli, key)
	if err != nil {
		log.Warn("fail to get the task position", zap.String("key", key), zap.Error(err))
		return err
	}
	saveCollectionPosition := func(metaPosition *meta.TaskCollectionPosition) error {
		metaPositionByte, err := json.Marshal(metaPosition)
		if err != nil {
			log.Warn("fail to marshal the collection position", zap.String("key", key), zap.Error(err))
			return err
		}
		err = util.EtcdPut(etcdCli, key, util.ToString(metaPositionByte))
		if err != nil {
			log.Warn("fail to save the collection position", zap.String("key", key), zap.Error(err))
		}
		return err
	}

	if len(resp.Kvs) == 0 {
		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         taskID,
			CollectionName: collectionName,
			Positions: map[string]*commonpb.KeyDataPair{
				pChannelName: position,
			},
		}
		return saveCollectionPosition(metaPosition)
	}

	metaPosition := &meta.TaskCollectionPosition{}
	err = json.Unmarshal(resp.Kvs[0].Value, metaPosition)
	if err != nil {
		log.Warn("fail to unmarshal the collection position", zap.String("key", key), zap.Error(err))
		return err
	}
	metaPosition.Positions[pChannelName] = position
	return saveCollectionPosition(metaPosition)
}

func deleteTaskCollectionPosition(etcdCli util.KVApi, rootPath string, taskID string, collectionID int64) error {
	key := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	err := util.EtcdDelete(etcdCli, key)
	if err != nil {
		log.Warn("fail to delete the task position", zap.String("key", key), zap.Error(err))
	}
	return err
}

func deleteTask(etcdCli util.KVApi, rootPath string, taskID string) (*meta.TaskInfo, error) {
	taskInfoKey := getTaskInfoKey(rootPath, taskID)
	taskCollectionPositionKey := getTaskCollectionPositionPrefixWithTaskID(rootPath, taskID)
	var err error
	info, err := getTaskInfo(etcdCli, rootPath, taskID)
	if err != nil {
		log.Warn("fail to get the task meta before delete the task", zap.String("task_id", taskID), zap.Error(err))
		return nil, errors.New("the task maybe has deleted")
	}
	err = util.EtcdTxn(etcdCli, func(txn clientv3.Txn) error {
		_, err = txn.Then(
			clientv3.OpDelete(taskInfoKey),
			clientv3.OpDelete(taskCollectionPositionKey, clientv3.WithPrefix()),
		).Commit()
		return err
	})
	if err != nil {
		log.Warn("fail to delete the task meta", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	taskNumVec.Delete(info.State)
	return info, nil
}
