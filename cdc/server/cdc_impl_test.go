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

	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus/cdc/server/model/meta"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	endpoints    = []string{"localhost:2379"}
	rootPath     = "/cdc"
	serverConfig = &CDCServerConfig{
		EtcdConfig: CDCEtcdConfig{
			Endpoints: endpoints,
			RootPath:  rootPath,
		},
		SourceConfig: MilvusSourceConfig{
			EtcdAddress: endpoints,
		},
	}
)

func TestNewMetaCDC(t *testing.T) {
	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return nil, errors.New("foo")
	}, func() {
		assert.Panics(t, func() {
			NewMetaCDC(serverConfig)
		})
	})

	i := 0
	mockKVApi := mocks.NewKVApi(t)
	mockKVApi.On("Endpoints").Return(endpoints)
	mockKVApi.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	newClientFunc := func(cfg clientv3.Config) (util.KVApi, error) {
		if i == 0 {
			i++
			return mockKVApi, nil
		}
		return nil, errors.New("foo")
	}

	util.MockEtcdClient(newClientFunc, func() {
		assert.Panics(t, func() {
			NewMetaCDC(serverConfig)
		})
	})

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockKVApi, nil
	}, func() {
		assert.NotPanics(t, func() {
			NewMetaCDC(serverConfig)
		})
	})
}

func TestReloadTask(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)

	key := getTaskInfoPrefix(rootPath)

	taskID1 := "123"
	info1 := &meta.TaskInfo{
		TaskID: taskID1,
	}
	value1, _ := json.Marshal(info1)

	taskID2 := "456"
	//info2 := &meta.TaskInfo{
	//	TaskID: taskID2,
	//}
	//value2, _ := json.Marshal(info2)

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		cdc := NewMetaCDC(serverConfig)
		t.Run("etcd get error", func(t *testing.T) {
			call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).
				Return(nil, errors.New("etcd error"))
			defer call.Unset()

			assert.Panics(t, func() {
				cdc.ReloadTask()
			})
		})

		t.Run("unmarshal error", func(t *testing.T) {
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
			assert.Panics(t, func() {
				cdc.ReloadTask()
			})
		})
	})
}
