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
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/cdc/core/pb"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	coremodel "github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/reader"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/cdc/core/writer"
	server_mocks "github.com/milvus-io/milvus/cdc/server/mocks"
	"github.com/milvus-io/milvus/cdc/server/model"
	"github.com/milvus-io/milvus/cdc/server/model/meta"
	"github.com/milvus-io/milvus/cdc/server/model/request"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	endpoints    = []string{"localhost:2379"}
	rootPath     = "cdc"
	serverConfig = &CDCServerConfig{
		EtcdConfig: CDCEtcdConfig{
			Endpoints: endpoints,
			RootPath:  rootPath,
		},
		SourceConfig: MilvusSourceConfig{
			EtcdAddress:     endpoints,
			EtcdRootPath:    "by-dev",
			EtcdMetaSubPath: "meta",
		},
	}
	collectionName = "coll"
	createRequest  = &request.CreateRequest{
		MilvusConnectParam: model.MilvusConnectParam{
			Host: "localhost",
			Port: 19530,
		},
		CollectionInfos: []model.CollectionInfo{
			{
				Name: collectionName,
			},
		},
	}
	starRequest = &request.CreateRequest{
		MilvusConnectParam: model.MilvusConnectParam{
			Host: "localhost",
			Port: 19530,
		},
		CollectionInfos: []model.CollectionInfo{
			{
				Name: "*",
			},
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
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	newClientFunc := func(cfg clientv3.Config) (util.KVApi, error) {
		if i == 0 {
			i++
			return mockEtcdCli, nil
		}
		return nil, errors.New("foo")
	}

	util.MockEtcdClient(newClientFunc, func() {
		assert.Panics(t, func() {
			NewMetaCDC(serverConfig)
		})
	})

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
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
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	key := getTaskInfoPrefix(rootPath)
	taskID1 := "123"
	info1 := &meta.TaskInfo{
		TaskID: taskID1,
		State:  meta.TaskStateRunning,
	}
	value1, _ := json.Marshal(info1)

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
			taskID2 := "456"
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

		t.Run("success", func(t *testing.T) {
			factoryMock := server_mocks.NewCDCFactory(t)
			cdc.factoryCreator = func(_ NewReaderFunc, _ NewWriterFunc) CDCFactory {
				return factoryMock
			}
			call := mockEtcdCli.On("Get", mock.Anything, key, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Key:   []byte(getTaskInfoKey(rootPath, taskID1)),
						Value: value1,
					},
				},
			}, nil)
			defer call.Unset()
			factoryMock.On("NewReader").Return(&reader.DefaultReader{}, nil)
			factoryMock.On("NewWriter").Return(&writer.DefaultWriter{}, nil)

			cdc.ReloadTask()
			cdc.cdcTasks.RLock()
			defer cdc.cdcTasks.RUnlock()
			assert.NotNil(t, cdc.cdcTasks.data[taskID1])
		})
	})
}

func TestValidCreateRequest(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		cdc := NewMetaCDC(serverConfig)
		assertion := assert.New(t)

		createRequest := &request.CreateRequest{}

		// check connect host
		assertion.Error(cdc.validCreateRequest(createRequest))

		// check connect port
		createRequest.MilvusConnectParam.Host = "localhost"
		assertion.Error(cdc.validCreateRequest(createRequest))

		// check connect username and password
		createRequest.MilvusConnectParam.Port = 19530
		createRequest.MilvusConnectParam.Username = "foo"
		assertion.Error(cdc.validCreateRequest(createRequest))
		createRequest.MilvusConnectParam.Username = ""
		createRequest.MilvusConnectParam.Password = "123"
		assertion.Error(cdc.validCreateRequest(createRequest))

		// assert connect timeout
		createRequest.MilvusConnectParam.Username = "foo"
		createRequest.MilvusConnectParam.ConnectTimeout = -1
		assertion.Error(cdc.validCreateRequest(createRequest))

		// check buffer period
		createRequest.MilvusConnectParam.ConnectTimeout = 10
		createRequest.BufferConfig.Period = -1
		assertion.Error(cdc.validCreateRequest(createRequest))

		// check buffer size
		createRequest.BufferConfig.Period = 10
		createRequest.BufferConfig.Size = -1
		assertion.Error(cdc.validCreateRequest(createRequest))

		// check collection info
		createRequest.BufferConfig.Size = 10
		assertion.Error(cdc.validCreateRequest(createRequest))
		cdc.config.MaxNameLength = 5
		createRequest.CollectionInfos = []model.CollectionInfo{
			{},
			{Name: "foooooo"},
		}
		assertion.Error(cdc.validCreateRequest(createRequest))

		createRequest.CollectionInfos = []model.CollectionInfo{
			{Name: "*"},
			{Name: "foooooo"},
		}
		assertion.Error(cdc.validCreateRequest(createRequest))

		// fail connect milvus
		createRequest.CollectionInfos = []model.CollectionInfo{
			{Name: "fo"},
			{Name: "ao"},
		}
		assertion.Error(cdc.validCreateRequest(createRequest))

		// mock server
		createRequest.MilvusConnectParam.Username = ""
		createRequest.MilvusConnectParam.Password = ""
		lis, err := net.Listen("tcp",
			fmt.Sprintf("%s:%d", createRequest.MilvusConnectParam.Host, createRequest.MilvusConnectParam.Port))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		s := grpc.NewServer()
		milvuspb.RegisterMilvusServiceServer(s, &milvuspb.UnimplementedMilvusServiceServer{})
		go func() {
			if err := s.Serve(lis); err != nil {
				assert.FailNow(t, err.Error())
			}
		}()
		defer s.Stop()
		assertion.NoError(cdc.validCreateRequest(createRequest))
	})
}

func MockMilvusServer(t *testing.T) func() {
	lis, err := net.Listen("tcp",
		fmt.Sprintf("%s:%d", "localhost", 19530))
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	s := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(s, &milvuspb.UnimplementedMilvusServiceServer{})
	go func() {
		if err := s.Serve(lis); err != nil {
			assert.FailNow(t, err.Error())
		}
	}()
	return func() {
		s.Stop()
	}
}

func TestCreateRequest(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		cdc := NewMetaCDC(serverConfig)
		assertion := assert.New(t)
		var (
			resp *request.CreateResponse
			err  error
		)
		stopFunc := MockMilvusServer(t)
		defer stopFunc()

		t.Run("check error", func(t *testing.T) {
			resp, err = cdc.Create(&request.CreateRequest{})
			assertion.Nil(resp)
			assertion.Error(err)
		})

		t.Run("success", func(t *testing.T) {
			factoryMock := server_mocks.NewCDCFactory(t)
			cdc.factoryCreator = func(_ NewReaderFunc, _ NewWriterFunc) CDCFactory {
				return factoryMock
			}
			info := &meta.TaskInfo{
				State: meta.TaskStateInitial,
			}
			infoByte, _ := json.Marshal(info)
			call1 := mockEtcdCli.On("Get", mock.Anything, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: infoByte,
					},
				},
			}, nil)
			defer call1.Unset()
			factoryMock.On("NewReader").Return(&reader.DefaultReader{}, nil)
			factoryMock.On("NewWriter").Return(&writer.DefaultWriter{}, nil)

			call2 := mockEtcdCli.On("Put", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("put error"))
			_, err = cdc.Create(createRequest)
			assertion.Error(err)
			call2.Unset()

			call2 = mockEtcdCli.On("Put", mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)
			defer call2.Unset()
			resp, err = cdc.Create(createRequest)
			assertion.NotEmpty(resp.TaskID)
			assertion.NoError(err)
			cdc.cdcTasks.RLock()
			assert.NotNil(t, cdc.cdcTasks.data[resp.TaskID])
			cdc.cdcTasks.RUnlock()

			// duplicate collection
			_, err = cdc.Create(createRequest)
			assertion.Error(err)

			// star collection
			_, err = cdc.Create(starRequest)
			assertion.NoError(err)

			_, err = cdc.Create(&request.CreateRequest{
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "localhost",
					Port: 19530,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "col2",
					},
				},
			})
			assertion.Error(err)
		})
	})
}

func TestDeleteRequest(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		cdc := NewMetaCDC(serverConfig)
		assertion := assert.New(t)
		var (
			resp *request.DeleteResponse
			err  error
		)

		t.Run("no task", func(t *testing.T) {
			resp, err = cdc.Delete(&request.DeleteRequest{TaskID: "foo"})
			assertion.Nil(resp)
			assertion.Error(err)
		})

		stopFunc := MockMilvusServer(t)
		defer stopFunc()

		t.Run("success", func(t *testing.T) {
			factoryMock := server_mocks.NewCDCFactory(t)
			cdc.factoryCreator = func(_ NewReaderFunc, _ NewWriterFunc) CDCFactory {
				return factoryMock
			}
			info := &meta.TaskInfo{
				CollectionInfos: []model.CollectionInfo{
					{Name: collectionName},
				},
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "localhost",
					Port: 19530,
				},
				State: meta.TaskStateInitial,
			}
			infoByte, _ := json.Marshal(info)
			call1 := mockEtcdCli.On("Get", mock.Anything, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: infoByte,
					},
				},
			}, nil)
			defer call1.Unset()
			call2 := mockEtcdCli.On("Put", mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)
			defer call2.Unset()
			factoryMock.On("NewReader").Return(&reader.DefaultReader{}, nil)
			factoryMock.On("NewWriter").Return(&writer.DefaultWriter{}, nil)

			createResp, err := cdc.Create(createRequest)
			assertion.NotEmpty(createResp.TaskID)
			assertion.NoError(err)

			_, err = cdc.Create(starRequest)
			assertion.NoError(err)

			call3 := mockEtcdCli.On("Txn", mock.Anything).Return(&MockTxn{err: errors.New("txn error")})
			_, err = cdc.Delete(&request.DeleteRequest{TaskID: createResp.TaskID})
			assertion.Error(err)
			call3.Unset()

			call3 = mockEtcdCli.On("Txn", mock.Anything).Return(&MockTxn{})
			defer call3.Unset()
			resp, err = cdc.Delete(&request.DeleteRequest{TaskID: createResp.TaskID})
			assertion.NotNil(resp)
			assertion.NoError(err)

			_, err = cdc.Create(&request.CreateRequest{
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "localhost",
					Port: 19530,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "col2",
					},
				},
			})
			assertion.Error(err)

			_, err = cdc.Create(createRequest)
			assertion.NoError(err)
		})
	})
}

type MockEmptyReader struct {
	reader.DefaultReader
}

func (m MockEmptyReader) StartRead(_ context.Context) <-chan *coremodel.CDCData {
	return make(<-chan *coremodel.CDCData)
}

func TestPauseResumeRequest(t *testing.T) {
	mockEtcdCli := mocks.NewKVApi(t)
	mockEtcdCli.On("Endpoints").Return(endpoints)
	mockEtcdCli.On("Status", mock.Anything, endpoints[0]).Return(&clientv3.StatusResponse{}, nil)
	util.EtcdOpRetryTime = 1
	defer func() {
		util.EtcdOpRetryTime = 5
	}()

	util.MockEtcdClient(func(cfg clientv3.Config) (util.KVApi, error) {
		return mockEtcdCli, nil
	}, func() {
		cdc := NewMetaCDC(serverConfig)
		assertion := assert.New(t)

		t.Run("pause no task", func(t *testing.T) {
			resp, err := cdc.Pause(&request.PauseRequest{TaskID: "foo"})
			assertion.Nil(resp)
			assertion.Error(err)
		})

		t.Run("resume no task", func(t *testing.T) {
			resp, err := cdc.Resume(&request.ResumeRequest{TaskID: "foo"})
			assertion.Nil(resp)
			assertion.Error(err)
		})

		stopFunc := MockMilvusServer(t)
		defer stopFunc()

		t.Run("success", func(t *testing.T) {
			factoryMock := server_mocks.NewCDCFactory(t)
			cdc.factoryCreator = func(_ NewReaderFunc, _ NewWriterFunc) CDCFactory {
				return factoryMock
			}
			info := &meta.TaskInfo{
				State: meta.TaskStateInitial,
			}
			infoByte, _ := json.Marshal(info)
			call2 := mockEtcdCli.On("Put", mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.PutResponse{}, nil)
			defer call2.Unset()
			factoryMock.On("NewReader").Return(&MockEmptyReader{}, nil)
			factoryMock.On("NewWriter").Return(&writer.DefaultWriter{}, nil)
			call1 := mockEtcdCli.On("Get", mock.Anything, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: infoByte,
					},
				},
			}, nil)
			createResp, err := cdc.Create(createRequest)
			call1.Unset()
			assertion.NotEmpty(createResp.TaskID)
			assertion.NoError(err)

			info = &meta.TaskInfo{
				State: meta.TaskStateRunning,
			}
			infoByte, _ = json.Marshal(info)
			call1 = mockEtcdCli.On("Get", mock.Anything, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: infoByte,
					},
				},
			}, nil)
			pauseResp, err := cdc.Pause(&request.PauseRequest{TaskID: createResp.TaskID})
			call1.Unset()
			assertion.NotNil(pauseResp)
			assertion.NoError(err)

			info = &meta.TaskInfo{
				State: meta.TaskStatePaused,
			}
			infoByte, _ = json.Marshal(info)
			call1 = mockEtcdCli.On("Get", mock.Anything, mock.Anything).Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: infoByte,
					},
				},
			}, nil)
			resumeResp, err := cdc.Resume(&request.ResumeRequest{TaskID: createResp.TaskID})
			call1.Unset()
			assertion.NotNil(resumeResp)
			assertion.NoError(err)
		})

	})
}

func TestGetShouldReadFunc(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		f := GetShouldReadFunc(&meta.TaskInfo{
			CollectionInfos: []model.CollectionInfo{
				{Name: "foo1"},
				{Name: "foo2"},
			},
		})
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo1"}}))
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo2"}}))
		assert.False(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo"}}))
	})

	t.Run("star", func(t *testing.T) {
		f := GetShouldReadFunc(&meta.TaskInfo{
			CollectionInfos: []model.CollectionInfo{
				{Name: "*"},
			},
		})
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo1"}}))
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo2"}}))
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo"}}))
	})

	t.Run("mix star", func(t *testing.T) {
		f := GetShouldReadFunc(&meta.TaskInfo{
			CollectionInfos: []model.CollectionInfo{
				{Name: "*"},
			},
			ExcludeCollections: []string{"foo1", "foo2"},
		})
		assert.False(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo1"}}))
		assert.False(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo2"}}))
		assert.True(t, f(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "foo"}}))
	})
}
