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

package grpcrootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/sessionutil"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type mockCore struct {
	types.RootCoordComponent
}

func (m *mockCore) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockCore) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockCore) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil
}

func (m *mockCore) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockCore) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (m *mockCore) UpdateStateCode(commonpb.StateCode) {
}

func (m *mockCore) SetAddress(address string) {
}

func (m *mockCore) SetEtcdClient(etcdClient *clientv3.Client) {
}

func (m *mockCore) SetDataCoord(types.DataCoord) error {
	return nil
}

func (m *mockCore) SetQueryCoord(types.QueryCoord) error {
	return nil
}

func (m *mockCore) SetProxyCreator(func(ctx context.Context, addr string) (types.Proxy, error)) {
}

func (m *mockCore) Register() error {
	return nil
}

func (m *mockCore) Init() error {
	return nil
}

func (m *mockCore) Start() error {
	return nil
}

func (m *mockCore) Stop() error {
	return fmt.Errorf("stop error")
}

type mockDataCoord struct {
	types.DataCoord
	initErr  error
	startErr error
}

func (m *mockDataCoord) Init() error {
	return m.initErr
}
func (m *mockDataCoord) Start() error {
	return m.startErr
}
func (m *mockDataCoord) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil
}
func (m *mockDataCoord) Stop() error {
	return fmt.Errorf("stop error")
}

type mockQueryCoord struct {
	types.QueryCoord
	initErr  error
	startErr error
}

func (m *mockQueryCoord) Init() error {
	return m.initErr
}

func (m *mockQueryCoord) Start() error {
	return m.startErr
}

func (m *mockQueryCoord) Stop() error {
	return fmt.Errorf("stop error")
}

func TestRun(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	svr := Server{
		rootCoord:   &mockCore{},
		ctx:         ctx,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	rcServerConfig := &paramtable.Get().RootCoordGrpcServerCfg
	paramtable.Get().Save(rcServerConfig.Port.Key, "1000000")
	err := svr.Run()
	assert.Error(t, err)
	assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")

	svr.newDataCoordClient = func(string, *clientv3.Client) types.DataCoord {
		return &mockDataCoord{}
	}
	svr.newQueryCoordClient = func(string, *clientv3.Client) types.QueryCoord {
		return &mockQueryCoord{}
	}

	paramtable.Get().Save(rcServerConfig.Port.Key, fmt.Sprintf("%d", rand.Int()%100+10000))
	etcdConfig := &paramtable.Get().EtcdCfg

	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	rootPath := fmt.Sprintf("/%d/test", randVal)
	rootcoord.Params.BaseTable.Save("etcd.rootPath", rootPath)
	rootcoord.Params.Init()

	etcdCli, err := etcd.GetEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	sessKey := path.Join(rootcoord.Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	assert.NoError(t, err)
	err = svr.Run()
	assert.NoError(t, err)

	t.Run("CheckHealth", func(t *testing.T) {
		ret, err := svr.CheckHealth(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, ret.IsHealthy)
	})

	t.Run("RenameCollection", func(t *testing.T) {
		_, err := svr.RenameCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		ret, err := svr.CreateDatabase(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, ret.ErrorCode)
	})

	t.Run("DropDatabase", func(t *testing.T) {
		ret, err := svr.DropDatabase(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, ret.ErrorCode)
	})

	t.Run("ListDatabases", func(t *testing.T) {
		ret, err := svr.ListDatabases(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, ret.Status.ErrorCode)
	})
	err = svr.Stop()
	assert.NoError(t, err)
}

func TestServerRun_DataCoordClientInitErr(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.newDataCoordClient = func(string, *clientv3.Client) types.DataCoord {
		return &mockDataCoord{initErr: errors.New("mock datacoord init error")}
	}
	assert.Panics(t, func() { server.Run() })

	err = server.Stop()
	assert.NoError(t, err)
}

func TestServerRun_DataCoordClientStartErr(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.newDataCoordClient = func(string, *clientv3.Client) types.DataCoord {
		return &mockDataCoord{startErr: errors.New("mock datacoord start error")}
	}
	assert.Panics(t, func() { server.Run() })

	err = server.Stop()
	assert.NoError(t, err)
}

func TestServerRun_QueryCoordClientInitErr(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.newQueryCoordClient = func(string, *clientv3.Client) types.QueryCoord {
		return &mockQueryCoord{initErr: errors.New("mock querycoord init error")}
	}
	assert.Panics(t, func() { server.Run() })

	err = server.Stop()
	assert.NoError(t, err)
}

func TestServer_QueryCoordClientStartErr(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	server.newQueryCoordClient = func(string, *clientv3.Client) types.QueryCoord {
		return &mockQueryCoord{startErr: errors.New("mock querycoord start error")}
	}
	assert.Panics(t, func() { server.Run() })

	err = server.Stop()
	assert.NoError(t, err)
}
