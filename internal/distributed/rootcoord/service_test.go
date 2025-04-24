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
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
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

func (m *mockCore) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
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

func (m *mockCore) SetTiKVClient(client *txnkv.Client) {
}

func (m *mockCore) SetDataCoordClient(client types.DataCoordClient) error {
	return nil
}

func (m *mockCore) SetQueryCoordClient(client types.QueryCoordClient) error {
	return nil
}

func (m *mockCore) SetProxyCreator(func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error)) {
}

func (m *mockCore) RegisterStreamingCoordGRPCService(server *grpc.Server) {
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
	return errors.New("stop error")
}

func (m *mockCore) GracefulStop() {
}

func TestRun(t *testing.T) {
	paramtable.Init()
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		coordclient.ResetRegistration()
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		rcServerConfig := &paramtable.Get().RootCoordGrpcServerCfg
		oldPort := rcServerConfig.Port.GetValue()
		paramtable.Get().Save(rcServerConfig.Port.Key, "1000000")
		svr, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		err = svr.Prepare()
		assert.Error(t, err)
		assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")
		paramtable.Get().Save(rcServerConfig.Port.Key, oldPort)

		svr, err = NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, svr)
		svr.rootCoord = &mockCore{}

		mockDataCoord := mocks.NewMockDataCoordClient(t)
		mockDataCoord.EXPECT().Close().Return(nil)
		svr.newDataCoordClient = func(_ context.Context) types.DataCoordClient {
			return mockDataCoord
		}

		mockQueryCoord := mocks.NewMockQueryCoordClient(t)
		mockQueryCoord.EXPECT().Close().Return(nil)
		svr.newQueryCoordClient = func(_ context.Context) types.QueryCoordClient {
			return mockQueryCoord
		}

		paramtable.Get().Save(rcServerConfig.Port.Key, fmt.Sprintf("%d", rand.Int()%100+10010))
		etcdConfig := &paramtable.Get().EtcdCfg

		rand.Seed(time.Now().UnixNano())
		randVal := rand.Int()
		rootPath := fmt.Sprintf("/%d/test", randVal)
		rootcoord.Params.Save("etcd.rootPath", rootPath)
		// Need to reset global etcd to follow new path
		// Need to reset global etcd to follow new path
		kvfactory.CloseEtcdClient()

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
		err = svr.Prepare()
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
			assert.Equal(t, commonpb.ErrorCode_Success, ret.GetStatus().GetErrorCode())
		})

		t.Run("AlterDatabase", func(t *testing.T) {
			ret, err := svr.AlterDatabase(ctx, nil)
			assert.Nil(t, err)
			assert.True(t, merr.Ok(ret))
		})

		err = svr.Stop()
		assert.NoError(t, err)
	}
}

func TestServerRun_DataCoordClientInitErr(t *testing.T) {
	paramtable.Init()
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		coordclient.ResetRegistration()
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mockDataCoord := mocks.NewMockDataCoordClient(t)
		mockDataCoord.EXPECT().Close().Return(nil)
		server.newDataCoordClient = func(_ context.Context) types.DataCoordClient {
			return mockDataCoord
		}
		err = server.Prepare()
		assert.NoError(t, err)
		assert.Panics(t, func() { server.Run() })

		err = server.Stop()
		assert.NoError(t, err)
	}
}

func TestServerRun_DataCoordClientStartErr(t *testing.T) {
	paramtable.Init()
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		coordclient.ResetRegistration()
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mockDataCoord := mocks.NewMockDataCoordClient(t)
		mockDataCoord.EXPECT().Close().Return(nil)
		server.newDataCoordClient = func(_ context.Context) types.DataCoordClient {
			return mockDataCoord
		}
		err = server.Prepare()
		assert.NoError(t, err)
		assert.Panics(t, func() { server.Run() })

		err = server.Stop()
		assert.NoError(t, err)
	}
}

func TestServerRun_QueryCoordClientInitErr(t *testing.T) {
	paramtable.Init()
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		coordclient.ResetRegistration()
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mockQueryCoord := mocks.NewMockQueryCoordClient(t)
		mockQueryCoord.EXPECT().Close().Return(nil)
		server.newQueryCoordClient = func(_ context.Context) types.QueryCoordClient {
			return mockQueryCoord
		}
		err = server.Prepare()
		assert.NoError(t, err)
		assert.Panics(t, func() { server.Run() })

		err = server.Stop()
		assert.NoError(t, err)
	}
}

func TestServer_QueryCoordClientStartErr(t *testing.T) {
	paramtable.Init()
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		coordclient.ResetRegistration()
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		ctx := context.Background()
		getTiKVClient = func(cfg *paramtable.TiKVConfig) (*txnkv.Client, error) {
			return tikv.SetupLocalTxn(), nil
		}
		defer func() {
			getTiKVClient = tikv.GetTiKVClient
		}()
		server, err := NewServer(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, server)

		mockQueryCoord := mocks.NewMockQueryCoordClient(t)
		mockQueryCoord.EXPECT().Close().Return(nil)
		server.newQueryCoordClient = func(_ context.Context) types.QueryCoordClient {
			return mockQueryCoord
		}
		err = server.Prepare()
		assert.NoError(t, err)
		assert.Panics(t, func() { server.Run() })

		err = server.Stop()
		assert.NoError(t, err)
	}
}
