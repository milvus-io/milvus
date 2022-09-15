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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type proxyMock struct {
	types.Proxy
	invalidateCollectionMetaCache func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}

func (p *proxyMock) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return p.invalidateCollectionMetaCache(ctx, request)
}

type mockCore struct {
	types.RootCoordComponent
}

func (m *mockCore) UpdateStateCode(internalpb.StateCode) {
}

func (m *mockCore) SetEtcdClient(etcdClient *clientv3.Client) {
}

func (m *mockCore) SetDataCoord(context.Context, types.DataCoord) error {
	return nil
}
func (m *mockCore) SetIndexCoord(types.IndexCoord) error {
	return nil
}

func (m *mockCore) SetQueryCoord(types.QueryCoord) error {
	return nil
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

func (m *mockCore) SetNewProxyClient(func(sess *sessionutil.Session) (types.Proxy, error)) {
}

type mockDataCoord struct {
	types.DataCoord
}

func (m *mockDataCoord) Init() error {
	return nil
}
func (m *mockDataCoord) Start() error {
	return nil
}
func (m *mockDataCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SubcomponentStates: []*internalpb.ComponentInfo{
			{
				StateCode: internalpb.StateCode_Healthy,
			},
		},
	}, nil
}
func (m *mockDataCoord) Stop() error {
	return fmt.Errorf("stop error")
}

type mockIndex struct {
	types.IndexCoord
}

func (m *mockIndex) Init() error {
	return nil
}

func (m *mockIndex) Stop() error {
	return fmt.Errorf("stop error")
}

type mockQuery struct {
	types.QueryCoord
}

func (m *mockQuery) Init() error {
	return nil
}

func (m *mockQuery) Start() error {
	return nil
}

func (m *mockQuery) Stop() error {
	return fmt.Errorf("stop error")
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	svr := Server{
		rootCoord:   &mockCore{},
		ctx:         ctx,
		cancel:      cancel,
		grpcErrChan: make(chan error),
	}
	Params.InitOnce(typeutil.RootCoordRole)
	Params.Port = 1000000
	err := svr.Run()
	assert.NotNil(t, err)
	assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")

	svr.newDataCoordClient = func(string, *clientv3.Client) types.DataCoord {
		return &mockDataCoord{}
	}
	svr.newIndexCoordClient = func(string, *clientv3.Client) types.IndexCoord {
		return &mockIndex{}
	}
	svr.newQueryCoordClient = func(string, *clientv3.Client) types.QueryCoord {
		return &mockQuery{}
	}

	Params.Port = rand.Int()%100 + 10000

	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	rootcoord.Params.Init()
	rootcoord.Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/test/meta", randVal)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	sessKey := path.Join(rootcoord.Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot)
	_, err = etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	assert.Nil(t, err)
	err = svr.Run()
	assert.Nil(t, err)

	err = svr.Stop()
	assert.Nil(t, err)

}

func initEtcd(etcdEndpoints []string) (*clientv3.Client, error) {
	var etcdCli *clientv3.Client
	connectEtcdFn := func() error {
		etcd, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			return err
		}
		etcdCli = etcd
		return nil
	}
	err := retry.Do(context.TODO(), connectEtcdFn, retry.Attempts(100))
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
