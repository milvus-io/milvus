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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type proxyMock struct {
	types.ProxyClient
	collArray []string
	collIDs   []UniqueID
	mutex     sync.Mutex

	returnError     bool
	returnGrpcError bool
}

func (p *proxyMock) Stop() error {
	return nil
}

func (p *proxyMock) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.returnError {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	if p.returnGrpcError {
		return nil, fmt.Errorf("grpc error")
	}
	p.collArray = append(p.collArray, request.CollectionName)
	p.collIDs = append(p.collIDs, request.CollectionID)
	return merr.Success(), nil
}

func (p *proxyMock) GetCollArray() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ret := make([]string, 0, len(p.collArray))
	ret = append(ret, p.collArray...)
	return ret
}

func (p *proxyMock) GetCollIDs() []UniqueID {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ret := p.collIDs
	return ret
}

func (p *proxyMock) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	if p.returnError {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	if p.returnGrpcError {
		return nil, fmt.Errorf("grpc error")
	}
	return merr.Success(), nil
}

func (p *proxyMock) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func TestProxyClientManager_GetProxyClients(t *testing.T) {
	paramtable.Init()

	core, err := NewCore(context.Background(), nil)
	assert.NoError(t, err)
	cli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	defer cli.Close()
	assert.NoError(t, err)
	core.etcdCli = cli
	core.proxyCreator = func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
		return nil, errors.New("failed")
	}

	pcm := newProxyClientManager(core.proxyCreator)

	session := &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 100,
			Address:  "localhost",
		},
	}

	sessions := []*sessionutil.Session{session}
	pcm.GetProxyClients(sessions)
}

func TestProxyClientManager_AddProxyClient(t *testing.T) {
	paramtable.Init()

	core, err := NewCore(context.Background(), nil)
	assert.NoError(t, err)
	cli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer cli.Close()
	core.etcdCli = cli

	core.proxyCreator = func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
		return nil, errors.New("failed")
	}

	pcm := newProxyClientManager(core.proxyCreator)

	session := &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 100,
			Address:  "localhost",
		},
	}

	pcm.AddProxyClient(session)
}

func TestProxyClientManager_InvalidateCollectionMetaCache(t *testing.T) {
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{}}
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), errors.New("error mock InvalidateCollectionMetaCache")
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		mockErr := errors.New("mock error")
		p1.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return merr.Status(mockErr), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock proxy service down", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return nil, merr.ErrNodeNotFound
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}

		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.InvalidateCollectionMetaCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_InvalidateCredentialCache(t *testing.T) {
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{}}
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.InvalidateCredentialCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), errors.New("error mock InvalidateCredentialCache")
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		mockErr := errors.New("mock error")
		p1.InvalidateCredentialCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
			return merr.Status(mockErr), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.InvalidateCredentialCacheFunc = func(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_RefreshPolicyInfoCache(t *testing.T) {
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{}}
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), errors.New("error mock RefreshPolicyInfoCache")
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		mockErr := errors.New("mock error")
		p1.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
			return merr.Status(mockErr), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := newMockProxy()
		p1.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}
		pcm := &proxyClientManager{proxyClient: map[int64]types.ProxyClient{
			TestProxyID: p1,
		}}
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.NoError(t, err)
	})
}
