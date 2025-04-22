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

package proxyutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type UniqueID = int64

var (
	Params      = paramtable.Get()
	TestProxyID = int64(1)
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
		return nil, errors.New("grpc error")
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
		return nil, errors.New("grpc error")
	}
	return merr.Success(), nil
}

func (p *proxyMock) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func TestProxyClientManager_AddProxyClients(t *testing.T) {
	proxyCreator := func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
		return nil, errors.New("failed")
	}

	pcm := NewProxyClientManager(proxyCreator)

	session := &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 100,
			Address:  "localhost",
		},
	}

	sessions := []*sessionutil.Session{session}
	pcm.AddProxyClients(sessions)
}

func TestProxyClientManager_AddProxyClient(t *testing.T) {
	proxyCreator := func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
		return nil, errors.New("failed")
	}

	pcm := NewProxyClientManager(proxyCreator)

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
		pcm := NewProxyClientManager(DefaultProxyCreator)
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("error mock InvalidateCollectionMetaCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(merr.Status(errors.New("mock error")), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock proxy service down", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil, merr.ErrNodeNotFound)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_InvalidateCredentialCache(t *testing.T) {
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		mockErr := errors.New("mock error")
		p1.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(merr.Status(mockErr), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_UpdateCredentialCache(t *testing.T) {
	TestProxyID := int64(1001)
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)

		err := pcm.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		mockErr := errors.New("mock error")
		p1.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(merr.Status(mockErr), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_RefreshPolicyInfoCache(t *testing.T) {
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)

		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("error mock RefreshPolicyInfoCache"))

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(merr.Status(errors.New("mock error")), nil)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)

		p1.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_TestGetProxyCount(t *testing.T) {
	p1 := mocks.NewMockProxyClient(t)
	pcm := NewProxyClientManager(DefaultProxyCreator)
	pcm.proxyClient.Insert(TestProxyID, p1)

	assert.Equal(t, pcm.GetProxyCount(), 1)
}

func TestProxyClientManager_GetProxyMetrics(t *testing.T) {
	TestProxyID := int64(1001)
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)
		_, err := pcm.GetProxyMetrics(ctx)
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().GetProxyMetrics(mock.Anything, mock.Anything).Return(nil, errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		_, err := pcm.GetProxyMetrics(ctx)
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		mockErr := errors.New("mock error")
		p1.EXPECT().GetProxyMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: merr.Status(mockErr)}, nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		_, err := pcm.GetProxyMetrics(ctx)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().GetProxyMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: merr.Success()}, nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		_, err := pcm.GetProxyMetrics(ctx)
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_SetRates(t *testing.T) {
	TestProxyID := int64(1001)
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)
		err := pcm.SetRates(ctx, &proxypb.SetRatesRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SetRates(mock.Anything, mock.Anything).Return(nil, errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.SetRates(ctx, &proxypb.SetRatesRequest{})
		assert.Error(t, err)
	})

	t.Run("mock error code", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		mockErr := errors.New("mock error")
		p1.EXPECT().SetRates(mock.Anything, mock.Anything).Return(merr.Status(mockErr), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.SetRates(ctx, &proxypb.SetRatesRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SetRates(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.SetRates(ctx, &proxypb.SetRatesRequest{})
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_GetComponentStates(t *testing.T) {
	TestProxyID := int64(1001)
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)
		_, err := pcm.GetComponentStates(ctx)
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		_, err := pcm.GetComponentStates(ctx)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{Status: merr.Success()}, nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		_, err := pcm.GetComponentStates(ctx)
		assert.NoError(t, err)
	})
}

func TestProxyClientManager_InvalidateShardLeaderCache(t *testing.T) {
	TestProxyID := int64(1001)
	t.Run("empty proxy list", func(t *testing.T) {
		ctx := context.Background()
		pcm := NewProxyClientManager(DefaultProxyCreator)

		err := pcm.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
		assert.NoError(t, err)
	})

	t.Run("mock rpc error", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).Return(nil, errors.New("error mock InvalidateCredentialCache"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(TestProxyID, p1)
		err := pcm.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
		assert.NoError(t, err)
	})
}
