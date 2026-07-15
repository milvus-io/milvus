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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProxyClientManager_SyncDataViewGate(t *testing.T) {
	ctx := context.Background()
	req := &proxypb.SyncDataViewGateRequest{
		CollectionID:  1,
		GatedFieldIds: []int64{100},
		Generation:    1,
	}

	t.Run("empty proxy list", func(t *testing.T) {
		pcm := NewProxyClientManager(DefaultProxyCreator)
		assert.NoError(t, pcm.SyncDataViewGate(ctx, req))
	})

	t.Run("success", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(1, p1)
		assert.NoError(t, pcm.SyncDataViewGate(ctx, req))
	})

	t.Run("rpc error is propagated", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("boom"))
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(1, p1)
		assert.Error(t, pcm.SyncDataViewGate(ctx, req))
	})

	t.Run("bad status is propagated", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(merr.Status(errors.New("bad")), nil)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(1, p1)
		assert.Error(t, pcm.SyncDataViewGate(ctx, req))
	})

	t.Run("node not found is skipped", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(nil, merr.ErrNodeNotFound)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(1, p1)
		assert.NoError(t, pcm.SyncDataViewGate(ctx, req))
	})

	t.Run("unimplemented fails closed with a retriable error", func(t *testing.T) {
		// An old (rolling-upgrade) proxy that can't participate in the gate/drain is alive and may hold an
		// in-flight complex-delete, so it is not evidence of drain: fail-closed with a retriable error.
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().SyncDataViewGate(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnimplemented)
		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(1, p1)
		err := pcm.SyncDataViewGate(ctx, req)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrCollectionSchemaChangeInProgress))
	})
}
