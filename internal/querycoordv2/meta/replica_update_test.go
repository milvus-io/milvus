// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReplicaUpdateHookCompoundPublication(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().ReleaseReplica(mock.Anything, int64(10), int64(100)).Return(nil).Once()
	mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), catalog)
	mgr.putReplicasInMemory(10, newReplica(&querypb.Replica{ID: 100, CollectionID: 10, ResourceGroup: "old"}))

	calls := 0
	mgr.SetReplicaUpdateHook(func(collectionID int64, publish func()) {
		calls++
		require.Equal(t, int64(10), collectionID)
		require.NotNil(t, mgr.Get(ctx, 100))
		require.Nil(t, mgr.Get(ctx, 1))
		publish()
		// Consumers reconcile once against the whole new configuration: they
		// must not see the redundant replica still present after the upsert.
		require.Nil(t, mgr.Get(ctx, 100))
		require.NotNil(t, mgr.Get(ctx, 1))
		require.Len(t, mgr.GetByCollection(ctx, collectionID), 1)
		require.Equal(t, int64(1), mgr.GetByCollection(ctx, collectionID)[0].GetID())
	})

	_, err := mgr.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
		CollectionID: 10,
		Configs:      []*messagespb.LoadReplicaConfig{{ReplicaId: 1, ResourceGroupName: "new"}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestReplicaUpdateHookRemoveCollection(t *testing.T) {
	ctx := context.Background()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(10)).Return(nil).Once()
	mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), catalog)
	mgr.putReplicasInMemory(10,
		newReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "rg"}),
		newReplica(&querypb.Replica{ID: 2, CollectionID: 10, ResourceGroup: "rg"}),
	)

	calls := 0
	mgr.SetReplicaUpdateHook(func(collectionID int64, publish func()) {
		calls++
		require.Len(t, mgr.GetByCollection(ctx, collectionID), 2)
		publish()
		require.Empty(t, mgr.GetByCollection(ctx, collectionID))
		require.Nil(t, mgr.Get(ctx, 1))
		require.Nil(t, mgr.Get(ctx, 2))
	})
	require.NoError(t, mgr.RemoveCollection(ctx, 10))
	require.Equal(t, 1, calls)
}

func TestReplicaUpdateHookPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	catalog := mocks.NewQueryCoordCatalog(t)
	saveErr := errors.New("save failed")
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()
	mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), catalog)
	mgr.putReplicasInMemory(10, newReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "old"}))
	mgr.SetReplicaUpdateHook(func(_ int64, publish func()) {
		t.Error("failed persistence must not publish or invalidate tasks")
		publish()
	})

	err := mgr.Put(ctx, newReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "new"}))
	require.ErrorIs(t, err, saveErr)
	require.Equal(t, "old", mgr.Get(ctx, 1).GetResourceGroup())
}

func TestReplicaUpdateHookSpawnPersistenceFailure(t *testing.T) {
	paramtable.Init()
	for _, operation := range []string{"save", "release"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			catalog := mocks.NewQueryCoordCatalog(t)
			persistErr := errors.New("replica persistence failed")
			if operation == "save" {
				catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(persistErr).Once()
			} else {
				catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
				catalog.EXPECT().ReleaseReplica(mock.Anything, int64(10), int64(100)).Return(persistErr).Once()
			}
			mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), catalog)
			mgr.putReplicasInMemory(10, newReplica(&querypb.Replica{ID: 100, CollectionID: 10, ResourceGroup: "old"}))
			mgr.SetReplicaUpdateHook(func(_ int64, publish func()) {
				t.Error("partially persisted assignment must not publish or invalidate tasks")
				publish()
			})
			_, err := mgr.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
				CollectionID: 10,
				Configs:      []*messagespb.LoadReplicaConfig{{ReplicaId: 1, ResourceGroupName: "new"}},
			})
			require.ErrorIs(t, err, persistErr)
			require.NotNil(t, mgr.Get(ctx, 100))
			require.Nil(t, mgr.Get(ctx, 1))
			require.Len(t, mgr.GetByCollection(ctx, 10), 1)
		})
	}
}

func TestReplicaUpdateHookCanBeClearedDuringPublication(t *testing.T) {
	mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), nil)
	calls := 0
	mgr.SetReplicaUpdateHook(func(_ int64, publish func()) {
		calls++
		// Publication must not retain replicaUpdateMu while invoking clients.
		mgr.SetReplicaUpdateHook(nil)
		publish()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.collLock.Lock(10)
		defer mgr.collLock.Unlock(10)
		mgr.putReplicasInMemory(10, newReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "rg"}))
		mgr.putReplicasInMemory(10, newReplica(&querypb.Replica{ID: 2, CollectionID: 10, ResourceGroup: "rg"}))
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publication hook blocked while clearing itself")
	}
	require.Equal(t, 1, calls)
	require.Len(t, mgr.GetByCollection(context.Background(), 10), 2)
}
