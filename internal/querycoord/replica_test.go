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

package querycoord

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type mockMetaKV struct {
	mock.Mock
}

func (m *mockMetaKV) Load(key string) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiLoad(keys []string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) LoadWithPrefix(key string) ([]string, []string, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) Save(key string, value string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiSave(kvs map[string]string) error {
	args := m.Called(kvs)
	return args.Error(0)
}

func (m *mockMetaKV) Remove(key string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiRemove(keys []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) RemoveWithPrefix(key string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) Close() {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) GetPath(key string) string {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) LoadWithRevision(key string) ([]string, []string, int64, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) Watch(key string) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) WatchWithPrefix(key string) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) SaveWithLease(key string, value string, id clientv3.LeaseID) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) SaveWithIgnoreLease(key string, value string) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) Grant(ttl int64) (id clientv3.LeaseID, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) KeepAlive(id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) CompareValueAndSwap(key string, value string, target string, opts ...clientv3.OpOption) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockMetaKV) CompareVersionAndSwap(key string, version int64, target string, opts ...clientv3.OpOption) error {
	panic("not implemented") // TODO: Implement
}

func TestReplicaInfos_ApplyBalancePlan(t *testing.T) {
	kv := &mockMetaKV{}
	kv.On("MultiSave", mock.AnythingOfType("map[string]string")).Return(nil)
	t.Run("source replica not exist", func(t *testing.T) {
		replicas := NewReplicaInfos()
		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{1},
			sourceReplica: 1,
			targetReplica: invalidReplicaID,
		}, kv)
		assert.Error(t, err)
	})

	t.Run("target replica not exist", func(t *testing.T) {
		replicas := NewReplicaInfos()
		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{1},
			sourceReplica: invalidReplicaID,
			targetReplica: 1,
		}, kv)

		assert.Error(t, err)
	})

	t.Run("add node to replica", func(t *testing.T) {
		replicas := NewReplicaInfos()

		replicas.Insert(&milvuspb.ReplicaInfo{
			ReplicaID: 1,
			NodeIds:   []int64{1},
		})

		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{2},
			sourceReplica: invalidReplicaID,
			targetReplica: 1,
		}, kv)

		assert.NoError(t, err)

		info, has := replicas.Get(1)
		require.True(t, has)
		require.NotNil(t, info)

		assert.Contains(t, info.GetNodeIds(), int64(2))

		result := replicas.GetReplicasByNodeID(2)
		assert.Equal(t, 1, len(result))
		kv.AssertCalled(t, "MultiSave", mock.AnythingOfType("map[string]string"))
	})

	t.Run("remove node from replica", func(t *testing.T) {
		replicas := NewReplicaInfos()

		replicas.Insert(&milvuspb.ReplicaInfo{
			ReplicaID: 1,
			NodeIds:   []int64{1},
		})

		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{1},
			sourceReplica: 1,
			targetReplica: invalidReplicaID,
		}, kv)

		assert.NoError(t, err)

		info, has := replicas.Get(1)
		require.True(t, has)
		require.NotNil(t, info)

		assert.NotContains(t, info.GetNodeIds(), int64(1))

		result := replicas.GetReplicasByNodeID(1)
		assert.Equal(t, 0, len(result))
		kv.AssertCalled(t, "MultiSave", mock.AnythingOfType("map[string]string"))
	})

	t.Run("remove non-existing node from replica", func(t *testing.T) {
		replicas := NewReplicaInfos()

		replicas.Insert(&milvuspb.ReplicaInfo{
			ReplicaID: 1,
			NodeIds:   []int64{1},
		})

		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{2},
			sourceReplica: 1,
			targetReplica: invalidReplicaID,
		}, kv)

		assert.NoError(t, err)
	})

	t.Run("save to etcd fail", func(t *testing.T) {
		kv := &mockMetaKV{}
		kv.On("MultiSave", mock.AnythingOfType("map[string]string")).Return(errors.New("mocked error"))
		replicas := NewReplicaInfos()

		replicas.Insert(&milvuspb.ReplicaInfo{
			ReplicaID: 1,
			NodeIds:   []int64{1},
		})

		err := replicas.ApplyBalancePlan(&balancePlan{
			nodes:         []UniqueID{2},
			sourceReplica: invalidReplicaID,
			targetReplica: 1,
		}, kv)

		kv.AssertCalled(t, "MultiSave", mock.AnythingOfType("map[string]string"))
		assert.Error(t, err)
	})
}
