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

package kv

import (
	"errors"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type MockBaseKV struct {
	InMemKv map[string]string
}

func (m *MockBaseKV) Load(key string) (string, error) {
	if val, ok := m.InMemKv[key]; ok {
		return val, nil
	}
	return "", nil
}

func (m *MockBaseKV) MultiLoad(keys []string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) LoadWithPrefix(key string) ([]string, []string, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) Save(key string, value string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) MultiSave(kvs map[string]string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) Remove(key string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) MultiRemove(keys []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) RemoveWithPrefix(key string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockBaseKV) Close() {
	panic("not implemented") // TODO: Implement
}

type MockTxnKV struct {
	MockBaseKV
}

func (m *MockTxnKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockTxnKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implemented") // TODO: Implement
}

func (m *MockTxnKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("not implemented") // TODO: Implement
}

type MockMetaKV struct {
	MockTxnKV

	LoadWithPrefixMockErr      bool
	SaveWithIgnoreLeaseMockErr bool
}

func (m *MockMetaKV) GetPath(key string) string {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	if m.LoadWithPrefixMockErr {
		return nil, nil, errors.New("mock err")
	}
	keys := make([]string, 0, len(m.InMemKv))
	values := make([]string, 0, len(m.InMemKv))
	for k, v := range m.InMemKv {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
			values = append(values, v)
		}
	}
	return keys, values, nil
}

func (m *MockMetaKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) LoadWithRevisionAndVersions(key string) ([]string, []string, []int64, int64, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) LoadWithRevision(key string) ([]string, []string, int64, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) Watch(key string) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) WatchWithPrefix(key string) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) SaveWithLease(key, value string, id clientv3.LeaseID) error {
	m.InMemKv[key] = value
	log.Debug("Doing SaveWithLease", zap.String("key", key))
	return nil
}

func (m *MockMetaKV) SaveWithIgnoreLease(key, value string) error {
	if m.SaveWithIgnoreLeaseMockErr {
		return errors.New("mock error")
	}
	m.InMemKv[key] = value
	log.Debug("Doing SaveWithIgnoreLease", zap.String("key", key))
	return nil
}

func (m *MockMetaKV) Grant(ttl int64) (id clientv3.LeaseID, err error) {
	return 1, nil
}

func (m *MockMetaKV) KeepAlive(id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) CompareValueAndSwap(key, value, target string, opts ...clientv3.OpOption) (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockMetaKV) CompareVersionAndSwap(key string, version int64, target string, opts ...clientv3.OpOption) (bool, error) {
	panic("not implemented") // TODO: Implement
}
