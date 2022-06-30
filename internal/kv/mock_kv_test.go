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
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const testKey = "key"
const testValue = "value"

func TestMockKV_MetaKV(t *testing.T) {
	mockKv := &MockMetaKV{}
	mockKv.InMemKv = make(map[string]string)

	var err error
	value, err := mockKv.Load(testKey)
	assert.Equal(t, "", value)
	assert.NoError(t, err)

	assert.Panics(t, func() {
		mockKv.MultiLoad([]string{testKey})
	})

	_, _, err = mockKv.LoadWithPrefix(testKey)
	assert.NoError(t, err)

	assert.Panics(t, func() {
		mockKv.Save(testKey, testValue)
	})

	assert.Panics(t, func() {
		mockKv.MultiSave(map[string]string{testKey: testValue})
	})

	assert.Panics(t, func() {
		mockKv.Remove(testKey)
	})

	assert.Panics(t, func() {
		mockKv.MultiRemove([]string{testKey})
	})

	assert.Panics(t, func() {
		mockKv.RemoveWithPrefix(testKey)
	})

	assert.Panics(t, func() {
		mockKv.Close()
	})

	assert.Panics(t, func() {
		mockKv.MultiSaveAndRemove(map[string]string{testKey: testValue}, []string{testKey})
	})

	assert.Panics(t, func() {
		mockKv.MultiRemoveWithPrefix([]string{testKey})
	})

	assert.Panics(t, func() {
		mockKv.MultiSaveAndRemoveWithPrefix(map[string]string{testKey: testValue}, []string{testKey})
	})

	assert.Panics(t, func() {
		mockKv.GetPath(testKey)
	})

	assert.Panics(t, func() {
		mockKv.LoadWithPrefix2(testKey)
	})

	assert.Panics(t, func() {
		mockKv.LoadWithPrefix2(testKey)
	})

	assert.Panics(t, func() {
		mockKv.LoadWithRevisionAndVersions(testKey)
	})

	assert.Panics(t, func() {
		mockKv.LoadWithRevision(testKey)
	})

	assert.Panics(t, func() {
		mockKv.Watch(testKey)
	})

	assert.Panics(t, func() {
		mockKv.WatchWithPrefix(testKey)
	})

	assert.Panics(t, func() {
		mockKv.WatchWithRevision(testKey, 100)
	})

	err = mockKv.SaveWithLease(testKey, testValue, 100)
	assert.NoError(t, err)

	err = mockKv.SaveWithIgnoreLease(testKey, testValue)
	assert.NoError(t, err)

	leaseID, err := mockKv.Grant(100)
	assert.Equal(t, clientv3.LeaseID(1), leaseID)
	assert.NoError(t, err)

	assert.Panics(t, func() {
		mockKv.KeepAlive(100)
	})
	assert.Panics(t, func() {
		mockKv.CompareValueAndSwap(testKey, testValue, testValue)
	})
	assert.Panics(t, func() {
		mockKv.CompareVersionAndSwap(testKey, 100, testKey)
	})
}
