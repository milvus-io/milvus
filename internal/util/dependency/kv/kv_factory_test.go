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

package kvfactory

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestKVFactory(te *testing.T) {
	te.Run("Test factory no error", func(t *testing.T) {
		createTiKV = func() (*txnkv.Client, error) {
			return &txnkv.Client{}, nil
		}
		defer func() {
			createTiKV = createTiKVClient
		}()
		createETCD = func() (*clientv3.Client, error) {
			return &clientv3.Client{}, nil
		}
		defer func() {
			createETCD = createETCDClient
		}()

		etcdRootPathParser = func() string {
			return "test"
		}
		defer func() {
			etcdRootPathParser = defaultEtcdRootPathParser
		}()
		tiKVRootPathParser = func() string {
			return "test"
		}
		defer func() {
			tiKVRootPathParser = defaultTiKVRootPathParser
		}()

		etcd_factory := NewETCDFactory()
		assert.NotEqual(te, etcd_factory, nil)
		tikv_factory := NewTiKVFactory()
		assert.NotEqual(te, tikv_factory, nil)
	})
	te.Run("Test factory with client error", func(t *testing.T) {
		createTiKV = func() (*txnkv.Client, error) {
			return nil, fmt.Errorf("Failed to create client")
		}
		defer func() {
			createTiKV = createTiKVClient
		}()
		createETCD = func() (*clientv3.Client, error) {
			return nil, fmt.Errorf("Failed to create client")
		}
		defer func() {
			createETCD = createETCDClient
		}()
		etcdRootPathParser = func() string {
			return "test"
		}
		defer func() {
			etcdRootPathParser = defaultEtcdRootPathParser
		}()
		tiKVRootPathParser = func() string {
			return "test"
		}
		defer func() {
			tiKVRootPathParser = defaultTiKVRootPathParser
		}()
		fatalLogger = func(store string, err error) {}
		defer func() { fatalLogger = defaultFatalLogFunc }()
		etcd_factory := NewETCDFactory()
		assert.Nil(te, etcd_factory)
		tikv_factory := NewTiKVFactory()
		assert.Nil(te, tikv_factory)
	})
}
