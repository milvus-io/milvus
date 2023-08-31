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

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestKVFactory(te *testing.T) {
	te.Run("Test factoy no error", func(t *testing.T) {
		ResetKVClientHandler()
		generateTiKVClient = func(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
			return &txnkv.Client{}, nil
		}
		defer func() {
			generateTiKVClient = createTiKVClient
		}()
		generateETCDClient = func(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
			return &clientv3.Client{}, nil
		}
		defer func() {
			generateETCDClient = createETCDClient
		}()
		etcd_factory := NewETCDFactory(nil)
		assert.NotEqual(te, etcd_factory, nil)
		tikv_factory := NewTiKVFactory(nil)
		assert.NotEqual(te, tikv_factory, nil)
	})
	te.Run("Test factory with client error", func(t *testing.T) {
		ResetKVClientHandler()
		generateTiKVClient = func(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
			return nil, fmt.Errorf("Failed to create client")
		}
		defer func() {
			generateTiKVClient = createTiKVClient
		}()
		generateETCDClient = func(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
			return nil, fmt.Errorf("Failed to create client")
		}
		defer func() {
			generateETCDClient = createETCDClient
		}()
		FatalLogger = func(store string, err error) {}
		defer func() { FatalLogger = FatalLogFunc }()
		ResetKVClientHandler()
		etcd_factory := NewETCDFactory(nil)
		assert.Nil(te, etcd_factory)
		tikv_factory := NewTiKVFactory(nil)
		assert.Nil(te, tikv_factory)
	})
}
