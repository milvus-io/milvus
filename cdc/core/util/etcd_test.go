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

package util

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMockEtcdClient(t *testing.T) {
	newEtcdClient = func(cfg clientv3.Config) (KVApi, error) {
		return nil, errors.New("foo")
	}
	_, err := newEtcdClient(clientv3.Config{})
	assert.Error(t, err)
	MockEtcdClient(func(cfg clientv3.Config) (KVApi, error) {
		return nil, nil
	}, func() {
		_, err := newEtcdClient(clientv3.Config{})
		assert.NoError(t, err)
	})
	_, err = newEtcdClient(clientv3.Config{})
	assert.Error(t, err)
}
