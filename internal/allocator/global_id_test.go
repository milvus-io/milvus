// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package allocator

import (
	"os"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

var gTestIDAllocator *GlobalIDAllocator

func TestGlobalTSOAllocator_All(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase(etcdEndpoints, "/test/root/kv", "gidTest"))

	t.Run("Initialize", func(t *testing.T) {
		err := gTestIDAllocator.Initialize()
		assert.Nil(t, err)
	})

	t.Run("AllocOne", func(t *testing.T) {
		one, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		ano, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		assert.NotEqual(t, one, ano)
	})

	t.Run("Alloc", func(t *testing.T) {
		count := uint32(2 << 10)
		idStart, idEnd, err := gTestIDAllocator.Alloc(count)
		assert.Nil(t, err)
		assert.Equal(t, count, uint32(idEnd-idStart))
	})
}
