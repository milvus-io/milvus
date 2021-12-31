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
	etcdKV, err := tsoutil.NewTSOKVBase(etcdEndpoints, "/test/root/kv", "gidTest")
	assert.NoError(t, err)

	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", etcdKV)

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

	t.Run("Alloc2", func(t *testing.T) {
		count1 := uint32(2 << 18)
		id1, err := gTestIDAllocator.allocator.GenerateTSO(count1)
		assert.Nil(t, err)

		count2 := uint32(2 << 8)
		id2, err := gTestIDAllocator.allocator.GenerateTSO(count2)
		assert.Nil(t, err)
		assert.Equal(t, id2-id1, uint64(count2))

	})
}
