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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestLocalAllocator(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		alloc := NewLocalAllocator(100, 200)
		for i := 0; i < 10; i++ {
			start, end, err := alloc.Alloc(10)
			assert.NoError(t, err)
			assert.Equal(t, int64(100+i*10), start)
			assert.Equal(t, int64(100+(i+1)*10), end)
		}
		_, _, err := alloc.Alloc(10)
		assert.Error(t, err)
		_, err = alloc.AllocOne()
		assert.Error(t, err)
		_, _, err = alloc.Alloc(0)
		assert.Error(t, err)
	})

	t.Run("concurrent", func(t *testing.T) {
		idMap := typeutil.NewConcurrentMap[int64, struct{}]()
		alloc := NewLocalAllocator(111, 1000111)
		fn := func(wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				start, end, err := alloc.Alloc(10)
				assert.NoError(t, err)
				for j := start; j < end; j++ {
					assert.False(t, idMap.Contain(j)) // check no duplicated id
					idMap.Insert(j, struct{}{})
				}
			}
		}
		wg := &sync.WaitGroup{}
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go fn(wg)
		}
		wg.Wait()
		assert.Equal(t, 1000000, idMap.Len())
		// should be exhausted
		assert.Equal(t, alloc.(*localAllocator).idEnd, alloc.(*localAllocator).idStart)
		_, err := alloc.AllocOne()
		assert.Error(t, err)
		t.Logf("%v", err)
	})
}
