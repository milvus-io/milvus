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

package conc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentQueue(t *testing.T) {
	var queue ConcurrentQueue[int]

	_, ok := queue.Dequeue()
	require.False(t, ok)

	queue.Enqueue(1)
	queue.Enqueue(2)
	assert.Equal(t, 2, queue.Len())

	item, ok := queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 1, item)

	item, ok = queue.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 2, item)
	assert.Equal(t, 0, queue.Len())
}

func TestConcurrentQueueConcurrentEnqueue(t *testing.T) {
	var queue ConcurrentQueue[int]
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Enqueue(i)
		}()
	}
	wg.Wait()

	assert.Equal(t, 100, queue.Len())
	seen := make(map[int]struct{}, 100)
	for {
		item, ok := queue.Dequeue()
		if !ok {
			break
		}
		seen[item] = struct{}{}
	}

	assert.Len(t, seen, 100)
}
