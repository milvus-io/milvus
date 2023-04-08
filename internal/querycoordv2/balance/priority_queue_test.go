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

package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinPriorityQueue(t *testing.T) {
	pq := newPriorityQueue()

	for i := 0; i < 5; i++ {
		priority := i % 3
		nodeItem := newNodeItem(priority, int64(i))
		pq.push(&nodeItem)
	}

	item := pq.pop()
	assert.Equal(t, item.getPriority(), 0)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(0))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 0)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(3))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(1))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(4))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 2)
	println(item.getPriority())
	assert.Equal(t, item.(*nodeItem).nodeID, int64(2))
}

func TestPopPriorityQueue(t *testing.T) {
	pq := newPriorityQueue()

	for i := 0; i < 1; i++ {
		priority := 1
		nodeItem := newNodeItem(priority, int64(i))
		pq.push(&nodeItem)
	}

	item := pq.pop()
	assert.Equal(t, item.getPriority(), 1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(0))
	pq.push(item)

	// if it's round robin, but not working
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(0))
}

func TestMaxPriorityQueue(t *testing.T) {
	pq := newPriorityQueue()

	for i := 0; i < 5; i++ {
		priority := i % 3
		nodeItem := newNodeItem(-priority, int64(i))
		pq.push(&nodeItem)
	}

	item := pq.pop()
	assert.Equal(t, item.getPriority(), -2)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(2))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), -1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(4))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), -1)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(1))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 0)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(3))
	item = pq.pop()
	assert.Equal(t, item.getPriority(), 0)
	assert.Equal(t, item.(*nodeItem).nodeID, int64(0))
}
