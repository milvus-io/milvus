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

package indexcoord

import (
	"container/heap"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
)

const QueueLen = 10

func newPriorityQueue() *PriorityQueue {
	ret := &PriorityQueue{
		policy: PeekClientV0,
	}
	for i := 1; i <= QueueLen; i++ {
		item := &PQItem{
			key:      UniqueID(i),
			priority: i,
			index:    i - 1,
			totalMem: 1000,
		}
		ret.items = append(ret.items, item)
	}
	heap.Init(ret)
	return ret
}

func TestPriorityQueue_Len(t *testing.T) {
	pq := newPriorityQueue()

	assert.Equal(t, QueueLen, pq.Len())
	pq = nil
}

func TestPriorityQueue_Push(t *testing.T) {
	pq := newPriorityQueue()
	for i := 1; i <= QueueLen; i++ {
		item := &PQItem{
			key:      UniqueID(i),
			priority: i,
			index:    i,
		}
		pq.Push(item)
		assert.Equal(t, i+QueueLen, pq.Len())
	}
}

func TestPriorityQueue_Remove(t *testing.T) {
	pq := newPriorityQueue()
	cnt := 0
	for i := 1; i <= QueueLen; i++ {
		if i%2 == 0 {
			continue
		}
		pq.Remove(UniqueID(i))
		cnt++
	}
	assert.Equal(t, QueueLen-cnt, pq.Len())
}

func TestPriorityQueue_UpdatePriority(t *testing.T) {
	pq := newPriorityQueue()
	key := UniqueID(pq.Len() / 2)
	pq.UpdatePriority(key, -pq.Len())
	peekKey := pq.Peek(10, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, key, peekKey)
}

func TestPriorityQueue_IncPriority(t *testing.T) {
	pq := newPriorityQueue()
	key := UniqueID(pq.Len() / 2)
	pq.IncPriority(key, -pq.Len())
	peekKey := pq.Peek(10, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, key, peekKey)
}

func TestPriorityQueue_SetMemory(t *testing.T) {
	ret := &PriorityQueue{
		policy: PeekClientV1,
	}
	for i := 0; i < QueueLen; i++ {
		item := &PQItem{
			key:      UniqueID(i),
			priority: i,
			index:    i,
			totalMem: 1000,
		}
		ret.items = append(ret.items, item)
	}
	heap.Init(ret)
	ret.SetMemory(1, 100000)
	peeKey := ret.Peek(99999, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, int64(1), peeKey)
}

func TestPriorityQueue(t *testing.T) {
	ret := &PriorityQueue{
		policy: PeekClientV0,
	}
	for i := 0; i < 4; i++ {
		item := &PQItem{
			key:      UniqueID(i),
			priority: 0,
			index:    i,
			totalMem: 1000,
		}
		ret.items = append(ret.items, item)
	}
	heap.Init(ret)

	peeKey1 := ret.Peek(10, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, int64(0), peeKey1)
	ret.IncPriority(peeKey1, 1)

	peeKey2 := ret.Peek(100, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, int64(1), peeKey2)
	ret.IncPriority(peeKey2, 1)

	ret.IncPriority(peeKey1, -1)
	ret.IncPriority(peeKey2, -1)

	peeKey1 = ret.Peek(10, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, int64(3), peeKey1)
	ret.IncPriority(peeKey1, 1)

	peeKey2 = ret.Peek(10, []*commonpb.KeyValuePair{}, []*commonpb.KeyValuePair{})
	assert.Equal(t, int64(2), peeKey2)
}
