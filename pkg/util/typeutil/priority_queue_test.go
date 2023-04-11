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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type PriorityQueueSuite struct {
	suite.Suite
}

type Item struct {
	ID       int
	Priority int
}

func (suite *PriorityQueueSuite) TestMinPriorityQueue(t *testing.T) {

	pq := NewPriorityQueue(func(a, b Item) bool { return a.Priority > b.Priority })

	for i := 0; i < 5; i++ {
		priority := i % 3
		item := Item{i, priority}
		pq.Push(item)
	}

	item := pq.Pop()
	suite.Equal(item.Priority, 0)
	suite.Equal(item.ID, 0)
	item = pq.Pop()
	suite.Equal(item.Priority, 0)
	suite.Equal(item.ID, 3)
	item = pq.Pop()
	suite.Equal(item.Priority, 1)
	suite.Equal(item.ID, 1)
	item = pq.Pop()
	suite.Equal(item.Priority, 1)
	suite.Equal(item.ID, 4)
	item = pq.Pop()
	suite.Equal(item.Priority, 2)
	println(item.Priority)
	suite.Equal(item.ID, 2)
}

func (suite *PriorityQueueSuite) TestPopPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue(func(a, b Item) bool { return a.Priority > b.Priority })

	for i := 0; i < 1; i++ {
		priority := 1
		item := Item{i, priority}
		pq.Push(item)
	}

	item := pq.Pop()
	suite.Equal(item.Priority, 1)
	suite.Equal(item.ID, 0)
	pq.Push(item)

	// if it's round robin, but not working
	item = pq.Pop()
	suite.Equal(item.Priority, 1)
	suite.Equal(item.ID, 0)
}

func (suite *PriorityQueueSuite) TestMaxPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue(func(a, b Item) bool { return a.Priority < b.Priority })

	for i := 0; i < 5; i++ {
		priority := i % 3
		item := Item{i, -priority}
		pq.Push(item)
	}

	item := pq.Pop()
	suite.Equal(item.Priority, -2)
	suite.Equal(item.ID, 2)
	item = pq.Pop()
	suite.Equal(item.Priority, -1)
	suite.Equal(item.ID, 4)
	item = pq.Pop()
	suite.Equal(item.Priority, -1)
	suite.Equal(item.ID, 1)
	item = pq.Pop()
	suite.Equal(item.Priority, 0)
	suite.Equal(item.ID, 3)
	item = pq.Pop()
	suite.Equal(item.Priority, 0)
	suite.Equal(item.ID, 0)
}
