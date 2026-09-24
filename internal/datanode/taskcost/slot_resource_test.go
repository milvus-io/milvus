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

package taskcost

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

func TestResourceFromSlot(t *testing.T) {
	const giB = int64(1) << 30
	capacity := taskcommon.Resource{CPU: 16, Memory: 64 * giB}

	// A quarter of the node's slots is a quarter of the node.
	assert.Equal(t, taskcommon.Resource{CPU: 4, Memory: 16 * giB},
		ResourceFromSlot(25, 100, capacity))

	// All of them is all of it.
	assert.Equal(t, capacity, ResourceFromSlot(100, 100, capacity))

	// More slots than the node has: it holds the whole node, never more.
	assert.Equal(t, capacity, ResourceFromSlot(1000, 100, capacity))

	// Rounds up, so a task holding a sliver is never charged nothing.
	small := ResourceFromSlot(1, 1000, taskcommon.Resource{CPU: 8, Memory: 1001})
	assert.Equal(t, int64(1), small.CPU)
	assert.Equal(t, int64(2), small.Memory)

	// Nothing to convert from: the ledger stays as it was without this helper.
	assert.True(t, ResourceFromSlot(0, 100, capacity).IsZero())
	assert.True(t, ResourceFromSlot(-1, 100, capacity).IsZero())
	assert.True(t, ResourceFromSlot(10, 0, capacity).IsZero())
	assert.True(t, ResourceFromSlot(10, 100, taskcommon.Resource{}).IsZero())
}

// TestResourceFromSlot_BoundedByTheNode is the property the design rests on:
// tasks whose slots fit the node book about the node and never materially more,
// so a pool running keyless and priced tasks side by side does not report
// memory it does not have.
//
// "About" is exact for the proportional part and loose by the rounding: up to
// one unit per task, always upwards. Upwards is the safe direction -- the node
// looks fuller than it is and refuses work, rather than accepting work it
// cannot hold.
func TestResourceFromSlot_BoundedByTheNode(t *testing.T) {
	const giB = int64(1) << 30
	capacity := taskcommon.Resource{CPU: 16, Memory: 64 * giB}
	const totalSlots = int64(96)

	// An arbitrary split of the node's slots across tasks.
	slots := []int64{1, 3, 8, 16, 31, 37} // sums to totalSlots
	var booked taskcommon.Resource
	for _, slot := range slots {
		booked = booked.Add(ResourceFromSlot(slot, totalSlots, capacity))
	}
	slack := int64(len(slots))
	// Never under-books: under-booking is what lets a node accept what it
	// cannot hold, and is the whole reason this helper exists.
	assert.GreaterOrEqual(t, booked.Memory, capacity.Memory)
	assert.GreaterOrEqual(t, booked.CPU, capacity.CPU)
	// And over-books only by the rounding.
	assert.LessOrEqual(t, booked.Memory, capacity.Memory+slack)
	assert.LessOrEqual(t, booked.CPU, capacity.CPU+slack)
}

func TestSlotShareNoOverflow(t *testing.T) {
	// A terabyte of memory over an awkward slot total must not wrap.
	const tiB = int64(1) << 40
	got := slotShare(tiB, 7, 13)
	assert.Positive(t, got)
	assert.LessOrEqual(t, got, tiB)
	assert.Equal(t, (tiB*7+12)/13, got)
}
