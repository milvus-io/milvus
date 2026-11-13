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

import "github.com/milvus-io/milvus/pkg/v3/taskcommon"

// ResourceFromSlot converts a scalar task slot into the share of the node that
// slot stands for: slot/totalSlots of the node's cpu and of its memory.
//
// It exists for a task that arrives with no cpu/memory estimate, which is what
// a coordinator older than those request properties always sends. Booking zero
// for such a task makes the node report memory it does not have, and a newer
// coordinator then stacks memory-priced work on top of it. That is not a
// bounded upgrade window for a shared worker pool: a pool serving several
// Milvus versions runs both kinds of task side by side indefinitely, so the
// node must account for both or its report means nothing.
//
// The conversion is honest inside one node, though not between nodes. Both
// totals describe the same machine under the same standalone discount
// (index.CalculateNodeSlots and the node's task capacity), so a task holding a
// tenth of the node's slots is charged a tenth of its cpu and memory.
//
// The proportional part is bounded by construction. A coordinator that sends no
// estimate is by definition one that places on available_slots, so the slots of
// all such tasks together cannot exceed totalSlots, and their shares of the node
// therefore sum to at most the node.
//
// Rounding is the exception, and it rounds up: a task holding a sliver of the
// node is charged one unit rather than nothing. Many small tasks can therefore
// book slightly more than the node in total, by at most one unit each. That is
// the safe direction -- the node reports itself fuller than it is and refuses
// work, instead of accepting work it cannot hold -- and Resource.Sub already
// clamps the reported remainder at zero.
func ResourceFromSlot(slot, totalSlots int64, capacity taskcommon.Resource) taskcommon.Resource {
	if slot <= 0 || totalSlots <= 0 {
		// Nothing to convert from. Answering zero leaves the ledger exactly as
		// it was before this conversion existed.
		return taskcommon.Resource{}
	}
	if slot > totalSlots {
		// A single task may be given more slots than the node has; the slot
		// picker places it anyway and drains the node. It holds the whole node.
		slot = totalSlots
	}
	return taskcommon.Resource{
		CPU:    slotShare(capacity.CPU, slot, totalSlots),
		Memory: slotShare(capacity.Memory, slot, totalSlots),
	}
}

// slotShare is ceil(total * slot / totalSlots), split so that a byte count
// times a slot count cannot overflow. It rounds up so that a task holding a
// small fraction of the node is never charged nothing at all.
func slotShare(total, slot, totalSlots int64) int64 {
	if total <= 0 {
		return 0
	}
	whole, remainder := total/totalSlots, total%totalSlots
	return whole*slot + (remainder*slot+totalSlots-1)/totalSlots
}
