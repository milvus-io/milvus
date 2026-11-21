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

// ResourceFromSlot converts a legacy CreateTask's positive slot demand at RPC
// ingress. Capacity and totalSlots are positive node capacities. Oversized
// demand books the whole node; rounding up preserves small task reservations.
func ResourceFromSlot(slot, totalSlots int64, capacity taskcommon.Resource) taskcommon.Resource {
	slot = min(slot, totalSlots)
	return taskcommon.Resource{
		CPU:    (capacity.CPU*slot + totalSlots - 1) / totalSlots,
		Memory: (capacity.Memory*slot + totalSlots - 1) / totalSlots,
	}
}
