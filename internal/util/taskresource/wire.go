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

package taskresource

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Canonical dimension names. The wire carries int64 only, so each name fixes
// its own unit; a renamed or re-united dimension must get a NEW name rather
// than a changed meaning, because a peer of another version matches on the
// name alone and has no way to detect a redefinition.
const (
	// DimMemoryBytes is expected peak resident bytes. It GATES placement: a
	// task that does not fit must not be put on the node, because exceeding
	// memory kills the process rather than slowing it down.
	DimMemoryBytes = "memory_bytes"
	// DimCPUMilli is milli-cores. It does NOT gate: two tasks on one core both
	// finish, just later, so it ranks candidates and never excludes one. See
	// the gating field on NodeResourceDim.
	DimCPUMilli = "cpu_milli"
)

// milliPerCore converts the Go-side float64 core count to the integral
// milli-cores the wire carries.
const milliPerCore = 1000

// ToProto renders a requirement as the wire vector.
//
// Both dimensions are always emitted, zero included. An absent dimension and a
// zero dimension mean different things to the receiver -- absent is "this peer
// does not model it", zero is "this task needs none of it" -- and a task that
// genuinely costs no CPU must not be read as one whose CPU is unknown.
func (r Requirement) ToProto() *datapb.TaskResources {
	return &datapb.TaskResources{
		Dims: []*datapb.TaskResourceAmount{
			{Name: DimMemoryBytes, Amount: r.Memory},
			{Name: DimCPUMilli, Amount: cpuToMilli(r.CPU)},
		},
	}
}

// RequirementFromProto reads the wire vector back.
//
// ok is false when the message carries no dimension this build understands --
// a nil field from a coordinator that predates it, or a vector of nothing but
// names added after it. The caller must then fall back rather than treat the
// zero Requirement as "this task is free"; that mistake is how a multi-GiB
// task comes to be admitted at no charge.
//
// Unknown names are skipped rather than rejected. That is what lets a
// coordinator send a dimension this worker has never heard of without the
// whole requirement becoming unreadable.
func RequirementFromProto(p *datapb.TaskResources) (Requirement, bool) {
	var (
		out Requirement
		ok  bool
	)
	for _, dim := range p.GetDims() {
		switch dim.GetName() {
		case DimMemoryBytes:
			out.Memory = dim.GetAmount()
			ok = true
		case DimCPUMilli:
			out.CPU = milliToCPU(dim.GetAmount())
			ok = true
		}
	}
	return out, ok
}

// NodeResourcesOf renders a worker's capacity report.
//
// committed is what the worker has ACCEPTED and not yet finished, never what
// it currently measures -- see the field's comment in data_coord.proto for
// why observation is the wrong input here.
//
// admitting is the safety valve: false means the worker has stopped taking
// work for a reason no dimension expresses, and a coordinator must treat it as
// having no room at all regardless of the arithmetic below.
func NodeResourcesOf(capacity, committed Capacity, admitting bool) *datapb.NodeResources {
	return &datapb.NodeResources{
		Admitting: admitting,
		Dims: []*datapb.NodeResourceDim{
			{
				Name:      DimMemoryBytes,
				Capacity:  capacity.Memory,
				Committed: committed.Memory,
				Gating:    true,
			},
			{
				Name:      DimCPUMilli,
				Capacity:  cpuToMilli(capacity.CPU),
				Committed: cpuToMilli(committed.CPU),
				Gating:    false,
			},
		},
	}
}

// NodeCapacityFromProto reads a worker's report back into the two-dimensional
// form the coordinator scores on. ok is false when nothing understood was
// reported, which is the signal to fall back to the scalar slot count.
func NodeCapacityFromProto(p *datapb.NodeResources) (capacity, committed Capacity, ok bool) {
	for _, dim := range p.GetDims() {
		switch dim.GetName() {
		case DimMemoryBytes:
			capacity.Memory, committed.Memory = dim.GetCapacity(), dim.GetCommitted()
			ok = true
		case DimCPUMilli:
			capacity.CPU, committed.CPU = milliToCPU(dim.GetCapacity()), milliToCPU(dim.GetCommitted())
			ok = true
		}
	}
	return capacity, committed, ok
}

// Free is what is left of a dimension after everything committed against it.
//
// It is deliberately NOT clamped at zero. A negative value means the node is
// over-committed -- its capacity shrank under work it had already accepted, or
// a task was placed on it that never fit -- and a scheduler that saw that as
// "exactly full" would keep the two cases apart nowhere.
func Free(capacity, committed Capacity) Capacity {
	return Capacity{
		CPU:    capacity.CPU - committed.CPU,
		Memory: capacity.Memory - committed.Memory,
	}
}

// cpuToMilli rounds rather than truncates: a 0.1-core import charge truncates
// to 100 either way, but a factor that lands on 1499.9999 through float
// arithmetic should read as 1.5 cores, not 1.4999.
func cpuToMilli(cores float64) int64 {
	if cores <= 0 {
		return 0
	}
	milli := cores * milliPerCore
	if milli > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(math.Round(milli))
}

func milliToCPU(milli int64) float64 {
	return float64(milli) / milliPerCore
}
