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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestRequirementRoundTrips(t *testing.T) {
	in := Requirement{CPU: 2.5, Memory: 3 << 30}

	got, ok := RequirementFromProto(in.ToProto())
	require.True(t, ok)
	assert.Equal(t, in, got)
}

// A task that costs no CPU must still be distinguishable from a task whose CPU
// was never reported, so both dimensions are always on the wire.
func TestZeroDimensionIsEmittedNotOmitted(t *testing.T) {
	p := Requirement{Memory: 1 << 20}.ToProto()

	names := make(map[string]int64, len(p.GetDims()))
	for _, d := range p.GetDims() {
		names[d.GetName()] = d.GetAmount()
	}
	require.Contains(t, names, DimCPUMilli, "a zero CPU charge must still be stated")
	assert.Equal(t, int64(0), names[DimCPUMilli])
}

// The whole point of the fallback signal: a coordinator that predates this
// field sends nothing, and the receiver must NOT read that as a free task.
func TestAbsentVectorIsNotAFreeTask(t *testing.T) {
	_, ok := RequirementFromProto(nil)
	assert.False(t, ok, "a nil vector must not be reported as understood")

	_, ok = RequirementFromProto(&datapb.TaskResources{})
	assert.False(t, ok, "an empty vector must not be reported as understood")

	_, ok = RequirementFromProto(&datapb.TaskResources{
		Dims: []*datapb.TaskResourceAmount{{Name: "disk_bytes_added_in_some_later_release", Amount: 7}},
	})
	assert.False(t, ok, "a vector of only unknown names must not be reported as understood")
}

// An unknown dimension alongside a known one must not make the known one
// unreadable -- that is what lets a newer coordinator talk to this worker.
func TestUnknownDimensionsAreSkippedNotFatal(t *testing.T) {
	got, ok := RequirementFromProto(&datapb.TaskResources{
		Dims: []*datapb.TaskResourceAmount{
			{Name: "disk_bytes_added_in_some_later_release", Amount: 7},
			{Name: DimMemoryBytes, Amount: 512 << 20},
		},
	})
	require.True(t, ok)
	assert.Equal(t, int64(512<<20), got.Memory)
}

func TestCPUMilliRoundsRatherThanTruncates(t *testing.T) {
	// 1.5 cores through float arithmetic can land just under 1500.
	p := Requirement{CPU: 0.5 + 1.0}.ToProto()
	for _, d := range p.GetDims() {
		if d.GetName() == DimCPUMilli {
			assert.Equal(t, int64(1500), d.GetAmount())
		}
	}
	// The import family charges fractional cores; they must survive the trip.
	got, ok := RequirementFromProto(Requirement{CPU: 0.1}.ToProto())
	require.True(t, ok)
	assert.InDelta(t, 0.1, got.CPU, 1e-9)
}

func TestNodeResourcesMarksOnlyMemoryAsGating(t *testing.T) {
	p := NodeResourcesOf(
		Capacity{CPU: 16, Memory: 48 << 30},
		Capacity{CPU: 4, Memory: 12 << 30},
		true,
	)
	require.True(t, p.GetAdmitting())

	gating := make(map[string]bool, len(p.GetDims()))
	for _, d := range p.GetDims() {
		gating[d.GetName()] = d.GetGating()
	}
	assert.True(t, gating[DimMemoryBytes], "memory must gate placement")
	assert.False(t, gating[DimCPUMilli],
		"CPU must never gate: filtering on it serializes classes that share no thread pool")

	capacity, committed, ok := NodeCapacityFromProto(p)
	require.True(t, ok)
	assert.Equal(t, int64(48<<30), capacity.Memory)
	assert.Equal(t, int64(12<<30), committed.Memory)
	assert.InDelta(t, 16.0, capacity.CPU, 1e-9)
	assert.InDelta(t, 4.0, committed.CPU, 1e-9)
}

func TestNodeCapacityFromProtoReportsWhenNothingUnderstood(t *testing.T) {
	_, _, ok := NodeCapacityFromProto(nil)
	assert.False(t, ok, "a worker that reported no vector must be distinguishable from an empty one")
}

// Over-commitment is a state the scheduler has to be able to see, so Free is
// not clamped.
func TestFreeGoesNegativeWhenOverCommitted(t *testing.T) {
	free := Free(
		Capacity{CPU: 8, Memory: 10 << 30},
		Capacity{CPU: 12, Memory: 16 << 30},
	)
	assert.Less(t, free.Memory, int64(0))
	assert.Less(t, free.CPU, 0.0)
}
