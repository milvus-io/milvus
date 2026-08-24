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

package resource

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// sampleWith drives one watermark sample at a chosen measured/total ratio.
func sampleWith(t *testing.T, g *guard, used, total uint64) {
	t.Helper()
	mkUsed := mockey.Mock(hardware.GetUsedMemoryCount).Return(used).Build()
	defer mkUsed.UnPatch()
	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(total).Build()
	defer mkTotal.UnPatch()
	g.sampleOnce()
}

func TestValveEngagesAboveTheHighWatermarkAndReleasesBelowTheLow(t *testing.T) {
	paramtable.Init()
	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	const total = uint64(64) << 30
	high := paramtable.Get().DataNodeCfg.ResourceHighWatermark.GetAsFloat()
	low := paramtable.Get().DataNodeCfg.ResourceLowWatermark.GetAsFloat()

	sampleWith(t, g, uint64(float64(total)*(high+0.02)), total)
	assert.False(t, g.Snapshot().Admitting, "above the high watermark the node stops taking work")

	sampleWith(t, g, uint64(float64(total)*(low-0.02)), total)
	assert.True(t, g.Snapshot().Admitting, "below the low watermark it takes work again")
}

// The band between the marks keeps the previous state. Without the hysteresis
// a node hovering at the threshold flaps, and every flap is a scheduling
// decision reversed.
func TestValveHoldsItsStateInsideTheHysteresisBand(t *testing.T) {
	paramtable.Init()
	g := NewGuard()

	const total = uint64(64) << 30
	high := paramtable.Get().DataNodeCfg.ResourceHighWatermark.GetAsFloat()
	low := paramtable.Get().DataNodeCfg.ResourceLowWatermark.GetAsFloat()
	between := uint64(float64(total) * (low + (high-low)/2))

	sampleWith(t, g, uint64(float64(total)*(high+0.02)), total)
	require.False(t, g.Snapshot().Admitting)
	sampleWith(t, g, between, total)
	assert.False(t, g.Snapshot().Admitting, "engaged stays engaged inside the band")

	sampleWith(t, g, uint64(float64(total)*(low-0.02)), total)
	require.True(t, g.Snapshot().Admitting)
	sampleWith(t, g, between, total)
	assert.True(t, g.Snapshot().Admitting, "released stays released inside the band")
}

// A failed reading is not evidence. total==0 makes the ratio +Inf or NaN, and
// moving the valve on that would stop a healthy node -- or restart a dying one.
func TestFailedReadingLeavesTheValveAlone(t *testing.T) {
	paramtable.Init()
	g := NewGuard()

	sampleWith(t, g, uint64(60)<<30, uint64(64)<<30)
	require.False(t, g.Snapshot().Admitting)

	sampleWith(t, g, 0, 0)
	assert.False(t, g.Snapshot().Admitting, "a failed reading must not release the valve")
}

// Measured memory must NEVER shrink the reported capacity. An earlier design
// fed the gap between measured and committed back in as a reservation, which
// charged freed-but-not-yet-returned RSS as though it belonged to someone else
// -- so every completed large task crushed the budget for the next one.
func TestMeasuredMemoryDoesNotChangeReportedCapacity(t *testing.T) {
	paramtable.Init()
	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 16, Memory: 48 * gib})

	before := g.Snapshot().Capacity

	// A reading far above what the ledger accounts for: the shape left behind
	// when a large task has just finished and Go has not scavenged yet.
	sampleWith(t, g, uint64(40)<<30, uint64(64)<<30)

	assert.Equal(t, before, g.Snapshot().Capacity,
		"capacity is a property of the machine, not of a memory reading")
	assert.Zero(t, g.Snapshot().Committed.Memory,
		"an observation must never be recorded as a commitment")
}

func TestWatermarkLoopStopsWithItsContext(t *testing.T) {
	paramtable.Init()
	g := NewGuard()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	g.startWatermarkLoop(ctx, done)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the sampling loop outlived its context")
	}
}

// The singleton is shared by all three executors; a second GetGuard must not
// build a second ledger, or the three would each account against their own.
func TestGetGuardIsASingleton(t *testing.T) {
	paramtable.Init()
	defer stopGlobalGuardForTest()

	first := GetGuard()
	assert.Same(t, first, GetGuard())

	require.NoError(t, first.Accept(context.Background(), 1, taskcommon.Compaction,
		taskresource.Requirement{Memory: gib}))
	assert.Equal(t, gib, GetGuard().Snapshot().Committed.Memory)
	GetGuard().Release(1)
}
